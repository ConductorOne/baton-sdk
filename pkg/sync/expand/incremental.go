package expand

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	batonGrant "github.com/conductorone/baton-sdk/pkg/types/grant"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ClearTransientState drops the graph's expansion working state — the action
// queue, the projection plan, and metrics — which a persisted graph doesn't
// need for a later reload and which can bloat the sync token. The structural
// graph (nodes, edges, mappings) is untouched.
func (g *EntitlementGraph) ClearTransientState() {
	g.Actions = nil
	g.ExpansionPlan = nil
	g.ExpansionMetrics = nil
}

// Clone returns a deep copy of the graph. Incremental expansion mutates the
// graph (adds edges), so callers that keep the base graph across retries must
// pass a clone — otherwise a failed run leaves the never-expanded edges in the
// caller's graph, and a retry would treat them as already present and finish
// with an unexpanded artifact.
func (g *EntitlementGraph) Clone() (*EntitlementGraph, error) {
	data, err := json.Marshal(g)
	if err != nil {
		return nil, fmt.Errorf("clone entitlement graph: %w", err)
	}
	out := &EntitlementGraph{}
	if err := json.Unmarshal(data, out); err != nil {
		return nil, fmt.Errorf("clone entitlement graph: %w", err)
	}
	// json leaves absent maps nil; reinit so the clone is immediately usable.
	if out.Nodes == nil {
		out.Nodes = map[int]Node{}
	}
	if out.EntitlementsToNodes == nil {
		out.EntitlementsToNodes = map[string]int{}
	}
	if out.SourcesToDestinations == nil {
		out.SourcesToDestinations = map[int]map[int]int{}
	}
	if out.DestinationsToSources == nil {
		out.DestinationsToSources = map[int]map[int]int{}
	}
	if out.Edges == nil {
		out.Edges = map[int]Edge{}
	}
	return out, nil
}

// ErrIncrementalFallback means a new edge closed a cycle; the caller should
// re-run a full expansion, which handles cycles correctly.
var ErrIncrementalFallback = errors.New("incremental expansion: change introduces a cycle, fall back to full expansion")

// ErrIncrementalRevocationDecline means the change is revocation-shaped (an
// existing edge's spec narrowed — shallow-ified, filter tightened, or a source
// dropped), which incremental expansion cannot apply without removing grants.
// The caller declines to full expansion. This is the named hook a future
// tombstone/deletion stage flips from "decline" to "apply deletions".
var ErrIncrementalRevocationDecline = errors.New("incremental expansion: revocation-shaped change, fall back to full expansion")

// NewEdge is one edge to fold in: members of Source also get Destination.
type NewEdge struct {
	SourceEntitlementID string
	DestEntitlementID   string
	Shallow             bool
	ResourceTypeIDs     []string
}

// IncrementalResult reports the impacted subgraph and how many grants were written.
type IncrementalResult struct {
	EntitlementsWalked []string
	GrantsWritten      int
}

// IncrementalExpander folds new edges into an already-expanded graph and
// propagates the change to only the affected subgraph, reading and writing
// through the same ExpanderStore as the full expander.
//
// Preconditions: graph is a prior completed expansion's graph (edges already
// expanded), and store holds that expansion's grants. Additions only; a new
// edge that closes a cycle returns ErrIncrementalFallback.
type IncrementalExpander struct {
	store ExpanderStore
	graph *EntitlementGraph
}

func NewIncrementalExpander(store ExpanderStore, graph *EntitlementGraph) *IncrementalExpander {
	return &IncrementalExpander{store: store, graph: graph}
}

// ExpandChanges recomputes grants for only the subgraph affected by a set of
// changes. Both kinds seed the walk: newEdges (new expandable relationships,
// added to the graph here) via their destinations, and changedEntitlementIDs
// (entitlements whose membership changed) via their own node. The second kind
// is essential — a membership change adds no edge, so seeding only from
// newEdges would silently drop it.
//
// changedEntitlementIDs is direction-neutral: it names entitlements whose
// membership changed in EITHER direction (added or removed). Whether a removal
// is actually applied is a WRITE-behavior concern, not a seed concern — today
// this method only adds grants (never removes), so callers decline
// revocation-shaped changes to full expansion. When a future stage learns to
// apply deletions, removed-membership entitlements flow through this same
// parameter with no signature change.
//
// The walk reads current membership from the store, so changed members
// (already merged in) propagate without being passed in. Returns
// ErrIncrementalFallback if a new edge closes a cycle.
func (ie *IncrementalExpander) ExpandChanges(ctx context.Context, newEdges []NewEdge, changedEntitlementIDs []string) (*IncrementalResult, error) {
	if len(newEdges) == 0 && len(changedEntitlementIDs) == 0 {
		return &IncrementalResult{}, nil
	}

	seeds := make(map[int]struct{})
	for _, e := range newEdges {
		ie.graph.AddEntitlementID(e.SourceEntitlementID)
		ie.graph.AddEntitlementID(e.DestEntitlementID)
		if err := ie.graph.AddEdge(ctx, e.SourceEntitlementID, e.DestEntitlementID, e.Shallow, e.ResourceTypeIDs); err != nil {
			return nil, fmt.Errorf("incremental expansion: add edge %s->%s: %w", e.SourceEntitlementID, e.DestEntitlementID, err)
		}
		if dst := ie.graph.GetNode(e.DestEntitlementID); dst != nil {
			seeds[dst.Id] = struct{}{}
		}
	}

	// A changed entitlement seeds its own node so descendants are recomputed.
	// Entitlements not in the graph have nothing downstream — safely ignored.
	for _, entitlementID := range changedEntitlementIDs {
		if n := ie.graph.GetNode(entitlementID); n != nil {
			seeds[n.Id] = struct{}{}
		}
	}

	if len(seeds) == 0 {
		return &IncrementalResult{}, nil
	}

	if cyclic, _ := ie.graph.ComputeCyclicComponents(ctx); len(cyclic) > 0 {
		return nil, ErrIncrementalFallback
	}

	// Only nodes forward-reachable from a seed are touched.
	affected := ie.forwardReachable(seeds)

	// Topological order so each destination reads already-finalized parents.
	order, err := topologicalNodeOrder(ie.graph)
	if err != nil {
		return nil, fmt.Errorf("incremental expansion: topological order: %w", err)
	}

	result := &IncrementalResult{}
	for _, nodeID := range order {
		if err := ctx.Err(); err != nil {
			return nil, err // cancelled / run-duration exceeded
		}
		if _, ok := affected[nodeID]; !ok {
			continue
		}
		node, ok := ie.graph.Nodes[nodeID]
		if !ok {
			continue
		}
		for _, destEntitlementID := range node.EntitlementIDs {
			written, err := ie.recomputeDestination(ctx, nodeID, destEntitlementID)
			if err != nil {
				return nil, err
			}
			result.EntitlementsWalked = append(result.EntitlementsWalked, destEntitlementID)
			result.GrantsWritten += written
		}
	}
	return result, nil
}

func (ie *IncrementalExpander) forwardReachable(seeds map[int]struct{}) map[int]struct{} {
	reached := make(map[int]struct{})
	queue := make([]int, 0, len(seeds))
	for id := range seeds {
		reached[id] = struct{}{}
		queue = append(queue, id)
	}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for child := range ie.graph.SourcesToDestinations[cur] {
			if _, ok := reached[child]; !ok {
				reached[child] = struct{}{}
				queue = append(queue, child)
			}
		}
	}
	return reached
}

// incrementalFlushChunk caps buffered new grants before a flush, so a whale
// destination doesn't materialize its whole output. Mirrors the full
// expander's expansionDirtyFlushChunk. A var only so tests can lower it.
var incrementalFlushChunk = 10000

// recomputeDestination writes destEntitlementID's implied grants that aren't
// already present, returning how many. Source grants stream a page at a time
// and writes flush in chunks, so peak memory is one page + one flush buffer +
// the destination's existing-key set — not the whole source or output.
func (ie *IncrementalExpander) recomputeDestination(ctx context.Context, nodeID int, destEntitlementID string) (int, error) {
	destEnt, err := ie.getEntitlement(ctx, destEntitlementID)
	if err != nil {
		return 0, err
	}
	if destEnt == nil {
		// Dangling ref: skip-with-warn, matching the full evaluator (don't
		// error into a fallback).
		ctxzap.Extract(ctx).Warn("incremental expansion: destination entitlement not in store; skipping",
			zap.String("entitlement_id", destEntitlementID))
		return 0, nil
	}

	// 1. Accumulate, per principal, the union of sources contributed by all
	// incoming edges. Streaming reads keep only one source page live, but the
	// contribution map holds one entry per distinct principal across ALL
	// sources feeding this destination — the same worst-case fan-in footprint
	// the full expander's per-destination reduce carries.
	contrib := make(map[string]*principalContribution)
	for sourceNodeID, edgeID := range ie.graph.DestinationsToSources[nodeID] {
		edge, ok := ie.graph.Edges[edgeID]
		if !ok {
			continue
		}
		sourceNode, ok := ie.graph.Nodes[sourceNodeID]
		if !ok {
			continue
		}
		for _, sourceEntitlementID := range sourceNode.EntitlementIDs {
			// The store's read path requires a full entitlement record (with
			// resource refs), not a bare id — fetch it. A dangling ref (source
			// not in the store) is skipped-with-warn, matching the full evaluator.
			sourceEnt, err := ie.getEntitlement(ctx, sourceEntitlementID)
			if err != nil {
				return 0, err
			}
			if sourceEnt == nil {
				ctxzap.Extract(ctx).Warn("incremental expansion: source entitlement not in store; skipping",
					zap.String("entitlement_id", sourceEntitlementID))
				continue
			}
			perGrantErr := ie.forEachGrant(ctx, sourceEnt, edge.ResourceTypeIDs, func(sourceGrant *v2.Grant) error {
				// Directness is relative to the source entitlement (matches the
				// full expander): a plain direct grant or one whose sources map
				// records this entitlement counts as direct.
				isSourceDirect := isGrantDirectOnEntitlement(sourceGrant, sourceEntitlementID)
				if edge.IsShallow && !isSourceDirect {
					return nil
				}
				principal := sourceGrant.GetPrincipal()
				pid := principal.GetId()
				key := pid.GetResourceType() + "\x00" + pid.GetResource()
				pc := contrib[key]
				if pc == nil {
					pc = &principalContribution{principal: principal}
					contrib[key] = pc
				}
				pc.addSource(sourceEntitlementID, isSourceDirect)
				return nil
			})
			if perGrantErr != nil {
				return 0, perGrantErr
			}
		}
	}
	if len(contrib) == 0 {
		return 0, nil
	}

	buf := make([]*v2.Grant, 0, incrementalFlushChunk)
	written := 0
	flush := func() error {
		if len(buf) == 0 {
			return nil
		}
		if err := ie.store.StoreExpandedGrants(ctx, buf...); err != nil {
			return fmt.Errorf("incremental expansion: store grants on %s: %w", destEntitlementID, err)
		}
		written += len(buf)
		buf = buf[:0]
		return nil
	}

	// 2. Merge contributions into the destination's existing grants (union the
	// sources map, upgrade direct-ness), streaming one page at a time. Only a
	// grant that actually changed is rewritten; matched principals leave contrib.
	mergeErr := ie.forEachGrant(ctx, destEnt, nil, func(g *v2.Grant) error {
		pid := g.GetPrincipal().GetId()
		key := pid.GetResourceType() + "\x00" + pid.GetResource()
		pc := contrib[key]
		if pc == nil {
			return nil
		}
		delete(contrib, key)
		updated := mergeContributionIntoExistingGrant(g, destEntitlementID, pc.sources)
		if updated == nil {
			return nil // already had these sources — no write
		}
		buf = append(buf, updated)
		if len(buf) >= incrementalFlushChunk {
			return flush()
		}
		return nil
	})
	if mergeErr != nil {
		return 0, mergeErr
	}

	// 3. Whatever is left in contrib are brand-new principals. Sort for
	// deterministic (byte-stable) output.
	newKeys := make([]string, 0, len(contrib))
	for key := range contrib {
		newKeys = append(newKeys, key)
	}
	sort.Strings(newKeys)
	for _, key := range newKeys {
		pc := contrib[key]
		grant, err := newExpandedGrantWithSources(destEnt, pc.principal, pc.sources)
		if err != nil {
			return 0, fmt.Errorf("incremental expansion: build grant on %s: %w", destEntitlementID, err)
		}
		buf = append(buf, grant)
		if len(buf) >= incrementalFlushChunk {
			if err := flush(); err != nil {
				return 0, err
			}
		}
	}

	if err := flush(); err != nil {
		return 0, err
	}
	return written, nil
}

// principalContribution accumulates the source entitlements contributing one
// principal to a destination. sources is a small slice (fan-in is tiny), deduped
// by entitlement id with direct-ness upgraded to true if any contribution is direct.
type principalContribution struct {
	principal *v2.Resource
	sources   batonGrant.Sources
}

func (pc *principalContribution) addSource(entitlementID string, isDirect bool) {
	for i := range pc.sources {
		if pc.sources[i].EntitlementID == entitlementID {
			if isDirect && !pc.sources[i].IsDirect {
				pc.sources[i].IsDirect = true
			}
			return
		}
	}
	pc.sources = append(pc.sources, batonGrant.Source{EntitlementID: entitlementID, IsDirect: isDirect})
}

// getEntitlement fetches an entitlement, returning (nil, nil) for a dangling
// ref (NotFound) so callers skip it — matching the full evaluator, which treats
// NotFound as skip rather than a hard error.
func (ie *IncrementalExpander) getEntitlement(ctx context.Context, entitlementID string) (*v2.Entitlement, error) {
	resp, err := ie.store.GetEntitlement(ctx, reader_v2.EntitlementsReaderServiceGetEntitlementRequest_builder{
		EntitlementId: entitlementID,
	}.Build())
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("incremental expansion: get entitlement %s: %w", entitlementID, err)
	}
	if resp == nil {
		return nil, nil
	}
	return resp.GetEntitlement(), nil
}

// forEachGrant streams an entitlement's grants (filtered by resourceTypeIDs)
// one page at a time, invoking fn per grant — never materializing the whole
// set. entitlement must be a full record (with resource refs); the store's read
// path rejects bare-id entitlements.
func (ie *IncrementalExpander) forEachGrant(ctx context.Context, entitlement *v2.Entitlement, resourceTypeIDs []string, fn func(*v2.Grant) error) error {
	pageToken := ""
	for {
		resp, err := ie.store.ListGrantsForEntitlement(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
			Entitlement:              entitlement,
			PrincipalResourceTypeIds: resourceTypeIDs,
			PageToken:                pageToken,
		}.Build())
		if err != nil {
			return fmt.Errorf("incremental expansion: list grants for %s: %w", entitlement.GetId(), err)
		}
		for _, g := range resp.GetList() {
			if err := fn(g); err != nil {
				return err
			}
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			return nil
		}
	}
}
