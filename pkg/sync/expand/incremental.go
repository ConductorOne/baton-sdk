package expand

import (
	"context"
	"errors"
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
)

// ErrIncrementalFallback means a new edge closed a cycle; the caller should
// re-run a full expansion, which handles cycles correctly.
var ErrIncrementalFallback = errors.New("incremental expansion: change introduces a cycle, fall back to full expansion")

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
// (entitlements whose membership changed, e.g. a new group member) via their
// own node. The second kind is essential — a new member adds no edge, so
// seeding only from newEdges would silently drop it.
//
// The walk reads current membership from the store, so changed members
// (already merged in) propagate without being passed in. Returns
// ErrIncrementalFallback if a new edge closes a cycle. Additions only: the
// caller must fall back to full expansion for change sets with revocations.
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
		return 0, nil
	}

	existing, err := ie.principalKeySet(ctx, destEntitlementID, nil)
	if err != nil {
		return 0, err
	}

	// added dedups principals across source edges; keys only, lives for the destination.
	added := make(map[string]struct{})
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
			// Stream the source a page at a time (never materialize a whale).
			perGrantErr := ie.forEachGrant(ctx, sourceEntitlementID, edge.ResourceTypeIDs, func(sourceGrant *v2.Grant) error {
				isSourceDirect := isDirectGrant(sourceGrant)
				if edge.IsShallow && !isSourceDirect {
					return nil
				}
				pid := sourceGrant.GetPrincipal().GetId()
				key := pid.GetResourceType() + "\x00" + pid.GetResource()
				if _, ok := existing[key]; ok {
					return nil
				}
				if _, ok := added[key]; ok {
					return nil
				}
				grant, err := newExpandedGrant(destEnt, sourceGrant.GetPrincipal(), sourceEntitlementID, isSourceDirect)
				if err != nil {
					return fmt.Errorf("incremental expansion: build grant on %s: %w", destEntitlementID, err)
				}
				buf = append(buf, grant)
				added[key] = struct{}{}
				if len(buf) >= incrementalFlushChunk {
					return flush()
				}
				return nil
			})
			if perGrantErr != nil {
				return 0, perGrantErr
			}
		}
	}

	if err := flush(); err != nil {
		return 0, err
	}
	return written, nil
}

func (ie *IncrementalExpander) getEntitlement(ctx context.Context, entitlementID string) (*v2.Entitlement, error) {
	resp, err := ie.store.GetEntitlement(ctx, reader_v2.EntitlementsReaderServiceGetEntitlementRequest_builder{
		EntitlementId: entitlementID,
	}.Build())
	if err != nil {
		return nil, fmt.Errorf("incremental expansion: get entitlement %s: %w", entitlementID, err)
	}
	if resp == nil {
		return nil, nil
	}
	return resp.GetEntitlement(), nil
}

// forEachGrant streams an entitlement's grants (filtered by resourceTypeIDs)
// one page at a time, invoking fn per grant — never materializing the whole set.
func (ie *IncrementalExpander) forEachGrant(ctx context.Context, entitlementID string, resourceTypeIDs []string, fn func(*v2.Grant) error) error {
	pageToken := ""
	for {
		resp, err := ie.store.ListGrantsForEntitlement(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
			Entitlement:              v2.Entitlement_builder{Id: entitlementID}.Build(),
			PrincipalResourceTypeIds: resourceTypeIDs,
			PageToken:                pageToken,
		}.Build())
		if err != nil {
			return fmt.Errorf("incremental expansion: list grants for %s: %w", entitlementID, err)
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

// principalKeySet returns the principal keys already granted on an entitlement
// (the diff baseline), using the keys-only fast path when available and
// streaming either way — never materializing the grants.
func (ie *IncrementalExpander) principalKeySet(ctx context.Context, entitlementID string, resourceTypeIDs []string) (map[string]struct{}, error) {
	set := make(map[string]struct{})

	if lister, ok := ie.store.(entitlementGrantPrincipalKeyLister); ok {
		ent := v2.Entitlement_builder{Id: entitlementID}.Build()
		pageToken := ""
		for {
			keys, next, err := lister.ListGrantPrincipalKeysForEntitlement(ctx, ent, pageToken, 0)
			if err != nil {
				return nil, fmt.Errorf("incremental expansion: list principal keys for %s: %w", entitlementID, err)
			}
			for _, k := range keys {
				set[k] = struct{}{}
			}
			if next == "" {
				return set, nil
			}
			pageToken = next
		}
	}

	// Fallback: stream grants and extract keys, holding one page at a time.
	err := ie.forEachGrant(ctx, entitlementID, resourceTypeIDs, func(g *v2.Grant) error {
		pid := g.GetPrincipal().GetId()
		set[pid.GetResourceType()+"\x00"+pid.GetResource()] = struct{}{}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return set, nil
}

// isDirectGrant reports whether a grant is a direct membership (no expansion
// sources) rather than one produced by a prior expansion.
func isDirectGrant(g *v2.Grant) bool {
	return len(g.GetSources().GetSources()) == 0
}
