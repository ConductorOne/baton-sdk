package expand

import (
	"context"
	"fmt"
	"sort"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// This file holds the shared topological expansion driver and merge semantics
// used by the Pebble-only streaming and projection evaluators
// (RunTopologicalMergeStreaming / RunTopologicalMergeProjection).

// expansionDirtyFlushChunk bounds how many dirty grants a single destination
// reduction buffers before flushing them to the store. It caps the per-
// destination write buffer so a whale destination (millions of changed
// principals) does not materialize its entire output in memory at once, while
// staying large enough to amortize the Pebble batch commit. The merge emits in
// principal order, so flushing in chunks is order-preserving.
//
// It is a var, not a const, only so tests can lower it to exercise the
// multi-flush path on small fixtures; production never mutates it.
var expansionDirtyFlushChunk = 10000

// destinationSink persists a batch of dirty grants for the destination currently
// being reduced. Reduce implementations call it repeatedly (via dirtyFlusher) so
// the write buffer stays bounded regardless of how many grants a destination
// produces.
type destinationSink func(ctx context.Context, dirty []*v2.Grant) error

// dirtyFlusher buffers dirty grants and flushes them through a destinationSink
// once the buffer reaches its limit. The buffer is reused across flushes; the
// sink consumes each batch synchronously before the buffer is truncated.
type dirtyFlusher struct {
	sink  destinationSink
	limit int
	buf   []*v2.Grant
}

func newDirtyFlusher(sink destinationSink) *dirtyFlusher {
	return &dirtyFlusher{sink: sink, limit: expansionDirtyFlushChunk}
}

func (f *dirtyFlusher) add(ctx context.Context, grant *v2.Grant) error {
	f.buf = append(f.buf, grant)
	if len(f.buf) >= f.limit {
		return f.flush(ctx)
	}
	return nil
}

func (f *dirtyFlusher) flush(ctx context.Context) error {
	if len(f.buf) == 0 {
		return nil
	}
	if err := f.sink(ctx, f.buf); err != nil {
		return err
	}
	f.buf = f.buf[:0]
	return nil
}

// topologicalRun configures one destination-first evaluation pass. reduce is the
// only required field; the rest let the projection variant build its scratch
// index, append finalized rows, enforce a context budget, and surface
// metrics/progress without forking the shared scheduling loop.
type topologicalRun struct {
	// reduce evaluates one destination entitlement and streams its dirty grants
	// to sink in bounded chunks (rather than returning the whole set), given the
	// destination's finalized incoming edges and the resolved entitlement set.
	reduce func(ctx context.Context, dest *v2.Entitlement, incoming []topoIncomingEdge, entitlements map[string]*v2.Entitlement, sink destinationSink) error
	// onStored, when set, runs after each batch of dirty grants is persisted
	// (projection appends matching projection rows so deeper nodes can read them).
	onStored func(ctx context.Context, dirty []*v2.Grant) error
	// checkBudget, when set, is polled before each node and each destination so a
	// cancelled context aborts promptly.
	checkBudget func() error
	// metrics, when set, accumulates per-destination/per-node counters.
	metrics *EntitlementGraphMetrics
	// progress, when set, is called once per node for periodic logging.
	progress func(nodeIdx, nodeTotal int)
}

// prepareTopological builds the expansion plan, computes the node topological
// order, and resolves every graph entitlement once. All three evaluators share
// it so ordering and the (possibly missing) entitlement set are derived
// identically.
func (e *Expander) prepareTopological(ctx context.Context) (map[string]*v2.Entitlement, []int, error) {
	if _, err := e.graph.ensureExpansionPlan(ctx); err != nil {
		return nil, nil, err
	}
	order, err := topologicalNodeOrder(e.graph)
	if err != nil {
		return nil, nil, err
	}
	entitlements, err := e.loadExpansionEntitlements(ctx)
	if err != nil {
		return nil, nil, err
	}
	return entitlements, order, nil
}

func (e *Expander) loadExpansionEntitlements(ctx context.Context) (map[string]*v2.Entitlement, error) {
	entitlements := make(map[string]*v2.Entitlement, len(e.graph.EntitlementsToNodes))
	for _, entID := range e.graph.GetEntitlements() {
		ent, ok, err := e.fetchEntitlement(ctx, entID)
		if err != nil {
			return nil, err
		}
		if ok {
			entitlements[entID] = ent
		}
	}
	return entitlements, nil
}

// driveTopological walks the collapsed DAG in topological order and reduces each
// destination entitlement of every node whose parents are finalized. It is the
// single scheduling loop behind RunTopologicalMergeStreaming and
// RunTopologicalMergeProjection; only the per-destination reduce strategy and
// the optional projection hooks differ between them.
func (e *Expander) driveTopological(
	ctx context.Context,
	entitlements map[string]*v2.Entitlement,
	order []int,
	run topologicalRun,
) error {
	logFanInWidth(ctx, e.graph, order)

	sink := func(ctx context.Context, dirty []*v2.Grant) error {
		if len(dirty) == 0 {
			return nil
		}
		if err := e.store.StoreExpandedGrants(ctx, dirty...); err != nil {
			return fmt.Errorf("topological merge: store expanded grants: %w", err)
		}
		if run.onStored != nil {
			if err := run.onStored(ctx, dirty); err != nil {
				return err
			}
		}
		if run.metrics != nil {
			run.metrics.DirtyGrantsWritten += int64(len(dirty))
		}
		return nil
	}
	for nodeIdx, nodeID := range order {
		if run.checkBudget != nil {
			if err := run.checkBudget(); err != nil {
				return err
			}
		}
		if run.progress != nil {
			run.progress(nodeIdx, len(order))
		}
		node, ok := e.graph.Nodes[nodeID]
		if !ok {
			continue
		}
		incoming := incomingEdgesSorted(e.graph, nodeID)
		if len(incoming) == 0 {
			continue
		}
		reducedAny := false
		for _, destID := range sortedCopy(node.EntitlementIDs) {
			if run.checkBudget != nil {
				if err := run.checkBudget(); err != nil {
					return err
				}
			}
			destEntitlement := entitlements[destID]
			if destEntitlement == nil {
				continue
			}
			if err := run.reduce(ctx, destEntitlement, incoming, entitlements, sink); err != nil {
				return err
			}
			reducedAny = true
			if run.metrics != nil {
				run.metrics.DestinationEntitlements++
			}
		}
		if reducedAny && run.metrics != nil {
			run.metrics.NodesReduced++
		}
	}
	return nil
}

// markExpansionComplete marks every edge expanded and clears the legacy action
// queue. The topological evaluators expand the whole graph in one pass, so they
// finalize all edges together at the end rather than per action.
func (e *Expander) markExpansionComplete() {
	for edgeID, edge := range e.graph.Edges {
		edge.IsExpanded = true
		e.graph.Edges[edgeID] = edge
	}
	e.graph.Actions = nil
}

func sortedCopy(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}

// logFanInWidth records the fan-in and fan-out distributions for one expansion.
// Both are telemetry only (counts/percentiles, never ids); cost is O(nodes).
//
// fan-in width = 1 (base stream) + incoming source entitlements: how many
// streams mergeDestinationStreams opens concurrently for a destination, i.e. the
// quantity that drives fan-in memory (live iterators/pages). It answers whether
// a bounded-width cascade is needed (docs/tasks/grant-expansion-streaming-merge.md §7).
//
// fan-out (out-degree) = destination nodes a source feeds. The destination-driven
// evaluator re-reads each unprojected source once per destination it feeds, so a
// high-fan-out "broadcast" source is a read-amplification hotspot. This answers
// whether projection-source selection should cover fan-out, not just fan-in.
func logFanInWidth(ctx context.Context, g *EntitlementGraph, order []int) {
	widths := make([]int, 0, len(order))
	outDegrees := make([]int, 0, len(order))
	maxParentEntitlements := 0
	for _, nodeID := range order {
		incoming := incomingEdgesSorted(g, nodeID)
		if len(incoming) > 0 {
			width := 1 // base(D) stream
			for _, in := range incoming {
				parent, ok := g.Nodes[in.sourceNodeID]
				if !ok {
					continue
				}
				width += len(parent.EntitlementIDs)
				if len(parent.EntitlementIDs) > maxParentEntitlements {
					maxParentEntitlements = len(parent.EntitlementIDs)
				}
			}
			widths = append(widths, width)
		}
		if outDegree := len(g.SourcesToDestinations[nodeID]); outDegree > 0 {
			outDegrees = append(outDegrees, outDegree)
		}
	}

	l := ctxzap.Extract(ctx)
	if len(widths) == 0 {
		l.Info("topological expansion: fan-in width distribution", zap.Int("nodes_with_incoming", 0))
	} else {
		sort.Ints(widths)
		l.Info("topological expansion: fan-in width distribution",
			zap.Int("nodes_with_incoming", len(widths)),
			zap.Int("max_merge_input_width", widths[len(widths)-1]),
			zap.Int("max_parent_entitlements", maxParentEntitlements),
			zap.Int("p50", percentile(widths, 50)),
			zap.Int("p90", percentile(widths, 90)),
			zap.Int("p99", percentile(widths, 99)),
			zap.Int("nodes_ge_128", atLeastCount(widths, 128)),
			zap.Int("nodes_ge_256", atLeastCount(widths, 256)),
			zap.Int("nodes_ge_512", atLeastCount(widths, 512)),
			zap.Int("nodes_ge_1024", atLeastCount(widths, 1024)),
		)
	}

	if len(outDegrees) == 0 {
		l.Info("topological expansion: fan-out distribution", zap.Int("nodes_with_outgoing", 0))
		return
	}
	sort.Ints(outDegrees)
	l.Info("topological expansion: fan-out distribution",
		zap.Int("nodes_with_outgoing", len(outDegrees)),
		zap.Int("max_out_degree", outDegrees[len(outDegrees)-1]),
		zap.Int("p50", percentile(outDegrees, 50)),
		zap.Int("p90", percentile(outDegrees, 90)),
		zap.Int("p99", percentile(outDegrees, 99)),
		zap.Int("nodes_ge_128", atLeastCount(outDegrees, 128)),
		zap.Int("nodes_ge_256", atLeastCount(outDegrees, 256)),
		zap.Int("nodes_ge_512", atLeastCount(outDegrees, 512)),
		zap.Int("nodes_ge_1024", atLeastCount(outDegrees, 1024)),
	)
}

// percentile returns the p-th percentile (p in [0,100]) of an ascending-sorted
// slice using nearest-rank.
func percentile(sorted []int, p int) int {
	if len(sorted) == 0 {
		return 0
	}
	idx := (p*len(sorted)+99)/100 - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// atLeastCount counts entries >= threshold in an ascending-sorted slice.
func atLeastCount(sorted []int, threshold int) int {
	return len(sorted) - sort.SearchInts(sorted, threshold)
}

func (e *Expander) fetchEntitlement(ctx context.Context, entitlementID string) (*v2.Entitlement, bool, error) {
	resp, err := e.store.GetEntitlement(ctx, reader_v2.EntitlementsReaderServiceGetEntitlementRequest_builder{
		EntitlementId: entitlementID,
	}.Build())
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("topological merge: get entitlement %q: %w", entitlementID, err)
	}
	if resp == nil || resp.GetEntitlement() == nil {
		return nil, false, nil
	}
	return resp.GetEntitlement(), true, nil
}

type topoIncomingEdge struct {
	sourceNodeID int
	edge         Edge
}

func incomingEdgesSorted(g *EntitlementGraph, nodeID int) []topoIncomingEdge {
	sources := g.DestinationsToSources[nodeID]
	out := make([]topoIncomingEdge, 0, len(sources))
	for sourceNodeID, edgeID := range sources {
		edge, ok := g.Edges[edgeID]
		if !ok {
			continue
		}
		out = append(out, topoIncomingEdge{
			sourceNodeID: sourceNodeID,
			edge:         edge,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].sourceNodeID != out[j].sourceNodeID {
			return out[i].sourceNodeID < out[j].sourceNodeID
		}
		return out[i].edge.EdgeID < out[j].edge.EdgeID
	})
	return out
}

func topologicalNodeOrder(g *EntitlementGraph) ([]int, error) {
	ids := make([]int, 0, len(g.Nodes))
	inDegree := make(map[int]int, len(g.Nodes))
	for id := range g.Nodes {
		ids = append(ids, id)
		inDegree[id] = 0
	}
	for _, edge := range g.Edges {
		if _, ok := g.Nodes[edge.SourceID]; !ok {
			continue
		}
		if _, ok := g.Nodes[edge.DestinationID]; !ok {
			continue
		}
		inDegree[edge.DestinationID]++
	}

	sort.Ints(ids)
	queue := make([]int, 0, len(ids))
	for _, id := range ids {
		if inDegree[id] == 0 {
			queue = append(queue, id)
		}
	}

	order := make([]int, 0, len(ids))
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		order = append(order, id)

		children := make([]int, 0, len(g.SourcesToDestinations[id]))
		for child := range g.SourcesToDestinations[id] {
			children = append(children, child)
		}
		sort.Ints(children)
		for _, child := range children {
			inDegree[child]--
			if inDegree[child] == 0 {
				queue = append(queue, child)
			}
		}
	}
	if len(order) != len(ids) {
		return nil, fmt.Errorf("topological merge: graph contains a cycle or dangling edge")
	}
	return order, nil
}

type topoPrincipalKey struct {
	resourceType string
	resource     string
}

func principalKeyFromResource(r *v2.Resource) (topoPrincipalKey, bool) {
	if r == nil || r.GetId() == nil {
		return topoPrincipalKey{}, false
	}
	return topoPrincipalKey{
		resourceType: r.GetId().GetResourceType(),
		resource:     r.GetId().GetResource(),
	}, true
}

func principalKeyLess(a, b topoPrincipalKey) bool {
	if a.resourceType != b.resourceType {
		return a.resourceType < b.resourceType
	}
	return a.resource < b.resource
}

type topoContribution struct {
	sources        map[string]bool
	principal      *v2.Resource
	principalBytes []byte
}

func (c *topoContribution) add(sourceEntitlementID string, isDirect bool, principal *v2.Resource) {
	c.addSource(sourceEntitlementID, isDirect)
	if c.principal == nil && principal != nil {
		c.principal = proto.Clone(principal).(*v2.Resource)
	}
}

// addSource records a source entitlement's contribution and its directness,
// upgrading an existing indirect entry to direct. It never touches the
// principal.
func (c *topoContribution) addSource(sourceEntitlementID string, isDirect bool) {
	if c.sources == nil {
		c.sources = make(map[string]bool)
	}
	if existing, ok := c.sources[sourceEntitlementID]; !ok || (isDirect && !existing) {
		c.sources[sourceEntitlementID] = isDirect
	}
}

func (c *topoContribution) merge(other *topoContribution) {
	if other == nil {
		return
	}
	for sourceID, isDirect := range other.sources {
		c.addSource(sourceID, isDirect)
	}
	// Take ownership of the principal exactly once, from the first contributor
	// that carries one. other.principalBytes may alias a stream-owned reusable
	// buffer (see projectionContributionStream), which is overwritten on the
	// next stream advance. merge runs during consume, before that advance, so
	// copy the bytes into a slice this contribution owns rather than aliasing.
	if c.principal == nil && c.principalBytes == nil {
		if other.principal != nil {
			c.principal = other.principal
		} else if len(other.principalBytes) > 0 {
			c.principalBytes = append([]byte(nil), other.principalBytes...)
		}
	}
}

func (c *topoContribution) principalResource() (*v2.Resource, error) {
	if c == nil {
		return nil, nil
	}
	if c.principal != nil {
		return c.principal, nil
	}
	if len(c.principalBytes) == 0 {
		return nil, nil
	}
	p := &v2.Resource{}
	if err := proto.Unmarshal(c.principalBytes, p); err != nil {
		return nil, err
	}
	c.principal = p
	c.principalBytes = nil
	return p, nil
}

func grantContributesOverEdge(grant *v2.Grant, sourceEntitlementID string, edge Edge) bool {
	if grant == nil || grant.GetPrincipal() == nil || grant.GetPrincipal().GetId() == nil {
		return false
	}
	if len(edge.ResourceTypeIDs) > 0 {
		rt := grant.GetPrincipal().GetId().GetResourceType()
		found := false
		for _, allowed := range edge.ResourceTypeIDs {
			if rt == allowed {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	if edge.IsShallow && !isGrantDirectOnEntitlement(grant, sourceEntitlementID) {
		return false
	}
	return true
}

func isGrantDirectOnEntitlement(grant *v2.Grant, entitlementID string) bool {
	sources := grant.GetSources().GetSources()
	return len(sources) == 0 || sources[entitlementID] != nil
}

func mergeContributionIntoExistingGrant(baseGrant *v2.Grant, destEntitlementID string, contrib map[string]bool) *v2.Grant {
	if baseGrant == nil || len(contrib) == 0 {
		return nil
	}
	grant := proto.Clone(baseGrant).(*v2.Grant)
	sourcesMap := grant.GetSources().GetSources()
	if sourcesMap == nil {
		sourcesMap = make(map[string]*v2.GrantSources_GrantSource, len(contrib)+1)
	}

	updated := false
	if len(sourcesMap) == 0 {
		sourcesMap[destEntitlementID] = &v2.GrantSources_GrantSource{IsDirect: true}
		updated = true
	}
	for sourceID, isDirect := range contrib {
		existingSource := sourcesMap[sourceID]
		if existingSource == nil {
			sourcesMap[sourceID] = &v2.GrantSources_GrantSource{IsDirect: isDirect}
			updated = true
			continue
		}
		if isDirect && !existingSource.GetIsDirect() {
			existingSource.SetIsDirect(true)
			updated = true
		}
	}
	if !updated {
		return nil
	}
	grant.SetSources(v2.GrantSources_builder{Sources: sourcesMap}.Build())
	return grant
}

func newExpandedGrantWithSources(descEntitlement *v2.Entitlement, principal *v2.Resource, sources map[string]bool) (*v2.Grant, error) {
	if len(sources) == 0 {
		return nil, fmt.Errorf("newExpandedGrantWithSources: empty sources")
	}
	sourceMap := make(map[string]*v2.GrantSources_GrantSource, len(sources))
	sourceIDs := make([]string, 0, len(sources))
	for sourceID := range sources {
		sourceIDs = append(sourceIDs, sourceID)
	}
	sort.Strings(sourceIDs)
	for _, sourceID := range sourceIDs {
		isDirect := sources[sourceID]
		sourceMap[sourceID] = &v2.GrantSources_GrantSource{IsDirect: isDirect}
	}
	enResource := descEntitlement.GetResource()
	if enResource == nil {
		return nil, fmt.Errorf("newExpandedGrantWithSources: entitlement has no resource")
	}
	if principal == nil {
		return nil, fmt.Errorf("newExpandedGrantWithSources: principal is nil")
	}
	pid := principal.GetId()
	grantID := descEntitlement.GetId() + ":" + pid.GetResourceType() + ":" + pid.GetResource()
	return v2.Grant_builder{
		Id:          grantID,
		Entitlement: descEntitlement,
		Principal:   principal,
		Sources:     v2.GrantSources_builder{Sources: sourceMap}.Build(),
		Annotations: annotations.Annotations{immutableAnnotationAny},
	}.Build(), nil
}
