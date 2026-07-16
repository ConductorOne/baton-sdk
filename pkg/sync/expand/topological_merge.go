package expand

import (
	"context"
	"fmt"
	"sort"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	batonGrant "github.com/conductorone/baton-sdk/pkg/types/grant"
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

// destinationSink persists dirty grants for the destination currently being
// reduced. It carries both the generic v2-grant path and an optional direct
// synthesized-contribution path for Pebble.
type destinationSink struct {
	store      func(ctx context.Context, dirty []*v2.Grant, allNew bool) error
	storeSynth func(ctx context.Context, dest *v2.Entitlement, principals []*v3.PrincipalRef, sources []batonGrant.Sources) error
}

// dirtyFlusher buffers synthesized and base-update grants separately so each
// flush is homogeneous for the optional all-new fast path.
type dirtyFlusher struct {
	sink             *destinationSink
	dest             *v2.Entitlement
	limit            int
	synth            []*v2.Grant
	synthPrincipals  []*v3.PrincipalRef
	synthSources     []batonGrant.Sources
	synthSourceArena []batonGrant.Source
	// refSlab hands out the PrincipalRef protos synthPrincipals point into.
	// Reset after each flush: the storeSynth implementations encode
	// everything during the call and retain nothing.
	refSlab principalRefSlab
	update  []*v2.Grant
}

// principalRefSlab hands out reusable *v3.PrincipalRef values from
// fixed-size chunks. Chunks are value slices allocated once and never
// resliced or copied (generated protos must not be moved), so growth only
// appends chunk headers. reset() recycles every handed-out ref; consumers
// re-set all fields on reuse (principalRefData.fillRef).
type principalRefSlab struct {
	chunks [][]v3.PrincipalRef
	chunk  int
	used   int
}

const principalRefSlabChunk = 4096

func (s *principalRefSlab) next() *v3.PrincipalRef {
	if s.chunk == len(s.chunks) {
		s.chunks = append(s.chunks, make([]v3.PrincipalRef, principalRefSlabChunk))
	}
	ref := &s.chunks[s.chunk][s.used]
	s.used++
	if s.used == principalRefSlabChunk {
		s.chunk++
		s.used = 0
	}
	return ref
}

func (s *principalRefSlab) reset() {
	s.chunk, s.used = 0, 0
}

func newDirtyFlusher(dest *v2.Entitlement, sink *destinationSink) *dirtyFlusher {
	return &dirtyFlusher{dest: dest, sink: sink, limit: expansionDirtyFlushChunk}
}

func (f *dirtyFlusher) add(ctx context.Context, grant *v2.Grant, isNew bool) error {
	if isNew {
		f.synth = append(f.synth, grant)
		if len(f.synth) >= f.limit {
			return f.flushSynth(ctx)
		}
		return nil
	}
	f.update = append(f.update, grant)
	if len(f.update) >= f.limit {
		return f.flushUpdate(ctx)
	}
	return nil
}

func (f *dirtyFlusher) addSynthesizedContribution(ctx context.Context, contrib *topoContribution, sources batonGrant.Sources) error {
	if f.sink.storeSynth == nil {
		principal, err := contrib.principalResource()
		if err != nil {
			return err
		}
		if principal == nil {
			return nil
		}
		grant, err := newExpandedGrantWithSources(f.dest, principal, sources)
		if err != nil {
			return err
		}
		return f.add(ctx, grant, true)
	}
	data, ok := contrib.principalRefForStore()
	if !ok {
		return nil
	}
	sourceStart := len(f.synthSourceArena)
	f.synthSourceArena = append(f.synthSourceArena, sources...)
	ref := f.refSlab.next()
	data.fillRef(ref)
	f.synthPrincipals = append(f.synthPrincipals, ref)
	f.synthSources = append(f.synthSources, f.synthSourceArena[sourceStart:])
	if len(f.synthPrincipals) >= f.limit {
		return f.flushSynth(ctx)
	}
	return nil
}

func (f *dirtyFlusher) flush(ctx context.Context) error {
	if err := f.flushSynth(ctx); err != nil {
		return err
	}
	return f.flushUpdate(ctx)
}

func (f *dirtyFlusher) flushSynth(ctx context.Context) error {
	if len(f.synthPrincipals) > 0 {
		if err := f.sink.storeSynth(ctx, f.dest, f.synthPrincipals, f.synthSources); err != nil {
			return err
		}
		f.synthPrincipals = f.synthPrincipals[:0]
		f.synthSources = f.synthSources[:0]
		f.synthSourceArena = f.synthSourceArena[:0]
		// storeSynth encoded everything during the call; the slab's refs are
		// free to reuse for the next batch.
		f.refSlab.reset()
	}
	if len(f.synth) == 0 {
		return nil
	}
	if err := f.sink.store(ctx, f.synth, true); err != nil {
		return err
	}
	f.synth = f.synth[:0]
	return nil
}

func (f *dirtyFlusher) flushUpdate(ctx context.Context) error {
	if len(f.update) == 0 {
		return nil
	}
	if err := f.sink.store(ctx, f.update, false); err != nil {
		return err
	}
	f.update = f.update[:0]
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
	reduce func(ctx context.Context, dest *v2.Entitlement, incoming []topoIncomingEdge, entitlements map[string]*v2.Entitlement, sink *destinationSink) error
	// onStored, when set, runs after each batch of dirty grants is persisted
	// (projection appends matching projection rows so deeper nodes can read them).
	onStored      func(ctx context.Context, dirty []*v2.Grant) error
	onStoredSynth func(ctx context.Context, dest *v2.Entitlement, principals []*v3.PrincipalRef, sources []batonGrant.Sources) error
	// checkBudget, when set, is polled before each node and each destination so a
	// cancelled context aborts promptly.
	checkBudget func() error
	// metrics, when set, accumulates per-destination/per-node counters.
	metrics *EntitlementGraphMetrics
	// progress, when set, is called once per node for periodic logging.
	progress func(nodeIdx, nodeTotal int)
}

// prepareTopological builds the expansion plan, computes the topological
// layer decomposition, and resolves every graph entitlement once. All
// evaluators share it so ordering and the (possibly missing) entitlement set
// are derived identically.
func (e *Expander) prepareTopological(ctx context.Context) (map[string]*v2.Entitlement, [][]int, error) {
	if _, err := e.graph.ensureExpansionPlan(ctx); err != nil {
		return nil, nil, err
	}
	layers, err := topologicalNodeLayers(e.graph)
	if err != nil {
		return nil, nil, err
	}
	entitlements, err := e.loadExpansionEntitlements(ctx)
	if err != nil {
		return nil, nil, err
	}
	return entitlements, layers, nil
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

// driveTopological walks the collapsed DAG layer by layer (Kahn levels) and
// reduces each destination entitlement of every node whose parents are
// finalized. It is the single scheduling loop behind
// RunTopologicalMergeStreaming and RunTopologicalMergeProjection; only the
// per-destination reduce strategy and the optional projection hooks differ
// between them.
//
// When the store supports layer sessions (Pebble), each layer's synthesized
// grants are streamed into one session and published as sorted bulk writes at
// segment/layer boundaries. That is safe because every parent of a layer-k
// node sits in a layer < k, so no reduce in the current layer reads rows the
// session is still holding.
func (e *Expander) driveTopological(
	ctx context.Context,
	entitlements map[string]*v2.Entitlement,
	layers [][]int,
	run topologicalRun,
) error {
	logFanInWidth(ctx, e.graph, layers)

	totalNodes := 0
	for _, layer := range layers {
		totalNodes += len(layer)
	}

	// activeLayer is non-nil only while a layer session is open; the
	// storeSynth closure below routes synthesized rows into it.
	var activeLayer synthesizedContributionLayerStorer
	sink := &destinationSink{
		store: func(ctx context.Context, dirty []*v2.Grant, allNew bool) error {
			if len(dirty) == 0 {
				return nil
			}
			if allNew {
				if fast, ok := e.store.(newExpandedGrantStorer); ok {
					if err := fast.StoreNewExpandedGrants(ctx, dirty...); err != nil {
						return fmt.Errorf("topological merge: store new expanded grants: %w", err)
					}
				} else if err := e.store.StoreExpandedGrants(ctx, dirty...); err != nil {
					return fmt.Errorf("topological merge: store expanded grants: %w", err)
				}
			} else {
				if err := e.store.StoreExpandedGrants(ctx, dirty...); err != nil {
					return fmt.Errorf("topological merge: store expanded grants: %w", err)
				}
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
		},
	}
	if fast, ok := e.store.(synthesizedContributionStorer); ok {
		sink.storeSynth = func(ctx context.Context, dest *v2.Entitlement, principals []*v3.PrincipalRef, sources []batonGrant.Sources) error {
			if len(principals) == 0 {
				return nil
			}
			if activeLayer != nil {
				if err := activeLayer.AddExpandedGrantLayerContributions(ctx, dest, principals, sources); err != nil {
					return fmt.Errorf("topological merge: add synthesized layer contributions: %w", err)
				}
			} else {
				if err := fast.StoreNewExpandedGrantContributions(ctx, dest, principals, sources); err != nil {
					return fmt.Errorf("topological merge: store new expanded grant contributions: %w", err)
				}
			}
			if run.onStoredSynth != nil {
				if err := run.onStoredSynth(ctx, dest, principals, sources); err != nil {
					return err
				}
			}
			if run.metrics != nil {
				run.metrics.DirtyGrantsWritten += int64(len(principals))
			}
			return nil
		}
	}

	var layerCandidate synthesizedContributionLayerStorer
	if sink.storeSynth != nil {
		layerCandidate, _ = e.store.(synthesizedContributionLayerStorer)
	}

	nodeIdx := 0
	for _, layer := range layers {
		if layerCandidate != nil {
			ok, err := layerCandidate.BeginExpandedGrantLayer(ctx)
			if err != nil {
				return fmt.Errorf("topological merge: begin synthesized layer: %w", err)
			}
			if ok {
				activeLayer = layerCandidate
			}
		}
		if err := e.driveTopologicalLayer(ctx, entitlements, layer, run, sink, nodeIdx, totalNodes); err != nil {
			if activeLayer != nil {
				_ = activeLayer.AbortExpandedGrantLayer(ctx)
				activeLayer = nil
			}
			return err
		}
		nodeIdx += len(layer)
		if activeLayer != nil {
			if err := activeLayer.FinishExpandedGrantLayer(ctx); err != nil {
				// Symmetric with the mid-layer error path above: if the
				// finish failed before the engine detached the session
				// (e.g. an error thrown by a wrapper before reaching the
				// engine, or an engine failure that left the session
				// attached), a same-process retry's BeginExpandedGrantLayer
				// would hit "session already open". Abort is a no-op when
				// the session is already gone.
				_ = activeLayer.AbortExpandedGrantLayer(ctx)
				activeLayer = nil
				return fmt.Errorf("topological merge: finish synthesized layer: %w", err)
			}
			activeLayer = nil
		}
	}
	return nil
}

// driveTopologicalLayer reduces every node of one topological layer against
// the shared sink. startIdx is the number of nodes completed in earlier
// layers, used only for progress reporting. Split out of driveTopological so
// a layer's error unwinds through one place where the caller can abort the
// layer's open session.
func (e *Expander) driveTopologicalLayer(
	ctx context.Context,
	entitlements map[string]*v2.Entitlement,
	layer []int,
	run topologicalRun,
	sink *destinationSink,
	startIdx int,
	totalNodes int,
) error {
	for i, nodeID := range layer {
		if run.checkBudget != nil {
			if err := run.checkBudget(); err != nil {
				return err
			}
		}
		if run.progress != nil {
			run.progress(startIdx+i, totalNodes)
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
				// Same drop-don't-fail policy as the legacy expander's
				// missing-descendant handling. Recorded on the sync-wide
				// aggregate (one warning per sync with distinct-id
				// examples); per-edge logging stays at Debug so a large
				// dangling family can't flood the logs. Skipping this
				// destination skips its ENTIRE reduction — every incoming
				// edge — so the total counts each one.
				e.dropStats.RecordDestinationMissingEdges(destID, len(incoming))
				ctxzap.Extract(ctx).Debug("topological expansion: destination entitlement not in store; skipping its reduction",
					zap.String("entitlement_id", destID))
				continue
			}
			if err := run.reduce(ctx, destEntitlement, incoming, entitlements, sink); err != nil {
				return fmt.Errorf("reduce destination entitlement %q: %w", destID, err)
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
func logFanInWidth(ctx context.Context, g *EntitlementGraph, layers [][]int) {
	widths := make([]int, 0, len(g.Nodes))
	outDegrees := make([]int, 0, len(g.Nodes))
	maxParentEntitlements := 0
	for _, layer := range layers {
		for _, nodeID := range layer {
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

// topologicalNodeLayers returns the graph's Kahn level decomposition: layer k
// holds every node whose parents all sit in layers < k, so nodes within one
// layer never depend on each other. The layer boundary is where synthesized
// grant writes can be published (and, later, checkpointed) — a node's reduce
// only ever reads parent output from strictly earlier layers. Each layer is
// sorted by node id for deterministic iteration.
func topologicalNodeLayers(g *EntitlementGraph) ([][]int, error) {
	inDegree := make(map[int]int, len(g.Nodes))
	for id := range g.Nodes {
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

	frontier := make([]int, 0, len(g.Nodes))
	for id, deg := range inDegree {
		if deg == 0 {
			frontier = append(frontier, id)
		}
	}
	sort.Ints(frontier)

	var layers [][]int
	seen := 0
	for len(frontier) > 0 {
		layers = append(layers, frontier)
		seen += len(frontier)
		var next []int
		for _, id := range frontier {
			for child := range g.SourcesToDestinations[id] {
				inDegree[child]--
				if inDegree[child] == 0 {
					next = append(next, child)
				}
			}
		}
		sort.Ints(next)
		frontier = next
	}
	if seen != len(g.Nodes) {
		return nil, fmt.Errorf("topological merge: graph contains a cycle or dangling edge")
	}
	return layers, nil
}

// topologicalNodeOrder flattens topologicalNodeLayers into a single valid
// topological order. Kept for callers that only need a linear order (the
// expansion plan builder and benchmarks).
func topologicalNodeOrder(g *EntitlementGraph) ([]int, error) {
	layers, err := topologicalNodeLayers(g)
	if err != nil {
		return nil, err
	}
	order := make([]int, 0, len(g.Nodes))
	for _, layer := range layers {
		order = append(order, layer...)
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

// topoContribution accumulates the source entitlements contributing to one
// principal on one destination. sources is a small slice, not a map: fan-in is
// tiny in practice (p50 ≈ 4, p99 ≈ 14 source entitlements per principal), so a
// linear-scan slice avoids the per-group map allocation that dominated the
// expansion allocation profile. addSource keeps entries unique.
type topoContribution struct {
	sources   batonGrant.Sources
	principal topoPrincipal
}

func (c *topoContribution) add(sourceEntitlementID string, isDirect bool, principal *v2.Resource) {
	c.addSource(sourceEntitlementID, isDirect)
	if c.principal.empty() && principal != nil {
		c.principal.setResource(principal)
	}
}

// addSource records a source entitlement's contribution and its directness,
// upgrading an existing indirect entry to direct. It never touches the
// principal.
func (c *topoContribution) addSource(sourceEntitlementID string, isDirect bool) {
	for i := range c.sources {
		if c.sources[i].EntitlementID == sourceEntitlementID {
			if isDirect && !c.sources[i].IsDirect {
				c.sources[i].IsDirect = true
			}
			return
		}
	}
	c.sources = append(c.sources, batonGrant.Source{EntitlementID: sourceEntitlementID, IsDirect: isDirect})
}

func (c *topoContribution) merge(other *topoContribution) {
	if other == nil {
		return
	}
	for _, src := range other.sources {
		c.addSource(src.EntitlementID, src.IsDirect)
	}
	if c.principal.empty() {
		c.principal.take(other.principal)
	}
}

// resetForReuse clears the accumulated sources and principal while keeping
// backing storage, so stream-owned contributions can be recycled across
// principal groups without reallocating.
func (c *topoContribution) resetForReuse() {
	c.sources = c.sources[:0]
	c.principal.reset()
}

func (c *topoContribution) principalRefForStore() (principalRefData, bool) {
	if c == nil {
		return principalRefData{}, false
	}
	return c.principal.refForStore()
}

func (c *topoContribution) principalResource() (*v2.Resource, error) {
	if c == nil {
		return nil, nil
	}
	return c.principal.resource()
}

// principalRefData is the identity-only principal form as plain strings —
// the value-type equivalent of v3.PrincipalRef. Contributions carry this
// instead of a proto so per-row proto allocation happens exactly once, in
// the flusher's reusable slab, rather than at every projection-row decode
// (54M+ rows per whale expansion made PrincipalRef_builder.Build a top
// allocation site).
type principalRefData struct {
	resourceTypeID       string
	resourceID           string
	parentResourceTypeID string
	parentResourceID     string
	ok                   bool
}

func (d principalRefData) fillRef(ref *v3.PrincipalRef) {
	// Every field is assigned unconditionally: refs come from a reused slab
	// and a conditionally-set field would leak the previous occupant's value.
	ref.SetResourceTypeId(d.resourceTypeID)
	ref.SetResourceId(d.resourceID)
	ref.SetParentResourceTypeId(d.parentResourceTypeID)
	ref.SetParentResourceId(d.parentResourceID)
}

type topoPrincipal struct {
	// full is the full principal payload used by generic fallback stores.
	full *v2.Resource
	// refData is the identity-only form Pebble can persist without
	// unmarshalling the full Resource. resourceBytes is kept only so fallback
	// stores preserve rich principal payload when projection rows are the
	// source.
	refData       principalRefData
	resourceBytes []byte
}

func (p *topoPrincipal) empty() bool {
	return p.full == nil && !p.refData.ok && len(p.resourceBytes) == 0
}

func (p *topoPrincipal) reset() {
	p.full = nil
	p.refData = principalRefData{}
	// Keep capacity: setRef appends into this buffer on the next group.
	p.resourceBytes = p.resourceBytes[:0]
}

func (p *topoPrincipal) setResource(resource *v2.Resource) {
	p.full = proto.Clone(resource).(*v2.Resource)
	p.refData = principalRefData{}
	p.resourceBytes = nil
}

func (p *topoPrincipal) setRef(data principalRefData, resourceBytes []byte) {
	p.full = nil
	p.refData = data
	p.resourceBytes = append(p.resourceBytes[:0], resourceBytes...)
}

func (p *topoPrincipal) take(other topoPrincipal) {
	if other.full != nil {
		p.full = other.full
		p.refData = principalRefData{}
		p.resourceBytes = nil
		return
	}
	p.full = nil
	p.refData = other.refData
	p.resourceBytes = append(p.resourceBytes[:0], other.resourceBytes...)
}

func (p *topoPrincipal) refForStore() (principalRefData, bool) {
	if p.refData.ok {
		return p.refData, true
	}
	if p.full == nil || p.full.GetId() == nil {
		return principalRefData{}, false
	}
	parent := p.full.GetParentResourceId()
	return principalRefData{
		resourceTypeID:       p.full.GetId().GetResourceType(),
		resourceID:           p.full.GetId().GetResource(),
		parentResourceTypeID: parent.GetResourceType(),
		parentResourceID:     parent.GetResource(),
		ok:                   true,
	}, true
}

func (p *topoPrincipal) resource() (*v2.Resource, error) {
	if p.full != nil {
		return p.full, nil
	}
	if len(p.resourceBytes) > 0 {
		resource := &v2.Resource{}
		if err := proto.Unmarshal(p.resourceBytes, resource); err != nil {
			return nil, err
		}
		p.full = resource
		p.resourceBytes = nil
		return resource, nil
	}
	if !p.refData.ok {
		return nil, nil
	}
	resource := principalResourceFromRefData(p.refData)
	p.full = resource
	return resource, nil
}

// principalResourceFromRef adapts a proto ref for consumers that hold one
// (fallback stores handed []*v3.PrincipalRef).
func principalResourceFromRef(ref *v3.PrincipalRef) *v2.Resource {
	if ref == nil {
		return nil
	}
	return principalResourceFromRefData(principalRefData{
		resourceTypeID:       ref.GetResourceTypeId(),
		resourceID:           ref.GetResourceId(),
		parentResourceTypeID: ref.GetParentResourceTypeId(),
		parentResourceID:     ref.GetParentResourceId(),
		ok:                   true,
	})
}

func principalResourceFromRefData(data principalRefData) *v2.Resource {
	var parent *v2.ResourceId
	if data.parentResourceID != "" {
		parent = v2.ResourceId_builder{
			ResourceType: data.parentResourceTypeID,
			Resource:     data.parentResourceID,
		}.Build()
	}
	return v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: data.resourceTypeID,
			Resource:     data.resourceID,
		}.Build(),
		ParentResourceId: parent,
	}.Build()
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

func mergeContributionIntoExistingGrant(baseGrant *v2.Grant, destEntitlementID string, contrib batonGrant.Sources) *v2.Grant {
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
	for _, src := range contrib {
		existingSource := sourcesMap[src.EntitlementID]
		if existingSource == nil {
			sourcesMap[src.EntitlementID] = &v2.GrantSources_GrantSource{IsDirect: src.IsDirect}
			updated = true
			continue
		}
		if src.IsDirect && !existingSource.GetIsDirect() {
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

func newExpandedGrantWithSources(descEntitlement *v2.Entitlement, principal *v2.Resource, sources batonGrant.Sources) (*v2.Grant, error) {
	if len(sources) == 0 {
		return nil, fmt.Errorf("newExpandedGrantWithSources: empty sources")
	}
	sourceMap := make(map[string]*v2.GrantSources_GrantSource, len(sources))
	for _, src := range sources {
		sourceMap[src.EntitlementID] = &v2.GrantSources_GrantSource{IsDirect: src.IsDirect}
	}
	enResource := descEntitlement.GetResource()
	if enResource == nil {
		return nil, fmt.Errorf("newExpandedGrantWithSources: entitlement has no resource")
	}
	// Guard the id too: batonGrant.NewGrantID panics on a nil resource id
	// (its callers are connector code where that is a programming error);
	// here the principal came out of a store and malformed data must be an
	// error, not a panic.
	if principal == nil || principal.GetId() == nil {
		return nil, fmt.Errorf("newExpandedGrantWithSources: principal has no resource id")
	}
	grantID := batonGrant.NewGrantID(principal, descEntitlement)
	return v2.Grant_builder{
		Id:          grantID,
		Entitlement: descEntitlement,
		Principal:   principal,
		Sources:     v2.GrantSources_builder{Sources: sourceMap}.Build(),
		Annotations: annotations.Annotations{immutableAnnotationAny},
	}.Build(), nil
}

// NewExpandedGrantForStore builds the generic v2 expanded grant used by store
// adapters that do not implement the direct synthesized-contribution fast path.
func NewExpandedGrantForStore(descEntitlement *v2.Entitlement, principal *v2.Resource, sources batonGrant.Sources) (*v2.Grant, error) {
	return newExpandedGrantWithSources(descEntitlement, principal, sources)
}
