package expand

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"

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

type synthLayerBuffer struct {
	store       synthesizedContributionLayerStorer
	limit       int
	tempDir     string
	chunks      []string
	dests       []*v2.Entitlement
	principals  [][]*v3.PrincipalRef
	sources     [][]batonGrant.Sources
	sourceArena []batonGrant.Source
	count       int
}

func newSynthLayerBuffer(store synthesizedContributionLayerStorer) *synthLayerBuffer {
	limit := 250000
	if raw := os.Getenv("BATON_PEBBLE_SYNTH_LAYER_BUFFER_ROWS"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			limit = parsed
		}
	}
	return &synthLayerBuffer{store: store, limit: limit}
}

func (b *synthLayerBuffer) add(dest *v2.Entitlement, principals []*v3.PrincipalRef, sources []batonGrant.Sources) {
	if len(principals) == 0 {
		return
	}
	principalCopy := make([]*v3.PrincipalRef, 0, len(principals))
	sourceCopy := make([]batonGrant.Sources, 0, len(sources))
	for i, principal := range principals {
		if principal == nil {
			continue
		}
		principalCopy = append(principalCopy, principal)
		start := len(b.sourceArena)
		b.sourceArena = append(b.sourceArena, sources[i]...)
		sourceCopy = append(sourceCopy, b.sourceArena[start:])
		b.count++
	}
	if len(principalCopy) == 0 {
		return
	}
	b.dests = append(b.dests, dest)
	b.principals = append(b.principals, principalCopy)
	b.sources = append(b.sources, sourceCopy)
}

func (b *synthLayerBuffer) shouldFlush() bool {
	return b != nil && b.limit > 0 && b.count >= b.limit
}

func (b *synthLayerBuffer) spill(ctx context.Context) error {
	if b == nil || b.count == 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if b.tempDir == "" {
		dir, err := os.MkdirTemp("", "expand-synth-layer-")
		if err != nil {
			return fmt.Errorf("topological merge: create synth layer temp dir: %w", err)
		}
		b.tempDir = dir
	}
	f, err := os.CreateTemp(b.tempDir, "chunk-*.bin")
	if err != nil {
		return fmt.Errorf("topological merge: create synth layer chunk: %w", err)
	}
	path := f.Name()
	success := false
	defer func() {
		_ = f.Close()
		if !success {
			_ = os.Remove(path)
		}
	}()
	w := bufio.NewWriterSize(f, 1<<20)
	for i, dest := range b.dests {
		destID, destRT, destRID := "", "", ""
		if dest != nil {
			destID = dest.GetId()
			if res := dest.GetResource(); res != nil && res.GetId() != nil {
				destRT = res.GetId().GetResourceType()
				destRID = res.GetId().GetResource()
			}
		}
		for j, principal := range b.principals[i] {
			if err := writeSynthLayerString(w, destID); err != nil {
				return err
			}
			if err := writeSynthLayerString(w, destRT); err != nil {
				return err
			}
			if err := writeSynthLayerString(w, destRID); err != nil {
				return err
			}
			if err := writeSynthLayerString(w, principal.GetResourceTypeId()); err != nil {
				return err
			}
			if err := writeSynthLayerString(w, principal.GetResourceId()); err != nil {
				return err
			}
			if err := writeSynthLayerString(w, principal.GetParentResourceTypeId()); err != nil {
				return err
			}
			if err := writeSynthLayerString(w, principal.GetParentResourceId()); err != nil {
				return err
			}
			srcs := b.sources[i][j]
			if err := binary.Write(w, binary.LittleEndian, uint32(len(srcs))); err != nil {
				return err
			}
			for _, src := range srcs {
				if err := writeSynthLayerString(w, src.EntitlementID); err != nil {
					return err
				}
				if src.IsDirect {
					if err := w.WriteByte(1); err != nil {
						return err
					}
				} else if err := w.WriteByte(0); err != nil {
					return err
				}
			}
		}
	}
	if err := w.Flush(); err != nil {
		return err
	}
	success = true
	b.chunks = append(b.chunks, path)
	b.clearMemory()
	return nil
}

func (b *synthLayerBuffer) flush(ctx context.Context) error {
	if b == nil || (b.count == 0 && len(b.chunks) == 0) {
		return nil
	}
	for _, chunk := range b.chunks {
		if err := b.flushChunk(ctx, chunk); err != nil {
			return err
		}
	}
	if err := b.store.StoreNewExpandedGrantContributionLayer(ctx, b.dests, b.principals, b.sources); err != nil {
		return err
	}
	b.clearMemory()
	if b.tempDir != "" {
		if err := os.RemoveAll(b.tempDir); err != nil {
			return err
		}
		b.tempDir = ""
	}
	b.chunks = b.chunks[:0]
	return nil
}

func (b *synthLayerBuffer) flushChunk(ctx context.Context, path string) error {
	f, err := os.Open(path) // #nosec G304 -- path is created under synth layer temp dir.
	if err != nil {
		return err
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 1<<20)
	var (
		dests      []*v2.Entitlement
		principals [][]*v3.PrincipalRef
		sources    [][]batonGrant.Sources
	)
	for {
		destID, err := readSynthLayerString(r)
		if errorsIsEOF(err) {
			break
		}
		if err != nil {
			return err
		}
		destRT, err := readSynthLayerString(r)
		if err != nil {
			return err
		}
		destRID, err := readSynthLayerString(r)
		if err != nil {
			return err
		}
		principalRT, err := readSynthLayerString(r)
		if err != nil {
			return err
		}
		principalID, err := readSynthLayerString(r)
		if err != nil {
			return err
		}
		parentRT, err := readSynthLayerString(r)
		if err != nil {
			return err
		}
		parentID, err := readSynthLayerString(r)
		if err != nil {
			return err
		}
		var sourceCount uint32
		if err := binary.Read(r, binary.LittleEndian, &sourceCount); err != nil {
			return err
		}
		srcs := make(batonGrant.Sources, 0, sourceCount)
		for i := uint32(0); i < sourceCount; i++ {
			entitlementID, err := readSynthLayerString(r)
			if err != nil {
				return err
			}
			isDirectByte, err := r.ReadByte()
			if err != nil {
				return err
			}
			srcs = append(srcs, batonGrant.Source{EntitlementID: entitlementID, IsDirect: isDirectByte == 1})
		}
		dests = append(dests, v2.Entitlement_builder{
			Id: destID,
			Resource: v2.Resource_builder{
				Id: v2.ResourceId_builder{ResourceType: destRT, Resource: destRID}.Build(),
			}.Build(),
		}.Build())
		principals = append(principals, []*v3.PrincipalRef{v3.PrincipalRef_builder{
			ResourceTypeId:       principalRT,
			ResourceId:           principalID,
			ParentResourceTypeId: parentRT,
			ParentResourceId:     parentID,
		}.Build()})
		sources = append(sources, []batonGrant.Sources{srcs})
		if len(dests) >= expansionDirtyFlushChunk {
			if err := b.store.StoreNewExpandedGrantContributionLayer(ctx, dests, principals, sources); err != nil {
				return err
			}
			dests = dests[:0]
			principals = principals[:0]
			sources = sources[:0]
		}
	}
	if len(dests) > 0 {
		return b.store.StoreNewExpandedGrantContributionLayer(ctx, dests, principals, sources)
	}
	return nil
}

func (b *synthLayerBuffer) clearMemory() {
	b.dests = b.dests[:0]
	b.principals = b.principals[:0]
	b.sources = b.sources[:0]
	b.sourceArena = b.sourceArena[:0]
	b.count = 0
}

func writeSynthLayerString(w io.Writer, s string) error {
	if err := binary.Write(w, binary.LittleEndian, uint32(len(s))); err != nil {
		return err
	}
	_, err := io.WriteString(w, s)
	return err
}

func readSynthLayerString(r io.Reader) (string, error) {
	var n uint32
	if err := binary.Read(r, binary.LittleEndian, &n); err != nil {
		return "", err
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

func errorsIsEOF(err error) bool {
	return err == io.EOF
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
	update           []*v2.Grant
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
	sourceStart := len(f.synthSourceArena)
	f.synthSourceArena = append(f.synthSourceArena, sources...)
	principalRef, ok := contrib.principalRefForStore()
	if !ok {
		return nil
	}
	f.synthPrincipals = append(f.synthPrincipals, principalRef)
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

	var activeLayer *synthLayerBuffer
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
				activeLayer.add(dest, principals, sources)
				if activeLayer.shouldFlush() {
					if err := activeLayer.spill(ctx); err != nil {
						return fmt.Errorf("topological merge: spill synthesized layer chunk: %w", err)
					}
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
		if os.Getenv("BATON_PEBBLE_SYNTH_LAYER_SST") != "" {
			if layerStore, ok := e.store.(synthesizedContributionLayerStorer); ok {
				activeLayer = newSynthLayerBuffer(layerStore)
			}
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
		if activeLayer != nil {
			if err := activeLayer.flush(ctx); err != nil {
				activeLayer = nil
				return fmt.Errorf("topological merge: flush synthesized layer: %w", err)
			}
			activeLayer = nil
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

func (c *topoContribution) principalRefForStore() (*v3.PrincipalRef, bool) {
	if c == nil {
		return nil, false
	}
	return c.principal.refForStore()
}

func (c *topoContribution) principalResource() (*v2.Resource, error) {
	if c == nil {
		return nil, nil
	}
	return c.principal.resource()
}

type topoPrincipal struct {
	// full is the full principal payload used by generic fallback stores.
	full *v2.Resource
	// ref is the identity-only form Pebble can persist without unmarshalling the
	// full Resource. resourceBytes is kept only so fallback stores preserve rich
	// principal payload when projection rows are the source.
	ref           *v3.PrincipalRef
	resourceBytes []byte
}

func (p *topoPrincipal) empty() bool {
	return p.full == nil && p.ref == nil && len(p.resourceBytes) == 0
}

func (p *topoPrincipal) reset() {
	p.full = nil
	p.ref = nil
	// Keep capacity: setRef appends into this buffer on the next group.
	p.resourceBytes = p.resourceBytes[:0]
}

func (p *topoPrincipal) setResource(resource *v2.Resource) {
	p.full = proto.Clone(resource).(*v2.Resource)
	p.ref = nil
	p.resourceBytes = nil
}

func (p *topoPrincipal) setRef(ref *v3.PrincipalRef, resourceBytes []byte) {
	p.full = nil
	p.ref = ref
	p.resourceBytes = append(p.resourceBytes[:0], resourceBytes...)
}

func (p *topoPrincipal) take(other topoPrincipal) {
	if other.full != nil {
		p.full = other.full
		p.ref = nil
		p.resourceBytes = nil
		return
	}
	p.full = nil
	p.ref = other.ref
	p.resourceBytes = append(p.resourceBytes[:0], other.resourceBytes...)
}

func (p *topoPrincipal) refForStore() (*v3.PrincipalRef, bool) {
	if p.ref != nil {
		return p.ref, true
	}
	if p.full == nil || p.full.GetId() == nil {
		return nil, false
	}
	parent := p.full.GetParentResourceId()
	return v3.PrincipalRef_builder{
		ResourceTypeId:       p.full.GetId().GetResourceType(),
		ResourceId:           p.full.GetId().GetResource(),
		ParentResourceTypeId: parent.GetResourceType(),
		ParentResourceId:     parent.GetResource(),
	}.Build(), true
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
	if p.ref == nil {
		return nil, nil
	}
	resource := principalResourceFromRef(p.ref)
	p.full = resource
	return resource, nil
}

func principalResourceFromRef(ref *v3.PrincipalRef) *v2.Resource {
	if ref == nil {
		return nil
	}
	var parent *v2.ResourceId
	if ref.GetParentResourceId() != "" {
		parent = v2.ResourceId_builder{
			ResourceType: ref.GetParentResourceTypeId(),
			Resource:     ref.GetParentResourceId(),
		}.Build()
	}
	return v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: ref.GetResourceTypeId(),
			Resource:     ref.GetResourceId(),
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
	if principal == nil {
		return nil, fmt.Errorf("newExpandedGrantWithSources: principal is nil")
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
