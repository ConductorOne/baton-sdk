package expand

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ~50s.
func BenchmarkExpandSmall(b *testing.B) {
	benchmarkExpand(b, "36zGvJw3uxU1QMJKU2yPVQ1hBOC", false)
}

func BenchmarkExpandSmallPebble(b *testing.B) {
	benchmarkExpandPebble(b, "36zGvJw3uxU1QMJKU2yPVQ1hBOC", false)
}

// ~70s.
func BenchmarkExpandSmallMedium(b *testing.B) {
	benchmarkExpand(b, "36zM46KKuaBq0wjSSvKh5o0350y", false)
}

func BenchmarkExpandSmallMediumPebble(b *testing.B) {
	benchmarkExpandPebble(b, "36zM46KKuaBq0wjSSvKh5o0350y", false)
}

// BenchmarkExpandSmallPerStep mirrors syncer.expandGrantsForEntitlements:
// a fresh Expander is constructed before each RunSingleStep call, looping
// until the graph reports IsDone. This validates that per-action prefetch
// wins are not artifacts of the long-lived-Expander harness shape.
func BenchmarkExpandSmallPerStep(b *testing.B) {
	benchmarkExpand(b, "36zGvJw3uxU1QMJKU2yPVQ1hBOC", true)
}

func BenchmarkExpandSmallPebblePerStep(b *testing.B) {
	benchmarkExpandPebble(b, "36zGvJw3uxU1QMJKU2yPVQ1hBOC", true)
}

func BenchmarkExpandSmallMediumPerStep(b *testing.B) {
	benchmarkExpand(b, "36zM46KKuaBq0wjSSvKh5o0350y", true)
}

func BenchmarkExpandSmallMediumPebblePerStep(b *testing.B) {
	benchmarkExpandPebble(b, "36zM46KKuaBq0wjSSvKh5o0350y", true)
}

func getTestdataPath(syncID string) string {
	_, filename, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(filename), "testdata", fmt.Sprintf("sync.%s.unexpanded", syncID))
}

func getPebbleTestdataPath(syncID string) string {
	_, filename, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(filename), "testdata", fmt.Sprintf("sync.%s.unexpanded.pebble.c1z", syncID))
}

// copyGraph creates a deep copy of an EntitlementGraph.
func copyGraph(g *EntitlementGraph) *EntitlementGraph {
	newGraph := &EntitlementGraph{
		NextNodeID:            g.NextNodeID,
		NextEdgeID:            g.NextEdgeID,
		Nodes:                 make(map[int]Node, len(g.Nodes)),
		EntitlementsToNodes:   make(map[string]int, len(g.EntitlementsToNodes)),
		SourcesToDestinations: make(map[int]map[int]int, len(g.SourcesToDestinations)),
		DestinationsToSources: make(map[int]map[int]int, len(g.DestinationsToSources)),
		Edges:                 make(map[int]Edge, len(g.Edges)),
		Loaded:                g.Loaded,
		Depth:                 g.Depth,
		Actions:               make([]*EntitlementGraphAction, len(g.Actions)),
		HasNoCycles:           g.HasNoCycles,
		ExpansionPlan:         copyExpansionPlan(g.ExpansionPlan),
		ExpansionMetrics:      copyExpansionMetrics(g.ExpansionMetrics),
	}

	for k, v := range g.Nodes {
		newGraph.Nodes[k] = Node{Id: v.Id, EntitlementIDs: slices.Clone(v.EntitlementIDs)}
	}

	maps.Copy(newGraph.EntitlementsToNodes, g.EntitlementsToNodes)

	for k, v := range g.SourcesToDestinations {
		newGraph.SourcesToDestinations[k] = maps.Clone(v)
	}

	for k, v := range g.DestinationsToSources {
		newGraph.DestinationsToSources[k] = maps.Clone(v)
	}

	for k, v := range g.Edges {
		newGraph.Edges[k] = Edge{
			EdgeID:          v.EdgeID,
			SourceID:        v.SourceID,
			DestinationID:   v.DestinationID,
			IsExpanded:      v.IsExpanded,
			IsShallow:       v.IsShallow,
			ResourceTypeIDs: slices.Clone(v.ResourceTypeIDs),
		}
	}

	for i, action := range g.Actions {
		newGraph.Actions[i] = &EntitlementGraphAction{
			SourceEntitlementID:     action.SourceEntitlementID,
			Descendants:             slices.Clone(action.Descendants),
			DescendantEntitlementID: action.DescendantEntitlementID,
			Shallow:                 action.Shallow,
			ResourceTypeIDs:         slices.Clone(action.ResourceTypeIDs),
			PageToken:               action.PageToken,
		}
	}

	return newGraph
}

// loadEntitlementGraphFromC1Z builds the entitlement graph by reading
// expansion metadata via the GrantStore iterator. Callers must have
// scoped the c1z to the desired sync (via SetSyncID) before calling —
// this is the case for both the correctness test and the benchmark.
func loadEntitlementGraphFromC1Z(ctx context.Context, c1f *dotc1z.C1File, syncID string) (*EntitlementGraph, error) {
	_ = syncID // caller sets scope via SetSyncID before calling.
	return loadEntitlementGraphFromStore(ctx, c1f)
}

func loadEntitlementGraphFromStore(ctx context.Context, store dotc1z.C1ZStore) (*EntitlementGraph, error) {
	graph := NewEntitlementGraph(ctx)

	for def, err := range store.Grants().PendingExpansion(ctx) {
		if status.Code(err) == codes.NotFound {
			graph.Loaded = true
			return graph, nil
		}
		if err != nil {
			return nil, err
		}
		for _, srcEntitlementID := range def.Annotation.GetEntitlementIds() {
			srcEntitlement, err := store.GetEntitlement(ctx, reader_v2.EntitlementsReaderServiceGetEntitlementRequest_builder{
				EntitlementId: srcEntitlementID,
			}.Build())
			if err != nil {
				continue // Skip if source entitlement not found
			}

			graph.AddEntitlementID(def.TargetEntitlementID)
			graph.AddEntitlementID(srcEntitlement.GetEntitlement().GetId())
			_ = graph.AddEdge(ctx,
				srcEntitlement.GetEntitlement().GetId(),
				def.TargetEntitlementID,
				def.Annotation.GetShallow(),
				def.Annotation.GetResourceTypeIds(),
			)
		}
	}

	graph.Loaded = true
	return graph, nil
}

type benchmarkExpanderStore struct {
	store dotc1z.C1ZStore
}

func (s benchmarkExpanderStore) GetEntitlement(
	ctx context.Context,
	req *reader_v2.EntitlementsReaderServiceGetEntitlementRequest,
) (*reader_v2.EntitlementsReaderServiceGetEntitlementResponse, error) {
	return s.store.GetEntitlement(ctx, req)
}

func (s benchmarkExpanderStore) ListGrantsForEntitlement(
	ctx context.Context,
	req *reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest,
) (*reader_v2.GrantsReaderServiceListGrantsForEntitlementResponse, error) {
	return s.store.ListGrantsForEntitlement(ctx, req)
}

func (s benchmarkExpanderStore) ListGrantPrincipalKeysForEntitlement(
	ctx context.Context,
	entitlement *v2.Entitlement,
	pageToken string,
	pageSize uint32,
) ([]string, string, error) {
	if store, ok := s.store.(interface {
		ListGrantPrincipalKeysForEntitlement(context.Context, *v2.Entitlement, string, uint32) ([]string, string, error)
	}); ok {
		return store.ListGrantPrincipalKeysForEntitlement(ctx, entitlement, pageToken, pageSize)
	}
	resp, err := s.store.ListGrantsForEntitlement(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
		Entitlement: entitlement,
		PageToken:   pageToken,
		PageSize:    pageSize,
	}.Build())
	if err != nil {
		return nil, "", err
	}
	keys := make([]string, 0, len(resp.GetList()))
	for _, g := range resp.GetList() {
		if g.GetPrincipal() == nil {
			continue
		}
		id := g.GetPrincipal().GetId()
		keys = append(keys, id.GetResourceType()+"\x00"+id.GetResource())
	}
	return keys, resp.GetNextPageToken(), nil
}

func (s benchmarkExpanderStore) StoreExpandedGrants(ctx context.Context, grants ...*v2.Grant) error {
	return s.store.Grants().StoreExpandedGrants(ctx, grants...)
}

func (s benchmarkExpanderStore) GrantsForEntitlementPrincipalSorted() bool {
	store, ok := s.store.(interface {
		GrantsForEntitlementPrincipalSorted() bool
	})
	return ok && store.GrantsForEntitlementPrincipalSorted()
}

func benchmarkExpand(b *testing.B, syncID string, perStep bool) {
	c1zPath := getTestdataPath(syncID)
	if _, err := os.Stat(c1zPath); os.IsNotExist(err) {
		b.Skipf("testdata file not found: %s", c1zPath)
	}

	// Do not use b.Context() here. Doing so causes the benchmark to run slower.
	// The SQL library's interruptOnDone() is called if ctx.Done() is not nil.
	ctx := context.Background()

	// Open the c1z file once to get stats
	c1f, err := dotc1z.NewC1ZFile(ctx, c1zPath)
	require.NoError(b, err)
	defer c1f.Close(ctx)

	// Scope reads to the requested sync.
	err = c1f.SetSyncID(ctx, syncID)
	require.NoError(b, err)

	// Load the graph
	graph, err := loadEntitlementGraphFromC1Z(ctx, c1f, syncID)
	require.NoError(b, err)

	b.Logf("Graph loaded: %d nodes, %d edges", len(graph.Nodes), len(graph.Edges))

	// Mirror the syncer's Phase 2: collapse cycles before expanding so a
	// graph with cycles doesn't trip the depth guard. No-op on acyclic graphs.
	require.NoError(b, graph.FixCycles(ctx))

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Func is for defers to be human undertandable.
		func(i int) {
			// Make a copy of the graph for each iteration
			graphCopy := copyGraph(graph)

			// Create a fresh c1z for each iteration (copy original)
			tmpFile, err := os.CreateTemp("", "bench-expand-*.c1z")
			require.NoError(b, err)
			tmpPath := tmpFile.Name()
			defer os.Remove(tmpPath)
			err = tmpFile.Close()
			require.NoError(b, err)

			// Copy original c1z to temp
			srcData, err := os.ReadFile(c1zPath)
			require.NoError(b, err)
			err = os.WriteFile(tmpPath, srcData, 0600) // #nosec G703 -- tmpPath is generated by os.CreateTemp in this benchmark.
			require.NoError(b, err)

			c1fCopy, err := dotc1z.NewC1ZFile(ctx, tmpPath)
			require.NoError(b, err)
			defer c1fCopy.Close(ctx)

			err = c1fCopy.SetSyncID(ctx, syncID)
			require.NoError(b, err)

			// ---------------------------------------

			b.StartTimer()
			if perStep {
				// Mirror syncer.expandGrantsForEntitlements: a fresh
				// Expander wraps the persistent graph + store for each
				// step, and the loop exits when IsDone reports the graph
				// is fully expanded. The graph mutates across steps just
				// as it does in production via state.EntitlementGraph.
				for {
					expander := NewExpander(c1fCopy, graphCopy)
					if err = expander.RunSingleStep(ctx); err != nil {
						break
					}
					if expander.IsDone(ctx) {
						break
					}
				}
			} else {
				expander := NewExpander(c1fCopy, graphCopy)
				err = expander.Run(ctx)
			}
			b.StopTimer()

			// ---------------------------------------
			require.NoError(b, err)
		}(i)
	}
}

// BenchmarkExpandWhalePebble runs the expansion benchmark against an
// arbitrary pebble c1z file supplied via the BATON_BENCH_PEBBLE_PATH
// environment variable. This lets us profile expansion on large,
// real-world syncs without committing the fixture to the repo.
//
//	BATON_BENCH_PEBBLE_PATH=/tmp/whale-bench/whale.pebble.c1z \
//	    go test -bench=BenchmarkExpandWhalePebble -benchmem ./pkg/sync/expand/...
func BenchmarkExpandWhalePebble(b *testing.B) {
	pebblePath := os.Getenv("BATON_BENCH_PEBBLE_PATH")
	if pebblePath == "" {
		b.Skip("BATON_BENCH_PEBBLE_PATH not set")
	}
	benchmarkExpandPebbleAtPath(b, pebblePath, false)
}

func BenchmarkExpandWhalePebble_NoDigestIndex(b *testing.B) {
	pebblePath := os.Getenv("BATON_BENCH_PEBBLE_PATH")
	if pebblePath == "" {
		b.Skip("BATON_BENCH_PEBBLE_PATH not set")
	}
	benchmarkExpandPebbleAtPath(b, pebblePath, false,
		dotc1z.WithGrantDigestIndex(false),
	)
}

func benchmarkExpandPebble(b *testing.B, syncID string, perStep bool) {
	pebblePath := getPebbleTestdataPath(syncID)
	if _, err := os.Stat(pebblePath); os.IsNotExist(err) {
		b.Skipf("Pebble testdata file not found: %s", pebblePath)
	}
	benchmarkExpandPebbleAtPath(b, pebblePath, perStep)
}

func benchmarkExpandPebbleAtPath(b *testing.B, pebblePath string, perStep bool, storeOpts ...dotc1z.C1ZOption) {
	if _, err := os.Stat(pebblePath); os.IsNotExist(err) { // #nosec G703 -- pebblePath is a test-controlled fixture/env path in this benchmark.
		b.Skipf("Pebble file not found: %s", pebblePath)
	}

	ctx := context.Background()
	storeWriter, err := dotc1z.NewStore(ctx, pebblePath, storeOpts...)
	require.NoError(b, err)
	latest, err := storeWriter.SyncMeta().LatestFullSync(ctx)
	require.NoError(b, err)
	require.NotNil(b, latest)
	destSyncID := latest.ID
	_, started, err := storeWriter.StartOrResumeSync(ctx, connectorstore.SyncTypeFull, destSyncID)
	require.NoError(b, err)
	require.False(b, started)

	graph, err := loadEntitlementGraphFromStore(ctx, storeWriter)
	require.NoError(b, err)
	require.NoError(b, storeWriter.Close(ctx))
	require.NotEmpty(b, graph.Edges)

	b.Logf("Graph loaded: %d nodes, %d edges", len(graph.Nodes), len(graph.Edges))

	// Mirror the syncer's Phase 2 (runGrantExpandAction): collapse cyclic
	// components before expanding. Without this, a graph containing cycles
	// (including single-node self-loops) drives the step loop until it
	// trips the depth guard with "max depth exceeded". Production always
	// runs this once after the graph finishes loading.
	require.NoError(b, graph.FixCycles(ctx))
	b.Logf("Cycles fixed: %d nodes, %d edges, has_no_cycles=%t", len(graph.Nodes), len(graph.Edges), graph.HasNoCycles)

	// Optional wall-clock budget: when set, the step loop stops cleanly
	// after the budget elapses instead of running to completion. Useful
	// for capturing a bounded CPU/heap profile slice on large syncs where
	// a full expansion is impractical (or errors out on max depth).
	var budget time.Duration
	if v := os.Getenv("BATON_BENCH_BUDGET"); v != "" {
		budget, err = time.ParseDuration(v)
		require.NoError(b, err)
		b.Logf("Budget set: expansion will stop after %s", budget)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		func(i int) {
			graphCopy := copyGraph(graph)

			tmpFile, err := os.CreateTemp("", "bench-expand-pebble-*.c1z")
			require.NoError(b, err)
			tmpPath := tmpFile.Name()
			defer os.Remove(tmpPath)
			require.NoError(b, tmpFile.Close())

			srcData, err := os.ReadFile(pebblePath) // #nosec G703 -- pebblePath is a test-controlled fixture/env path in this benchmark.
			require.NoError(b, err)
			require.NoError(b, os.WriteFile(tmpPath, srcData, 0600)) // #nosec G703 -- tmpPath is generated by os.CreateTemp in this benchmark.

			storeWriter, err := dotc1z.NewStore(ctx, tmpPath, storeOpts...)
			require.NoError(b, err)
			_, started, err := storeWriter.StartOrResumeSync(ctx, connectorstore.SyncTypeFull, destSyncID)
			require.NoError(b, err)
			require.False(b, started)
			defer storeWriter.Close(ctx)

			b.StartTimer()
			step := 0
			start := time.Now()
			budgetHit := false
			// The budget is enforced via a context deadline (so a monolithic
			// topological pass self-interrupts mid-call) plus the per-step
			// wall-clock check below (so multi-step algorithms stop cleanly at a
			// step boundary). Either way we stop for a bounded profile slice.
			stepCtx := ctx
			if budget > 0 {
				var cancel context.CancelFunc
				stepCtx, cancel = context.WithTimeout(ctx, budget)
				defer cancel()
			}
			for {
				expander := NewExpander(benchmarkExpanderStore{store: storeWriter}, graphCopy)
				err = expander.RunSingleStep(stepCtx)
				if err != nil {
					if budget > 0 && errors.Is(err, context.DeadlineExceeded) {
						budgetHit = true
						err = nil
					}
					break
				}
				if expander.IsDone(stepCtx) {
					break
				}
				step++
				if budget > 0 && time.Since(start) >= budget {
					budgetHit = true
					break
				}
				if !perStep {
					// This benchmark still uses the production step loop so
					// debug output can identify long-running actions.
					continue
				}
			}
			b.StopTimer()

			if m := graphCopy.ExpansionMetrics; m != nil {
				b.ReportMetric(float64(m.ProjectionRowsBuilt), "projection_rows/op")
				b.ReportMetric(float64(m.NodesReduced), "nodes_reduced/op")
				b.ReportMetric(float64(m.DestinationEntitlements), "dest_ents_reduced/op")
				b.ReportMetric(float64(m.DirtyGrantsWritten), "dirty_grants/op")
			}

			if budgetHit {
				b.Logf("Budget reached after %d steps in %s; stopping cleanly for profile slice", step, time.Since(start))
				return
			}
			require.NoError(b, err)
		}(i)
	}
}
