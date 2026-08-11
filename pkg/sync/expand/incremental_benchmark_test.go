package expand

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

type incrementalBenchFixture struct {
	store   *MockExpanderStore
	graph   *EntitlementGraph
	changed []string
}

// BenchmarkTopologicalAffectedNodeOrderWideFanout exercises the shape that
// made repeated frontier sorting quadratic: one root makes every child ready
// during the same iteration.
func BenchmarkTopologicalAffectedNodeOrderWideFanout(b *testing.B) {
	ctx := context.Background()
	g := NewEntitlementGraph(ctx)
	g.AddEntitlementID("root")

	const children = 10_000
	affected := make(map[int]struct{}, children+1)
	affected[g.GetNode("root").Id] = struct{}{}
	for i := 0; i < children; i++ {
		child := fmt.Sprintf("child:%05d", i)
		g.AddEntitlementID(child)
		if err := g.AddEdge(ctx, "root", child, false, nil); err != nil {
			b.Fatal(err)
		}
		affected[g.GetNode(child).Id] = struct{}{}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		order, err := topologicalAffectedNodeOrder(g, affected)
		if err != nil {
			b.Fatal(err)
		}
		if len(order) != children+1 {
			b.Fatalf("unexpected order length: %d", len(order))
		}
	}
}

// TestIncrementalPerformanceGates is intentionally opt-in because its
// 100k-entitlement/100k-principal fixtures allocate hundreds of megabytes.
// It enforces allocation/work gates; wall time remains benchmark evidence.
func TestIncrementalPerformanceGates(t *testing.T) {
	if os.Getenv("BATON_INCREMENTAL_PERF") == "" {
		t.Skip("set BATON_INCREMENTAL_PERF=1 to run production-scale performance gates")
	}
	const (
		entitlements = 100_000
		principals   = 100_000
	)
	ctx := context.Background()

	sparse := buildIncrementalBenchFixture(t, entitlements, principals, 1, true)
	var sparseResult *IncrementalResult
	sparseAlloc := measuredTotalAlloc(func() {
		var err error
		sparseResult, err = NewIncrementalExpander(sparse.store, sparse.graph).
			ExpandChanges(ctx, nil, sparse.changed)
		require.NoError(t, err)
	})
	require.LessOrEqual(t, len(sparseResult.EntitlementsWalked), 3,
		"sparse work must stay bounded by its modeled affected component")

	dense := buildIncrementalBenchFixture(t, entitlements, principals, 10_000, true)
	denseEligibilityAlloc := measuredTotalAlloc(func() {
		_, err := NewIncrementalExpander(dense.store, dense.graph).
			ExpandChanges(ctx, nil, dense.changed)
		require.ErrorIs(t, err, ErrIncrementalDenseChangeDecline)
	})

	full := buildIncrementalBenchFixture(t, entitlements, principals, 10_000, false)
	fullAlloc := measuredTotalAlloc(func() {
		require.NoError(t, NewExpander(full.store, full.graph).Run(ctx))
	})

	// P3: deciding to fall back must cost at most 10% of full expansion.
	require.LessOrEqual(t, denseEligibilityAlloc*10, fullAlloc)
	// P4: dense eligibility plus full fallback must stay within 1.15x full.
	require.LessOrEqual(t, (denseEligibilityAlloc+fullAlloc)*100, fullAlloc*115)
	// P5: a truly sparse incremental run must allocate less than full.
	require.Less(t, sparseAlloc, fullAlloc)
	t.Logf("allocations: sparse=%d dense-eligibility=%d full=%d", sparseAlloc, denseEligibilityAlloc, fullAlloc)
}

func measuredTotalAlloc(run func()) uint64 {
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	run()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

// BenchmarkIncrementalExpand measures only the change application. Fixture
// construction and the prior completed expansion are outside the timer.
func BenchmarkIncrementalExpand(b *testing.B) {
	for _, entitlements := range []int{1_000, 10_000, 100_000} {
		for _, principals := range []int{10_000, 100_000} {
			for _, delta := range []int{1, 100, 250, 500, 1_000, 2_500, 5_000, 10_000} {
				if delta > principals {
					continue
				}
				name := fmt.Sprintf("E=%d/P=%d/K=%d", entitlements, principals, delta)
				b.Run(name, func(b *testing.B) {
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						b.StopTimer()
						fixture := buildIncrementalBenchFixture(b, entitlements, principals, delta, true)
						runtime.GC()
						var before runtime.MemStats
						runtime.ReadMemStats(&before)
						b.StartTimer()
						_, err := NewIncrementalExpander(fixture.store, fixture.graph).
							ExpandChanges(context.Background(), nil, fixture.changed)
						b.StopTimer()
						if errors.Is(err, ErrIncrementalDenseChangeDecline) {
							b.ReportMetric(1, "dense-decline/op")
						} else if err != nil {
							b.Fatal(err)
						}
						reportHeapDelta(b, &before)
					}
				})
			}
		}
	}
}

// BenchmarkFullExpand is the fresh-rebuild oracle for the same post-delta
// states used by BenchmarkIncrementalExpand.
func BenchmarkFullExpand(b *testing.B) {
	for _, entitlements := range []int{1_000, 10_000, 100_000} {
		for _, principals := range []int{10_000, 100_000} {
			for _, delta := range []int{1, 100, 250, 500, 1_000, 2_500, 5_000, 10_000} {
				if delta > principals {
					continue
				}
				name := fmt.Sprintf("E=%d/P=%d/K=%d", entitlements, principals, delta)
				b.Run(name, func(b *testing.B) {
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						b.StopTimer()
						fixture := buildIncrementalBenchFixture(b, entitlements, principals, delta, false)
						runtime.GC()
						var before runtime.MemStats
						runtime.ReadMemStats(&before)
						b.StartTimer()
						err := NewExpander(fixture.store, fixture.graph).Run(context.Background())
						b.StopTimer()
						if err != nil {
							b.Fatal(err)
						}
						reportHeapDelta(b, &before)
					}
				})
			}
		}
	}
}

func buildIncrementalBenchFixture(
	b testing.TB,
	entitlementCount int,
	principalCount int,
	deltaCount int,
	prepareBase bool,
) incrementalBenchFixture {
	b.Helper()
	ctx := context.Background()
	// Three nodes per component: source -> left and source -> right. This gives
	// about 2E/3 edges and bounds total expanded rows near 3P.
	components := max(1, entitlementCount/3)
	store := NewMockExpanderStore()
	graph := NewEntitlementGraph(ctx)
	sources := make([]string, components)
	for i := 0; i < components; i++ {
		source := fmt.Sprintf("source:%06d", i)
		left := fmt.Sprintf("left:%06d", i)
		right := fmt.Sprintf("right:%06d", i)
		sources[i] = source
		for _, id := range []string{source, left, right} {
			store.AddEntitlement(makeEntitlement(id, makeResource("group", id)))
			graph.AddEntitlementID(id)
		}
		if err := graph.AddEdge(ctx, source, left, false, nil); err != nil {
			b.Fatal(err)
		}
		if err := graph.AddEdge(ctx, source, right, false, nil); err != nil {
			b.Fatal(err)
		}
	}
	for i := 0; i < principalCount; i++ {
		source := sources[i%len(sources)]
		store.AddGrant(directGrant(source, makeResource("user", fmt.Sprintf("p:%07d", i))))
	}

	if prepareBase {
		if err := graph.FixCycles(ctx); err != nil {
			b.Fatal(err)
		}
		if err := NewExpander(store, graph).Run(ctx); err != nil {
			b.Fatal(err)
		}
		graph.Loaded = true
	}

	changedSet := make(map[string]struct{}, deltaCount)
	for i := 0; i < deltaCount; i++ {
		source := sources[i%len(sources)]
		store.AddGrant(directGrant(source, makeResource("user", fmt.Sprintf("delta:%07d", i))))
		changedSet[source] = struct{}{}
	}
	changed := make([]string, 0, len(changedSet))
	for source := range changedSet {
		changed = append(changed, source)
	}
	return incrementalBenchFixture{store: store, graph: graph, changed: changed}
}

func reportHeapDelta(b *testing.B, before *runtime.MemStats) {
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	if after.TotalAlloc >= before.TotalAlloc {
		b.ReportMetric(float64(after.TotalAlloc-before.TotalAlloc), "total-alloc-bytes/op")
	}
	if after.HeapInuse >= before.HeapInuse {
		b.ReportMetric(float64(after.HeapInuse-before.HeapInuse), "heap-inuse-delta-bytes/op")
	}
}
