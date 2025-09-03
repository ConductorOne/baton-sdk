package expand

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
)

// generateEdges constructs a deterministic set of edges for N nodes, aiming for ~M unique edges.
// It embeds numCycles rings of length cycleLen, and fills the rest randomly using seed.
func generateEdges(n, m, cycleLen, numCycles int, seed int64) [][2]int {
	rng := rand.New(rand.NewSource(seed)) //nolint:gosec // Deterministic seed is fine for testing/benchmarking.
	edgeSet := make(map[[2]int]struct{}, m)
	add := func(u, v int) {
		if u == v {
			return
		}
		key := [2]int{u, v}
		edgeSet[key] = struct{}{}
	}
	// embed cycles
	if cycleLen > 1 && numCycles > 0 {
		stride := n / (numCycles + 1)
		idx := 1
		for c := 0; c < numCycles; c++ {
			start := idx
			for k := 0; k < cycleLen-1 && idx < n; k++ {
				add(idx, idx+1)
				idx++
			}
			if idx <= n {
				add(idx, start)
			}
			idx += stride
			if idx > n {
				idx = n
			}
		}
	}
	// fill random until ~M
	for len(edgeSet) < m {
		u := 1 + rng.Intn(n)
		v := 1 + rng.Intn(n)
		add(u, v)
	}
	edges := make([][2]int, 0, len(edgeSet))
	for e := range edgeSet {
		edges = append(edges, e)
	}
	return edges
}

// buildGraphFromEdges creates an EntitlementGraph populated only with fields required by FixCycles.
func buildGraphFromEdges(n int, edges [][2]int) *EntitlementGraph {
	g := &EntitlementGraph{
		NextNodeID:            n,
		NextEdgeID:            0,
		Nodes:                 make(map[int]Node, n),
		EntitlementsToNodes:   make(map[string]int),
		SourcesToDestinations: make(map[int]map[int]int, n),
		DestinationsToSources: make(map[int]map[int]int, n),
		Edges:                 make(map[int]Edge, len(edges)),
	}
	for i := 1; i <= n; i++ {
		g.Nodes[i] = Node{Id: i}
	}
	for _, e := range edges {
		src, dst := e[0], e[1]
		if src == dst {
			continue
		}
		row := g.SourcesToDestinations[src]
		if row == nil {
			row = make(map[int]int)
			g.SourcesToDestinations[src] = row
		}
		if _, ok := row[dst]; ok {
			continue
		}
		g.NextEdgeID++
		ed := Edge{EdgeID: g.NextEdgeID, SourceID: src, DestinationID: dst}
		g.Edges[ed.EdgeID] = ed
		row[dst] = ed.EdgeID
		col := g.DestinationsToSources[dst]
		if col == nil {
			col = make(map[int]int)
			g.DestinationsToSources[dst] = col
		}
		col[src] = ed.EdgeID
	}
	return g
}

func benchmarkFixCycles(b *testing.B, n, m, cycleLen, numCycles int) {
	b.Helper()
	edges := generateEdges(n, m, cycleLen, numCycles, 1)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		g := buildGraphFromEdges(n, edges)
		if err := g.FixCycles(ctx); err != nil {
			b.Fatalf("FixCycles error: %v", err)
		}
	}
}

const enableLongTests = false

func BenchmarkSCC_FixCycles_Large(b *testing.B) {
	shortCases := []struct{ N, M int }{
		{N: 10000, M: 100000},
		{N: 20000, M: 200000},
	}
	fullCases := []struct{ N, M int }{
		{N: 50000, M: 500000},
		{N: 100000, M: 1000000},
	}
	cases := shortCases
	if !testing.Short() && enableLongTests {
		cases = append(cases, fullCases...)
	}
	for _, c := range cases {
		name := fmt.Sprintf("N=%d_M=%d", c.N, c.M)
		b.Run(name, func(b *testing.B) {
			cycleLen := 16
			numCycles := c.N / 2048
			benchmarkFixCycles(b, c.N, c.M, cycleLen, numCycles)
		})
	}
}
