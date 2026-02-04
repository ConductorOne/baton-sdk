package expand

import (
	"context"
	"fmt"
	"strconv"
	"testing"
)

func buildRing(b *testing.B, n int) *EntitlementGraph {
	b.Helper()
	ctx := context.Background()
	g := NewEntitlementGraph(ctx)
	ids := make([]string, n)
	for i := 0; i < n; i++ {
		ids[i] = strconv.Itoa(i + 1)
		g.AddEntitlementID(ids[i])
	}
	for i := 0; i < n; i++ {
		next := (i + 1) % n
		if err := g.AddEdge(ctx, ids[i], ids[next], false, nil); err != nil {
			b.Fatalf("add edge: %v", err)
		}
	}
	if err := g.Validate(); err != nil {
		b.Fatalf("validate: %v", err)
	}
	return g
}

func buildChain(b *testing.B, n int) *EntitlementGraph {
	b.Helper()
	ctx := context.Background()
	g := NewEntitlementGraph(ctx)
	ids := make([]string, n)
	for i := 0; i < n; i++ {
		ids[i] = strconv.Itoa(i + 1)
		g.AddEntitlementID(ids[i])
	}
	for i := 0; i+1 < n; i++ {
		if err := g.AddEdge(ctx, ids[i], ids[i+1], false, nil); err != nil {
			b.Fatalf("add edge: %v", err)
		}
	}
	if err := g.Validate(); err != nil {
		b.Fatalf("validate: %v", err)
	}
	return g
}

func buildClique(b *testing.B, n int) *EntitlementGraph {
	b.Helper()
	ctx := context.Background()
	g := NewEntitlementGraph(ctx)
	ids := make([]string, n)
	for i := 0; i < n; i++ {
		ids[i] = strconv.Itoa(i + 1)
		g.AddEntitlementID(ids[i])
	}
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i == j {
				continue
			}
			if err := g.AddEdge(ctx, ids[i], ids[j], false, nil); err != nil {
				b.Fatalf("add edge: %v", err)
			}
		}
	}
	if err := g.Validate(); err != nil {
		b.Fatalf("validate: %v", err)
	}
	return g
}

func buildTailIntoRing(b *testing.B, tail, ring int) *EntitlementGraph {
	b.Helper()
	ctx := context.Background()
	total := tail + ring
	g := NewEntitlementGraph(ctx)
	ids := make([]string, total)
	for i := 0; i < total; i++ {
		ids[i] = strconv.Itoa(i + 1)
		g.AddEntitlementID(ids[i])
	}

	// Tail 1 -> 2 -> ... -> tail -> (tail+1)
	for i := 0; i+1 < tail+1; i++ {
		if err := g.AddEdge(ctx, ids[i], ids[i+1], false, nil); err != nil {
			b.Fatalf("add edge: %v", err)
		}
	}
	// Ring (tail+1) -> ... -> (tail+ring) -> (tail+1)
	for i := 0; i < ring; i++ {
		curr := tail + i
		next := tail + ((i + 1) % ring)
		if err := g.AddEdge(ctx, ids[curr], ids[next], false, nil); err != nil {
			b.Fatalf("add edge: %v", err)
		}
	}
	if err := g.Validate(); err != nil {
		b.Fatalf("validate: %v", err)
	}
	return g
}

func BenchmarkCycleDetectionHelper(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sizes := []int{100, 1000}

	for _, n := range sizes {
		b.Run(fmt.Sprintf("ring-%d", n), func(b *testing.B) {
			g := buildRing(b, n)
			start := g.EntitlementsToNodes["1"]
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = g.cycleDetectionHelper(ctx, start)
			}
		})
	}

	for _, n := range sizes {
		b.Run(fmt.Sprintf("chain-%d", n), func(b *testing.B) {
			g := buildChain(b, n)
			start := g.EntitlementsToNodes["1"]
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = g.cycleDetectionHelper(ctx, start)
			}
		})
	}

	b.Run("clique-5", func(b *testing.B) {
		g := buildClique(b, 5)
		start := g.EntitlementsToNodes["1"]
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = g.cycleDetectionHelper(ctx, start)
		}
	})

	b.Run("tail10-ring20", func(b *testing.B) {
		g := buildTailIntoRing(b, 10, 20)
		start := g.EntitlementsToNodes["1"]
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = g.cycleDetectionHelper(ctx, start)
		}
	})
}

func BenchmarkGetFirstCycle(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sizes := []int{100, 1000}

	for _, n := range sizes {
		b.Run(fmt.Sprintf("ring-%d", n), func(b *testing.B) {
			g := buildRing(b, n)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = g.GetFirstCycle(ctx)
			}
		})
	}

	for _, n := range sizes {
		b.Run(fmt.Sprintf("chain-%d", n), func(b *testing.B) {
			g := buildChain(b, n)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = g.GetFirstCycle(ctx)
			}
		})
	}
}
