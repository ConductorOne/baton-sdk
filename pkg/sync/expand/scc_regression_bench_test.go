package expand

import (
	"context"
	"fmt"
	"testing"
)

func BenchmarkRegression_SCCFixCycles(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping regression benchmark in short mode")
	}

	edges := generateEdges(sccNodes, sccEdges, sccCycleLen, sccCycles, 42)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		g := buildGraphFromEdges(sccNodes, edges)
		for id, node := range g.Nodes {
			entID := fmt.Sprintf("ent:%d", id)
			node.EntitlementIDs = []string{entID}
			g.Nodes[id] = node
			g.EntitlementsToNodes[entID] = id
		}
		if err := g.FixCycles(ctx); err != nil {
			b.Fatal(err)
		}
	}
}
