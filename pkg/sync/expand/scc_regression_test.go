package expand

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	sccNodes    = 20_000
	sccEdges    = 200_000
	sccCycleLen = 16
	sccCycles   = 10
	sccMaxTime  = 2 * time.Minute
)

// TestRegression_SCCFixCyclesPerformance builds a large entitlement graph with
// embedded cycles and measures the time to detect and fix all cycles via
// FixCycles (SCC decomposition + node merging).
//
// The graph has sccNodes nodes, ~sccEdges edges, and sccCycles embedded rings
// of length sccCycleLen. The rest of the edges are random.
func TestRegression_SCCFixCyclesPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping regression test in short mode")
	}

	ctx := t.Context()

	edges := generateEdges(sccNodes, sccEdges, sccCycleLen, sccCycles, 42)
	g := buildGraphFromEdges(sccNodes, edges)

	// Populate EntitlementsToNodes so FixCycles can work with entitlement IDs.
	for id, node := range g.Nodes {
		entID := fmt.Sprintf("ent:%d", id)
		node.EntitlementIDs = []string{entID}
		g.Nodes[id] = node
		g.EntitlementsToNodes[entID] = id
	}

	t.Logf("Graph: %d nodes, %d edges, %d embedded cycles of length %d",
		len(g.Nodes), len(g.Edges), sccCycles, sccCycleLen)

	// Verify cycles exist before we fix them.
	comps, _ := g.ComputeCyclicComponents(ctx)
	require.NotEmpty(t, comps, "expected at least one cyclic component")
	t.Logf("Cyclic components found: %d", len(comps))

	start := time.Now()
	require.NoError(t, g.FixCycles(ctx))
	elapsed := time.Since(start)

	t.Logf("FixCycles completed in %v", elapsed)
	require.LessOrEqual(t, elapsed, sccMaxTime,
		"FixCycles took %v, limit is %v", elapsed, sccMaxTime)

	// Correctness: graph should be acyclic after fixing.
	require.True(t, g.HasNoCycles, "HasNoCycles flag should be set after FixCycles")
	postComps, _ := g.ComputeCyclicComponents(context.Background())
	require.Empty(t, postComps, "expected no cyclic components after FixCycles")
}
