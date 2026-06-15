package expand

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestWhaleGraphCycles loads the pebble graph and checks whether it
// contains cycles — the thing the production syncer collapses in
// "Phase 2" before expanding. If cycles exist and the benchmark harness
// skips fixing them, the expander loops until it trips the depth guard.
// Set BATON_PEBBLE_PATH to run.
func TestWhaleGraphCycles(t *testing.T) {
	pebblePath := os.Getenv("BATON_PEBBLE_PATH")
	if pebblePath == "" {
		t.Skip("set BATON_PEBBLE_PATH")
	}
	ctx := context.Background()

	g := loadPebbleGraph(ctx, t, pebblePath)
	t.Logf("graph: %d nodes, %d edges", len(g.Nodes), len(g.Edges))

	comps, metrics := g.ComputeCyclicComponents(ctx)
	t.Logf("cyclic components: %d  scc_metrics: %+v", len(comps), metrics)
	biggest := 0
	for _, c := range comps {
		if len(c) > biggest {
			biggest = len(c)
		}
	}
	t.Logf("largest cyclic component: %d nodes", biggest)

	require.NoError(t, g.FixCycles(ctx))
	require.True(t, g.HasNoCycles, "graph should report no cycles after FixCycles")

	after, _ := g.ComputeCyclicComponents(ctx)
	t.Logf("after FixCycles: %d nodes, %d edges, %d cyclic components",
		len(g.Nodes), len(g.Edges), len(after))
	require.Empty(t, after, "no cyclic components should remain after FixCycles")
}
