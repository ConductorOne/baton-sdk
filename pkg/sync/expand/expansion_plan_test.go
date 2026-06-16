package expand

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildExpansionPlan_ChainProjectsNothing(t *testing.T) {
	ctx := context.Background()
	graph := parseExpression(t, ctx, "1>2>3>4")

	plan, err := graph.BuildExpansionPlan(ctx)
	require.NoError(t, err)
	require.Same(t, plan, graph.ExpansionPlan)
	require.Len(t, plan.Order, len(graph.Nodes))

	// Every node is single-parent, so nothing is worth projecting.
	require.Empty(t, plan.ProjectionSources)
}

func TestBuildExpansionPlan_FanInProjectsParents(t *testing.T) {
	ctx := context.Background()
	graph := parseExpression(t, ctx, "1>4 2>4 3>4")

	plan, err := graph.BuildExpansionPlan(ctx)
	require.NoError(t, err)

	// Node 4 has in-degree 3, so its three parents are projected.
	require.ElementsMatch(t, []string{"1", "2", "3"}, plan.ProjectionSources)
}

func TestBuildExpansionPlan_FanOutProjectsNothing(t *testing.T) {
	ctx := context.Background()
	graph := parseExpression(t, ctx, "1>2 1>3 1>4 1>5")

	plan, err := graph.BuildExpansionPlan(ctx)
	require.NoError(t, err)

	// Pure fan-out: every destination is single-parent, so nothing projects.
	// (This is the shape where the destination-driven evaluator re-reads the
	// source per destination; tracked via the fan-out telemetry, not the plan.)
	require.Empty(t, plan.ProjectionSources)
}

func TestBuildExpansionPlan_HighFanOutSourceIsProjected(t *testing.T) {
	ctx := context.Background()
	// Node 1 broadcasts to projectionFanOutThreshold single-parent destinations.
	// None of those destinations is multi-parent, so the fan-in rule alone would
	// leave node 1 unprojected and the merge would re-read it once per
	// destination. The fan-out rule must project it.
	expr := ""
	for i := 0; i < projectionFanOutThreshold; i++ {
		if i > 0 {
			expr += " "
		}
		expr += "1>" + strconv.Itoa(i+2)
	}
	graph := parseExpression(t, ctx, expr)

	plan, err := graph.BuildExpansionPlan(ctx)
	require.NoError(t, err)
	require.Contains(t, plan.ProjectionSources, "1", "high-fan-out source should be projected")

	// One fewer destination keeps it below the threshold → not projected.
	expr2 := ""
	for i := 0; i < projectionFanOutThreshold-1; i++ {
		if i > 0 {
			expr2 += " "
		}
		expr2 += "10>" + strconv.Itoa(i+20)
	}
	graph2 := parseExpression(t, ctx, expr2)
	plan2, err := graph2.BuildExpansionPlan(ctx)
	require.NoError(t, err)
	require.Empty(t, plan2.ProjectionSources, "below-threshold fan-out should not project")
}

func TestBuildExpansionPlan_MultiMemberDestinationProjectsParents(t *testing.T) {
	ctx := context.Background()
	graph := parseExpression(t, ctx, "1>3 2>3")
	node := graph.GetNode("3")
	require.NotNil(t, node)
	// Simulate a collapsed destination node with two member entitlements. This
	// makes one parent source stream feed more than one destination entitlement,
	// which is where the projection index amortizes.
	node.EntitlementIDs = append(node.EntitlementIDs, "3b")
	graph.Nodes[node.Id] = *node
	graph.EntitlementsToNodes["3b"] = node.Id

	plan, err := graph.BuildExpansionPlan(ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"1", "2"}, plan.ProjectionSources)
}
