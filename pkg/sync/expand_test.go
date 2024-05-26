package sync

import (
	"context"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/stretchr/testify/require"
)

func TestGetDescendants(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	graph := NewEntitlementGraph(context.Background())
	graph.AddEntitlement(&v2.Entitlement{Id: "1"})
	graph.AddEntitlement(&v2.Entitlement{Id: "2"})
	graph.AddEntitlement(&v2.Entitlement{Id: "3"})
	graph.AddEntitlement(&v2.Entitlement{Id: "4"})
	err := graph.AddEdge(ctx, "1", "2", false, nil)
	require.NoError(t, err)
	err = graph.AddEdge(ctx, "1", "3", false, nil)
	require.NoError(t, err)
	err = graph.AddEdge(ctx, "1", "4", false, nil)
	require.NoError(t, err)
	node := graph.GetNode("1")
	require.NotNil(t, node)
	err = graph.Validate()
	require.NoError(t, err)

	descendantEntitlements := graph.GetDescendantEntitlements("1")
	expectedEntitlements := map[string]*grantInfo{
		"2": {
			Expanded:        false,
			Shallow:         false,
			ResourceTypeIDs: nil,
		},
		"3": {
			Expanded:        false,
			Shallow:         false,
			ResourceTypeIDs: nil,
		},
		"4": {
			Expanded:        false,
			Shallow:         false,
			ResourceTypeIDs: nil,
		},
	}
	require.EqualValues(t, expectedEntitlements, descendantEntitlements)
}

func TestRemoveNode(t *testing.T) {
	graph := NewEntitlementGraph(context.Background())
	graph.AddEntitlement(&v2.Entitlement{Id: "1"})
	err := graph.Validate()
	require.NoError(t, err)

	node := graph.GetNode("1")
	require.NotNil(t, node)

	graph.removeNode(1)

	node = graph.GetNode("1")
	require.Nil(t, node)
}

func cyclicGraph(t *testing.T) (*EntitlementGraph, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	graph := NewEntitlementGraph(context.Background())
	graph.AddEntitlement(&v2.Entitlement{Id: "1"})
	graph.AddEntitlement(&v2.Entitlement{Id: "2"})
	graph.AddEntitlement(&v2.Entitlement{Id: "3"})
	graph.AddEntitlement(&v2.Entitlement{Id: "4"})
	err := graph.AddEdge(ctx, "1", "2", false, nil)
	require.NoError(t, err)
	err = graph.AddEdge(ctx, "2", "3", false, []string{"group"})
	require.NoError(t, err)
	err = graph.AddEdge(ctx, "3", "4", false, []string{"user"})
	require.NoError(t, err)
	err = graph.AddEdge(ctx, "4", "2", false, nil)
	require.NoError(t, err)
	err = graph.Validate()
	require.NoError(t, err)
	return graph, err
}

func TestGetCycles(t *testing.T) {
	graph, err := cyclicGraph(t)
	require.NoError(t, err)
	cycles, isCycle := graph.GetCycles()
	require.True(t, isCycle)
	require.Equal(t, [][]int{{2, 3, 4}}, cycles)
}

func TestHandleCycle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	graph, err := cyclicGraph(t)
	require.NoError(t, err)

	cycles, isCycle := graph.GetCycles()
	require.True(t, isCycle)
	require.Equal(t, [][]int{{2, 3, 4}}, cycles)

	err = graph.FixCycles()
	require.NoError(t, err)
	err = graph.Validate()
	require.NoError(t, err)
	cycles, isCycle = graph.GetCycles()
	require.False(t, isCycle)
	require.Empty(t, cycles)

	// Simplest cycle 1 -> 1
	graph = NewEntitlementGraph(context.Background())
	graph.AddEntitlement(&v2.Entitlement{Id: "1"})
	graph.AddEntitlement(&v2.Entitlement{Id: "2"})
	err = graph.AddEdge(ctx, "1", "1", false, []string{"group"})
	require.NoError(t, err)
	err = graph.Validate()
	require.NoError(t, err)

	// Simple cycle 1 -> 2, 2 -> 1
	graph = NewEntitlementGraph(context.Background())
	graph.AddEntitlement(&v2.Entitlement{Id: "1"})
	graph.AddEntitlement(&v2.Entitlement{Id: "2"})
	err = graph.AddEdge(ctx, "1", "2", false, []string{"group"})
	require.NoError(t, err)
	err = graph.AddEdge(ctx, "2", "1", false, []string{"user"})
	require.NoError(t, err)
	err = graph.Validate()
	require.NoError(t, err)

	err = graph.FixCycles()
	require.NoError(t, err)
	err = graph.Validate()
	require.NoError(t, err)

	cycles, isCycle = graph.GetCycles()
	require.False(t, isCycle)
	require.Empty(t, cycles)
}
