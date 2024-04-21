package sync

import (
	"context"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/stretchr/testify/require"
)

func TestGetDescendants(t *testing.T) {
	graph := NewEntitlementGraph(context.Background())
	graph.AddEntitlement(&v2.Entitlement{Id: "1"})
	graph.AddEntitlement(&v2.Entitlement{Id: "2"})
	graph.AddEntitlement(&v2.Entitlement{Id: "3"})
	graph.AddEntitlement(&v2.Entitlement{Id: "4"})
	err := graph.AddEdge("1", "2", false, nil)
	require.NoError(t, err)
	err = graph.AddEdge("1", "3", false, nil)
	require.NoError(t, err)
	err = graph.AddEdge("1", "4", false, nil)
	require.NoError(t, err)
	node := graph.GetNode("1")
	require.NotNil(t, node)
	err = graph.Validate()
	require.NoError(t, err)

	descendants := graph.GetDescendants(node.Id)
	expected := []Node{
		{
			Id:           2,
			Entitlements: []Entitlement{{Id: "2"}},
		},
		{
			Id:           3,
			Entitlements: []Entitlement{{Id: "3"}},
		},
		{
			Id:           4,
			Entitlements: []Entitlement{{Id: "4"}},
		},
	}
	require.ElementsMatch(t, expected, descendants)
}

func cyclicGraph(t *testing.T) (*EntitlementGraph, error) {
	graph := NewEntitlementGraph(context.Background())
	graph.AddEntitlement(&v2.Entitlement{Id: "1"})
	graph.AddEntitlement(&v2.Entitlement{Id: "2"})
	graph.AddEntitlement(&v2.Entitlement{Id: "3"})
	graph.AddEntitlement(&v2.Entitlement{Id: "4"})
	err := graph.AddEdge("1", "2", false, nil)
	require.NoError(t, err)
	err = graph.AddEdge("2", "3", false, nil)
	require.NoError(t, err)
	err = graph.AddEdge("3", "4", false, nil)
	require.NoError(t, err)
	err = graph.AddEdge("4", "2", false, nil)
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
	graph, err := cyclicGraph(t)
	require.NoError(t, err)
	graph.FixCycles()
	cycles, isCycle := graph.GetCycles()
	require.False(t, isCycle)
	require.Empty(t, cycles)
}
