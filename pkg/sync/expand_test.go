package sync

import (
	"context"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/stretchr/testify/require"
)

func TestGetCycles(t *testing.T) {
	graph := NewEntitlementGraph(context.Background())
	graph.AddEntitlement(&v2.Entitlement{Id: "1"})
	graph.AddEntitlement(&v2.Entitlement{Id: "2"})
	graph.AddEntitlement(&v2.Entitlement{Id: "3"})
	graph.AddEntitlement(&v2.Entitlement{Id: "4"})
	graph.AddEdge("1", "2")
	graph.AddEdge("2", "3")
	graph.AddEdge("3", "4")
	graph.AddEdge("4", "2")
	cycles, isCycle := graph.GetCycles()
	require.True(t, isCycle)
	require.Equal(t, [][]string{{"2", "3", "4"}}, cycles)
}
