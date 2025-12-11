package expand

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestComputeTransitiveDescendants(t *testing.T) {
	ctx := context.Background()
	g := NewEntitlementGraph(ctx)

	// Build graph: A -> C, B -> C, C -> D, E -> D
	//
	//   A ──→ C ──→ D
	//   B ──↗       ↑
	//               │
	//   E ──────────┘

	// Add entitlements
	g.AddEntitlementID("A")
	g.AddEntitlementID("B")
	g.AddEntitlementID("C")
	g.AddEntitlementID("D")
	g.AddEntitlementID("E")

	// Add edges
	err := g.AddEdgeByID(ctx, "A", "C", false, nil)
	require.NoError(t, err)
	err = g.AddEdgeByID(ctx, "B", "C", false, nil)
	require.NoError(t, err)
	err = g.AddEdgeByID(ctx, "C", "D", false, nil)
	require.NoError(t, err)
	err = g.AddEdgeByID(ctx, "E", "D", false, nil)
	require.NoError(t, err)

	// Compute transitive descendants
	descendants := g.ComputeTransitiveDescendants()

	// A's descendants: C, D
	require.ElementsMatch(t, []string{"C", "D"}, descendants["A"])

	// B's descendants: C, D
	require.ElementsMatch(t, []string{"C", "D"}, descendants["B"])

	// C's descendants: D
	require.ElementsMatch(t, []string{"D"}, descendants["C"])

	// D's descendants: none
	require.Empty(t, descendants["D"])

	// E's descendants: D
	require.ElementsMatch(t, []string{"D"}, descendants["E"])
}

func TestTopologicalOrder(t *testing.T) {
	ctx := context.Background()
	g := NewEntitlementGraph(ctx)

	// Build graph: A -> B -> C -> D
	g.AddEntitlementID("A")
	g.AddEntitlementID("B")
	g.AddEntitlementID("C")
	g.AddEntitlementID("D")

	err := g.AddEdgeByID(ctx, "A", "B", false, nil)
	require.NoError(t, err)
	err = g.AddEdgeByID(ctx, "B", "C", false, nil)
	require.NoError(t, err)
	err = g.AddEdgeByID(ctx, "C", "D", false, nil)
	require.NoError(t, err)

	order := g.TopologicalOrder()

	// A must come before B, B before C, C before D
	require.Len(t, order, 4)

	indexOf := func(s string) int {
		for i, v := range order {
			if v == s {
				return i
			}
		}
		return -1
	}

	require.Less(t, indexOf("A"), indexOf("B"))
	require.Less(t, indexOf("B"), indexOf("C"))
	require.Less(t, indexOf("C"), indexOf("D"))
}

func TestGetSourceEntitlements(t *testing.T) {
	ctx := context.Background()
	g := NewEntitlementGraph(ctx)

	// Build graph: A -> C, B -> C
	g.AddEntitlementID("A")
	g.AddEntitlementID("B")
	g.AddEntitlementID("C")

	err := g.AddEdgeByID(ctx, "A", "C", false, nil)
	require.NoError(t, err)
	err = g.AddEdgeByID(ctx, "B", "C", false, nil)
	require.NoError(t, err)

	// C's sources: A, B
	sources := g.GetSourceEntitlements("C")
	require.ElementsMatch(t, []string{"A", "B"}, sources)

	// A's sources: none
	sources = g.GetSourceEntitlements("A")
	require.Empty(t, sources)
}

// Helper methods for testing
func (g *EntitlementGraph) AddEntitlementID(id string) {
	if g.GetNode(id) != nil {
		return
	}
	g.NextNodeID++
	node := Node{
		Id:             g.NextNodeID,
		EntitlementIDs: []string{id},
	}
	g.Nodes[node.Id] = node
	g.EntitlementsToNodes[id] = node.Id
}

func (g *EntitlementGraph) AddEdgeByID(ctx context.Context, srcID, dstID string, isShallow bool, resourceTypeIDs []string) error {
	srcNode := g.GetNode(srcID)
	if srcNode == nil {
		return ErrNoEntitlement
	}
	dstNode := g.GetNode(dstID)
	if dstNode == nil {
		return ErrNoEntitlement
	}

	if g.SourcesToDestinations[srcNode.Id] == nil {
		g.SourcesToDestinations[srcNode.Id] = make(map[int]int)
	}
	if g.DestinationsToSources[dstNode.Id] == nil {
		g.DestinationsToSources[dstNode.Id] = make(map[int]int)
	}

	g.NextEdgeID++
	edge := Edge{
		EdgeID:          g.NextEdgeID,
		SourceID:        srcNode.Id,
		DestinationID:   dstNode.Id,
		IsExpanded:      false,
		IsShallow:       isShallow,
		ResourceTypeIDs: resourceTypeIDs,
	}

	g.Edges[g.NextEdgeID] = edge
	g.SourcesToDestinations[srcNode.Id][dstNode.Id] = edge.EdgeID
	g.DestinationsToSources[dstNode.Id][srcNode.Id] = edge.EdgeID
	return nil
}

