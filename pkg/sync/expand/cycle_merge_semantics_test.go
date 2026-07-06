package expand

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// These tests pin the widest-grant union semantics of edge merging in two
// places that previously got them wrong and that no differential test can
// catch (both evaluators consume the same collapsed graph, so they agree on
// a wrong answer):
//
//   - fixCycle folding one neighbor's parallel edges into/out of a collapsed
//     cycle node, and
//   - AddEdge folding duplicate (src, dst) edge specs from the datasource.
//
// The rules: an empty resource-type filter means match-all, so any
// unfiltered edge makes the merged edge unfiltered (set-union across it
// would NARROW it); a deep edge admits strictly more than a shallow one, so
// any deep edge makes the merged edge deep, and only an all-shallow fold
// stays shallow.

// edgeBetweenEnts returns the single edge between the nodes owning the two
// entitlement IDs, failing the test if it doesn't exist.
func edgeBetweenEnts(t *testing.T, g *EntitlementGraph, srcEnt, dstEnt string) Edge {
	t.Helper()
	srcNode, ok := g.EntitlementsToNodes[srcEnt]
	require.True(t, ok, "no node for src entitlement %q", srcEnt)
	dstNode, ok := g.EntitlementsToNodes[dstEnt]
	require.True(t, ok, "no node for dst entitlement %q", dstEnt)
	edgeID, ok := g.SourcesToDestinations[srcNode][dstNode]
	require.True(t, ok, "no edge %q -> %q", srcEnt, dstEnt)
	edge, ok := g.Edges[edgeID]
	require.True(t, ok, "edge id %d in adjacency but not in Edges", edgeID)
	return edge
}

func newTestGraphWithEnts(ents ...string) *EntitlementGraph {
	g := NewEntitlementGraph(context.Background())
	for _, e := range ents {
		g.AddEntitlementID(e)
	}
	return g
}

func TestFixCycleUnfilteredIncomingEdgeWins(t *testing.T) {
	ctx := context.Background()
	// A feeds cycle members B and C over parallel edges: one unfiltered,
	// one filtered to ["user"]. The collapsed {B,C} node must keep the
	// unfiltered path — a set-union would narrow it to ["user"] and
	// silently stop propagating group-principal grants from A.
	g := newTestGraphWithEnts("A", "B", "C")
	require.NoError(t, g.AddEdge(ctx, "A", "B", false, nil))
	require.NoError(t, g.AddEdge(ctx, "A", "C", false, []string{"user"}))
	require.NoError(t, g.AddEdge(ctx, "B", "C", false, nil))
	require.NoError(t, g.AddEdge(ctx, "C", "B", false, nil))
	require.NoError(t, g.FixCycles(ctx))
	require.NoError(t, g.Validate())

	merged := edgeBetweenEnts(t, g, "A", "B")
	require.Empty(t, merged.ResourceTypeIDs, "merged incoming edge must stay unfiltered (any-unfiltered-wins)")
	require.False(t, merged.IsShallow, "deep+deep fold must stay deep")
	// B and C now share a node.
	require.Equal(t, g.EntitlementsToNodes["B"], g.EntitlementsToNodes["C"], "cycle members not collapsed")
}

func TestFixCycleUnfilteredOutgoingEdgeWins(t *testing.T) {
	ctx := context.Background()
	// Mirror case on the outgoing side: cycle members B and C both feed D,
	// one edge filtered, one unfiltered.
	g := newTestGraphWithEnts("B", "C", "D")
	require.NoError(t, g.AddEdge(ctx, "B", "C", false, nil))
	require.NoError(t, g.AddEdge(ctx, "C", "B", false, nil))
	require.NoError(t, g.AddEdge(ctx, "B", "D", false, []string{"group"}))
	require.NoError(t, g.AddEdge(ctx, "C", "D", false, nil))
	require.NoError(t, g.FixCycles(ctx))
	require.NoError(t, g.Validate())

	merged := edgeBetweenEnts(t, g, "B", "D")
	require.Empty(t, merged.ResourceTypeIDs, "merged outgoing edge must stay unfiltered (any-unfiltered-wins)")
}

func TestFixCycleFilteredEdgesUnionFilters(t *testing.T) {
	ctx := context.Background()
	// When EVERY parallel edge is filtered, the union of the sets is the
	// faithful merge.
	g := newTestGraphWithEnts("A", "B", "C")
	require.NoError(t, g.AddEdge(ctx, "A", "B", false, []string{"user"}))
	require.NoError(t, g.AddEdge(ctx, "A", "C", false, []string{"group"}))
	require.NoError(t, g.AddEdge(ctx, "B", "C", false, nil))
	require.NoError(t, g.AddEdge(ctx, "C", "B", false, nil))
	require.NoError(t, g.FixCycles(ctx))

	merged := edgeBetweenEnts(t, g, "A", "B")
	require.ElementsMatch(t, []string{"user", "group"}, merged.ResourceTypeIDs, "all-filtered fold unions the filters")
}

func TestFixCycleAllShallowStaysShallow(t *testing.T) {
	ctx := context.Background()
	// Every edge from A into the cycle is shallow: the rebuilt edge must
	// stay shallow. Hard-coding deep would let A's indirect grants flow
	// into the collapsed node — spurious expanded grants.
	g := newTestGraphWithEnts("A", "B", "C")
	require.NoError(t, g.AddEdge(ctx, "A", "B", true, nil))
	require.NoError(t, g.AddEdge(ctx, "A", "C", true, nil))
	require.NoError(t, g.AddEdge(ctx, "B", "C", false, nil))
	require.NoError(t, g.AddEdge(ctx, "C", "B", false, nil))
	require.NoError(t, g.FixCycles(ctx))

	merged := edgeBetweenEnts(t, g, "A", "B")
	require.True(t, merged.IsShallow, "all-shallow fold must stay shallow")
}

func TestFixCycleMixedShallowDeepIsDeep(t *testing.T) {
	ctx := context.Background()
	g := newTestGraphWithEnts("A", "B", "C")
	require.NoError(t, g.AddEdge(ctx, "A", "B", true, nil))
	require.NoError(t, g.AddEdge(ctx, "A", "C", false, nil))
	require.NoError(t, g.AddEdge(ctx, "B", "C", false, nil))
	require.NoError(t, g.AddEdge(ctx, "C", "B", false, nil))
	require.NoError(t, g.FixCycles(ctx))

	merged := edgeBetweenEnts(t, g, "A", "B")
	require.False(t, merged.IsShallow, "shallow+deep fold must widen to deep")
}

// TestFixCycleSelfLoop pins the single-node cyclic component: a self-loop
// collapses into a fresh node that keeps the neighbor edges' merged specs,
// and the self-edge itself is discarded (intra-cycle edges never survive a
// collapse).
func TestFixCycleSelfLoop(t *testing.T) {
	ctx := context.Background()
	g := newTestGraphWithEnts("A", "B", "C")
	require.NoError(t, g.AddEdge(ctx, "A", "B", true, []string{"user"}))
	require.NoError(t, g.AddEdge(ctx, "B", "B", false, nil)) // self-loop
	require.NoError(t, g.AddEdge(ctx, "B", "C", true, nil))
	require.NoError(t, g.FixCycles(ctx))
	require.NoError(t, g.Validate())

	in := edgeBetweenEnts(t, g, "A", "B")
	require.True(t, in.IsShallow, "sole incoming edge was shallow; the collapse must not deepen it")
	require.Equal(t, []string{"user"}, in.ResourceTypeIDs, "sole incoming edge's filter must survive the collapse")
	out := edgeBetweenEnts(t, g, "B", "C")
	require.True(t, out.IsShallow, "sole outgoing edge was shallow; the collapse must not deepen it")
	require.Empty(t, out.ResourceTypeIDs, "sole outgoing edge was unfiltered; the collapse must not narrow it")

	// The collapsed node has no self-edge left.
	bNode := g.EntitlementsToNodes["B"]
	_, hasSelf := g.SourcesToDestinations[bNode][bNode]
	require.False(t, hasSelf, "self-loop must be discarded by the collapse")
	require.False(t, g.HasCycles(ctx), "graph must be acyclic after FixCycles")
}

// TestFixCycleAdjacencyMapHygiene pins the internal-map invariant that
// removeNode previously violated (it deleted SourcesToDestinations twice and
// never deleted DestinationsToSources): after a collapse, no removed node
// may linger in either adjacency map, and every edge ID the maps reference
// must exist in Edges. The stale entries had no behavioral symptom — every
// consumer happened to guard against them — but they were serialized into
// every checkpoint and were a landmine for any future map-wide iteration.
func TestFixCycleAdjacencyMapHygiene(t *testing.T) {
	ctx := context.Background()
	g := newTestGraphWithEnts("A", "B", "C", "D")
	require.NoError(t, g.AddEdge(ctx, "A", "B", false, nil))
	require.NoError(t, g.AddEdge(ctx, "B", "C", false, nil))
	require.NoError(t, g.AddEdge(ctx, "C", "B", false, nil))
	require.NoError(t, g.AddEdge(ctx, "C", "D", false, nil))
	require.NoError(t, g.FixCycles(ctx))
	require.NoError(t, g.Validate())

	for nodeID := range g.SourcesToDestinations {
		_, ok := g.Nodes[nodeID]
		require.True(t, ok, "SourcesToDestinations references removed node %d", nodeID)
	}
	for nodeID, sources := range g.DestinationsToSources {
		_, ok := g.Nodes[nodeID]
		require.True(t, ok, "DestinationsToSources references removed node %d", nodeID)
		for srcID, edgeID := range sources {
			_, ok := g.Nodes[srcID]
			require.True(t, ok, "DestinationsToSources[%d] references removed source node %d", nodeID, srcID)
			_, ok = g.Edges[edgeID]
			require.True(t, ok, "DestinationsToSources[%d][%d] references deleted edge %d", nodeID, srcID, edgeID)
		}
	}
	for nodeID, dests := range g.SourcesToDestinations {
		for dstID, edgeID := range dests {
			_, ok := g.Nodes[dstID]
			require.True(t, ok, "SourcesToDestinations[%d] references removed dest node %d", nodeID, dstID)
			_, ok = g.Edges[edgeID]
			require.True(t, ok, "SourcesToDestinations[%d][%d] references deleted edge %d", nodeID, dstID, edgeID)
		}
	}
}

// TestAddEdgeDuplicateMergesDeterministically pins the duplicate-edge fold:
// conflicting specs for the same (src, dst) pair must merge to the same
// widest-grant result regardless of arrival order. First-wins would let
// SQLite and Pebble — which page pending expansions in different orders —
// build different graphs from identical connector data.
func TestAddEdgeDuplicateMergesDeterministically(t *testing.T) {
	ctx := context.Background()
	build := func(order [][2]interface{}) Edge {
		g := newTestGraphWithEnts("A", "B")
		for _, spec := range order {
			shallow := spec[0].(bool)
			rtids, _ := spec[1].([]string)
			require.NoError(t, g.AddEdge(ctx, "A", "B", shallow, rtids))
		}
		return edgeBetweenEnts(t, g, "A", "B")
	}

	specA := [2]interface{}{true, []string{"user"}} // shallow, filtered
	specB := [2]interface{}{false, nil}             // deep, unfiltered

	forward := build([][2]interface{}{specA, specB})
	reverse := build([][2]interface{}{specB, specA})

	for name, got := range map[string]Edge{"forward": forward, "reverse": reverse} {
		require.False(t, got.IsShallow, "%s: deep edge in the fold must win", name)
		require.Empty(t, got.ResourceTypeIDs, "%s: unfiltered edge in the fold must win", name)
	}

	// Two filtered specs union their sets, order-independently.
	f1 := [2]interface{}{false, []string{"user"}}
	f2 := [2]interface{}{false, []string{"group"}}
	e1 := build([][2]interface{}{f1, f2})
	e2 := build([][2]interface{}{f2, f1})
	require.ElementsMatch(t, []string{"user", "group"}, e1.ResourceTypeIDs, "filtered fold unions")
	require.ElementsMatch(t, e1.ResourceTypeIDs, e2.ResourceTypeIDs, "filtered fold is order-independent")
}
