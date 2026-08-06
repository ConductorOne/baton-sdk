package expand

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestIncrementalExhaustiveFourNodeGraphs gives bounded closure for the
// additions-only contract over every simple directed graph with 0..4 nodes,
// every possible edge set, and every seed node. Each non-empty cell adds one
// new direct member, then compares incremental output with both a fresh full
// expansion and the graph-library-free fixed-point oracle.
func TestIncrementalExhaustiveFourNodeGraphs(t *testing.T) {
	const wantCells = 16_586
	ctx := context.Background()
	cells := 0
	for nodeCount := 0; nodeCount <= 4; nodeCount++ {
		entitlements := make([]string, nodeCount)
		var possibleEdges []sqliteEdgeSpec
		for i := 0; i < nodeCount; i++ {
			entitlements[i] = fmt.Sprintf("e%d", i)
			for j := 0; j < nodeCount; j++ {
				if i != j {
					possibleEdges = append(possibleEdges, sqliteEdgeSpec{
						src: fmt.Sprintf("e%d", i),
						dst: fmt.Sprintf("e%d", j),
					})
				}
			}
		}
		edgeSets := 1 << len(possibleEdges)
		if nodeCount == 0 {
			cells++ // the single empty graph
			continue
		}
		for edgeMask := 0; edgeMask < edgeSets; edgeMask++ {
			edges := make([]sqliteEdgeSpec, 0, len(possibleEdges))
			for edgeIndex, edge := range possibleEdges {
				if edgeMask&(1<<edgeIndex) != 0 {
					edges = append(edges, edge)
				}
			}
			for seedIndex, seed := range entitlements {
				cells++
				base := sqliteParityCase{
					entitlementIDs: append([]string(nil), entitlements...),
					edges:          append([]sqliteEdgeSpec(nil), edges...),
					grants: []sqliteGrantSpec{{
						id: "alice-" + seed, entitlementID: seed,
						principalRT: "user", principalID: "alice",
					}},
				}
				incStore, incGraph := mockStoreFromCase(t, ctx, base)
				require.NoErrorf(t, NewExpander(incStore, incGraph).Run(ctx),
					"n=%d mask=%d seed=%d base expansion", nodeCount, edgeMask, seedIndex)
				incGraph.Loaded = true
				if incGraph.HasCollapsedCycles() {
					// The public compactor rejects every collapsed-SCC base before
					// calling IncrementalExpander. Full expansion's internal ordering
					// inside a collapsed SCC is not the incremental feature's oracle.
					// The directed public-path decline is pinned separately.
					continue
				}
				bob := directGrant(seed, makeResource("user", "bob"))
				incStore.AddGrant(bob)
				current := base
				current.grants = append(current.grants, sqliteGrantSpec{
					id: bob.GetId(), entitlementID: seed,
					principalRT: "user", principalID: "bob",
				})

				_, err := NewIncrementalExpander(incStore, incGraph).
					ExpandChanges(ctx, nil, []string{seed})
				require.NoErrorf(t, err, "n=%d mask=%d seed=%d incremental", nodeCount, edgeMask, seedIndex)

				fullStore, fullGraph := mockStoreFromCase(t, ctx, current)
				require.NoErrorf(t, NewExpander(fullStore, fullGraph).Run(ctx),
					"n=%d mask=%d seed=%d full oracle", nodeCount, edgeMask, seedIndex)
				require.Equalf(t, snapshotStoreGrants(fullStore), snapshotStoreGrants(incStore),
					"full mismatch n=%d mask=%d seed=%d", nodeCount, edgeMask, seedIndex)
				require.Equalf(t, independentAccessOracle(current), snapshotAccessPairs(incStore),
					"model mismatch n=%d mask=%d seed=%d", nodeCount, edgeMask, seedIndex)
			}
		}
	}
	require.Equal(t, wantCells, cells, "cell count is the bounded-space coverage guard")
}
