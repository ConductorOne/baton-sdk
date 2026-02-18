package expand

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"testing"

	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/stretchr/testify/require"
)

// ~50s.
func BenchmarkExpandSmall(b *testing.B) {
	benchmarkExpand(b, "36zGvJw3uxU1QMJKU2yPVQ1hBOC")
}

// ~70s.
func BenchmarkExpandSmallMedium(b *testing.B) {
	benchmarkExpand(b, "36zM46KKuaBq0wjSSvKh5o0350y")
}

func getTestdataPath(syncID string) string {
	_, filename, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(filename), "testdata", fmt.Sprintf("sync.%s.unexpanded", syncID))
}

// copyGraph creates a deep copy of an EntitlementGraph.
func copyGraph(g *EntitlementGraph) *EntitlementGraph {
	newGraph := &EntitlementGraph{
		NextNodeID:            g.NextNodeID,
		NextEdgeID:            g.NextEdgeID,
		Nodes:                 make(map[int]Node, len(g.Nodes)),
		EntitlementsToNodes:   make(map[string]int, len(g.EntitlementsToNodes)),
		SourcesToDestinations: make(map[int]map[int]int, len(g.SourcesToDestinations)),
		DestinationsToSources: make(map[int]map[int]int, len(g.DestinationsToSources)),
		Edges:                 make(map[int]Edge, len(g.Edges)),
		Loaded:                g.Loaded,
		Depth:                 g.Depth,
		Actions:               make([]*EntitlementGraphAction, len(g.Actions)),
		HasNoCycles:           g.HasNoCycles,
	}

	for k, v := range g.Nodes {
		newGraph.Nodes[k] = Node{Id: v.Id, EntitlementIDs: slices.Clone(v.EntitlementIDs)}
	}

	maps.Copy(newGraph.EntitlementsToNodes, g.EntitlementsToNodes)

	for k, v := range g.SourcesToDestinations {
		newGraph.SourcesToDestinations[k] = maps.Clone(v)
	}

	for k, v := range g.DestinationsToSources {
		newGraph.DestinationsToSources[k] = maps.Clone(v)
	}

	for k, v := range g.Edges {
		newGraph.Edges[k] = Edge{
			EdgeID:          v.EdgeID,
			SourceID:        v.SourceID,
			DestinationID:   v.DestinationID,
			IsExpanded:      v.IsExpanded,
			IsShallow:       v.IsShallow,
			ResourceTypeIDs: slices.Clone(v.ResourceTypeIDs),
		}
	}

	for i, action := range g.Actions {
		newGraph.Actions[i] = &EntitlementGraphAction{
			SourceEntitlementID:     action.SourceEntitlementID,
			DescendantEntitlementID: action.DescendantEntitlementID,
			Shallow:                 action.Shallow,
			ResourceTypeIDs:         slices.Clone(action.ResourceTypeIDs),
			PageToken:               action.PageToken,
		}
	}

	return newGraph
}

// loadEntitlementGraphFromC1Z builds the entitlement graph by reading expansion
// metadata directly from ListGrantsInternal rows.
func loadEntitlementGraphFromC1Z(ctx context.Context, c1f *dotc1z.C1File, syncID string) (*EntitlementGraph, error) {
	graph := NewEntitlementGraph(ctx)

	pageToken := ""
	for {
		resp, err := c1f.ListGrantsInternal(ctx, connectorstore.GrantListOptions{
			Mode:      connectorstore.GrantListModeExpansion,
			SyncID:    syncID,
			PageToken: pageToken,
		})
		if errors.Is(err, sql.ErrNoRows) {
			return graph, nil
		}

		if err != nil {
			return nil, err
		}

		for _, row := range resp.Rows {
			def := row.Expansion
			if def == nil {
				continue
			}
			for _, srcEntitlementID := range def.SourceEntitlementIDs {
				srcEntitlement, err := c1f.GetEntitlement(ctx, reader_v2.EntitlementsReaderServiceGetEntitlementRequest_builder{
					EntitlementId: srcEntitlementID,
				}.Build())
				if err != nil {
					continue // Skip if source entitlement not found
				}

				graph.AddEntitlementID(def.TargetEntitlementID)
				graph.AddEntitlementID(srcEntitlement.GetEntitlement().GetId())
				_ = graph.AddEdge(ctx,
					srcEntitlement.GetEntitlement().GetId(),
					def.TargetEntitlementID,
					def.Shallow,
					def.ResourceTypeIDs,
				)
			}
		}

		pageToken = resp.NextPageToken
		if pageToken == "" {
			break
		}
	}

	graph.Loaded = true
	return graph, nil
}

func benchmarkExpand(b *testing.B, syncID string) {
	c1zPath := getTestdataPath(syncID)
	if _, err := os.Stat(c1zPath); os.IsNotExist(err) {
		b.Skipf("testdata file not found: %s", c1zPath)
	}

	ctx := b.Context()

	// Open the c1z file once to get stats
	c1f, err := dotc1z.NewC1ZFile(ctx, c1zPath)
	require.NoError(b, err)
	defer c1f.Close(ctx)

	// Load the graph
	graph, err := loadEntitlementGraphFromC1Z(ctx, c1f, syncID)
	require.NoError(b, err)

	b.Logf("Graph loaded: %d nodes, %d edges", len(graph.Nodes), len(graph.Edges))

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Func is for defers to be human undertandable.
		func(i int) {
			// Make a copy of the graph for each iteration
			graphCopy := copyGraph(graph)

			// Create a fresh c1z for each iteration (copy original)
			tmpFile, err := os.CreateTemp("", "bench-expand-*.c1z")
			require.NoError(b, err)
			tmpPath := tmpFile.Name()
			defer os.Remove(tmpPath)
			err = tmpFile.Close()
			require.NoError(b, err)

			// Copy original c1z to temp
			srcData, err := os.ReadFile(c1zPath)
			require.NoError(b, err)
			//nolint:gosec // tmpPath is generated by os.CreateTemp in this test.
			err = os.WriteFile(tmpPath, srcData, 0600)
			require.NoError(b, err)

			c1fCopy, err := dotc1z.NewC1ZFile(ctx, tmpPath)
			require.NoError(b, err)
			defer c1fCopy.Close(ctx)

			err = c1fCopy.SetSyncID(ctx, syncID)
			require.NoError(b, err)

			expander := NewExpander(c1fCopy, graphCopy)

			// ---------------------------------------

			b.StartTimer()
			err = expander.Run(ctx)
			b.StopTimer()

			// ---------------------------------------
			require.NoError(b, err)
		}(i)
	}
}
