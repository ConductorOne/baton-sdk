package expand

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/stretchr/testify/require"
)

func BenchmarkRegression_Expansion(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping regression benchmark in short mode")
	}

	ctx := context.Background()

	tmpDir, err := os.MkdirTemp("", "bench-expand-*")
	require.NoError(b, err)
	b.Cleanup(func() { os.RemoveAll(tmpDir) })

	c1zPath := filepath.Join(tmpDir, "expand.c1z")

	// Seed and close to get a c1z file on disk.
	syncID := seedExpansionC1ZForBench(ctx, b, c1zPath, tmpDir)

	// Open once to load the graph (graph is read-only metadata).
	c1f, err := dotc1z.NewC1ZFile(ctx, c1zPath, dotc1z.WithTmpDir(tmpDir), dotc1z.WithReadOnly(true))
	require.NoError(b, err)

	graph, err := loadEntitlementGraphFromC1Z(ctx, c1f, syncID)
	require.NoError(b, err)
	require.NoError(b, c1f.Close(ctx))

	b.Logf("Graph: %d nodes, %d edges", len(graph.Nodes), len(graph.Edges))

	srcData, err := os.ReadFile(c1zPath)
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		func() {
			b.StopTimer()
			graphCopy := copyGraph(graph)

			iterPath := filepath.Join(tmpDir, fmt.Sprintf("iter-%d.c1z", i))
			require.NoError(b, os.WriteFile(iterPath, srcData, 0600))
			defer os.Remove(iterPath)

			c1fIter, err := dotc1z.NewC1ZFile(ctx, iterPath, dotc1z.WithTmpDir(tmpDir))
			require.NoError(b, err)
			defer c1fIter.Close(ctx)

			require.NoError(b, c1fIter.SetSyncID(ctx, syncID))

			expander := NewExpander(c1fIter, graphCopy)
			b.StartTimer()

			if err := expander.Run(ctx); err != nil {
				b.Fatal(err)
			}
		}()
	}
}

func seedExpansionC1ZForBench(ctx context.Context, b *testing.B, c1zPath, tmpDir string) string {
	b.Helper()

	c1f, err := dotc1z.NewC1ZFile(ctx, c1zPath, dotc1z.WithTmpDir(tmpDir))
	require.NoError(b, err)

	syncID, err := c1f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(b, err)

	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()
	require.NoError(b, c1f.PutResourceTypes(ctx, groupRT, userRT))

	type entInfo struct {
		resource    *v2.Resource
		entitlement *v2.Entitlement
	}
	chainEnts := make([][]entInfo, expansionChains)

	for c := range expansionChains {
		chainEnts[c] = make([]entInfo, expansionChainDepth)
		for d := range expansionChainDepth {
			resID := fmt.Sprintf("group-c%d-d%d", c, d)
			res := v2.Resource_builder{
				Id:          v2.ResourceId_builder{ResourceType: "group", Resource: resID}.Build(),
				DisplayName: resID,
			}.Build()
			require.NoError(b, c1f.PutResources(ctx, res))

			entID := fmt.Sprintf("ent:c%d:d%d:member", c, d)
			ent := v2.Entitlement_builder{Id: entID, Resource: res}.Build()
			require.NoError(b, c1f.PutEntitlements(ctx, ent))

			chainEnts[c][d] = entInfo{resource: res, entitlement: ent}
		}
	}

	const grantBatch = 1000
	grantBuf := make([]*v2.Grant, 0, grantBatch)
	flush := func() {
		if len(grantBuf) == 0 {
			return
		}
		require.NoError(b, c1f.PutGrants(ctx, grantBuf...))
		grantBuf = grantBuf[:0]
	}

	for u := range expansionUsers {
		userRes := v2.Resource_builder{
			Id:          v2.ResourceId_builder{ResourceType: "user", Resource: fmt.Sprintf("user-%d", u)}.Build(),
			DisplayName: fmt.Sprintf("User %d", u),
		}.Build()
		require.NoError(b, c1f.PutResources(ctx, userRes))

		for c := range expansionChains {
			rootEnt := chainEnts[c][0].entitlement
			grantBuf = append(grantBuf, v2.Grant_builder{
				Id:          fmt.Sprintf("grant:u%d:c%d", u, c),
				Entitlement: rootEnt,
				Principal:   userRes,
			}.Build())
			if len(grantBuf) >= grantBatch {
				flush()
			}
		}
	}
	flush()

	for c := range expansionChains {
		seedPrincipal := v2.Resource_builder{
			Id:          v2.ResourceId_builder{ResourceType: "user", Resource: fmt.Sprintf("seed-user-%d", c)}.Build(),
			DisplayName: fmt.Sprintf("Seed User %d", c),
		}.Build()
		require.NoError(b, c1f.PutResources(ctx, seedPrincipal))

		for d := 1; d < expansionChainDepth; d++ {
			srcEnt := chainEnts[c][d-1].entitlement.GetId()
			targetEnt := chainEnts[c][d].entitlement

			edgeGrant := v2.Grant_builder{
				Id:          fmt.Sprintf("edgegrant:c%d:d%d", c, d),
				Entitlement: targetEnt,
				Principal:   seedPrincipal,
				Annotations: annotations.New(v2.GrantExpandable_builder{
					EntitlementIds:  []string{srcEnt},
					Shallow:         false,
					ResourceTypeIds: []string{"user"},
				}.Build()),
			}.Build()
			require.NoError(b, c1f.PutGrants(ctx, edgeGrant))
		}
	}

	require.NoError(b, c1f.EndSync(ctx))
	require.NoError(b, c1f.Close(ctx))

	return syncID
}
