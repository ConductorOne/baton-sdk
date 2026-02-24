package expand

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/stretchr/testify/require"
)

const (
	expansionChains     = 10
	expansionChainDepth = 8
	expansionUsers      = 3000
	expansionMaxTime    = 3 * time.Minute
)

// TestRegression_GrantExpansionPerformance builds a synthetic entitlement graph
// with multiple chains and many users, then runs the expander end-to-end.
// The expansion must complete within expansionMaxTime.
func TestRegression_GrantExpansionPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping regression test in short mode")
	}

	ctx := t.Context()

	tmpDir, err := os.MkdirTemp("", "regression-expand-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	c1zPath := filepath.Join(tmpDir, "expand.c1z")

	c1f, syncID := seedExpansionC1Z(ctx, t, c1zPath, tmpDir)
	defer c1f.Close(ctx)

	err = c1f.SetSyncID(ctx, syncID)
	require.NoError(t, err)

	graph, err := loadEntitlementGraphFromC1Z(ctx, c1f, syncID)
	require.NoError(t, err)

	t.Logf("Graph: %d nodes, %d edges; users=%d chains=%d depth=%d",
		len(graph.Nodes), len(graph.Edges),
		expansionUsers, expansionChains, expansionChainDepth)

	expander := NewExpander(c1f, graph)

	start := time.Now()
	err = expander.Run(ctx)
	elapsed := time.Since(start)
	require.NoError(t, err)

	t.Logf("Expansion completed in %v", elapsed)
	require.LessOrEqual(t, elapsed, expansionMaxTime,
		"expansion took %v, limit is %v", elapsed, expansionMaxTime)

	// Correctness guard: expansion should produce grants for every chain/depth/user.
	// Plus one "edge-definition" grant per non-root depth in each chain.
	expectedTotal := (expansionUsers * expansionChains * expansionChainDepth) +
		(expansionChains * (expansionChainDepth - 1))
	total, err := countAllGrants(ctx, c1f)
	require.NoError(t, err)
	require.Equal(t, expectedTotal, total, "unexpected total grant count after expansion")

	// Correctness guard: verify directness semantics for a sample principal on one chain.
	checkExpansionSampleSemantics(ctx, t, c1f)
}

// seedExpansionC1Z creates a c1z with a synthetic dataset designed to exercise
// grant expansion. It builds `expansionChains` independent entitlement chains,
// each `expansionChainDepth` levels deep, and gives each of `expansionUsers`
// users a grant on every chain's root entitlement.
//
// Returns the open C1File and the sync ID.
func seedExpansionC1Z(
	ctx context.Context,
	t *testing.T,
	c1zPath, tmpDir string,
) (*dotc1z.C1File, string) {
	t.Helper()

	c1f, err := dotc1z.NewC1ZFile(ctx, c1zPath, dotc1z.WithTmpDir(tmpDir))
	require.NoError(t, err)

	syncID, err := c1f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()
	require.NoError(t, c1f.PutResourceTypes(ctx, groupRT, userRT))

	// Create one group resource per entitlement (one per chain level).
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
			require.NoError(t, c1f.PutResources(ctx, res))

			entID := fmt.Sprintf("ent:c%d:d%d:member", c, d)
			ent := v2.Entitlement_builder{Id: entID, Resource: res}.Build()
			require.NoError(t, c1f.PutEntitlements(ctx, ent))

			chainEnts[c][d] = entInfo{resource: res, entitlement: ent}
		}
	}

	// Create user resources and grant each user membership in every chain's
	// root entitlement.
	const grantBatch = 1000
	grantBuf := make([]*v2.Grant, 0, grantBatch)

	flush := func() {
		if len(grantBuf) == 0 {
			return
		}
		require.NoError(t, c1f.PutGrants(ctx, grantBuf...))
		grantBuf = grantBuf[:0]
	}

	for u := range expansionUsers {
		userRes := v2.Resource_builder{
			Id:          v2.ResourceId_builder{ResourceType: "user", Resource: fmt.Sprintf("user-%d", u)}.Build(),
			DisplayName: fmt.Sprintf("User %d", u),
		}.Build()
		require.NoError(t, c1f.PutResources(ctx, userRes))

		for c := range expansionChains {
			rootEnt := chainEnts[c][0].entitlement

			grant := v2.Grant_builder{
				Id:          fmt.Sprintf("grant:u%d:c%d", u, c),
				Entitlement: rootEnt,
				Principal:   userRes,
			}.Build()

			grantBuf = append(grantBuf, grant)
			if len(grantBuf) >= grantBatch {
				flush()
			}
		}
	}
	flush()

	// Add a tiny set of grants carrying GrantExpandable annotations to define
	// graph edges in the same direction used in production graph loading:
	// annotation entitlement IDs are sources, grant entitlement is target.
	for c := range expansionChains {
		seedPrincipal := v2.Resource_builder{
			Id:          v2.ResourceId_builder{ResourceType: "user", Resource: fmt.Sprintf("seed-user-%d", c)}.Build(),
			DisplayName: fmt.Sprintf("Seed User %d", c),
		}.Build()
		require.NoError(t, c1f.PutResources(ctx, seedPrincipal))

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
			require.NoError(t, c1f.PutGrants(ctx, edgeGrant))
		}
	}

	require.NoError(t, c1f.EndSync(ctx))

	return c1f, syncID
}

func countAllGrants(ctx context.Context, c1f *dotc1z.C1File) (int, error) {
	total := 0
	pageToken := ""
	for {
		resp, err := c1f.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{
			PageToken: pageToken,
		}.Build())
		if err != nil {
			return 0, err
		}
		total += len(resp.GetList())
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			return total, nil
		}
	}
}

func checkExpansionSampleSemantics(ctx context.Context, t *testing.T, c1f *dotc1z.C1File) {
	t.Helper()

	samplePrincipal := v2.ResourceId_builder{ResourceType: "user", Resource: "user-0"}.Build()

	// Depth 1 should be sourced directly from the root entitlement.
	entD1, err := c1f.GetEntitlement(ctx, reader_v2.EntitlementsReaderServiceGetEntitlementRequest_builder{
		EntitlementId: "ent:c0:d1:member",
	}.Build())
	require.NoError(t, err)
	grantsD1, err := c1f.ListGrantsForEntitlement(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
		Entitlement: entD1.GetEntitlement(),
		PrincipalId: samplePrincipal,
	}.Build())
	require.NoError(t, err)
	require.Len(t, grantsD1.GetList(), 1)

	sourcesD1 := grantsD1.GetList()[0].GetSources().GetSources()
	require.Contains(t, sourcesD1, "ent:c0:d0:member")
	require.True(t, sourcesD1["ent:c0:d0:member"].GetIsDirect())

	// Depth 2 should be sourced transitively from depth 1.
	entD2, err := c1f.GetEntitlement(ctx, reader_v2.EntitlementsReaderServiceGetEntitlementRequest_builder{
		EntitlementId: "ent:c0:d2:member",
	}.Build())
	require.NoError(t, err)
	grantsD2, err := c1f.ListGrantsForEntitlement(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
		Entitlement: entD2.GetEntitlement(),
		PrincipalId: samplePrincipal,
	}.Build())
	require.NoError(t, err)
	require.Len(t, grantsD2.GetList(), 1)

	sourcesD2 := grantsD2.GetList()[0].GetSources().GetSources()
	require.Contains(t, sourcesD2, "ent:c0:d1:member")
	require.False(t, sourcesD2["ent:c0:d1:member"].GetIsDirect())
}
