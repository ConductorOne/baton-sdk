package incrementalexpansion_test

import (
	"context"
	"path/filepath"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/sync/incrementalexpansion"
	batonEntitlement "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	batonGrant "github.com/conductorone/baton-sdk/pkg/types/grant"
	"github.com/stretchr/testify/require"
)

// TestExpandDirtySubgraph_MissingSourceEntitlementSkipped verifies that when a grant
// references a source entitlement that doesn't exist (sql.ErrNoRows), the edge is
// skipped gracefully rather than causing an error.
func TestExpandDirtySubgraph_MissingSourceEntitlementSkipped(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.c1z")

	// Create test data
	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()

	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(), DisplayName: "G1"}.Build()
	g2 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g2"}.Build(), DisplayName: "G2"}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(), DisplayName: "U1"}.Build()

	// Only create e2 - e1 will be referenced but NOT exist
	e2 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g2, "member"), Resource: g2, Slug: "member", DisplayName: "member"}.Build()

	// Direct grant U1 -> E2
	grantU1E2 := v2.Grant_builder{
		Id:          batonGrant.NewGrantID(u1, e2),
		Entitlement: e2,
		Principal:   u1,
	}.Build()

	// Nesting grant G1 -> E2 referencing NON-EXISTENT entitlement "group:g1:member"
	// The source entitlement e1 is never created, so GetEntitlement will return sql.ErrNoRows
	nonExistentEntitlementID := batonEntitlement.NewEntitlementID(g1, "member")
	grantG1E2 := v2.Grant_builder{
		Id:          batonGrant.NewGrantID(g1, e2),
		Entitlement: e2,
		Principal:   g1,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds:  []string{nonExistentEntitlementID}, // References non-existent entitlement
			Shallow:         false,
			ResourceTypeIds: []string{"user"},
		}.Build()),
	}.Build()

	// Create the c1z file
	c1f, err := dotc1z.NewC1ZFile(ctx, dbPath)
	require.NoError(t, err)
	defer c1f.Close(ctx)

	syncID, err := c1f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, c1f.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, c1f.PutResources(ctx, g1, g2, u1))
	require.NoError(t, c1f.PutEntitlements(ctx, e2)) // Note: e1 is intentionally NOT added
	require.NoError(t, c1f.PutGrants(ctx, grantU1E2, grantG1E2))
	require.NoError(t, c1f.EndSync(ctx))

	// Mark expandable grants as needing expansion
	require.NoError(t, c1f.SetNeedsExpansionForGrants(ctx, syncID, []string{grantG1E2.GetId()}, true))

	// This should NOT error - it should skip the edge with the missing source entitlement
	err = incrementalexpansion.ExpandDirtySubgraph(ctx, c1f, syncID)
	require.NoError(t, err, "ExpandDirtySubgraph should skip missing source entitlements gracefully")

	// Verify the grant with the missing source wasn't somehow modified incorrectly
	require.NoError(t, c1f.SetSyncID(ctx, ""))
	require.NoError(t, c1f.ViewSync(ctx, syncID))

	// Count grants - should still have original 2 grants (no derived grants created
	// because the source entitlement doesn't exist)
	grantCount := 0
	pageToken := ""
	for {
		resp, err := c1f.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{PageToken: pageToken}.Build())
		require.NoError(t, err)
		grantCount += len(resp.GetList())
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	require.Equal(t, 2, grantCount, "should have exactly 2 grants (no derived grants from missing source)")
}

// TestExpandDirtySubgraph_ValidSourceEntitlementWorks verifies that when a grant
// references a valid source entitlement, the edge is processed correctly.
func TestExpandDirtySubgraph_ValidSourceEntitlementWorks(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.c1z")

	// Create test data
	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()

	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(), DisplayName: "G1"}.Build()
	g2 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g2"}.Build(), DisplayName: "G2"}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(), DisplayName: "U1"}.Build()

	e1 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g1, "member"), Resource: g1, Slug: "member", DisplayName: "member"}.Build()
	e2 := v2.Entitlement_builder{Id: batonEntitlement.NewEntitlementID(g2, "member"), Resource: g2, Slug: "member", DisplayName: "member"}.Build()

	// Direct grant U1 -> E1
	grantU1E1 := v2.Grant_builder{
		Id:          batonGrant.NewGrantID(u1, e1),
		Entitlement: e1,
		Principal:   u1,
	}.Build()

	// Nesting grant G1 -> E2 referencing EXISTING entitlement e1
	grantG1E2 := v2.Grant_builder{
		Id:          batonGrant.NewGrantID(g1, e2),
		Entitlement: e2,
		Principal:   g1,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds:  []string{e1.GetId()},
			Shallow:         false,
			ResourceTypeIds: []string{"user"},
		}.Build()),
	}.Build()

	// Create the c1z file
	c1f, err := dotc1z.NewC1ZFile(ctx, dbPath)
	require.NoError(t, err)
	defer c1f.Close(ctx)

	syncID, err := c1f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, c1f.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, c1f.PutResources(ctx, g1, g2, u1))
	require.NoError(t, c1f.PutEntitlements(ctx, e1, e2)) // Both entitlements exist
	require.NoError(t, c1f.PutGrants(ctx, grantU1E1, grantG1E2))
	require.NoError(t, c1f.EndSync(ctx))

	// Mark expandable grants as needing expansion
	require.NoError(t, c1f.SetNeedsExpansionForGrants(ctx, syncID, []string{grantG1E2.GetId()}, true))

	// This should succeed and create derived grants
	err = incrementalexpansion.ExpandDirtySubgraph(ctx, c1f, syncID)
	require.NoError(t, err)

	// Verify derived grants were created
	require.NoError(t, c1f.SetSyncID(ctx, ""))
	require.NoError(t, c1f.ViewSync(ctx, syncID))

	// Count grants - should have 3 grants now:
	// 1. grantU1E1 (direct)
	// 2. grantG1E2 (nesting)
	// 3. derived grant U1 -> E2 (from expansion)
	grantCount := 0
	pageToken := ""
	for {
		resp, err := c1f.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{PageToken: pageToken}.Build())
		require.NoError(t, err)
		grantCount += len(resp.GetList())
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	require.Equal(t, 3, grantCount, "should have 3 grants (2 original + 1 derived)")
}
