package dotc1z

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
)

// TestListExpandableGrants_MatchesFullScan verifies that ListExpandableGrants
// returns the same grants as manually scanning all grants and filtering by GrantExpandable annotation.
func TestListExpandableGrants_MatchesFullScan(t *testing.T) {
	ctx := context.Background()

	tmpFile, err := os.CreateTemp("", "test-expandable-*.c1z")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	c1f, err := NewC1ZFile(ctx, tmpFile.Name())
	require.NoError(t, err)
	defer c1f.Close(ctx)

	_, err = c1f.StartNewSync(ctx, "full", "")
	require.NoError(t, err)

	// Create test data: mix of expandable and non-expandable grants
	userRT := v2.ResourceType_builder{Id: "user"}.Build()
	groupRT := v2.ResourceType_builder{Id: "group"}.Build()
	require.NoError(t, c1f.PutResourceTypes(ctx, userRT, groupRT))

	group1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build()}.Build()
	group2 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g2"}.Build()}.Build()
	user1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build()}.Build()
	require.NoError(t, c1f.PutResources(ctx, group1, group2, user1))

	ent1 := v2.Entitlement_builder{Id: "ent1", Resource: group1}.Build()
	ent2 := v2.Entitlement_builder{Id: "ent2", Resource: group2}.Build()
	ent3 := v2.Entitlement_builder{Id: "ent3", Resource: group1}.Build()
	require.NoError(t, c1f.PutEntitlements(ctx, ent1, ent2, ent3))

	// Grant 1: WITHOUT expandable annotation
	normalGrant := v2.Grant_builder{
		Id:          "grant-normal",
		Entitlement: ent1,
		Principal:   user1,
	}.Build()

	// Grant 2: WITH expandable annotation (single source)
	expandableAnno1, err := anypb.New(v2.GrantExpandable_builder{
		EntitlementIds:  []string{"ent1"},
		Shallow:         true,
		ResourceTypeIds: []string{"user"},
	}.Build())
	require.NoError(t, err)
	expandableGrant1 := v2.Grant_builder{
		Id:          "grant-expandable-1",
		Entitlement: ent2,
		Principal:   group1,
		Annotations: []*anypb.Any{expandableAnno1},
	}.Build()

	// Grant 3: WITH expandable annotation (multiple sources)
	expandableAnno2, err := anypb.New(v2.GrantExpandable_builder{
		EntitlementIds:  []string{"ent1", "ent2"},
		Shallow:         false,
		ResourceTypeIds: []string{"user", "group"},
	}.Build())
	require.NoError(t, err)
	expandableGrant2 := v2.Grant_builder{
		Id:          "grant-expandable-2",
		Entitlement: ent3,
		Principal:   group2,
		Annotations: []*anypb.Any{expandableAnno2},
	}.Build()

	// Grant 4: WITH empty expandable annotation (should NOT be expandable)
	emptyExpandableAnno, err := anypb.New(v2.GrantExpandable_builder{
		EntitlementIds: []string{},
	}.Build())
	require.NoError(t, err)
	emptyExpandableGrant := v2.Grant_builder{
		Id:          "grant-empty-expandable",
		Entitlement: ent1,
		Principal:   group2,
		Annotations: []*anypb.Any{emptyExpandableAnno},
	}.Build()

	require.NoError(t, c1f.PutGrants(ctx, normalGrant, expandableGrant1, expandableGrant2, emptyExpandableGrant))

	// Method 1: ListExpandableGrants (new optimized path)
	var expandableFromQuery []*ExpandableGrantDef
	pageToken := ""
	for {
		defs, nextToken, err := c1f.ListExpandableGrants(ctx, WithExpandableGrantsPageToken(pageToken))
		require.NoError(t, err)
		expandableFromQuery = append(expandableFromQuery, defs...)
		if nextToken == "" {
			break
		}
		pageToken = nextToken
	}

	// Method 2: Full scan and filter (simulates old behavior)
	type scannedGrant struct {
		externalID        string
		srcEntitlementIDs []string
		shallow           bool
		resourceTypeIDs   []string
		dstEntitlementID  string
		principalRTID     string
		principalRID      string
	}
	expandableFromScan := make(map[string]*scannedGrant)
	pageToken = ""
	for {
		resp, err := c1f.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{PageToken: pageToken}.Build())
		require.NoError(t, err)
		for _, g := range resp.GetList() {
			annos := annotations.Annotations(g.GetAnnotations())
			ge := &v2.GrantExpandable{}
			if _, err := annos.Pick(ge); err == nil && len(ge.GetEntitlementIds()) > 0 {
				expandableFromScan[g.GetId()] = &scannedGrant{
					externalID:        g.GetId(),
					srcEntitlementIDs: ge.GetEntitlementIds(),
					shallow:           ge.GetShallow(),
					resourceTypeIDs:   ge.GetResourceTypeIds(),
					dstEntitlementID:  g.GetEntitlement().GetId(),
					principalRTID:     g.GetPrincipal().GetId().GetResourceType(),
					principalRID:      g.GetPrincipal().GetId().GetResource(),
				}
			}
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}

	// Compare results: same count
	require.Len(t, expandableFromQuery, len(expandableFromScan),
		"ListExpandableGrants returned %d grants, full scan found %d",
		len(expandableFromQuery), len(expandableFromScan))

	// Compare results: same grants with matching fields
	for _, def := range expandableFromQuery {
		scanned, ok := expandableFromScan[def.GrantExternalID]
		require.True(t, ok, "grant %s found by ListExpandableGrants but not by full scan", def.GrantExternalID)

		require.Equal(t, scanned.dstEntitlementID, def.DstEntitlementID,
			"DstEntitlementID mismatch for grant %s", def.GrantExternalID)
		require.Equal(t, scanned.principalRTID, def.PrincipalResourceTypeID,
			"PrincipalResourceTypeID mismatch for grant %s", def.GrantExternalID)
		require.Equal(t, scanned.principalRID, def.PrincipalResourceID,
			"PrincipalResourceID mismatch for grant %s", def.GrantExternalID)
		require.ElementsMatch(t, scanned.srcEntitlementIDs, def.SrcEntitlementIDs,
			"SrcEntitlementIDs mismatch for grant %s", def.GrantExternalID)
		require.Equal(t, scanned.shallow, def.Shallow,
			"Shallow mismatch for grant %s", def.GrantExternalID)
		require.ElementsMatch(t, scanned.resourceTypeIDs, def.PrincipalResourceTypeIDs,
			"PrincipalResourceTypeIDs mismatch for grant %s", def.GrantExternalID)
	}

	// Verify we found exactly the expected expandable grants
	require.Len(t, expandableFromQuery, 2, "should have exactly 2 expandable grants")

	grantIDs := make([]string, len(expandableFromQuery))
	for i, def := range expandableFromQuery {
		grantIDs[i] = def.GrantExternalID
	}
	require.ElementsMatch(t, []string{"grant-expandable-1", "grant-expandable-2"}, grantIDs)
}
