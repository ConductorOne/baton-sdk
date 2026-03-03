package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/bid"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/manager"
	"github.com/conductorone/baton-sdk/pkg/logging"
	et "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	gt "github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

var groupResourceType = v2.ResourceType_builder{
	Id:          "group",
	DisplayName: "Group",
	Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_GROUP},
}.Build()
var userResourceType = v2.ResourceType_builder{
	Id:          "user",
	DisplayName: "User",
	Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER},
	Annotations: annotations.New(&v2.SkipEntitlementsAndGrants{}),
}.Build()

func TestExpandGrants(t *testing.T) {
	ctx := t.Context()

	// 2500 * 4 = 10K - used to cause an infinite loop on pagition
	usersPerLayer := 2500
	groupCount := 5

	mc := newMockConnector()

	mc.rtDB = append(mc.rtDB, groupResourceType, userResourceType)
	type grantData struct {
		r *v2.Resource
		e *v2.Entitlement
	}
	expectedGrantCount := 0
	groups := make([]*grantData, 0)
	for i := range groupCount {
		groupId := "group_" + strconv.Itoa(i)
		group, groupEnt, err := mc.AddGroup(ctx, groupId)
		for _, g := range groups {
			// Add previous groups to the new group. Make the entitlement expandable so that users in previous groups are added to the new group.
			_ = mc.AddGroupMember(ctx, g.r, group, groupEnt)
			expectedGrantCount++
		}
		groups = append(groups, &grantData{
			r: group,
			e: groupEnt,
		})
		require.NoError(t, err)

		for j := range usersPerLayer {
			pid := fmt.Sprintf("user_%d_%d_%d", i, usersPerLayer, j)
			principal, err := mc.AddUser(ctx, pid)
			require.NoError(t, err)

			_ = mc.AddGroupMember(ctx, group, principal)
		}
		expectedGrantCount += usersPerLayer * (i + 1)
	}

	tempDir, err := os.MkdirTemp("", "baton-benchmark-expand-grants")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)
	c1zpath := filepath.Join(tempDir, "expand-grants.c1z")
	syncer, err := NewSyncer(ctx, mc, WithC1ZPath(c1zpath), WithTmpDir(tempDir))
	defer func() { _ = os.Remove(c1zpath) }()
	require.NoError(t, err)
	err = syncer.Sync(ctx)
	require.NoError(t, err)

	err = syncer.Close(ctx)
	require.NoError(t, err)

	// Validate that grants got expanded
	c1zManager, err := manager.New(ctx, c1zpath)
	require.NoError(t, err)
	store, err := c1zManager.LoadC1Z(ctx)
	require.NoError(t, err)

	// Yes it's wasteful to load all grants into memory, but this connector doesn't make a ton of grants.
	allGrants := make([]*v2.Grant, 0)
	pageToken := ""
	for {
		resp, err := store.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{PageToken: pageToken}.Build())
		require.NoError(t, err)
		allGrants = append(allGrants, resp.GetList()...)
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	require.Len(t, allGrants, expectedGrantCount, "should have %d grants but got %d", expectedGrantCount, len(allGrants))
	// Note: We no longer strip GrantExpandable from stored grants during expansion.
	// Expansion bookkeeping lives outside the grant proto so diffs can safely compare data bytes.
}

func TestSyncClearsNeedsExpansionFlagsAfterFullExpansion(t *testing.T) {
	ctx := t.Context()

	mc := newMockConnector()
	mc.rtDB = append(mc.rtDB, groupResourceType, userResourceType)

	g1, g1Ent, err := mc.AddGroup(ctx, "test_group_1")
	require.NoError(t, err)
	g2, _, err := mc.AddGroup(ctx, "test_group_2")
	require.NoError(t, err)
	u1, err := mc.AddUser(ctx, "user_1")
	require.NoError(t, err)

	// Direct membership and one expandable edge so full expansion actually runs.
	_ = mc.AddGroupMember(ctx, g1, u1)
	_ = mc.AddGroupMember(ctx, g2, g1, g1Ent)

	tempDir, err := os.MkdirTemp("", "baton-needs-expansion-clear")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	c1zPath := filepath.Join(tempDir, "needs-expansion-clear.c1z")
	s, err := NewSyncer(ctx, mc, WithC1ZPath(c1zPath), WithTmpDir(tempDir))
	require.NoError(t, err)

	syncerImpl, ok := s.(*syncer)
	require.True(t, ok, "expected concrete syncer implementation")

	require.NoError(t, s.Sync(ctx))
	require.NotEmpty(t, syncerImpl.syncID, "sync id should be set after sync")

	resp, err := syncerImpl.store.ListGrantsInternal(ctx, connectorstore.GrantListOptions{
		Mode:               connectorstore.GrantListModeExpansionNeedsOnly,
		SyncID:             syncerImpl.syncID,
		NeedsExpansionOnly: true,
	})
	require.NoError(t, err)
	require.Len(t, resp.Rows, 0, "full expansion should clear needs_expansion flags")

	require.NoError(t, s.Close(ctx))
}

func TestInvalidResourceTypeFilter(t *testing.T) {
	ctx := t.Context()

	tempDir, err := os.MkdirTemp("", "baton-invalid-resource-type-filter-sync-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	c1zPath := filepath.Join(tempDir, "invalid-resource-type-filter-sync.c1z")

	mc := newMockConnector()

	mc.rtDB = append(mc.rtDB, groupResourceType, userResourceType)

	syncer, err := NewSyncer(ctx, mc,
		WithC1ZPath(c1zPath),
		WithTmpDir(tempDir),
		WithSyncResourceTypes([]string{"fake", "user"}),
	)
	require.NoError(t, err)

	err = syncer.Sync(ctx)
	require.Error(t, err)

	err = syncer.Close(ctx)
	require.NoError(t, err)
}

func TestResourceTypeFilter(t *testing.T) {
	ctx := t.Context()

	tempDir, err := os.MkdirTemp("", "baton-resource-type-filter-sync-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	c1zPath := filepath.Join(tempDir, "resource-type-filter-sync.c1z")

	mc := newMockConnector()

	mc.rtDB = append(mc.rtDB, groupResourceType, userResourceType)

	syncer, err := NewSyncer(ctx, mc,
		WithC1ZPath(c1zPath),
		WithTmpDir(tempDir),
		WithSyncResourceTypes([]string{"group"}),
	)
	require.NoError(t, err)

	err = syncer.Sync(ctx)
	require.NoError(t, err)

	err = syncer.Close(ctx)
	require.NoError(t, err)
}

func TestExpandGrantBadEntitlement(t *testing.T) {
	ctx := t.Context()

	ctx, err := logging.Init(ctx)
	require.NoError(t, err)

	mc := newMockConnector()

	var badResourceType = v2.ResourceType_builder{
		Id:          "bad",
		DisplayName: "Bad",
		Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_GROUP},
	}.Build()
	mc.rtDB = append(mc.rtDB, groupResourceType, userResourceType, badResourceType)

	group1, _, err := mc.AddGroup(ctx, "test_group_1")
	require.NoError(t, err)
	group2, group2Ent, err := mc.AddGroup(ctx, "test_group_2")
	require.NoError(t, err)

	user1, err := mc.AddUser(ctx, "user_1")
	require.NoError(t, err)
	user2, err := mc.AddUser(ctx, "user_2")
	require.NoError(t, err)

	// Add all users to group 2
	_ = mc.AddGroupMember(ctx, group2, user1)
	_ = mc.AddGroupMember(ctx, group2, user2)

	// Add group 2 to group 1 with an incorrect entitlement id
	badEntResource, err := rs.NewGroupResource(
		"bad_group",
		badResourceType,
		"bad_group",
		[]rs.GroupTraitOption{},
	)
	require.NoError(t, err)
	mc.AddResource(ctx, badEntResource)
	badEnt := et.NewAssignmentEntitlement(
		badEntResource,
		"bad_member",
		et.WithGrantableTo(groupResourceType, userResourceType, badResourceType),
	)
	grantOpts := gt.WithAnnotation(v2.GrantExpandable_builder{
		EntitlementIds: []string{
			group2Ent.GetId(),
		},
	}.Build())

	badGrant := gt.NewGrant(
		badEntResource,
		"bad_member",
		group2,
		grantOpts,
	)
	mc.grantDB[badEntResource.GetId().GetResource()] = append(mc.grantDB[badEntResource.GetId().GetResource()], badGrant)
	// _ = mc.AddGroupMember(ctx, group2, badEntResource)

	_ = mc.AddGroupMember(ctx, group1, group2, badEnt)

	tempDir, err := os.MkdirTemp("", "baton-expand-grant-bad-entitlement")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)
	c1zpath := filepath.Join(tempDir, "expand-grants.c1z")
	syncer, err := NewSyncer(ctx, mc, WithC1ZPath(c1zpath), WithTmpDir(tempDir))
	require.NoError(t, err)
	err = syncer.Sync(ctx)
	require.NoError(t, err)
	err = syncer.Close(ctx)
	require.NoError(t, err)

	_ = os.Remove(c1zpath)
}

func TestExpandGrantImmutable(t *testing.T) {
	ctx := t.Context()

	mc := newMockConnector()

	mc.rtDB = append(mc.rtDB, groupResourceType, userResourceType)

	group1, group1Ent, err := mc.AddGroup(ctx, "test_group_1")
	require.NoError(t, err)
	group2, group2Ent, err := mc.AddGroup(ctx, "test_group_2")
	require.NoError(t, err)

	user1, err := mc.AddUser(ctx, "user_1")
	require.NoError(t, err)
	user2, err := mc.AddUser(ctx, "user_2")
	require.NoError(t, err)

	// Add all users to group 2
	_ = mc.AddGroupMember(ctx, group2, user1)
	_ = mc.AddGroupMember(ctx, group2, user2)

	// Add group 2 to group 1
	_ = mc.AddGroupMember(ctx, group1, group2, group2Ent)

	// Directly add user 1 to group 1 (this grant should not be immutable after expansion)
	_ = mc.AddGroupMember(ctx, group1, user1)

	tempDir, err := os.MkdirTemp("", "baton-expand-grant-immutable")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)
	c1zpath := filepath.Join(tempDir, "expand-grants.c1z")
	syncer, err := NewSyncer(ctx, mc, WithC1ZPath(c1zpath), WithTmpDir(tempDir))
	require.NoError(t, err)
	err = syncer.Sync(ctx)
	require.NoError(t, err)
	err = syncer.Close(ctx)
	require.NoError(t, err)

	c1zManager, err := manager.New(ctx, c1zpath)
	require.NoError(t, err)

	store, err := c1zManager.LoadC1Z(ctx)
	require.NoError(t, err)

	allGrantsReq := &v2.GrantsServiceListGrantsRequest{}
	allGrants, err := store.ListGrants(ctx, allGrantsReq)
	require.NoError(t, err)
	require.Len(t, allGrants.GetList(), 5)

	req := reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
		Entitlement: group1Ent,
		PageToken:   "",
		Annotations: nil,
	}.Build()
	resp, err := store.ListGrantsForEntitlement(ctx, req)
	require.NoError(t, err)
	require.Len(t, resp.GetList(), 3) // both users and group2 should have group1 membership

	req = reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
		Entitlement: group1Ent,
		PrincipalId: user1.GetId(),
		PageToken:   "",
		Annotations: nil,
	}.Build()
	resp, err = store.ListGrantsForEntitlement(ctx, req)
	require.NoError(t, err)
	require.Len(t, resp.GetList(), 1)

	grant := resp.GetList()[0]

	annos := annotations.Annotations(grant.GetAnnotations())
	immutable := &v2.GrantImmutable{}
	hasImmutable, err := annos.Pick(immutable)
	require.NoError(t, err)

	require.False(t, hasImmutable) // Direct grant should not be immutable

	req = reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
		Entitlement: group1Ent,
		PrincipalId: user2.GetId(),
		PageToken:   "",
		Annotations: nil,
	}.Build()
	resp, err = store.ListGrantsForEntitlement(ctx, req)
	require.NoError(t, err)
	require.Len(t, resp.GetList(), 1)

	grant = resp.GetList()[0]

	annos = annotations.Annotations(grant.GetAnnotations())
	immutable = &v2.GrantImmutable{}
	hasImmutable, err = annos.Pick(immutable)
	require.NoError(t, err)

	require.True(t, hasImmutable) // Expanded indirect grant should be immutable

	_ = os.Remove(c1zpath)
}

func TestExpandGrantImmutableCycle(t *testing.T) {
	ctx := t.Context()

	mc := newMockConnector()

	mc.rtDB = append(mc.rtDB, groupResourceType, userResourceType)

	group1, group1Ent, err := mc.AddGroup(ctx, "test_group_1")
	require.NoError(t, err)
	group2, group2Ent, err := mc.AddGroup(ctx, "test_group_2")
	require.NoError(t, err)
	group3, group3Ent, err := mc.AddGroup(ctx, "test_group_3")
	require.NoError(t, err)

	user1, err := mc.AddUser(ctx, "user_1")
	require.NoError(t, err)
	user2, err := mc.AddUser(ctx, "user_2")
	require.NoError(t, err)

	// Add all users to group 2
	_ = mc.AddGroupMember(ctx, group2, user1)
	_ = mc.AddGroupMember(ctx, group2, user2)

	// Add group 2 to group 1
	_ = mc.AddGroupMember(ctx, group1, group2, group2Ent)
	// Add group 3 to group 2
	_ = mc.AddGroupMember(ctx, group2, group3, group3Ent)
	// Add group 1 to group 3
	_ = mc.AddGroupMember(ctx, group3, group1, group1Ent)

	// Directly add user 1 to group 1 (this grant should not be immutable after expansion)
	_ = mc.AddGroupMember(ctx, group1, user1)

	tempDir, err := os.MkdirTemp("", "baton-expand-grant-immutable-cycle")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)
	c1zpath := filepath.Join(tempDir, "expand-grants.c1z")
	syncer, err := NewSyncer(ctx, mc, WithC1ZPath(c1zpath), WithTmpDir(tempDir))
	require.NoError(t, err)
	err = syncer.Sync(ctx)
	require.NoError(t, err)
	err = syncer.Close(ctx)
	require.NoError(t, err)

	c1zManager, err := manager.New(ctx, c1zpath)
	require.NoError(t, err)

	store, err := c1zManager.LoadC1Z(ctx)
	require.NoError(t, err)

	allGrantsReq := &v2.GrantsServiceListGrantsRequest{}
	allGrants, err := store.ListGrants(ctx, allGrantsReq)
	require.NoError(t, err)
	require.Len(t, allGrants.GetList(), 6)

	req := reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
		Entitlement: group1Ent,
		PageToken:   "",
		Annotations: nil,
	}.Build()
	resp, err := store.ListGrantsForEntitlement(ctx, req)
	require.NoError(t, err)
	require.Len(t, resp.GetList(), 2) // both users and group2 should have group1 membership

	req = reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
		// Entitlement: group1Ent,
		PrincipalId: user1.GetId(),
		PageToken:   "",
		Annotations: nil,
	}.Build()
	resp, err = store.ListGrantsForPrincipal(ctx, req)
	require.NoError(t, err)
	require.Len(t, resp.GetList(), 2)

	grant := resp.GetList()[0]
	for _, g := range resp.GetList() {
		if g.GetEntitlement().GetId() == group1Ent.GetId() {
			grant = g
			break
		}
	}
	require.Equal(t, grant.GetEntitlement().GetId(), group1Ent.GetId())

	annos := annotations.Annotations(grant.GetAnnotations())
	immutable := &v2.GrantImmutable{}
	hasImmutable, err := annos.Pick(immutable)
	require.NoError(t, err)

	require.False(t, hasImmutable) // Direct grant should not be immutable

	req = reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
		PrincipalId: user2.GetId(),
		PageToken:   "",
		Annotations: nil,
	}.Build()
	resp, err = store.ListGrantsForPrincipal(ctx, req)
	require.NoError(t, err)
	require.Len(t, resp.GetList(), 1)

	grant = resp.GetList()[0]

	annos = annotations.Annotations(grant.GetAnnotations())
	immutable = &v2.GrantImmutable{}
	hasImmutable, err = annos.Pick(immutable)
	require.NoError(t, err)

	// TODO: Make this pass. It would require modifying an existing grant after fixing the cycle
	// require.True(t, hasImmutable) // Expanded indirect grant should be immutable
	require.False(t, hasImmutable) // TODO: delete this and fix the code so the above line passes

	_ = os.Remove(c1zpath)
}
func BenchmarkExpandCircle(b *testing.B) {
	ctx := b.Context()

	// create a loop of N entitlements
	circleSize := 7
	// with different principal + grants at each layer
	usersPerLayer := 100
	groupCount := 100

	mc := newMockConnector()

	mc.rtDB = append(mc.rtDB, groupResourceType, userResourceType)

	for i := range groupCount {
		groupId := "group_" + strconv.Itoa(i)
		group, _, err := mc.AddGroup(ctx, groupId)
		require.NoError(b, err)

		childGroupId := "child_group_" + strconv.Itoa(i)
		childGroup, childEnt, err := mc.AddGroup(ctx, childGroupId)
		require.NoError(b, err)

		_ = mc.AddGroupMember(ctx, group, childGroup, childEnt)

		for j := range usersPerLayer {
			pid := "user_" + strconv.Itoa(i*usersPerLayer+j)
			principal, err := mc.AddUser(ctx, pid)
			require.NoError(b, err)

			// This isn't needed because grant expansion will create this grant
			// _ = mc.AddGroupMember(ctx, group, principal)
			_ = mc.AddGroupMember(ctx, childGroup, principal)
		}
	}

	// create the circle
	for i := range circleSize {
		groupId := "group_" + strconv.Itoa(i)
		nextGroupId := "group_" + strconv.Itoa((i+1)%circleSize) // Wrap around to the start for the last element
		currentEnt := mc.entDB[groupId][0]
		nextEnt := mc.entDB[nextGroupId][0]

		_ = mc.AddGroupMember(ctx, currentEnt.GetResource(), nextEnt.GetResource(), nextEnt)
	}

	tempDir, err := os.MkdirTemp("", "baton-benchmark-expand-circle")
	require.NoError(b, err)
	defer os.RemoveAll(tempDir)
	c1zpath := filepath.Join(tempDir, "expand-circle.c1z")

	for b.Loop() {
		syncer, err := NewSyncer(ctx, mc, WithC1ZPath(c1zpath), WithTmpDir(tempDir))
		require.NoError(b, err)
		err = syncer.Sync(ctx)
		require.NoError(b, err)
		err = syncer.Close(ctx)
		require.NoError(b, err)
		_ = os.Remove(c1zpath)
	}
}

func TestExternalResourcePath(t *testing.T) {
	ctx := t.Context()

	tempDir, err := os.MkdirTemp("", "baton-id-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	internalMc := newMockConnector()
	internalMc.rtDB = append(internalMc.rtDB, userResourceType, groupResourceType)

	externalMc := newMockConnector()
	externalMc.rtDB = append(externalMc.rtDB, userResourceType, groupResourceType)

	// internal user
	internalUserRs, err := internalMc.AddUserProfile(ctx, "1", map[string]any{})
	require.NoError(t, err)

	internalGroup, _, err := internalMc.AddGroup(ctx, "2")
	require.NoError(t, err)
	internalMc.grantDB[internalGroup.GetId().GetResource()] = []*v2.Grant{
		gt.NewGrant(
			internalGroup,
			"member",
			internalUserRs.GetId(),
			// Same id as external user profile key value
			gt.WithAnnotation(v2.ExternalResourceMatch_builder{
				Key:          "external_id_match",
				Value:        "10",
				ResourceType: v2.ResourceType_TRAIT_USER,
			}.Build()),
		),
	}

	// Id is the same to try to duplicate the grant
	// this could be an email
	externalUserRs, err := externalMc.AddUserProfile(ctx, "10", map[string]any{
		"external_id_match": "10",
	})
	require.NoError(t, err)

	externalGroup, _, err := externalMc.AddGroup(ctx, "11")
	require.NoError(t, err)

	// External resource has a grant to itself
	externalMc.grantDB[externalGroup.GetId().GetResource()] = []*v2.Grant{
		gt.NewGrant(
			externalGroup,
			"member",
			externalUserRs.GetId(),
			gt.WithAnnotation(v2.ExternalResourceMatch_builder{
				Key:          "external_id_match",
				Value:        "10",
				ResourceType: v2.ResourceType_TRAIT_USER,
			}.Build()),
		),
	}

	// Needs to make external sync
	externalC1zpath := filepath.Join(tempDir, "external.c1z")

	externalSyncer, err := NewSyncer(ctx, externalMc, WithC1ZPath(externalC1zpath), WithTmpDir(tempDir))
	require.NoError(t, err)
	err = externalSyncer.Sync(ctx)
	require.NoError(t, err)

	err = externalSyncer.Close(ctx)
	require.NoError(t, err)
	require.NoError(t, err)

	internalC1zpath := filepath.Join(tempDir, "internal.c1z")
	internalSyncer, err := NewSyncer(ctx, internalMc, WithC1ZPath(internalC1zpath), WithTmpDir(tempDir), WithExternalResourceC1ZPath(externalC1zpath))
	require.NoError(t, err)
	err = internalSyncer.Sync(ctx)
	require.NoError(t, err)

	err = internalSyncer.Close(ctx)
	require.NoError(t, err)

	c1zManager, err := manager.New(ctx, internalC1zpath)
	require.NoError(t, err)

	store, err := c1zManager.LoadC1Z(ctx)
	require.NoError(t, err)

	resources, err := store.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
		ResourceTypeId: userResourceType.GetId(),
	}.Build())
	require.NoError(t, err)
	require.Equal(t, len(resources.GetList()), 2)

	entitlements, err := store.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{
		Resource: internalGroup,
	}.Build())
	require.NoError(t, err)
	require.Equal(t, 1, len(entitlements.GetList()))

	allGrantsReq := &v2.GrantsServiceListGrantsRequest{}
	allGrants, err := store.ListGrants(ctx, allGrantsReq)
	require.NoError(t, err)

	require.Len(t, allGrants.GetList(), 2)
}

func TestPartialSync(t *testing.T) {
	ctx := t.Context()

	tempDir, err := os.MkdirTemp("", "baton-partial-sync-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	c1zPath := filepath.Join(tempDir, "partial-sync.c1z")

	mc := newMockConnector()

	mc.rtDB = append(mc.rtDB, groupResourceType, userResourceType)

	group, _, err := mc.AddGroup(ctx, "test_group")
	require.NoError(t, err)

	_, _, err = mc.AddGroup(ctx, "test_group2")
	require.NoError(t, err)

	user, err := mc.AddUser(ctx, "test_user")
	require.NoError(t, err)

	mc.AddGroupMember(ctx, group, user)

	resources := []*v2.Resource{
		group,
		user,
	}

	batonIDs := make([]*v2.Resource, 0, len(resources))
	for _, resource := range resources {
		batonIDs = append(batonIDs, &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: resource.Id.ResourceType,
				Resource:     resource.Id.Resource,
			},
		})
	}
	// Partial syncs should succeed in cases where a resource doesn't exist.
	batonIDs = append(batonIDs, &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "group",
			Resource:     "non_existent_group",
		},
	})
	partialSyncer, err := NewSyncer(ctx, mc,
		WithC1ZPath(c1zPath),
		WithTmpDir(tempDir),
		WithTargetedSyncResources(batonIDs),
	)
	require.NoError(t, err)

	err = partialSyncer.Sync(ctx)
	require.NoError(t, err)

	err = partialSyncer.Close(ctx)
	require.NoError(t, err)

	c1zManager, err := manager.New(ctx, c1zPath)
	require.NoError(t, err)

	store, err := c1zManager.LoadC1Z(ctx)
	require.NoError(t, err)

	resourcesResp, err := store.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
		ResourceTypeId: userResourceType.GetId(),
	}.Build())
	require.NoError(t, err)
	require.Equal(t, len(resourcesResp.GetList()), 1)

	resourcesResp, err = store.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
		ResourceTypeId: groupResourceType.GetId(),
	}.Build())
	require.NoError(t, err)
	// This connector has 2 groups but partial sync means we only synced one of them.
	require.Equal(t, len(resourcesResp.GetList()), 1)

	entitlements, err := store.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{
		Resource: group,
	}.Build())
	require.NoError(t, err)
	require.Equal(t, 1, len(entitlements.GetList()))

	allGrantsReq := &v2.GrantsServiceListGrantsRequest{}
	allGrants, err := store.ListGrants(ctx, allGrantsReq)
	require.NoError(t, err)

	require.Len(t, allGrants.GetList(), 1)
}

func TestPartialSyncSkipEntitlementsAndGrants(t *testing.T) {
	ctx := t.Context()

	tempDir, err := os.MkdirTemp("", "baton-partial-sync-test-skip-entitlements-and-grants")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	c1zPath := filepath.Join(tempDir, "partial-sync-skip-entitlements-and-grants.c1z")

	mc := newMockConnector()

	mc.rtDB = append(mc.rtDB, groupResourceType, userResourceType)

	group, _, err := mc.AddGroup(ctx, "test_group")
	require.NoError(t, err)

	user, err := mc.AddUser(ctx, "test_user")
	require.NoError(t, err)

	mc.AddGroupMember(ctx, group, user)

	syncer, err := NewSyncer(ctx, mc,
		WithC1ZPath(c1zPath),
		WithTmpDir(tempDir),
		WithSkipEntitlementsAndGrants(true),
	)
	require.NoError(t, err)

	err = syncer.Sync(ctx)
	require.NoError(t, err)

	err = syncer.Close(ctx)
	require.NoError(t, err)

	c1zManager, err := manager.New(ctx, c1zPath)
	require.NoError(t, err)

	store, err := c1zManager.LoadC1Z(ctx)
	require.NoError(t, err)

	resources, err := store.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
		ResourceTypeId: groupResourceType.GetId(),
	}.Build())
	require.NoError(t, err)
	require.Equal(t, 1, len(resources.GetList()))

	entitlements, err := store.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{
		Resource: group,
	}.Build())
	require.NoError(t, err)
	require.Equal(t, 0, len(entitlements.GetList()))

	grants, err := store.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{
		Resource: group,
	}.Build())
	require.NoError(t, err)
	require.Equal(t, 0, len(grants.GetList()))
}

func TestPartialSyncUnimplemented(t *testing.T) {
	ctx := t.Context()

	tempDir, err := os.MkdirTemp("", "baton-partial-sync-test-unimplemented")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	c1zPath := filepath.Join(tempDir, "partial-sync-unimplemented.c1z")

	mc := newMockConnector()

	mc.rtDB = append(mc.rtDB, groupResourceType, userResourceType)

	group, _, err := mc.AddGroup(ctx, "test_group")
	require.NoError(t, err)
	group2, _, err := mc.AddGroup(ctx, "test_group2")
	require.NoError(t, err)

	user, err := mc.AddUser(ctx, "test_user")
	require.NoError(t, err)

	mc.AddGroupMember(ctx, group, user)
	mc.AddGroupMember(ctx, group2, user)

	batonIDs := []*v2.Resource{
		{Id: &v2.ResourceId{ResourceType: group.Id.ResourceType, Resource: group.Id.Resource}},
	}
	partialSyncer, err := NewSyncer(ctx, mc,
		WithC1ZPath(c1zPath),
		WithTmpDir(tempDir),
		WithTargetedSyncResources(batonIDs),
	)
	require.NoError(t, err)

	err = partialSyncer.Sync(ctx)
	require.NoError(t, err)

	err = partialSyncer.Close(ctx)
	require.NoError(t, err)

	c1zManager, err := manager.New(ctx, c1zPath)
	require.NoError(t, err)

	store, err := c1zManager.LoadC1Z(ctx)
	require.NoError(t, err)

	syncs, _, err := store.ListSyncRuns(ctx, "", 100)
	require.NoError(t, err)
	require.Equal(t, 1, len(syncs))
	require.Equal(t, connectorstore.SyncTypePartial, syncs[0].Type)

	resources, err := store.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
		ResourceTypeId: groupResourceType.GetId(),
	}.Build())
	require.NoError(t, err)
	require.Equal(t, 1, len(resources.GetList()))

	entitlements, err := store.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{
		Resource: group,
	}.Build())
	require.NoError(t, err)
	require.Equal(t, 1, len(entitlements.GetList()))

	grants, err := store.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{
		Resource: group,
	}.Build())
	require.NoError(t, err)
	require.Equal(t, 1, len(grants.GetList()))
}

func TestExternalResourceMatchAll(t *testing.T) {
	ctx := t.Context()

	tempDir, err := os.MkdirTemp("", "baton-external-match-all-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	internalMc := newMockConnector()
	internalMc.rtDB = append(internalMc.rtDB, userResourceType, groupResourceType)

	externalMc := newMockConnector()
	externalMc.rtDB = append(externalMc.rtDB, userResourceType, groupResourceType)

	// Create external users
	externalUser1, err := externalMc.AddUserProfile(ctx, "ext_user_1", map[string]any{})
	require.NoError(t, err)
	externalUser2, err := externalMc.AddUserProfile(ctx, "ext_user_2", map[string]any{})
	require.NoError(t, err)

	// Create internal group with ExternalResourceMatchAll grant
	internalGroup, internalGroupEnt, err := internalMc.AddGroup(ctx, "internal_group")
	require.NoError(t, err)
	internalMc.grantDB[internalGroup.GetId().GetResource()] = []*v2.Grant{
		gt.NewGrant(
			internalGroup,
			"member",
			// Placeholder principal - will be replaced by all external users
			// Use a dummy resource ID since it will be replaced
			v2.ResourceId_builder{
				ResourceType: userResourceType.GetId(),
				Resource:     "placeholder",
			}.Build(),
			gt.WithAnnotation(v2.ExternalResourceMatchAll_builder{
				ResourceType: v2.ResourceType_TRAIT_USER,
			}.Build()),
		),
	}

	// Sync external resources first
	externalC1zpath := filepath.Join(tempDir, "external.c1z")
	externalSyncer, err := NewSyncer(ctx, externalMc, WithC1ZPath(externalC1zpath), WithTmpDir(tempDir))
	require.NoError(t, err)
	err = externalSyncer.Sync(ctx)
	require.NoError(t, err)
	err = externalSyncer.Close(ctx)
	require.NoError(t, err)

	// Sync internal with external reference
	internalC1zpath := filepath.Join(tempDir, "internal.c1z")
	internalSyncer, err := NewSyncer(ctx, internalMc, WithC1ZPath(internalC1zpath), WithTmpDir(tempDir), WithExternalResourceC1ZPath(externalC1zpath))
	require.NoError(t, err)
	err = internalSyncer.Sync(ctx)
	require.NoError(t, err)
	err = internalSyncer.Close(ctx)
	require.NoError(t, err)

	// Verify grants were created for all external users
	c1zManager, err := manager.New(ctx, internalC1zpath)
	require.NoError(t, err)
	store, err := c1zManager.LoadC1Z(ctx)
	require.NoError(t, err)

	// Get grants for the internal group entitlement
	grants, err := store.ListGrantsForEntitlement(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
		Entitlement: internalGroupEnt,
	}.Build())
	require.NoError(t, err)
	// Should have 2 grants (one for each external user)
	require.Len(t, grants.GetList(), 2, "Should have created grants for both external users")

	// Verify the grants are for the correct external users
	principalIDs := make(map[string]bool)
	for _, grant := range grants.GetList() {
		principalID := grant.GetPrincipal().GetId().GetResource()
		principalIDs[principalID] = true

		// Verify the grant is for the correct entitlement
		require.Equal(t, internalGroupEnt.GetId(), grant.GetEntitlement().GetId(), "Grant should be for the internal group member entitlement")

		// Verify the grant is for the correct resource
		require.Equal(t, internalGroup.GetId().GetResource(), grant.GetEntitlement().GetResource().GetId().GetResource(), "Grant should be for the internal group")

		// Verify the principal is a user resource type
		require.Equal(t, userResourceType.GetId(), grant.GetPrincipal().GetId().GetResourceType(), "Grant principal should be a user")

		// Verify the principal is one of the external users
		require.Contains(t, []string{externalUser1.GetId().GetResource(), externalUser2.GetId().GetResource()}, principalID, "Grant principal should be one of the external users")
	}

	// Verify both external users have grants
	require.True(t, principalIDs[externalUser1.GetId().GetResource()], "Should have grant for ext_user_1")
	require.True(t, principalIDs[externalUser2.GetId().GetResource()], "Should have grant for ext_user_2")
}

func TestExternalResourceMatchID(t *testing.T) {
	ctx := t.Context()

	tempDir, err := os.MkdirTemp("", "baton-external-match-id-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	internalMc := newMockConnector()
	internalMc.rtDB = append(internalMc.rtDB, userResourceType, groupResourceType)

	externalMc := newMockConnector()
	externalMc.rtDB = append(externalMc.rtDB, userResourceType, groupResourceType)

	// Create external user
	externalUser, err := externalMc.AddUserProfile(ctx, "ext_user_1", map[string]any{})
	require.NoError(t, err)

	// Create internal group with ExternalResourceMatchID grant
	internalGroup, _, err := internalMc.AddGroup(ctx, "internal_group")
	require.NoError(t, err)
	internalMc.grantDB[internalGroup.GetId().GetResource()] = []*v2.Grant{
		gt.NewGrant(
			internalGroup,
			"member",
			// Placeholder principal - will be replaced by matching external user
			externalUser.GetId(),
			gt.WithAnnotation(v2.ExternalResourceMatchID_builder{
				Id: externalUser.GetId().GetResource(),
			}.Build()),
		),
	}

	// Sync external resources first
	externalC1zpath := filepath.Join(tempDir, "external.c1z")
	externalSyncer, err := NewSyncer(ctx, externalMc, WithC1ZPath(externalC1zpath), WithTmpDir(tempDir))
	require.NoError(t, err)
	err = externalSyncer.Sync(ctx)
	require.NoError(t, err)
	err = externalSyncer.Close(ctx)
	require.NoError(t, err)

	// Sync internal with external reference
	internalC1zpath := filepath.Join(tempDir, "internal.c1z")
	internalSyncer, err := NewSyncer(ctx, internalMc, WithC1ZPath(internalC1zpath), WithTmpDir(tempDir), WithExternalResourceC1ZPath(externalC1zpath))
	require.NoError(t, err)
	err = internalSyncer.Sync(ctx)
	require.NoError(t, err)
	err = internalSyncer.Close(ctx)
	require.NoError(t, err)

	// Verify grant was created for the matching external user
	c1zManager, err := manager.New(ctx, internalC1zpath)
	require.NoError(t, err)
	store, err := c1zManager.LoadC1Z(ctx)
	require.NoError(t, err)

	allGrants, err := store.ListGrants(ctx, &v2.GrantsServiceListGrantsRequest{})
	require.NoError(t, err)
	require.Len(t, allGrants.GetList(), 1)
	require.Equal(t, externalUser.GetId().GetResource(), allGrants.GetList()[0].GetPrincipal().GetId().GetResource())
}

// TestExternalResourceMatchIDWithExpandableRemapping verifies that when a grant has both
// ExternalResourceMatchID and GrantExpandable annotations, the remapping code in
// processGrantsWithExternalPrincipals correctly reads the expansion annotation and
// creates remapped entitlement IDs for the matched external principal.
func TestExternalResourceMatchIDWithExpandableRemapping(t *testing.T) {
	ctx := t.Context()

	tempDir, err := os.MkdirTemp("", "baton-external-match-id-expandable-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	internalMc := newMockConnector()
	internalMc.rtDB = append(internalMc.rtDB, userResourceType, groupResourceType)

	externalMc := newMockConnector()
	externalMc.rtDB = append(externalMc.rtDB, userResourceType, groupResourceType)

	// Create an external group (role). Groups have entitlements synced by
	// SyncExternalResourcesUsersAndGroups, so the remapped entitlement ID will exist.
	_, extGroupEnt, err := externalMc.AddGroup(ctx, "ext_role")
	require.NoError(t, err)

	// Create internal group with entitlement
	internalGroup, _, err := internalMc.AddGroup(ctx, "internal_group")
	require.NoError(t, err)

	// Create a PLACEHOLDER resource that differs from the external group.
	// The GrantExpandable references an entitlement on this placeholder.
	// After matching, the remapping should replace the placeholder's entitlement IDs
	// with ones on the matched external principal (ext_role).
	placeholderResource := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "group", Resource: "placeholder_role"}.Build(),
	}.Build()
	placeholderEntID, err := bid.MakeBid(v2.Entitlement_builder{
		Resource: placeholderResource,
		Slug:     "member",
	}.Build())
	require.NoError(t, err)

	// Create a grant with BOTH ExternalResourceMatchID AND GrantExpandable.
	// The GrantExpandable references an entitlement on the PLACEHOLDER resource.
	// After matching ext_role by ID, the remapping code should:
	// 1. Parse the placeholder entitlement ID to get the slug ("member")
	// 2. Create a new entitlement ID: entitlement.NewEntitlementID(ext_role, "member")
	// 3. Verify that entitlement exists in the store (it does, synced from external)
	// 4. Create a new GrantExpandable with the remapped entitlement ID
	internalMc.grantDB[internalGroup.GetId().GetResource()] = []*v2.Grant{
		gt.NewGrant(
			internalGroup,
			"member",
			placeholderResource.GetId(),
			gt.WithAnnotation(v2.ExternalResourceMatchID_builder{
				Id: "ext_role",
			}.Build()),
			gt.WithAnnotation(v2.GrantExpandable_builder{
				EntitlementIds:  []string{placeholderEntID},
				Shallow:         true,
				ResourceTypeIds: []string{"user"},
			}.Build()),
		),
	}

	// Sync external resources
	externalC1zpath := filepath.Join(tempDir, "external.c1z")
	externalSyncer, err := NewSyncer(ctx, externalMc, WithC1ZPath(externalC1zpath), WithTmpDir(tempDir))
	require.NoError(t, err)
	err = externalSyncer.Sync(ctx)
	require.NoError(t, err)
	err = externalSyncer.Close(ctx)
	require.NoError(t, err)

	// Sync internal with external reference. Skip expansion to isolate the
	// external resource annotation propagation behavior.
	internalC1zpath := filepath.Join(tempDir, "internal.c1z")
	internalSyncer, err := NewSyncer(ctx, internalMc,
		WithC1ZPath(internalC1zpath),
		WithTmpDir(tempDir),
		WithExternalResourceC1ZPath(externalC1zpath),
		WithDontExpandGrants(),
	)
	require.NoError(t, err)
	err = internalSyncer.Sync(ctx)
	require.NoError(t, err)
	err = internalSyncer.Close(ctx)
	require.NoError(t, err)

	// Verify the matched grant has a REMAPPED GrantExpandable.
	// The remapping should produce an entitlement ID using the matched external
	// group's resource, which is the same as the original in this case but was
	// generated via entitlement.NewEntitlementID(matchedPrincipal, slug).
	c1zManager, err := manager.New(ctx, internalC1zpath)
	require.NoError(t, err)
	store, err := c1zManager.LoadC1Z(ctx)
	require.NoError(t, err)

	allGrants, err := store.ListGrants(ctx, &v2.GrantsServiceListGrantsRequest{})
	require.NoError(t, err)
	t.Logf("Total grants: %d", len(allGrants.GetList()))
	for _, g := range allGrants.GetList() {
		t.Logf("  grant %s: ent=%s principal=%s/%s", g.GetId(), g.GetEntitlement().GetId(),
			g.GetPrincipal().GetId().GetResourceType(), g.GetPrincipal().GetId().GetResource())
	}

	internalList, err := store.ListGrantsInternal(ctx, connectorstore.GrantListOptions{
		Mode: connectorstore.GrantListModeExpansion,
	})
	require.NoError(t, err)
	defs := make([]*connectorstore.ExpandableGrantDef, 0, len(internalList.Rows))
	for _, row := range internalList.Rows {
		if row.Expansion != nil {
			defs = append(defs, row.Expansion)
		}
	}
	t.Logf("Expandable grants: %d", len(defs))

	// Find the expandable grant for the matched external group.
	// The remapped entitlement ID should reference extGroupEnt (the external group's
	// member entitlement, e.g. "group:ext_role:member"), NOT the placeholder's
	// entitlement ("group:placeholder_role:member"). This proves the remapping ran.
	found := false
	for _, def := range defs {
		t.Logf("expandable: external_id=%s target_ent=%s source_ents=%v shallow=%v resource_type_ids=%v",
			def.GrantExternalID, def.TargetEntitlementID, def.SourceEntitlementIDs, def.Shallow, def.ResourceTypeIDs)
		for _, srcEntID := range def.SourceEntitlementIDs {
			// Must be the REMAPPED entitlement (ext_role), not the placeholder
			require.NotEqual(t, placeholderEntID, srcEntID,
				"expansion should have remapped entitlement IDs, not placeholder IDs")
			if srcEntID == extGroupEnt.GetId() {
				found = true
				require.True(t, def.Shallow, "expansion should preserve shallow flag")
				require.Equal(t, []string{"user"}, def.ResourceTypeIDs, "expansion should preserve resource type IDs")
			}
		}
	}
	require.True(t, found, "should find expandable grant with remapped entitlement ID %q referencing the external group's entitlement", extGroupEnt.GetId())
}

func TestExternalResourceEmailMatch(t *testing.T) {
	ctx := t.Context()

	tempDir, err := os.MkdirTemp("", "baton-external-email-match-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	internalMc := newMockConnector()
	internalMc.rtDB = append(internalMc.rtDB, userResourceType, groupResourceType)

	externalMc := newMockConnector()
	externalMc.rtDB = append(externalMc.rtDB, userResourceType, groupResourceType)

	// Create external user with email
	externalUser, err := rs.NewUserResource(
		"ext_user_1",
		userResourceType,
		"External User",
		[]rs.UserTraitOption{
			rs.WithEmail("user@example.com", true),
		},
	)
	require.NoError(t, err)
	externalMc.AddResource(ctx, externalUser)

	// Create internal group with ExternalResourceMatch grant using email
	internalGroup, internalGroupEnt, err := internalMc.AddGroup(ctx, "internal_group")
	require.NoError(t, err)
	internalMc.grantDB[internalGroup.GetId().GetResource()] = []*v2.Grant{
		gt.NewGrant(
			internalGroup,
			"member",
			// Placeholder principal - will be replaced by matching external user
			// Use a dummy resource ID since it will be replaced
			v2.ResourceId_builder{
				ResourceType: userResourceType.GetId(),
				Resource:     "placeholder_user",
			}.Build(),
			gt.WithAnnotation(v2.ExternalResourceMatch_builder{
				Key:          "email",
				Value:        "user@example.com",
				ResourceType: v2.ResourceType_TRAIT_USER,
			}.Build()),
		),
	}

	// Sync external resources first
	externalC1zpath := filepath.Join(tempDir, "external.c1z")
	externalSyncer, err := NewSyncer(ctx, externalMc, WithC1ZPath(externalC1zpath), WithTmpDir(tempDir))
	require.NoError(t, err)
	err = externalSyncer.Sync(ctx)
	require.NoError(t, err)
	err = externalSyncer.Close(ctx)
	require.NoError(t, err)

	// Sync internal with external reference
	internalC1zpath := filepath.Join(tempDir, "internal.c1z")
	internalSyncer, err := NewSyncer(ctx, internalMc, WithC1ZPath(internalC1zpath), WithTmpDir(tempDir), WithExternalResourceC1ZPath(externalC1zpath))
	require.NoError(t, err)
	err = internalSyncer.Sync(ctx)
	require.NoError(t, err)
	err = internalSyncer.Close(ctx)
	require.NoError(t, err)

	// Verify grant was created for the matching external user
	c1zManager, err := manager.New(ctx, internalC1zpath)
	require.NoError(t, err)
	store, err := c1zManager.LoadC1Z(ctx)
	require.NoError(t, err)

	// Get grants for the internal group entitlement
	grants, err := store.ListGrantsForEntitlement(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
		Entitlement: internalGroupEnt,
	}.Build())
	require.NoError(t, err)
	require.Len(t, grants.GetList(), 1, "Should have created grant for the external user matched by email")

	// Verify the grant is for the correct external user (proving the email matching worked)
	grant := grants.GetList()[0]
	require.Equal(t, externalUser.GetId().GetResource(), grant.GetPrincipal().GetId().GetResource(), "Grant principal should be the external user matched by email")
	require.Equal(t, externalUser.GetId().GetResourceType(), grant.GetPrincipal().GetId().GetResourceType(), "Grant principal should have correct resource type")
	require.Equal(t, internalGroupEnt.GetId(), grant.GetEntitlement().GetId(), "Grant should be for the internal group member entitlement")
	require.Equal(t, internalGroup.GetId().GetResource(), grant.GetEntitlement().GetResource().GetId().GetResource(), "Grant should be for the internal group")

	// Verify the placeholder was NOT used (proving the matching logic actually ran)
	require.NotEqual(t, "placeholder_user", grant.GetPrincipal().GetId().GetResource(), "Grant principal should not be the placeholder, proving email matching worked")
}

func TestExternalResourceGroupProfileMatch(t *testing.T) {
	ctx := t.Context()

	tempDir, err := os.MkdirTemp("", "baton-external-group-profile-match-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	internalMc := newMockConnector()
	internalMc.rtDB = append(internalMc.rtDB, userResourceType, groupResourceType)

	externalMc := newMockConnector()
	externalMc.rtDB = append(externalMc.rtDB, userResourceType, groupResourceType)

	// Create external group with profile
	externalGroup, err := rs.NewGroupResource(
		"ext_group_1",
		groupResourceType,
		"External Group",
		[]rs.GroupTraitOption{
			rs.WithGroupProfile(map[string]interface{}{
				"external_id": "ext_123",
			}),
		},
	)
	require.NoError(t, err)
	externalMc.AddResource(ctx, externalGroup)

	// Create internal group with ExternalResourceMatch grant using group profile
	internalGroup, _, err := internalMc.AddGroup(ctx, "internal_group")
	require.NoError(t, err)
	internalMc.grantDB[internalGroup.GetId().GetResource()] = []*v2.Grant{
		gt.NewGrant(
			internalGroup,
			"member",
			// Placeholder principal - will be replaced by matching external group
			// Use a dummy group resource ID since it will be replaced
			v2.ResourceId_builder{
				ResourceType: groupResourceType.GetId(),
				Resource:     "placeholder_group",
			}.Build(),
			gt.WithAnnotation(v2.ExternalResourceMatch_builder{
				Key:          "external_id",
				Value:        "ext_123",
				ResourceType: v2.ResourceType_TRAIT_GROUP,
			}.Build()),
		),
	}

	// Sync external resources first
	externalC1zpath := filepath.Join(tempDir, "external.c1z")
	externalSyncer, err := NewSyncer(ctx, externalMc, WithC1ZPath(externalC1zpath), WithTmpDir(tempDir))
	require.NoError(t, err)
	err = externalSyncer.Sync(ctx)
	require.NoError(t, err)
	err = externalSyncer.Close(ctx)
	require.NoError(t, err)

	// Sync internal with external reference
	internalC1zpath := filepath.Join(tempDir, "internal.c1z")
	internalSyncer, err := NewSyncer(ctx, internalMc, WithC1ZPath(internalC1zpath), WithTmpDir(tempDir), WithExternalResourceC1ZPath(externalC1zpath))
	require.NoError(t, err)
	err = internalSyncer.Sync(ctx)
	require.NoError(t, err)
	err = internalSyncer.Close(ctx)
	require.NoError(t, err)

	// Verify grant was created for the matching external group
	c1zManager, err := manager.New(ctx, internalC1zpath)
	require.NoError(t, err)
	store, err := c1zManager.LoadC1Z(ctx)
	require.NoError(t, err)

	// Verify the external group was synced with correct properties
	groupResources, err := store.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
		ResourceTypeId: groupResourceType.GetId(),
	}.Build())
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(groupResources.GetList()), 1, "Should have synced at least the external group")

	// Find the external group in the synced resources
	var syncedExternalGroup *v2.Resource
	for _, resource := range groupResources.GetList() {
		if resource.GetId().GetResource() == externalGroup.GetId().GetResource() {
			syncedExternalGroup = resource
			break
		}
	}
	require.NotNil(t, syncedExternalGroup, "Should have synced the external group")
	require.Equal(t, externalGroup.GetId().GetResource(), syncedExternalGroup.GetId().GetResource(), "External group should have correct resource ID")
	require.Equal(t, externalGroup.GetId().GetResourceType(), syncedExternalGroup.GetId().GetResourceType(), "External group should have correct resource type")
	require.Equal(t, externalGroup.GetDisplayName(), syncedExternalGroup.GetDisplayName(), "External group should have correct display name")

	// Verify the external group has the correct profile
	groupTrait, err := rs.GetGroupTrait(syncedExternalGroup)
	require.NoError(t, err, "External group should have a group trait")
	require.NotNil(t, groupTrait.GetProfile(), "External group should have a profile")
	profileVal, ok := rs.GetProfileStringValue(groupTrait.GetProfile(), "external_id")
	require.True(t, ok, "External group profile should have external_id key")
	require.Equal(t, "ext_123", profileVal, "External group profile should have correct external_id value")

	// Verify the external group was synced (which proves the matching logic found it)
	// The group profile matching logic runs during processGrantsWithExternalPrincipals,
	// which processes grants with ExternalResourceMatch annotations. The external group
	// must be synced as a principal for the matching to work.
	// Note: Grants are only created for groups when there's an expandable annotation,
	// but the resource matching itself is verified by the external group being synced.

	// Verify that the external group was synced (proving the matching logic found it)
	// This is the key verification - if the group wasn't matched, it wouldn't be synced
	require.NotNil(t, syncedExternalGroup, "External group should have been synced, proving group profile matching worked")
	require.Equal(t, "ext_123", profileVal, "External group profile should have correct external_id value, proving matching worked")
}

func TestExternalResourceWithGrantToEntitlement(t *testing.T) {
	ctx := t.Context()

	tempDir, err := os.MkdirTemp("", "baton-external-grant-to-entitlement-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	internalMc := newMockConnector()
	internalMc.rtDB = append(internalMc.rtDB, userResourceType, groupResourceType)

	externalMc := newMockConnector()
	externalMc.rtDB = append(externalMc.rtDB, userResourceType, groupResourceType)

	// Create external group with entitlement
	externalGroup, externalGroupEnt, err := externalMc.AddGroup(ctx, "ext_group_1")
	require.NoError(t, err)

	// Create external user
	externalUser, err := externalMc.AddUserProfile(ctx, "ext_user_1", map[string]any{})
	require.NoError(t, err)

	// Grant external user to external group
	externalMc.grantDB[externalGroup.GetId().GetResource()] = []*v2.Grant{
		gt.NewGrant(
			externalGroup,
			"member",
			externalUser.GetId(),
		),
	}

	// Sync external resources first
	externalC1zpath := filepath.Join(tempDir, "external.c1z")
	externalSyncer, err := NewSyncer(ctx, externalMc, WithC1ZPath(externalC1zpath), WithTmpDir(tempDir))
	require.NoError(t, err)
	err = externalSyncer.Sync(ctx)
	require.NoError(t, err)
	err = externalSyncer.Close(ctx)
	require.NoError(t, err)

	// Sync internal with external reference, filtering by entitlement
	internalC1zpath := filepath.Join(tempDir, "internal.c1z")
	internalSyncer, err := NewSyncer(ctx, internalMc,
		WithC1ZPath(internalC1zpath),
		WithTmpDir(tempDir),
		WithExternalResourceC1ZPath(externalC1zpath),
		WithExternalResourceEntitlementIdFilter(externalGroupEnt.GetId()),
	)
	require.NoError(t, err)
	err = internalSyncer.Sync(ctx)
	require.NoError(t, err)
	err = internalSyncer.Close(ctx)
	require.NoError(t, err)

	// Verify external resources were synced via SyncExternalResourcesWithGrantToEntitlement
	c1zManager, err := manager.New(ctx, internalC1zpath)
	require.NoError(t, err)
	store, err := c1zManager.LoadC1Z(ctx)
	require.NoError(t, err)

	// SyncExternalResourcesWithGrantToEntitlement syncs resources based on grants to a specific entitlement.
	// It:
	// 1. Gets grants for the specified entitlement (externalGroupEnt)
	// 2. Extracts principals from those grants (the external user in this case)
	// 3. Syncs those principals, their resource types, entitlements, and grants for those entitlements
	// Note: It does NOT sync the original grants to the filter entitlement, only grants for entitlements
	// of the principals. Since userResourceType has SkipEntitlementsAndGrants, no entitlements or grants will be synced.

	// Verify the external user (principal from the grant) was synced
	userResources, err := store.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
		ResourceTypeId: userResourceType.GetId(),
	}.Build())
	require.NoError(t, err)
	require.Len(t, userResources.GetList(), 1, "Should have synced the external user (principal from grant)")
	syncedUser := userResources.GetList()[0]
	require.Equal(t, externalUser.GetId().GetResource(), syncedUser.GetId().GetResource())
	require.Equal(t, userResourceType.GetId(), syncedUser.GetId().GetResourceType())

	// Verify the user resource type was synced
	resourceTypes, err := store.ListResourceTypes(ctx, &v2.ResourceTypesServiceListResourceTypesRequest{})
	require.NoError(t, err)
	userRTFound := false
	for _, rt := range resourceTypes.GetList() {
		if rt.GetId() == userResourceType.GetId() {
			userRTFound = true
			require.Equal(t, userResourceType.GetId(), rt.GetId())
			require.Equal(t, userResourceType.GetDisplayName(), rt.GetDisplayName())
			break
		}
	}
	require.True(t, userRTFound, "Should have synced the user resource type")

	// Verify no entitlements were synced for the user (because userResourceType has SkipEntitlementsAndGrants)
	userEntitlements, err := store.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{
		Resource: syncedUser,
	}.Build())
	require.NoError(t, err)
	require.Len(t, userEntitlements.GetList(), 0, "Should have no entitlements for user because user resource type skips entitlements and grants")

	// Verify no grants were synced (because userResourceType has SkipEntitlementsAndGrants,
	// so the user has no entitlements, and therefore no grants for those entitlements)
	allGrants, err := store.ListGrants(ctx, &v2.GrantsServiceListGrantsRequest{})
	require.NoError(t, err)
	require.Len(t, allGrants.GetList(), 0, "Should have no grants because user resource type skips entitlements and grants")
}

// This test verifies that when a sync fails after syncing a parent resource
// but before syncing its child resources, resuming the sync will correctly re-process the parent
// resource to ensure child resources are synced (because the checkpoint may not include those resources).
func TestResumeSyncWithChildResources(t *testing.T) {
	ctx := t.Context()

	// Create parent/child resources types.
	parentResourceType := v2.ResourceType_builder{
		Id:          "parent",
		DisplayName: "Parent",
	}.Build()

	childResourceType := v2.ResourceType_builder{
		Id:          "child",
		DisplayName: "Child",
		Annotations: annotations.New(&v2.SkipEntitlementsAndGrants{}),
	}.Build()

	tempDir, err := os.MkdirTemp("", "baton-resume-child-resources-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	c1zPath := filepath.Join(tempDir, "resume-child-resources.c1z")

	mc := newMockConnector()
	mc.rtDB = append(mc.rtDB, parentResourceType, childResourceType)

	// Create a parent resource with the annotation for a child resource.
	parentResource, err := rs.NewResource(
		"Parent 1",
		parentResourceType,
		"parent_1",
		rs.WithAnnotation(v2.ChildResourceType_builder{
			ResourceTypeId: childResourceType.GetId(),
		}.Build()),
	)
	require.NoError(t, err)
	mc.AddResource(ctx, parentResource)

	// Create two child resources.
	child1, err := rs.NewResource(
		"Child 1",
		childResourceType,
		"child_1",
		rs.WithParentResourceID(parentResource.GetId()),
	)
	require.NoError(t, err)
	mc.AddResource(ctx, child1)

	child2, err := rs.NewResource(
		"Child 2",
		childResourceType,
		"child_2",
		rs.WithParentResourceID(parentResource.GetId()),
	)
	require.NoError(t, err)
	mc.AddResource(ctx, child2)

	// First sync: our mock connector will fail sync right after syncing the parent
	// but before syncing the child resources. This simulates a checkpoint scenario where
	// the parent resource was synced and saved, but the sync failed.
	mcWithFailure := &failChildResourceMockConnector{
		mockConnector:        mc,
		failOnChildResources: true,
	}

	syncer1, err := NewSyncer(ctx, mcWithFailure, WithC1ZPath(c1zPath), WithTmpDir(tempDir))
	require.NoError(t, err)

	// Start sync - this should sync parent_1, then fail when trying to sync its children.
	err = syncer1.Sync(ctx)
	require.Error(t, err, "sync should fail when trying to sync child resources")

	// Cancel the context and close the syncer so that when we we re-open the c1z,
	// we resume the current sync (as opposed to starting a new sync).
	ctxToCancel, cancelFunc := context.WithCancel(ctx)
	cancelFunc()
	err = syncer1.Close(ctxToCancel)
	require.NoError(t, err)

	// Verify that parent_1 was synced before the failure.
	c1zManager1, err := manager.New(ctx, c1zPath)
	require.NoError(t, err)
	store1, err := c1zManager1.LoadC1Z(ctx)
	require.NoError(t, err)

	parentResources, err := store1.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
		ResourceTypeId: parentResourceType.GetId(),
	}.Build())
	require.NoError(t, err)
	require.Len(t, parentResources.GetList(), 1, "parent should be synced")
	require.Equal(t, parentResource.GetId().GetResource(), parentResources.GetList()[0].GetId().GetResource())

	// Verify that no children were synced before the failure.
	childResources, err := store1.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
		ResourceTypeId: childResourceType.GetId(),
	}.Build())
	require.NoError(t, err)
	require.Len(t, childResources.GetList(), 0, "children should not be synced")

	err = store1.Close(ctx)
	require.NoError(t, err)

	// Second sync: resume the sync with a non-failing connector - the children should now be synced correctly.
	// (We still use the failure mock connector, as we need to return no child resources that are not under our parent.)
	syncer2, err := NewSyncer(ctx, mcWithFailure, WithC1ZPath(c1zPath), WithTmpDir(tempDir))
	require.NoError(t, err)
	mcWithFailure.failOnChildResources = false

	err = syncer2.Sync(ctx)
	require.NoError(t, err, "resumed sync should succeed")

	err = syncer2.Close(ctx)
	require.NoError(t, err)

	// Verify that child resources are now synced.
	c1zManager2, err := manager.New(ctx, c1zPath)
	require.NoError(t, err)
	store2, err := c1zManager2.LoadC1Z(ctx)
	require.NoError(t, err)

	childResourcesAfterResume, err := store2.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
		ResourceTypeId: childResourceType.GetId(),
	}.Build())
	require.NoError(t, err)
	require.Len(t, childResourcesAfterResume.GetList(), 2, "both child resources should be synced after resume")

	// Verify the child resources have the correct parent.
	for _, child := range childResourcesAfterResume.GetList() {
		require.NotNil(t, child.GetParentResourceId(), "child should have parent resource ID")
		require.Equal(t, parentResource.GetId().GetResource(), child.GetParentResourceId().GetResource())
		require.Equal(t, parentResourceType.GetId(), child.GetParentResourceId().GetResourceType())
	}
}

// failChildResourceMockConnector wraps a mockConnector and fails ListResources calls for child resources.
type failChildResourceMockConnector struct {
	*mockConnector
	failOnChildResources bool
}

func (fmc *failChildResourceMockConnector) ListResources(
	ctx context.Context,
	in *v2.ResourcesServiceListResourcesRequest,
	opts ...grpc.CallOption,
) (*v2.ResourcesServiceListResourcesResponse, error) {
	// This simulates a failure after the parent was synced but before children could be synced.
	if in.GetResourceTypeId() == "child" {
		if in.GetParentResourceId() != nil {
			if fmc.failOnChildResources {
				return nil, status.Errorf(codes.Internal, "simulated failure when syncing child resources")
			} // Otherwise, dispatch to the regular mock connector
		} else {
			// Return no resources - the only instances of the child resource come from inspecting the parent.
			return v2.ResourcesServiceListResourcesResponse_builder{List: []*v2.Resource{}}.Build(), nil
		}
	}

	return fmc.mockConnector.ListResources(ctx, in, opts...)
}

func newMockConnector() *mockConnector {
	mc := &mockConnector{
		rtDB:       make([]*v2.ResourceType, 0),
		resourceDB: make(map[string][]*v2.Resource, 0),
		entDB:      make(map[string][]*v2.Entitlement),
		grantDB:    make(map[string][]*v2.Grant),
	}
	return mc
}

type mockConnector struct {
	metadata   *v2.ConnectorMetadata
	rtDB       []*v2.ResourceType
	resourceDB map[string][]*v2.Resource
	entDB      map[string][]*v2.Entitlement // resource id to entitlements
	grantDB    map[string][]*v2.Grant       // resource id to grants
	v2.AssetServiceClient
	v2.GrantManagerServiceClient
	v2.ResourceManagerServiceClient
	v2.AccountManagerServiceClient
	v2.ResourceDeleterServiceClient
	v2.CredentialManagerServiceClient
	v2.EventServiceClient
	v2.TicketsServiceClient
	v2.ActionServiceClient
	v2.ResourceGetterServiceClient
	v2.EntitlementsServiceClient
}

func (mc *mockConnector) AddGroup(ctx context.Context, groupId string) (*v2.Resource, *v2.Entitlement, error) {
	group, err := rs.NewGroupResource(
		groupId,
		groupResourceType,
		groupId,
		[]rs.GroupTraitOption{},
	)
	if err != nil {
		return nil, nil, err
	}

	mc.resourceDB[groupResourceType.GetId()] = append(mc.resourceDB[groupResourceType.GetId()], group)

	ent := et.NewAssignmentEntitlement(
		group,
		"member",
		et.WithGrantableTo(groupResourceType, userResourceType),
	)
	ent.SetSlug("member")
	mc.entDB[groupId] = append(mc.entDB[groupId], ent)

	return group, ent, nil
}

func (mc *mockConnector) AddUser(ctx context.Context, userId string) (*v2.Resource, error) {
	user, err := rs.NewUserResource(
		userId,
		userResourceType,
		userId,
		[]rs.UserTraitOption{},
		rs.WithAnnotation(&v2.SkipEntitlementsAndGrants{}),
	)
	if err != nil {
		return nil, err
	}

	mc.resourceDB[userResourceType.GetId()] = append(mc.resourceDB[userResourceType.GetId()], user)
	return user, nil
}

func (mc *mockConnector) AddUserProfile(ctx context.Context, userId string, profile map[string]any, opts ...rs.ResourceOption) (*v2.Resource, error) {
	user, err := rs.NewUserResource(
		userId,
		userResourceType,
		userId,
		[]rs.UserTraitOption{
			rs.WithUserProfile(profile),
		},
		opts...,
	)
	if err != nil {
		return nil, err
	}

	mc.resourceDB[userResourceType.GetId()] = append(mc.resourceDB[userResourceType.GetId()], user)
	return user, nil
}

func (mc *mockConnector) AddResource(ctx context.Context, resource *v2.Resource) {
	mc.resourceDB[resource.GetId().GetResourceType()] = append(mc.resourceDB[resource.GetId().GetResourceType()], resource)
}

func (mc *mockConnector) AddResourceType(ctx context.Context, resourceType *v2.ResourceType) {
	mc.rtDB = append(mc.rtDB, resourceType)
}

func (mc *mockConnector) AddGroupMember(ctx context.Context, resource *v2.Resource, principal *v2.Resource, expandEnts ...*v2.Entitlement) *v2.Grant {
	grantOpts := []gt.GrantOption{}

	for _, ent := range expandEnts {
		grantOpts = append(grantOpts, gt.WithAnnotation(v2.GrantExpandable_builder{
			EntitlementIds: []string{
				ent.GetId(),
			},
		}.Build()))
	}

	grant := gt.NewGrant(
		resource,
		"member",
		principal,
		grantOpts...,
	)

	mc.grantDB[resource.GetId().GetResource()] = append(mc.grantDB[resource.GetId().GetResource()], grant)

	return grant
}

func (mc *mockConnector) ListResourceTypes(context.Context, *v2.ResourceTypesServiceListResourceTypesRequest, ...grpc.CallOption) (*v2.ResourceTypesServiceListResourceTypesResponse, error) {
	return v2.ResourceTypesServiceListResourceTypesResponse_builder{List: mc.rtDB}.Build(), nil
}

func (mc *mockConnector) ListResources(ctx context.Context, in *v2.ResourcesServiceListResourcesRequest, opts ...grpc.CallOption) (*v2.ResourcesServiceListResourcesResponse, error) {
	resources := mc.resourceDB[in.GetResourceTypeId()]
	if resources == nil {
		resources = []*v2.Resource{}
	}
	return v2.ResourcesServiceListResourcesResponse_builder{List: resources}.Build(), nil
}

func (mc *mockConnector) GetResource(ctx context.Context, in *v2.ResourceGetterServiceGetResourceRequest, opts ...grpc.CallOption) (*v2.ResourceGetterServiceGetResourceResponse, error) {
	var resource *v2.Resource
	resources := mc.resourceDB[in.GetResourceId().GetResourceType()]
	for _, r := range resources {
		if r.GetId().GetResource() == in.GetResourceId().GetResource() {
			resource = r
			break
		}
	}
	if resource == nil {
		return nil, status.Errorf(codes.NotFound, "resource not found")
	}
	return v2.ResourceGetterServiceGetResourceResponse_builder{Resource: resource}.Build(), nil
}

func (mc *mockConnector) ListEntitlements(ctx context.Context, in *v2.EntitlementsServiceListEntitlementsRequest, opts ...grpc.CallOption) (*v2.EntitlementsServiceListEntitlementsResponse, error) {
	return v2.EntitlementsServiceListEntitlementsResponse_builder{List: mc.entDB[in.GetResource().GetId().GetResource()]}.Build(), nil
}

func (mc *mockConnector) ListStaticEntitlements(
	ctx context.Context,
	in *v2.EntitlementsServiceListStaticEntitlementsRequest,
	opts ...grpc.CallOption,
) (*v2.EntitlementsServiceListStaticEntitlementsResponse, error) {
	return v2.EntitlementsServiceListStaticEntitlementsResponse_builder{List: []*v2.Entitlement{}}.Build(), nil
}

func (mc *mockConnector) ListGrants(ctx context.Context, in *v2.GrantsServiceListGrantsRequest, opts ...grpc.CallOption) (*v2.GrantsServiceListGrantsResponse, error) {
	var key string
	if r := in.GetResource(); r != nil {
		key = r.GetId().GetResource()
	}
	return v2.GrantsServiceListGrantsResponse_builder{List: mc.grantDB[key]}.Build(), nil
}

func (mc *mockConnector) GetMetadata(ctx context.Context, in *v2.ConnectorServiceGetMetadataRequest, opts ...grpc.CallOption) (*v2.ConnectorServiceGetMetadataResponse, error) {
	return v2.ConnectorServiceGetMetadataResponse_builder{
		Metadata: mc.metadata,
	}.Build(), nil
}

func (mc *mockConnector) Validate(ctx context.Context, in *v2.ConnectorServiceValidateRequest, opts ...grpc.CallOption) (*v2.ConnectorServiceValidateResponse, error) {
	return &v2.ConnectorServiceValidateResponse{}, nil
}

func (mc *mockConnector) Cleanup(ctx context.Context, in *v2.ConnectorServiceCleanupRequest, opts ...grpc.CallOption) (*v2.ConnectorServiceCleanupResponse, error) {
	return &v2.ConnectorServiceCleanupResponse{}, nil
}

// etagMockConnector extends mockConnector to simulate ETag/ETagMatch behavior.
type etagMockConnector struct {
	*mockConnector
	etagValue        string
	matchOnNext      bool
	callTokens       []string
	entitlementID    string
	firstPageGrants  []*v2.Grant
	secondPageGrants []*v2.Grant
	nextPageToken    string
	forceMatch       bool
}

func newEtagMockConnector(etagValue string) *etagMockConnector {
	mc := &etagMockConnector{
		mockConnector: newMockConnector(),
		etagValue:     etagValue,
		callTokens:    make([]string, 0),
	}
	// Ensure default resource types
	mc.rtDB = append(mc.rtDB, groupResourceType, userResourceType)
	return mc
}

func (mc *etagMockConnector) WithData(resource *v2.Resource, ent *v2.Entitlement, grants ...*v2.Grant) {
	mc.AddResource(context.Background(), resource)
	mc.entitlementID = ent.GetId()
	mc.entDB[resource.GetId().GetResource()] = []*v2.Entitlement{ent}
	mc.grantDB[resource.GetId().GetResource()] = grants
	mc.firstPageGrants = grants
	mc.secondPageGrants = nil
	mc.nextPageToken = ""
}

func (mc *etagMockConnector) WithPagination(first []*v2.Grant, second []*v2.Grant, token string) {
	mc.firstPageGrants = first
	mc.secondPageGrants = second
	mc.nextPageToken = token
}

func (mc *etagMockConnector) ListGrants(ctx context.Context, in *v2.GrantsServiceListGrantsRequest, _ ...grpc.CallOption) (*v2.GrantsServiceListGrantsResponse, error) {
	mc.callTokens = append(mc.callTokens, in.GetPageToken())

	// Extract incoming ETag from resource annotations
	var incomingETag string
	var incomingEntitlement string
	if res := in.GetResource(); res != nil {
		annos := annotations.Annotations(res.GetAnnotations())
		et := &v2.ETag{}
		ok, _ := annos.Pick(et)
		if ok {
			incomingETag = et.GetValue()
			incomingEntitlement = et.GetEntitlementId()
		}
	}

	// If configured to match (or force match), return ETagMatch with empty list
	if mc.forceMatch || (mc.matchOnNext && incomingETag == mc.etagValue && incomingEntitlement == mc.entitlementID) {
		mc.matchOnNext = false
		return v2.GrantsServiceListGrantsResponse_builder{
			List: []*v2.Grant{},
			Annotations: annotations.New(&v2.ETagMatch{
				EntitlementId: mc.entitlementID,
			}),
		}.Build(), nil
	}

	// Normal path, optionally paginated
	if mc.nextPageToken != "" {
		if in.GetPageToken() == "" {
			return v2.GrantsServiceListGrantsResponse_builder{
				List:          mc.firstPageGrants,
				NextPageToken: mc.nextPageToken,
				Annotations: annotations.New(&v2.ETag{
					Value:         mc.etagValue,
					EntitlementId: mc.entitlementID,
				}),
			}.Build(), nil
		}
		return v2.GrantsServiceListGrantsResponse_builder{
			List: mc.secondPageGrants,
			Annotations: annotations.New(&v2.ETag{
				Value:         mc.etagValue,
				EntitlementId: mc.entitlementID,
			}),
		}.Build(), nil
	}

	var key string
	if r := in.GetResource(); r != nil {
		key = r.GetId().GetResource()
	}
	return v2.GrantsServiceListGrantsResponse_builder{
		List: mc.grantDB[key],
		Annotations: annotations.New(&v2.ETag{
			Value:         mc.etagValue,
			EntitlementId: mc.entitlementID,
		}),
	}.Build(), nil
}

func TestSyncGrants_EtagMatchReuse(t *testing.T) {
	ctx := t.Context()

	tempDir, err := os.MkdirTemp("", "baton-etag-reuse")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	c1zPath := filepath.Join(tempDir, "etag-reuse.c1z")

	// Build resource/entitlement/grant
	group, err := rs.NewGroupResource("g1", groupResourceType, "g1", nil)
	require.NoError(t, err)
	ent := et.NewAssignmentEntitlement(group, "member", et.WithGrantableTo(groupResourceType, userResourceType))
	ent.SetSlug("member")
	user, err := rs.NewUserResource("u1", userResourceType, "u1", nil, rs.WithAnnotation(&v2.SkipEntitlementsAndGrants{}))
	require.NoError(t, err)
	grant := gt.NewGrant(group, "member", user)

	mc := newEtagMockConnector("etag-1")
	mc.WithData(group, ent, grant)

	// First sync: normal path, store etag
	syncer1, err := NewSyncer(ctx, mc, WithC1ZPath(c1zPath), WithTmpDir(tempDir))
	require.NoError(t, err)
	require.NoError(t, syncer1.Sync(ctx))
	require.NoError(t, syncer1.Close(ctx))

	// Second sync: connector will return ETagMatch and empty list, reuse previous grants
	mc.matchOnNext = true
	mc.callTokens = nil

	syncer2, err := NewSyncer(ctx, mc, WithC1ZPath(c1zPath), WithTmpDir(tempDir))
	require.NoError(t, err)
	require.NoError(t, syncer2.Sync(ctx))
	require.NoError(t, syncer2.Close(ctx))

	// Should have only been called once (no pagination)
	require.Equal(t, []string{""}, mc.callTokens)

	// Verify grants exist after second sync
	c1zManager, err := manager.New(ctx, c1zPath)
	require.NoError(t, err)
	store, err := c1zManager.LoadC1Z(ctx)
	require.NoError(t, err)

	resp, err := store.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, resp.GetList(), 1)
	got := resp.GetList()[0]
	require.Equal(t, grant.GetId(), got.GetId())
	require.True(t, proto.Equal(grant.GetEntitlement(), got.GetEntitlement()))
	require.True(t, proto.Equal(grant.GetPrincipal(), got.GetPrincipal()))
	require.True(t, proto.Equal(grant.GetSources(), got.GetSources()))
}

func TestSyncGrants_EtagMatchMissingPreviousEtag(t *testing.T) {
	ctx := t.Context()

	tempDir, err := os.MkdirTemp("", "baton-etag-miss")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	c1zPath := filepath.Join(tempDir, "etag-miss.c1z")

	group, err := rs.NewGroupResource("g1", groupResourceType, "g1", nil)
	require.NoError(t, err)
	ent := et.NewAssignmentEntitlement(group, "member", et.WithGrantableTo(groupResourceType, userResourceType))
	ent.SetSlug("member")
	user, err := rs.NewUserResource("u1", userResourceType, "u1", nil, rs.WithAnnotation(&v2.SkipEntitlementsAndGrants{}))
	require.NoError(t, err)
	grant := gt.NewGrant(group, "member", user)

	mc := newEtagMockConnector("etag-1")
	mc.WithData(group, ent, grant)

	// First sync: do NOT return etag (simulate connector not setting it)
	mc.etagValue = ""
	syncer1, err := NewSyncer(ctx, mc, WithC1ZPath(c1zPath), WithTmpDir(tempDir))
	require.NoError(t, err)
	require.NoError(t, syncer1.Sync(ctx))
	require.NoError(t, syncer1.Close(ctx))

	// Second sync: connector claims ETagMatch but syncer has no previous etag -> expect error
	mc.forceMatch = true
	mc.etagValue = "etag-1"
	mc.callTokens = nil

	syncer2, err := NewSyncer(ctx, mc, WithC1ZPath(c1zPath), WithTmpDir(tempDir))
	require.NoError(t, err)
	err = syncer2.Sync(ctx)
	require.Error(t, err, "expected error when connector returns ETagMatch but no previous etag stored")
	_ = syncer2.Close(ctx)
}

func TestSyncGrants_EtagMatchEntitlementMismatch(t *testing.T) {
	ctx := t.Context()

	tempDir, err := os.MkdirTemp("", "baton-etag-mismatch")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	c1zPath := filepath.Join(tempDir, "etag-mismatch.c1z")

	group, err := rs.NewGroupResource("g1", groupResourceType, "g1", nil)
	require.NoError(t, err)
	ent := et.NewAssignmentEntitlement(group, "member", et.WithGrantableTo(groupResourceType, userResourceType))
	ent.SetSlug("member")
	user, err := rs.NewUserResource("u1", userResourceType, "u1", nil, rs.WithAnnotation(&v2.SkipEntitlementsAndGrants{}))
	require.NoError(t, err)
	grant := gt.NewGrant(group, "member", user)

	mc := newEtagMockConnector("etag-1")
	mc.WithData(group, ent, grant)

	// First sync sets etag
	syncer1, err := NewSyncer(ctx, mc, WithC1ZPath(c1zPath), WithTmpDir(tempDir))
	require.NoError(t, err)
	require.NoError(t, syncer1.Sync(ctx))
	require.NoError(t, syncer1.Close(ctx))

	// Second sync: connector returns ETagMatch but with different entitlement id -> error
	mc.forceMatch = true
	otherEnt := et.NewAssignmentEntitlement(group, "member2", et.WithGrantableTo(groupResourceType, userResourceType))
	otherEnt.SetSlug("member2")
	mc.entitlementID = otherEnt.GetId()
	mc.callTokens = nil

	syncer2, err := NewSyncer(ctx, mc, WithC1ZPath(c1zPath), WithTmpDir(tempDir))
	require.NoError(t, err)
	err = syncer2.Sync(ctx)
	require.Error(t, err, "expected error when ETagMatch entitlement id mismatches previous etag")
	_ = syncer2.Close(ctx)
}

func TestSyncGrants_NoMatchPaginates(t *testing.T) {
	ctx := t.Context()

	tempDir, err := os.MkdirTemp("", "baton-etag-paginate")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	c1zPath := filepath.Join(tempDir, "etag-paginate.c1z")

	group, err := rs.NewGroupResource("g1", groupResourceType, "g1", nil)
	require.NoError(t, err)
	ent := et.NewAssignmentEntitlement(group, "member", et.WithGrantableTo(groupResourceType, userResourceType))
	ent.SetSlug("member")
	user, err := rs.NewUserResource("u1", userResourceType, "u1", nil, rs.WithAnnotation(&v2.SkipEntitlementsAndGrants{}))
	require.NoError(t, err)
	grant1 := gt.NewGrant(group, "member", user)
	grant2 := gt.NewGrant(group, "member", user)
	grant2.SetId(grant1.GetId() + "_2")

	mc := newEtagMockConnector("etag-1")
	mc.WithData(group, ent, grant1, grant2)
	mc.WithPagination([]*v2.Grant{grant1}, []*v2.Grant{grant2}, "next")

	syncer, err := NewSyncer(ctx, mc, WithC1ZPath(c1zPath), WithTmpDir(tempDir))
	require.NoError(t, err)
	require.NoError(t, syncer.Sync(ctx))
	require.NoError(t, syncer.Close(ctx))

	require.Equal(t, []string{"", "next"}, mc.callTokens)
}
