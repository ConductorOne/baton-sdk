package sync

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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 2500 * 4 = 10K - used to cause an infinite loop on pagition
	usersPerLayer := 2500
	groupCount := 5

	mc := newMockConnector()

	mc.rtDB = append(mc.rtDB, groupResourceType, userResourceType)
	type asdf struct {
		r *v2.Resource
		e *v2.Entitlement
	}
	groups := make([]*asdf, 0)
	for i := 0; i < groupCount; i++ {
		groupId := "group_" + strconv.Itoa(i)
		group, groupEnt, err := mc.AddGroup(ctx, groupId)
		for _, g := range groups {
			_ = mc.AddGroupMember(ctx, g.r, group, groupEnt)
		}
		groups = append(groups, &asdf{
			r: group,
			e: groupEnt,
		})
		require.NoError(t, err)

		for j := 0; j < usersPerLayer; j++ {
			pid := fmt.Sprintf("user_%d_%d_%d", i, usersPerLayer, j)
			principal, err := mc.AddUser(ctx, pid)
			require.NoError(t, err)

			_ = mc.AddGroupMember(ctx, group, principal)
		}
	}

	tempDir, err := os.MkdirTemp("", "baton-benchmark-expand-grants")
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

func TestInvalidResourceTypeFilter(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// create a loop of N entitlements
	circleSize := 7
	// with different principal + grants at each layer
	usersPerLayer := 100
	groupCount := 100

	mc := newMockConnector()

	mc.rtDB = append(mc.rtDB, groupResourceType, userResourceType)

	for i := 0; i < groupCount; i++ {
		groupId := "group_" + strconv.Itoa(i)
		group, _, err := mc.AddGroup(ctx, groupId)
		require.NoError(b, err)

		childGroupId := "child_group_" + strconv.Itoa(i)
		childGroup, childEnt, err := mc.AddGroup(ctx, childGroupId)
		require.NoError(b, err)

		_ = mc.AddGroupMember(ctx, group, childGroup, childEnt)

		for j := 0; j < usersPerLayer; j++ {
			pid := "user_" + strconv.Itoa(i*usersPerLayer+j)
			principal, err := mc.AddUser(ctx, pid)
			require.NoError(b, err)

			// This isn't needed because grant expansion will create this grant
			// _ = mc.AddGroupMember(ctx, group, principal)
			_ = mc.AddGroupMember(ctx, childGroup, principal)
		}
	}

	// create the circle
	for i := 0; i < circleSize; i += 1 {
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
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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

	batonIDs := []string{}
	for _, resource := range resources {
		batonId, err := bid.MakeBid(resource)
		require.NoError(t, err)
		batonIDs = append(batonIDs, batonId)
	}
	// Partial syncs should succeed in cases where a resource doesn't exist.
	batonIDs = append(batonIDs, "bid:r:group/non_existent_group")
	partialSyncer, err := NewSyncer(ctx, mc,
		WithC1ZPath(c1zPath),
		WithTmpDir(tempDir),
		WithTargetedSyncResourceIDs(batonIDs),
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

func TestPartialSyncBadIDs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tempDir, err := os.MkdirTemp("", "baton-partial-sync-test-bad-ids")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	c1zPath := filepath.Join(tempDir, "partial-sync-bad-ids.c1z")

	mc := newMockConnector()

	batonIDs := []string{
		"bid:r:bad_resource_id",
	}
	partialSyncer, err := NewSyncer(ctx, mc,
		WithC1ZPath(c1zPath),
		WithTmpDir(tempDir),
		WithTargetedSyncResourceIDs(batonIDs),
	)
	require.NoError(t, err)

	err = partialSyncer.Sync(ctx)
	require.Error(t, err)
	require.ErrorContains(t, err, "error parsing baton id 'bid:r:bad_resource_id'")
	err = partialSyncer.Close(ctx)
	require.NoError(t, err)

	c1zManager, err := manager.New(ctx, c1zPath)
	require.NoError(t, err)

	store, err := c1zManager.LoadC1Z(ctx)
	require.NoError(t, err)

	syncs, _, err := store.ListSyncRuns(ctx, "", 100)
	require.NoError(t, err)
	require.Equal(t, len(syncs), 0)
}

func TestPartialSyncSkipEntitlementsAndGrants(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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

	batonId, err := bid.MakeBid(group)
	require.NoError(t, err)
	batonIDs := []string{
		batonId,
	}
	partialSyncer, err := NewSyncer(ctx, mc,
		WithC1ZPath(c1zPath),
		WithTmpDir(tempDir),
		WithTargetedSyncResourceIDs(batonIDs),
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

func (mc *mockConnector) ListGrants(ctx context.Context, in *v2.GrantsServiceListGrantsRequest, opts ...grpc.CallOption) (*v2.GrantsServiceListGrantsResponse, error) {
	return v2.GrantsServiceListGrantsResponse_builder{List: mc.grantDB[in.GetResource().GetId().GetResource()]}.Build(), nil
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
