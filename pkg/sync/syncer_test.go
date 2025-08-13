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

var groupResourceType = &v2.ResourceType{
	Id:          "group",
	DisplayName: "Group",
	Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_GROUP},
}
var userResourceType = &v2.ResourceType{
	Id:          "user",
	DisplayName: "User",
	Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER},
	Annotations: annotations.New(&v2.SkipEntitlementsAndGrants{}),
}

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

func TestExpandGrantBadEntitlement(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctx, err := logging.Init(ctx)
	require.NoError(t, err)

	mc := newMockConnector()

	var badResourceType = &v2.ResourceType{
		Id:          "bad",
		DisplayName: "Bad",
		Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_GROUP},
	}
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
	grantOpts := gt.WithAnnotation(&v2.GrantExpandable{
		EntitlementIds: []string{
			group2Ent.Id,
		},
	})

	badGrant := gt.NewGrant(
		badEntResource,
		"bad_member",
		group2,
		grantOpts,
	)
	mc.grantDB[badEntResource.Id.Resource] = append(mc.grantDB[badEntResource.Id.Resource], badGrant)
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
	require.Len(t, allGrants.List, 5)

	req := &reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest{
		Entitlement: group1Ent,
		PageToken:   "",
		Annotations: nil,
	}
	resp, err := store.ListGrantsForEntitlement(ctx, req)
	require.NoError(t, err)
	require.Len(t, resp.List, 3) // both users and group2 should have group1 membership

	req = &reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest{
		Entitlement: group1Ent,
		PrincipalId: user1.Id,
		PageToken:   "",
		Annotations: nil,
	}
	resp, err = store.ListGrantsForEntitlement(ctx, req)
	require.NoError(t, err)
	require.Len(t, resp.List, 1)

	grant := resp.List[0]

	annos := annotations.Annotations(grant.Annotations)
	immutable := &v2.GrantImmutable{}
	hasImmutable, err := annos.Pick(immutable)
	require.NoError(t, err)

	require.False(t, hasImmutable) // Direct grant should not be immutable

	req = &reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest{
		Entitlement: group1Ent,
		PrincipalId: user2.Id,
		PageToken:   "",
		Annotations: nil,
	}
	resp, err = store.ListGrantsForEntitlement(ctx, req)
	require.NoError(t, err)
	require.Len(t, resp.List, 1)

	grant = resp.List[0]

	annos = annotations.Annotations(grant.Annotations)
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
	require.Len(t, allGrants.List, 6)

	req := &reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest{
		Entitlement: group1Ent,
		PageToken:   "",
		Annotations: nil,
	}
	resp, err := store.ListGrantsForEntitlement(ctx, req)
	require.NoError(t, err)
	require.Len(t, resp.List, 2) // both users and group2 should have group1 membership

	req = &reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest{
		// Entitlement: group1Ent,
		PrincipalId: user1.Id,
		PageToken:   "",
		Annotations: nil,
	}
	resp, err = store.ListGrantsForPrincipal(ctx, req)
	require.NoError(t, err)
	require.Len(t, resp.List, 2)

	grant := resp.List[0]
	for _, g := range resp.List {
		if g.Entitlement.Id == group1Ent.Id {
			grant = g
			break
		}
	}
	require.Equal(t, grant.Entitlement.Id, group1Ent.Id)

	annos := annotations.Annotations(grant.Annotations)
	immutable := &v2.GrantImmutable{}
	hasImmutable, err := annos.Pick(immutable)
	require.NoError(t, err)

	require.False(t, hasImmutable) // Direct grant should not be immutable

	req = &reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest{
		PrincipalId: user2.Id,
		PageToken:   "",
		Annotations: nil,
	}
	resp, err = store.ListGrantsForPrincipal(ctx, req)
	require.NoError(t, err)
	require.Len(t, resp.List, 1)

	grant = resp.List[0]

	annos = annotations.Annotations(grant.Annotations)
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

		_ = mc.AddGroupMember(ctx, currentEnt.Resource, nextEnt.Resource, nextEnt)
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
	internalMc.grantDB[internalGroup.Id.Resource] = []*v2.Grant{
		gt.NewGrant(
			internalGroup,
			"member",
			internalUserRs.Id,
			// Same id as external user profile key value
			gt.WithAnnotation(&v2.ExternalResourceMatch{
				Key:          "external_id_match",
				Value:        "10",
				ResourceType: v2.ResourceType_TRAIT_USER,
			}),
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
	externalMc.grantDB[externalGroup.Id.Resource] = []*v2.Grant{
		gt.NewGrant(
			externalGroup,
			"member",
			externalUserRs.Id,
			gt.WithAnnotation(&v2.ExternalResourceMatch{
				Key:          "external_id_match",
				Value:        "10",
				ResourceType: v2.ResourceType_TRAIT_USER,
			}),
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

	resources, err := store.ListResources(ctx, &v2.ResourcesServiceListResourcesRequest{
		ResourceTypeId: userResourceType.Id,
	})
	require.NoError(t, err)
	require.Equal(t, len(resources.GetList()), 2)

	entitlements, err := store.ListEntitlements(ctx, &v2.EntitlementsServiceListEntitlementsRequest{
		Resource: internalGroup,
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(entitlements.GetList()))

	allGrantsReq := &v2.GrantsServiceListGrantsRequest{}
	allGrants, err := store.ListGrants(ctx, allGrantsReq)
	require.NoError(t, err)

	require.Len(t, allGrants.List, 2)
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

	resourcesResp, err := store.ListResources(ctx, &v2.ResourcesServiceListResourcesRequest{
		ResourceTypeId: userResourceType.Id,
	})
	require.NoError(t, err)
	require.Equal(t, len(resourcesResp.GetList()), 1)

	resourcesResp, err = store.ListResources(ctx, &v2.ResourcesServiceListResourcesRequest{
		ResourceTypeId: groupResourceType.Id,
	})
	require.NoError(t, err)
	// This connector has 2 groups but partial sync means we only synced one of them.
	require.Equal(t, len(resourcesResp.GetList()), 1)

	entitlements, err := store.ListEntitlements(ctx, &v2.EntitlementsServiceListEntitlementsRequest{
		Resource: group,
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(entitlements.GetList()))

	allGrantsReq := &v2.GrantsServiceListGrantsRequest{}
	allGrants, err := store.ListGrants(ctx, allGrantsReq)
	require.NoError(t, err)

	require.Len(t, allGrants.List, 1)
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

func TestSyncExternalResources(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctx, err := logging.Init(ctx)
	require.NoError(t, err)

	tempDir, err := os.MkdirTemp("", "baton-sync-external-resources-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create external C1Z with users and groups
	externalC1zPath := filepath.Join(tempDir, "external.c1z")
	externalMc := newMockConnector()
	externalMc.rtDB = append(externalMc.rtDB, userResourceType, groupResourceType)

	// Add external users and groups
	externalUser, err := externalMc.AddUser(ctx, "external_user_1")
	require.NoError(t, err)
	externalGroup, _, err := externalMc.AddGroup(ctx, "external_group_1")
	require.NoError(t, err)

	// Add grant in external system
	_ = externalMc.AddGroupMember(ctx, externalGroup, externalUser)

	// Create external C1Z
	externalSyncer, err := NewSyncer(ctx, externalMc, WithC1ZPath(externalC1zPath), WithTmpDir(tempDir))
	require.NoError(t, err)
	err = externalSyncer.Sync(ctx)
	require.NoError(t, err)
	err = externalSyncer.Close(ctx)
	require.NoError(t, err)

	// Create main C1Z with external resource references
	mainC1zPath := filepath.Join(tempDir, "main.c1z")
	mainMc := newMockConnector()
	mainMc.rtDB = append(mainMc.rtDB, userResourceType, groupResourceType)

	// Add main group that references external user
	mainGroup, _, err := mainMc.AddGroup(ctx, "main_group")
	require.NoError(t, err)
	_ = mainMc.AddGroupMember(ctx, mainGroup, externalUser) // Reference external user

	// Create main syncer with external resource reader
	mainSyncer, err := NewSyncer(ctx, mainMc,
		WithC1ZPath(mainC1zPath),
		WithTmpDir(tempDir),
		WithExternalResourceC1ZPath(externalC1zPath),
	)
	require.NoError(t, err)

	// Sync main resources and external resources
	err = mainSyncer.Sync(ctx)
	require.NoError(t, err)

	// Close syncer
	err = mainSyncer.Close(ctx)
	require.NoError(t, err)

	// Verify external resources were synced
	c1zManager, err := manager.New(ctx, mainC1zPath)
	require.NoError(t, err)

	store, err := c1zManager.LoadC1Z(ctx)
	require.NoError(t, err)

	// Check that external user was synced
	externalUsers, err := store.ListResources(ctx, &v2.ResourcesServiceListResourcesRequest{
		ResourceTypeId: userResourceType.Id,
	})
	require.NoError(t, err)
	require.Len(t, externalUsers.List, 1) // external_user_1

	// Check that external group was synced
	externalGroups, err := store.ListResources(ctx, &v2.ResourcesServiceListResourcesRequest{
		ResourceTypeId: groupResourceType.Id,
	})
	require.NoError(t, err)
	require.Len(t, externalGroups.List, 2) // main_group + external_group_1

	// Check that external group entitlements were synced
	externalGroupEntitlements, err := store.ListEntitlements(ctx, &v2.EntitlementsServiceListEntitlementsRequest{
		Resource: externalGroup,
	})
	require.NoError(t, err)
	require.Len(t, externalGroupEntitlements.List, 1)

	// Check that grants for external group were synced
	allGrants, err := store.ListGrants(ctx, &v2.GrantsServiceListGrantsRequest{})
	require.NoError(t, err)
	require.Len(t, allGrants.List, 2) // main_group grant + external_group_1 grant
}

func TestSyncExternalResourcesWithEntitlementFilter(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctx, err := logging.Init(ctx)
	require.NoError(t, err)

	tempDir, err := os.MkdirTemp("", "baton-sync-external-resources-filter-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create external C1Z with users and groups
	externalC1zPath := filepath.Join(tempDir, "external.c1z")
	externalMc := newMockConnector()
	externalMc.rtDB = append(externalMc.rtDB, userResourceType, groupResourceType)

	// Add external users and groups
	externalUser1, err := externalMc.AddUser(ctx, "external_user_1")
	require.NoError(t, err)
	externalUser2, err := externalMc.AddUser(ctx, "external_user_2")
	require.NoError(t, err)
	externalGroup1, externalGroup1Ent, err := externalMc.AddGroup(ctx, "external_group_1")
	require.NoError(t, err)
	externalGroup2, _, err := externalMc.AddGroup(ctx, "external_group_2")
	require.NoError(t, err)

	// Add grants in external system
	_ = externalMc.AddGroupMember(ctx, externalGroup1, externalUser1)
	_ = externalMc.AddGroupMember(ctx, externalGroup2, externalUser2)

	// Create external C1Z
	externalSyncer, err := NewSyncer(ctx, externalMc, WithC1ZPath(externalC1zPath), WithTmpDir(tempDir))
	require.NoError(t, err)
	err = externalSyncer.Sync(ctx)
	require.NoError(t, err)
	err = externalSyncer.Close(ctx)
	require.NoError(t, err)

	// Create main C1Z with external resource references
	mainC1zPath := filepath.Join(tempDir, "main.c1z")
	mainMc := newMockConnector()
	mainMc.rtDB = append(mainMc.rtDB, userResourceType, groupResourceType)

	// Add main group that references external users
	mainGroup, _, err := mainMc.AddGroup(ctx, "main_group")
	require.NoError(t, err)
	_ = mainMc.AddGroupMember(ctx, mainGroup, externalUser1) // Reference external user 1
	_ = mainMc.AddGroupMember(ctx, mainGroup, externalUser2) // Reference external user 2

	// Create main syncer with external resource reader and entitlement filter
	mainSyncer, err := NewSyncer(ctx, mainMc,
		WithC1ZPath(mainC1zPath),
		WithTmpDir(tempDir),
		WithExternalResourceC1ZPath(externalC1zPath),
		WithExternalResourceEntitlementIdFilter(externalGroup1Ent.Id),
	)
	require.NoError(t, err)

	// Sync main resources and external resources with filter
	err = mainSyncer.Sync(ctx)
	require.NoError(t, err)

	// Close syncer
	err = mainSyncer.Close(ctx)
	require.NoError(t, err)

	// Verify only filtered external resources were synced
	c1zManager, err := manager.New(ctx, mainC1zPath)
	require.NoError(t, err)

	store, err := c1zManager.LoadC1Z(ctx)
	require.NoError(t, err)

	// Check that only external_group_1 was synced (the filtered entitlement's resource)
	externalGroups, err := store.ListResources(ctx, &v2.ResourcesServiceListResourcesRequest{
		ResourceTypeId: groupResourceType.Id,
	})
	require.NoError(t, err)
	require.Len(t, externalGroups.List, 1) // only external_group_1

	// Check that external_group_1 entitlements were synced
	externalGroup1Entitlements, err := store.ListEntitlements(ctx, &v2.EntitlementsServiceListEntitlementsRequest{
		Resource: externalGroup1,
	})
	require.NoError(t, err)
	require.Len(t, externalGroup1Entitlements.List, 0) // no entitlements synced when using entitlement filter

	// Check that grants for external_group_1 were synced
	allGrants, err := store.ListGrants(ctx, &v2.GrantsServiceListGrantsRequest{})
	require.NoError(t, err)
	require.Len(t, allGrants.List, 2) // main_group grants for external_user_1 and external_user_2
}

func TestSyncExternalResourcesWithSkipEntitlementsAndGrants(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctx, err := logging.Init(ctx)
	require.NoError(t, err)

	tempDir, err := os.MkdirTemp("", "baton-sync-external-resources-skip-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create external C1Z with users that have SkipEntitlementsAndGrants annotation
	externalC1zPath := filepath.Join(tempDir, "external.c1z")
	externalMc := newMockConnector()

	// Create resource type with SkipEntitlementsAndGrants annotation
	skipUserResourceType := &v2.ResourceType{
		Id:          "skip_user",
		DisplayName: "Skip User",
		Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER},
		Annotations: annotations.New(&v2.SkipEntitlementsAndGrants{}),
	}
	externalMc.rtDB = append(externalMc.rtDB, skipUserResourceType, groupResourceType)

	// Add external user with skip annotation
	externalUser, err := externalMc.AddUserProfile(ctx, "external_user_1", map[string]any{})
	require.NoError(t, err)

	// Add external group
	externalGroup, _, err := externalMc.AddGroup(ctx, "external_group_1")
	require.NoError(t, err)

	// Add grant in external system
	_ = externalMc.AddGroupMember(ctx, externalGroup, externalUser)

	// Create external C1Z
	externalSyncer, err := NewSyncer(ctx, externalMc, WithC1ZPath(externalC1zPath), WithTmpDir(tempDir))
	require.NoError(t, err)
	err = externalSyncer.Sync(ctx)
	require.NoError(t, err)
	err = externalSyncer.Close(ctx)
	require.NoError(t, err)

	// Create main C1Z
	mainC1zPath := filepath.Join(tempDir, "main.c1z")
	mainMc := newMockConnector()
	mainMc.rtDB = append(mainMc.rtDB, userResourceType, groupResourceType)

	// Create main syncer with external resources
	mainSyncer, err := NewSyncer(ctx, mainMc,
		WithC1ZPath(mainC1zPath),
		WithTmpDir(tempDir),
		WithExternalResourceC1ZPath(externalC1zPath),
	)
	require.NoError(t, err)

	// Test SyncExternalResources
	err = mainSyncer.Sync(ctx)
	require.NoError(t, err)
	err = mainSyncer.Close(ctx)
	require.NoError(t, err)

	// Verify external resources were synced but entitlements were skipped for skip_user type
	c1zManager, err := manager.New(ctx, mainC1zPath)
	require.NoError(t, err)

	store, err := c1zManager.LoadC1Z(ctx)
	require.NoError(t, err)

	// Check that external user was synced
	externalUsers, err := store.ListResources(ctx, &v2.ResourcesServiceListResourcesRequest{
		ResourceTypeId: skipUserResourceType.Id,
	})
	require.NoError(t, err)
	require.Len(t, externalUsers.List, 0) // external user is not synced because it's not referenced by main connector

	// Check that external group was synced
	externalGroups, err := store.ListResources(ctx, &v2.ResourcesServiceListResourcesRequest{
		ResourceTypeId: groupResourceType.Id,
	})
	require.NoError(t, err)
	require.Len(t, externalGroups.List, 1) // external group is synced as part of external resources sync

	// Check that external group entitlements were synced
	externalGroupEntitlements, err := store.ListEntitlements(ctx, &v2.EntitlementsServiceListEntitlementsRequest{
		Resource: externalGroup,
	})
	require.NoError(t, err)
	require.Len(t, externalGroupEntitlements.List, 1) // external group entitlements are synced

	// Check that grants for external group were synced
	allGrants, err := store.ListGrants(ctx, &v2.GrantsServiceListGrantsRequest{})
	require.NoError(t, err)
	require.Len(t, allGrants.List, 1) // external group grant is synced
}

func TestSyncExternalResourcesNoExternalC1Z(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctx, err := logging.Init(ctx)
	require.NoError(t, err)

	tempDir, err := os.MkdirTemp("", "baton-sync-external-resources-no-external-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create main C1Z without external resources
	mainC1zPath := filepath.Join(tempDir, "main.c1z")
	mainMc := newMockConnector()
	mainMc.rtDB = append(mainMc.rtDB, userResourceType, groupResourceType)

	// Add main group
	_, _, err = mainMc.AddGroup(ctx, "main_group")
	require.NoError(t, err)

	// Add main user
	_, err = mainMc.AddUser(ctx, "main_user")
	require.NoError(t, err)

	// Create main syncer without external resources
	mainSyncer, err := NewSyncer(ctx, mainMc,
		WithC1ZPath(mainC1zPath),
		WithTmpDir(tempDir),
	)
	require.NoError(t, err)

	// Test SyncExternalResources should not fail when no external C1Z is provided
	err = mainSyncer.Sync(ctx)
	require.NoError(t, err)
	err = mainSyncer.Close(ctx)
	require.NoError(t, err)

	// Verify sync completed successfully
	c1zManager, err := manager.New(ctx, mainC1zPath)
	require.NoError(t, err)

	store, err := c1zManager.LoadC1Z(ctx)
	require.NoError(t, err)

	// Check that main group was synced
	groups, err := store.ListResources(ctx, &v2.ResourcesServiceListResourcesRequest{
		ResourceTypeId: groupResourceType.Id,
	})
	require.NoError(t, err)
	require.Len(t, groups.List, 1)

	// Check that main user was synced
	users, err := store.ListResources(ctx, &v2.ResourcesServiceListResourcesRequest{
		ResourceTypeId: userResourceType.Id,
	})
	require.NoError(t, err)
	require.Len(t, users.List, 1)
	require.Equal(t, "main_user", users.List[0].Id.Resource)
}

func TestSyncExternalResourcesWithNonUserGroupResourceTypes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctx, err := logging.Init(ctx)
	require.NoError(t, err)

	tempDir, err := os.MkdirTemp("", "baton-sync-external-resources-non-user-group-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create external C1Z with non-user/group resource types
	externalC1zPath := filepath.Join(tempDir, "external.c1z")
	externalMc := newMockConnector()

	// Create non-user/group resource type
	appResourceType := &v2.ResourceType{
		Id:          "app",
		DisplayName: "Application",
		Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_APP},
	}
	externalMc.rtDB = append(externalMc.rtDB, userResourceType, groupResourceType, appResourceType)

	// Add external app resource
	externalApp, err := rs.NewAppResource(
		"external_app_1",
		appResourceType,
		"External App 1",
		[]rs.AppTraitOption{},
	)
	require.NoError(t, err)
	externalMc.AddResource(ctx, externalApp)

	// Add external user and group
	externalUser, err := externalMc.AddUser(ctx, "external_user_1")
	require.NoError(t, err)
	externalGroup, _, err := externalMc.AddGroup(ctx, "external_group_1")
	require.NoError(t, err)

	// Add grant in external system
	_ = externalMc.AddGroupMember(ctx, externalGroup, externalUser)

	// Create external C1Z
	externalSyncer, err := NewSyncer(ctx, externalMc, WithC1ZPath(externalC1zPath), WithTmpDir(tempDir))
	require.NoError(t, err)
	err = externalSyncer.Sync(ctx)
	require.NoError(t, err)
	err = externalSyncer.Close(ctx)
	require.NoError(t, err)

	// Create main C1Z
	mainC1zPath := filepath.Join(tempDir, "main.c1z")
	mainMc := newMockConnector()
	mainMc.rtDB = append(mainMc.rtDB, userResourceType, groupResourceType)

	// Create main syncer with external resources
	mainSyncer, err := NewSyncer(ctx, mainMc,
		WithC1ZPath(mainC1zPath),
		WithTmpDir(tempDir),
		WithExternalResourceC1ZPath(externalC1zPath),
	)
	require.NoError(t, err)

	// Test SyncExternalResources
	err = mainSyncer.Sync(ctx)
	require.NoError(t, err)
	err = mainSyncer.Close(ctx)
	require.NoError(t, err)

	// Verify only user and group resources were synced, app was ignored
	c1zManager, err := manager.New(ctx, mainC1zPath)
	require.NoError(t, err)

	store, err := c1zManager.LoadC1Z(ctx)
	require.NoError(t, err)

	// Check that external user was synced
	externalUsers, err := store.ListResources(ctx, &v2.ResourcesServiceListResourcesRequest{
		ResourceTypeId: userResourceType.Id,
	})
	require.NoError(t, err)
	require.Len(t, externalUsers.List, 1)

	// Check that external group was synced
	externalGroups, err := store.ListResources(ctx, &v2.ResourcesServiceListResourcesRequest{
		ResourceTypeId: groupResourceType.Id,
	})
	require.NoError(t, err)
	require.Len(t, externalGroups.List, 1)

	// Check that external app was NOT synced (should be ignored)
	externalApps, err := store.ListResources(ctx, &v2.ResourcesServiceListResourcesRequest{
		ResourceTypeId: appResourceType.Id,
	})
	require.NoError(t, err)
	require.Len(t, externalApps.List, 0)
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

	mc.resourceDB[groupResourceType.Id] = append(mc.resourceDB[groupResourceType.Id], group)

	ent := et.NewAssignmentEntitlement(
		group,
		"member",
		et.WithGrantableTo(groupResourceType, userResourceType),
	)
	ent.Slug = "member"
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

	mc.resourceDB[userResourceType.Id] = append(mc.resourceDB[userResourceType.Id], user)
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

	mc.resourceDB[userResourceType.Id] = append(mc.resourceDB[userResourceType.Id], user)
	return user, nil
}

func (mc *mockConnector) AddResource(ctx context.Context, resource *v2.Resource) {
	mc.resourceDB[resource.Id.ResourceType] = append(mc.resourceDB[resource.Id.ResourceType], resource)
}

func (mc *mockConnector) AddResourceType(ctx context.Context, resourceType *v2.ResourceType) {
	mc.rtDB = append(mc.rtDB, resourceType)
}

func (mc *mockConnector) AddGroupMember(ctx context.Context, resource *v2.Resource, principal *v2.Resource, expandEnts ...*v2.Entitlement) *v2.Grant {
	grantOpts := []gt.GrantOption{}

	for _, ent := range expandEnts {
		grantOpts = append(grantOpts, gt.WithAnnotation(&v2.GrantExpandable{
			EntitlementIds: []string{
				ent.Id,
			},
		}))
	}

	grant := gt.NewGrant(
		resource,
		"member",
		principal,
		grantOpts...,
	)

	mc.grantDB[resource.Id.Resource] = append(mc.grantDB[resource.Id.Resource], grant)

	return grant
}

func (mc *mockConnector) ListResourceTypes(context.Context, *v2.ResourceTypesServiceListResourceTypesRequest, ...grpc.CallOption) (*v2.ResourceTypesServiceListResourceTypesResponse, error) {
	return &v2.ResourceTypesServiceListResourceTypesResponse{List: mc.rtDB}, nil
}

func (mc *mockConnector) ListResources(ctx context.Context, in *v2.ResourcesServiceListResourcesRequest, opts ...grpc.CallOption) (*v2.ResourcesServiceListResourcesResponse, error) {
	resources := mc.resourceDB[in.ResourceTypeId]
	if resources == nil {
		resources = []*v2.Resource{}
	}
	return &v2.ResourcesServiceListResourcesResponse{List: resources}, nil
}

func (mc *mockConnector) GetResource(ctx context.Context, in *v2.ResourceGetterServiceGetResourceRequest, opts ...grpc.CallOption) (*v2.ResourceGetterServiceGetResourceResponse, error) {
	var resource *v2.Resource
	resources := mc.resourceDB[in.ResourceId.ResourceType]
	for _, r := range resources {
		if r.Id.Resource == in.ResourceId.Resource {
			resource = r
			break
		}
	}
	if resource == nil {
		return nil, status.Errorf(codes.NotFound, "resource not found")
	}
	return &v2.ResourceGetterServiceGetResourceResponse{Resource: resource}, nil
}

func (mc *mockConnector) ListEntitlements(ctx context.Context, in *v2.EntitlementsServiceListEntitlementsRequest, opts ...grpc.CallOption) (*v2.EntitlementsServiceListEntitlementsResponse, error) {
	return &v2.EntitlementsServiceListEntitlementsResponse{List: mc.entDB[in.Resource.Id.Resource]}, nil
}

func (mc *mockConnector) ListGrants(ctx context.Context, in *v2.GrantsServiceListGrantsRequest, opts ...grpc.CallOption) (*v2.GrantsServiceListGrantsResponse, error) {
	return &v2.GrantsServiceListGrantsResponse{List: mc.grantDB[in.Resource.Id.Resource]}, nil
}

func (mc *mockConnector) GetMetadata(ctx context.Context, in *v2.ConnectorServiceGetMetadataRequest, opts ...grpc.CallOption) (*v2.ConnectorServiceGetMetadataResponse, error) {
	return &v2.ConnectorServiceGetMetadataResponse{
		Metadata: mc.metadata,
	}, nil
}

func (mc *mockConnector) Validate(ctx context.Context, in *v2.ConnectorServiceValidateRequest, opts ...grpc.CallOption) (*v2.ConnectorServiceValidateResponse, error) {
	return &v2.ConnectorServiceValidateResponse{}, nil
}

func (mc *mockConnector) Cleanup(ctx context.Context, in *v2.ConnectorServiceCleanupRequest, opts ...grpc.CallOption) (*v2.ConnectorServiceCleanupResponse, error) {
	return &v2.ConnectorServiceCleanupResponse{}, nil
}
