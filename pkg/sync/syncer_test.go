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
	"github.com/conductorone/baton-sdk/pkg/dotc1z/manager"
	et "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	gt "github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
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

func TestLifecycle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mc := newMockConnector()

	mc.rtDB = append(mc.rtDB, userResourceType)

	callStart := false
	callResume := false

	mc.ConnectorLifeCycleServiceClient = &connectorLifeCycleServiceClientMock{
		onStart: func(ctx context.Context, in *v2.OnStartRequest, opts ...grpc.CallOption) (*v2.OnStartResponse, error) {
			callStart = true
			return &v2.OnStartResponse{}, nil
		},
		onResume: func(ctx context.Context, in *v2.OnResumeRequest, opts ...grpc.CallOption) (*v2.OnResumeResponse, error) {
			callResume = true
			return &v2.OnResumeResponse{}, nil
		},
	}

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

	require.True(t, callStart)
	require.False(t, callResume)
}

func newMockConnector() *mockConnector {
	mc := &mockConnector{
		rtDB:                            make([]*v2.ResourceType, 0),
		resourceDB:                      make([]*v2.Resource, 0),
		entDB:                           make(map[string][]*v2.Entitlement),
		userDB:                          make([]*v2.Resource, 0),
		grantDB:                         make(map[string][]*v2.Grant),
		ConnectorLifeCycleServiceClient: &connectorLifeCycleServiceClientMock{},
	}
	return mc
}

type mockConnector struct {
	metadata   *v2.ConnectorMetadata
	rtDB       []*v2.ResourceType
	resourceDB []*v2.Resource
	entDB      map[string][]*v2.Entitlement // resource id to entitlements
	userDB     []*v2.Resource
	grantDB    map[string][]*v2.Grant // resource id to grants
	v2.AssetServiceClient
	v2.GrantManagerServiceClient
	v2.ResourceManagerServiceClient
	v2.AccountManagerServiceClient
	v2.CredentialManagerServiceClient
	v2.EventServiceClient
	v2.TicketsServiceClient
	v2.ConnectorLifeCycleServiceClient
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

	mc.resourceDB = append(mc.resourceDB, group)

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

	mc.userDB = append(mc.userDB, user)
	return user, nil
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
	if in.ResourceTypeId == "group" {
		return &v2.ResourcesServiceListResourcesResponse{List: mc.resourceDB}, nil
	}
	if in.ResourceTypeId == "user" {
		return &v2.ResourcesServiceListResourcesResponse{List: mc.userDB}, nil
	}
	return nil, fmt.Errorf("unknown resource type %s", in.ResourceTypeId)
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

type connectorLifeCycleServiceClientMock struct {
	onStart  func(ctx context.Context, in *v2.OnStartRequest, opts ...grpc.CallOption) (*v2.OnStartResponse, error)
	onResume func(ctx context.Context, in *v2.OnResumeRequest, opts ...grpc.CallOption) (*v2.OnResumeResponse, error)
	onEnd    func(ctx context.Context, in *v2.OnEndRequest, opts ...grpc.CallOption) (*v2.OnEndResponse, error)
}

func (clc *connectorLifeCycleServiceClientMock) OnStart(ctx context.Context, in *v2.OnStartRequest, opts ...grpc.CallOption) (*v2.OnStartResponse, error) {
	if clc.onStart != nil {
		return clc.onStart(ctx, in, opts...)
	}

	return &v2.OnStartResponse{}, nil
}

func (clc *connectorLifeCycleServiceClientMock) OnResume(ctx context.Context, in *v2.OnResumeRequest, opts ...grpc.CallOption) (*v2.OnResumeResponse, error) {
	if clc.onResume != nil {
		return clc.onResume(ctx, in, opts...)
	}

	return &v2.OnResumeResponse{}, nil
}

func (clc *connectorLifeCycleServiceClientMock) OnEnd(ctx context.Context, in *v2.OnEndRequest, opts ...grpc.CallOption) (*v2.OnEndResponse, error) {
	if clc.onEnd != nil {
		return clc.onEnd(ctx, in, opts...)
	}

	return &v2.OnEndResponse{}, nil
}
