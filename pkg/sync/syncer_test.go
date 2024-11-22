package sync

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
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

func BenchmarkExpandCircle(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// create a loop of N entitlements
	circleSize := 10
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

			_ = mc.AddGroupMember(ctx, group, principal)
			_ = mc.AddGroupMember(ctx, childGroup, principal)
		}
	}

	// create the circle
	for i := 0; i < circleSize; i++ {
		currentResource := mc.resourceDB[i]
		currentEnt := mc.entDB[currentResource.Id.Resource][0]
		nextResource := mc.resourceDB[(i+1)%circleSize] // Wrap around to the start for the last element
		nextEnt := mc.entDB[nextResource.Id.Resource][0]

		_ = mc.AddGroupMember(ctx, nextEnt.Resource, currentEnt.Resource, currentEnt)
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

func newMockConnector() *mockConnector {
	mc := &mockConnector{
		rtDB:       make([]*v2.ResourceType, 0),
		resourceDB: make([]*v2.Resource, 0),
		entDB:      make(map[string][]*v2.Entitlement),
		userDB:     make([]*v2.Resource, 0),
		grantDB:    make(map[string][]*v2.Grant),
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
