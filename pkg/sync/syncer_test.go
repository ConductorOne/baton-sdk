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

func BenchmarkExpandCircle(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// create a loop of N entitlements
	circleSize := 9
	// with different principal + grants at each layer
	usersPerLayer := 100
	groupCount := 100

	mc := newMockConnector()

	groupResourceType := &v2.ResourceType{
		Id:          "group",
		DisplayName: "Group",
		Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_GROUP},
	}
	userResourceType := &v2.ResourceType{
		Id:          "user",
		DisplayName: "User",
		Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER},
		Annotations: annotations.New(&v2.SkipEntitlementsAndGrants{}),
	}
	mc.rtDB = append(mc.rtDB, groupResourceType, userResourceType)

	for i := 0; i < groupCount; i++ {
		groupId := "group_" + strconv.Itoa(i)
		group, err := rs.NewGroupResource(
			groupId,
			groupResourceType,
			groupId,
			[]rs.GroupTraitOption{},
		)
		require.NoError(b, err)
		mc.resourceDB = append(mc.resourceDB, group)

		ent := et.NewAssignmentEntitlement(
			group,
			"member",
			et.WithGrantableTo(groupResourceType, userResourceType),
		)
		ent.Slug = "member"
		mc.entDB[groupId] = append(mc.entDB[groupId], ent)

		childGroupId := "child_group_" + strconv.Itoa(i)
		childGroup, err := rs.NewGroupResource(
			childGroupId,
			groupResourceType,
			childGroupId,
			[]rs.GroupTraitOption{},
		)
		require.NoError(b, err)

		mc.resourceDB = append(mc.resourceDB, childGroup)

		childEnt := et.NewAssignmentEntitlement(
			childGroup,
			"member",
			et.WithGrantableTo(groupResourceType, userResourceType),
		)
		childEnt.Slug = "member"
		mc.entDB[childGroupId] = append(mc.entDB[childGroupId], childEnt)

		grant := gt.NewGrant(
			group,
			"member",
			childGroup,
			gt.WithAnnotation(&v2.GrantExpandable{
				EntitlementIds: []string{
					childEnt.Id,
				},
			}),
		)

		mc.grantDB[childGroupId] = append(mc.grantDB[childGroupId], grant)

		for j := 0; j < usersPerLayer; j++ {
			pid := "user_circle_" + strconv.Itoa(i*usersPerLayer+j)
			principal, err := rs.NewUserResource(
				pid,
				userResourceType,
				pid,
				[]rs.UserTraitOption{},
				rs.WithAnnotation(&v2.SkipEntitlementsAndGrants{}),
			)
			require.NoError(b, err)
			mc.userDB = append(mc.userDB, principal)

			grant := gt.NewGrant(
				group,
				"member",
				principal,
			)
			mc.grantDB[groupId] = append(mc.grantDB[groupId], grant)

			childGroupGrant := gt.NewGrant(
				childGroup,
				"member",
				principal,
			)
			mc.grantDB[childGroupId] = append(mc.grantDB[childGroupId], childGroupGrant)
		}
	}

	// create the circle
	for i := 0; i < circleSize; i++ {
		currentResource := mc.resourceDB[i]
		currentEnt := mc.entDB[currentResource.Id.Resource][0]
		nextResource := mc.resourceDB[(i+1)%circleSize] // Wrap around to the start for the last element
		nextEnt := mc.entDB[nextResource.Id.Resource][0]

		grant := gt.NewGrant(
			nextEnt.Resource,
			"member",
			currentEnt.Resource,
			gt.WithAnnotation(&v2.GrantExpandable{
				EntitlementIds: []string{
					currentEnt.Id,
				},
			}),
		)

		mc.grantDB[nextEnt.Resource.Id.Resource] = append(mc.grantDB[nextEnt.Resource.Id.Resource], grant)
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

func (mc *mockConnector) ListResourceTypes(context.Context, *v2.ResourceTypesServiceListResourceTypesRequest, ...grpc.CallOption) (*v2.ResourceTypesServiceListResourceTypesResponse, error) {
	return &v2.ResourceTypesServiceListResourceTypesResponse{List: mc.rtDB}, nil
}

func (mc *mockConnector) ListResources(ctx context.Context, in *v2.ResourcesServiceListResourcesRequest, opts ...grpc.CallOption) (*v2.ResourcesServiceListResourcesResponse, error) {
	all := make([]*v2.Resource, 0, len(mc.resourceDB)+len(mc.userDB))
	all = append(all, mc.resourceDB...)
	all = append(all, mc.userDB...)
	return &v2.ResourcesServiceListResourcesResponse{List: all}, nil
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
