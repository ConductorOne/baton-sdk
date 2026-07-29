package connectorbuilder

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	"github.com/conductorone/baton-sdk/pkg/types/resource"
)

type testTypeScopedSyncer struct {
	testResourceSyncerV2Simple
	entitlementCalls         int
	grantCalls               int
	ordinaryEntitlementCalls int
	ordinaryGrantCalls       int
	lastEntitlementType      string
	lastGrantType            string
	lastEntitlementOpts      resource.SyncOpAttrs
	lastGrantOpts            resource.SyncOpAttrs
}

func (s *testTypeScopedSyncer) Entitlements(ctx context.Context, r *v2.Resource, opts resource.SyncOpAttrs) ([]*v2.Entitlement, *resource.SyncOpResults, error) {
	s.ordinaryEntitlementCalls++
	return s.testResourceSyncerV2Simple.Entitlements(ctx, r, opts)
}

func (s *testTypeScopedSyncer) Grants(ctx context.Context, r *v2.Resource, opts resource.SyncOpAttrs) ([]*v2.Grant, *resource.SyncOpResults, error) {
	s.ordinaryGrantCalls++
	return s.testResourceSyncerV2Simple.Grants(ctx, r, opts)
}

func (s *testTypeScopedSyncer) EntitlementsForResourceType(_ context.Context, resourceTypeID string, opts resource.SyncOpAttrs) ([]*v2.Entitlement, *resource.SyncOpResults, error) {
	s.entitlementCalls++
	s.lastEntitlementType = resourceTypeID
	s.lastEntitlementOpts = opts
	return []*v2.Entitlement{v2.Entitlement_builder{Id: "type-entitlement"}.Build()}, &resource.SyncOpResults{
		NextPageToken: "ent-next",
		Annotations:   annotations.New(v2.EnqueuePageTokens_builder{PageTokens: []string{"ent-sibling"}}.Build()),
	}, nil
}

func (s *testTypeScopedSyncer) GrantsForResourceType(_ context.Context, resourceTypeID string, opts resource.SyncOpAttrs) ([]*v2.Grant, *resource.SyncOpResults, error) {
	s.grantCalls++
	s.lastGrantType = resourceTypeID
	s.lastGrantOpts = opts
	return []*v2.Grant{v2.Grant_builder{Id: "type-grant"}.Build()}, &resource.SyncOpResults{
		NextPageToken: "grant-next",
		Annotations:   annotations.New(v2.EnqueuePageTokens_builder{PageTokens: []string{"grant-sibling"}}.Build()),
	}, nil
}

func typeScopedResource(resourceTypeID string) *v2.Resource {
	return v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: resourceTypeID, Resource: resourceTypeID}.Build(),
	}.Build()
}

func TestTypeScopedRequestsRouteToOptionalInterfaces(t *testing.T) {
	ctx := context.Background()
	s := &testTypeScopedSyncer{testResourceSyncerV2Simple: testResourceSyncerV2Simple{resourceType: "group"}}
	conn, err := NewConnector(ctx, &testConnectorV2{resourceSyncers: []ResourceSyncerV2{s}})
	require.NoError(t, err)

	entResp, err := conn.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{
		Resource:     typeScopedResource("group"),
		Annotations:  annotations.New(&v2.TypeScopedEntitlements{}),
		ActiveSyncId: "sync-1",
		PageToken:    "ent-page",
	}.Build())
	require.NoError(t, err)
	require.Len(t, entResp.GetList(), 1)
	require.Equal(t, 1, s.entitlementCalls)
	require.Equal(t, "group", s.lastEntitlementType)
	require.Equal(t, "sync-1", s.lastEntitlementOpts.SyncID)
	require.Equal(t, "ent-page", s.lastEntitlementOpts.PageToken.Token)
	require.Equal(t, "ent-next", entResp.GetNextPageToken())
	entRespAnnos := annotations.Annotations(entResp.GetAnnotations())
	require.True(t, entRespAnnos.Contains(&v2.EnqueuePageTokens{}))

	grantResp, err := conn.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{
		Resource:     typeScopedResource("group"),
		Annotations:  annotations.New(&v2.TypeScopedGrants{}),
		ActiveSyncId: "sync-1",
		PageToken:    "grant-page",
	}.Build())
	require.NoError(t, err)
	require.Len(t, grantResp.GetList(), 1)
	require.Equal(t, 1, s.grantCalls)
	require.Equal(t, "group", s.lastGrantType)
	require.Equal(t, "sync-1", s.lastGrantOpts.SyncID)
	require.Equal(t, "grant-page", s.lastGrantOpts.PageToken.Token)
	require.Equal(t, "grant-next", grantResp.GetNextPageToken())
	grantRespAnnos := annotations.Annotations(grantResp.GetAnnotations())
	require.True(t, grantRespAnnos.Contains(&v2.EnqueuePageTokens{}))
	require.Zero(t, s.ordinaryEntitlementCalls)
	require.Zero(t, s.ordinaryGrantCalls)
}

type annotatedWithoutTypeScopedInterface struct {
	testResourceSyncerV2Simple
	annotation annotations.Annotations
}

func (s *annotatedWithoutTypeScopedInterface) ResourceType(context.Context) *v2.ResourceType {
	return v2.ResourceType_builder{
		Id:          "group",
		DisplayName: "Group",
		Annotations: s.annotation,
	}.Build()
}

func TestTypeScopedAnnotationRequiresMatchingInterface(t *testing.T) {
	for _, tc := range []struct {
		name       string
		annotation annotations.Annotations
		want       string
	}{
		{"grants", annotations.New(&v2.TypeScopedGrants{}), "does not implement TypeScopedGrantsSyncer"},
		{"entitlements", annotations.New(&v2.TypeScopedEntitlements{}), "does not implement TypeScopedEntitlementsSyncer"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &annotatedWithoutTypeScopedInterface{
				testResourceSyncerV2Simple: testResourceSyncerV2Simple{resourceType: "group"},
				annotation:                 tc.annotation,
			}
			_, err := NewConnector(context.Background(), &testConnectorV2{resourceSyncers: []ResourceSyncerV2{s}})
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestTypeScopedMarkerRequiresMatchingInterfaceAtRuntime(t *testing.T) {
	ctx := context.Background()
	s := &testResourceSyncerV2Simple{resourceType: "group"}
	conn, err := NewConnector(ctx, &testConnectorV2{resourceSyncers: []ResourceSyncerV2{s}})
	require.NoError(t, err)

	_, err = conn.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{
		Resource:    typeScopedResource("group"),
		Annotations: annotations.New(&v2.TypeScopedEntitlements{}),
	}.Build())
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	_, err = conn.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{
		Resource:    typeScopedResource("group"),
		Annotations: annotations.New(&v2.TypeScopedGrants{}),
	}.Build())
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestTypeScopedInterfacesDoNotChangeUnmarkedRouting(t *testing.T) {
	ctx := context.Background()
	s := &testTypeScopedSyncer{testResourceSyncerV2Simple: testResourceSyncerV2Simple{resourceType: "group"}}
	conn, err := NewConnector(ctx, &testConnectorV2{resourceSyncers: []ResourceSyncerV2{s}})
	require.NoError(t, err)

	_, err = conn.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{
		Resource: typeScopedResource("group"),
	}.Build())
	require.NoError(t, err)
	_, err = conn.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{
		Resource: typeScopedResource("group"),
	}.Build())
	require.NoError(t, err)

	require.Equal(t, 1, s.ordinaryEntitlementCalls)
	require.Equal(t, 1, s.ordinaryGrantCalls)
	require.Zero(t, s.entitlementCalls)
	require.Zero(t, s.grantCalls)
}

func TestListEntitlementsRejectsNilResource(t *testing.T) {
	ctx := context.Background()
	s := &testResourceSyncerV2Simple{resourceType: "group"}
	conn, err := NewConnector(ctx, &testConnectorV2{resourceSyncers: []ResourceSyncerV2{s}})
	require.NoError(t, err)

	_, err = conn.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{}.Build())
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

type typeScopedV1Syncer struct {
	grantCalls int
	entCalls   int
}

func (s *typeScopedV1Syncer) ResourceType(context.Context) *v2.ResourceType {
	return v2.ResourceType_builder{
		Id:          "group",
		DisplayName: "Group",
		Annotations: annotations.New(&v2.TypeScopedGrants{}, &v2.TypeScopedEntitlements{}),
	}.Build()
}

func (s *typeScopedV1Syncer) List(context.Context, *v2.ResourceId, *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	return nil, "", nil, nil
}

func (s *typeScopedV1Syncer) Entitlements(context.Context, *v2.Resource, *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	return nil, "", nil, nil
}

func (s *typeScopedV1Syncer) Grants(context.Context, *v2.Resource, *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	return nil, "", nil, nil
}

func (s *typeScopedV1Syncer) GrantsForResourceType(_ context.Context, resourceTypeID string, _ resource.SyncOpAttrs) ([]*v2.Grant, *resource.SyncOpResults, error) {
	s.grantCalls++
	return []*v2.Grant{v2.Grant_builder{Id: "type-grant"}.Build()}, &resource.SyncOpResults{}, nil
}

func (s *typeScopedV1Syncer) EntitlementsForResourceType(_ context.Context, resourceTypeID string, _ resource.SyncOpAttrs) ([]*v2.Entitlement, *resource.SyncOpResults, error) {
	s.entCalls++
	return []*v2.Entitlement{v2.Entitlement_builder{Id: "type-entitlement"}.Build()}, &resource.SyncOpResults{}, nil
}

func TestTypeScopedV1AdapterForwardsOptionalInterfaces(t *testing.T) {
	ctx := context.Background()
	s := &typeScopedV1Syncer{}
	conn, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{s}))
	require.NoError(t, err)

	_, err = conn.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{
		Resource:    typeScopedResource("group"),
		Annotations: annotations.New(&v2.TypeScopedGrants{}),
	}.Build())
	require.NoError(t, err)
	_, err = conn.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{
		Resource:    typeScopedResource("group"),
		Annotations: annotations.New(&v2.TypeScopedEntitlements{}),
	}.Build())
	require.NoError(t, err)
	require.Equal(t, 1, s.grantCalls)
	require.Equal(t, 1, s.entCalls)
}

func TestTypeScopedV1AdapterDoesNotOverclaim(t *testing.T) {
	adapted := newResourceSyncerV1toV2(newTestResourceSyncer("group"))
	_, grants := adapted.(TypeScopedGrantsSyncer)
	_, entitlements := adapted.(TypeScopedEntitlementsSyncer)
	require.False(t, grants)
	require.False(t, entitlements)
}
