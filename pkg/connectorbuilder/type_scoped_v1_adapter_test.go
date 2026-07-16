package connectorbuilder

// The V1→V2 resource-syncer adapter must FORWARD the optional
// type-scoped interfaces. Registration validates the ORIGINAL syncer
// (validateTypeScopedRegistration) but routing type-asserts the STORED
// value — before the composite adapter shapes in newResourceSyncerV1toV2,
// a V1 syncer implementing TypeScopedGrantsSyncer /
// TypeScopedEntitlementsSyncer passed construction and then failed every
// type-scoped call at runtime with InvalidArgument.

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

// typeScopedV1Syncer is a V1 ResourceSyncer (pagination.Token
// signatures) that also implements both type-scoped interfaces and
// carries both annotations on its resource type.
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
	r := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: resourceTypeID, Resource: "g1"}.Build(),
	}.Build()
	return []*v2.Grant{
		v2.Grant_builder{
			Id:          "grant-type-1",
			Entitlement: v2.Entitlement_builder{Id: "ent-1", Resource: r}.Build(),
			Principal:   v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build()}.Build(),
		}.Build(),
	}, &resource.SyncOpResults{}, nil
}

func (s *typeScopedV1Syncer) EntitlementsForResourceType(_ context.Context, resourceTypeID string, _ resource.SyncOpAttrs) ([]*v2.Entitlement, *resource.SyncOpResults, error) {
	s.entCalls++
	r := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: resourceTypeID, Resource: "g1"}.Build(),
	}.Build()
	return []*v2.Entitlement{
		v2.Entitlement_builder{Id: "ent-type-1", Resource: r}.Build(),
	}, &resource.SyncOpResults{}, nil
}

func typeScopedV1GrantsRequest(annos annotations.Annotations) *v2.GrantsServiceListGrantsRequest {
	return v2.GrantsServiceListGrantsRequest_builder{
		Resource: v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "group", Resource: "group"}.Build(),
		}.Build(),
		Annotations: annos,
	}.Build()
}

// TestTypeScopedV1SyncerRoutesThroughAdapter pins the adapter
// forwarding: a V1 syncer's type-scoped methods must be reachable
// through the stored (wrapped) syncer for both phases.
func TestTypeScopedV1SyncerRoutesThroughAdapter(t *testing.T) {
	ctx := context.Background()
	ts := &typeScopedV1Syncer{}
	conn, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{ts}))
	require.NoError(t, err, "a V1 type-scoped syncer must pass registration")

	gResp, err := conn.ListGrants(ctx, typeScopedV1GrantsRequest(annotations.New(&v2.TypeScopedGrants{})))
	require.NoError(t, err, "type-scoped grants routing must reach the V1 syncer through the adapter")
	require.Len(t, gResp.GetList(), 1)
	require.Equal(t, "grant-type-1", gResp.GetList()[0].GetId())
	require.Equal(t, 1, ts.grantCalls)

	eResp, err := conn.ListEntitlements(ctx, typeScopedEntitlementsRequest(annotations.New(&v2.TypeScopedEntitlements{})))
	require.NoError(t, err, "type-scoped entitlements routing must reach the V1 syncer through the adapter")
	require.Len(t, eResp.GetList(), 1)
	require.Equal(t, "ent-type-1", eResp.GetList()[0].GetId())
	require.Equal(t, 1, ts.entCalls)
}

// TestTypeScopedV1AdapterDoesNotOverclaim: a plain V1 syncer (no
// type-scoped interfaces) wrapped by the adapter must still FAIL the
// type-scoped routing assertions — the composite shapes must not make
// every adapted syncer claim the interfaces.
func TestTypeScopedV1AdapterDoesNotOverclaim(t *testing.T) {
	adapted := newResourceSyncerV1toV2(newTestResourceSyncer("group"))
	_, isTSG := adapted.(TypeScopedGrantsSyncer)
	require.False(t, isTSG)
	_, isTSE := adapted.(TypeScopedEntitlementsSyncer)
	require.False(t, isTSE)

	ctx := context.Background()
	conn, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{newTestResourceSyncer("group")}))
	require.NoError(t, err)
	_, err = conn.ListGrants(ctx, typeScopedV1GrantsRequest(annotations.New(&v2.TypeScopedGrants{})))
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}
