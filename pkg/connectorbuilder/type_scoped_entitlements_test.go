package connectorbuilder

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/sourcecache"
	"github.com/conductorone/baton-sdk/pkg/types/resource"
)

type typeScopedEntitlementsSyncer struct {
	testResourceSyncerV2Simple
	calls          int
	perResource    int
	deferredLookup bool
}

func (s *typeScopedEntitlementsSyncer) Entitlements(ctx context.Context, r *v2.Resource, opts resource.SyncOpAttrs) ([]*v2.Entitlement, *resource.SyncOpResults, error) {
	s.perResource++
	return s.testResourceSyncerV2Simple.Entitlements(ctx, r, opts)
}

func (s *typeScopedEntitlementsSyncer) EntitlementsForResourceType(ctx context.Context, resourceTypeID string, opts resource.SyncOpAttrs) ([]*v2.Entitlement, *resource.SyncOpResults, error) {
	s.calls++
	if s.deferredLookup {
		_, _, err := opts.SourceCache.Lookup(ctx, sourcecache.RowKindEntitlements, "ents/type")
		if err != nil {
			return nil, nil, err
		}
	}
	r := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: resourceTypeID, Resource: "g1"}.Build(),
	}.Build()
	return []*v2.Entitlement{
		v2.Entitlement_builder{
			Id:       "ent-type-1",
			Resource: r,
		}.Build(),
	}, &resource.SyncOpResults{}, nil
}

func typeScopedEntitlementsRequest(annos annotations.Annotations) *v2.EntitlementsServiceListEntitlementsRequest {
	return v2.EntitlementsServiceListEntitlementsRequest_builder{
		Resource: v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "group", Resource: "group"}.Build(),
		}.Build(),
		Annotations: annos,
	}.Build()
}

// annotatedNoInterfaceSyncer carries the TypeScopedEntitlements annotation
// on its resource type but does NOT implement TypeScopedEntitlementsSyncer.
type annotatedNoInterfaceSyncer struct {
	testResourceSyncerV2Simple
}

func (s *annotatedNoInterfaceSyncer) ResourceType(ctx context.Context) *v2.ResourceType {
	rt := s.testResourceSyncerV2Simple.ResourceType(ctx)
	rt.SetAnnotations(annotations.New(&v2.TypeScopedEntitlements{}))
	return rt
}

// TestNewConnector_TypeScopedAnnotationRequiresInterface pins the
// registration contract: an annotated type whose syncer lacks the
// matching TypeScoped*Syncer interface fails at NewConnector instead of
// dying mid-sync with InvalidArgument on the first type-scoped call.
func TestNewConnector_TypeScopedAnnotationRequiresInterface(t *testing.T) {
	ctx := context.Background()
	bad := &annotatedNoInterfaceSyncer{testResourceSyncerV2Simple{resourceType: "group"}}
	_, err := NewConnector(ctx, &testConnectorV2{resourceSyncers: []ResourceSyncerV2{bad}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "TypeScopedEntitlements annotation")
}

func TestListEntitlements_TypeScopedRoutes(t *testing.T) {
	ctx := context.Background()
	ts := &typeScopedEntitlementsSyncer{testResourceSyncerV2Simple: testResourceSyncerV2Simple{resourceType: "group"}}
	conn, err := NewConnector(ctx, &testConnectorV2{resourceSyncers: []ResourceSyncerV2{ts}})
	require.NoError(t, err)

	resp, err := conn.ListEntitlements(ctx, typeScopedEntitlementsRequest(annotations.New(&v2.TypeScopedEntitlements{})))
	require.NoError(t, err)
	require.Len(t, resp.GetList(), 1)
	require.Equal(t, "ent-type-1", resp.GetList()[0].GetId())
	require.Equal(t, 1, ts.calls)
	require.Zero(t, ts.perResource)
}

func TestListEntitlements_TypeScopedMissingInterface(t *testing.T) {
	ctx := context.Background()
	ts := &testResourceSyncerV2Simple{resourceType: "group"}
	conn, err := NewConnector(ctx, &testConnectorV2{resourceSyncers: []ResourceSyncerV2{ts}})
	require.NoError(t, err)

	_, err = conn.ListEntitlements(ctx, typeScopedEntitlementsRequest(annotations.New(&v2.TypeScopedEntitlements{})))
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestListEntitlements_UnmarkedStillPerResource(t *testing.T) {
	ctx := context.Background()
	ts := &typeScopedEntitlementsSyncer{testResourceSyncerV2Simple: testResourceSyncerV2Simple{resourceType: "group"}}
	conn, err := NewConnector(ctx, &testConnectorV2{resourceSyncers: []ResourceSyncerV2{ts}})
	require.NoError(t, err)

	resp, err := conn.ListEntitlements(ctx, typeScopedEntitlementsRequest(nil))
	require.NoError(t, err)
	require.Len(t, resp.GetList(), 1)
	require.Equal(t, "entitlement-1", resp.GetList()[0].GetId())
	require.Zero(t, ts.calls)
	require.Equal(t, 1, ts.perResource)
}

func TestListEntitlements_TypeScopedContinuationPreservesMarker(t *testing.T) {
	ctx := context.Background()
	ts := &typeScopedEntitlementsSyncer{
		testResourceSyncerV2Simple: testResourceSyncerV2Simple{resourceType: "group"},
		deferredLookup:             true,
	}
	conn, err := NewConnector(ctx, &testConnectorV2{resourceSyncers: []ResourceSyncerV2{ts}})
	require.NoError(t, err)

	offer := annotations.New(&v2.SourceCacheLookupOffer{}, &v2.TypeScopedEntitlements{})
	resp, err := conn.ListEntitlements(ctx, typeScopedEntitlementsRequest(offer))
	require.NoError(t, err)
	require.Empty(t, resp.GetList())
	respAnnos := annotations.Annotations(resp.GetAnnotations())
	require.True(t, respAnnos.Contains(&v2.SourceCacheLookupAsk{}))
	require.Equal(t, 1, ts.calls)

	answers := annotations.New(&v2.SourceCacheLookupOffer{}, &v2.TypeScopedEntitlements{})
	answers.Update(sourcecache.AnswersProto([]sourcecache.Answer{
		{Query: sourcecache.Query{RowKind: sourcecache.RowKindEntitlements, ScopeKey: "ents/type"}, Found: false},
	}))
	resp2, err := conn.ListEntitlements(ctx, typeScopedEntitlementsRequest(answers))
	require.NoError(t, err)
	require.Len(t, resp2.GetList(), 1)
	require.Equal(t, 2, ts.calls)
	require.Zero(t, ts.perResource)
}
