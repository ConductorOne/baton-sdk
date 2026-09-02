package connectorbuilder

import (
	"context"
	"errors"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/stretchr/testify/require"
)

// testResourceDeleterAlreadyGone is a ResourceDeleter (V1) whose Delete
// reports that the addressed resource is already absent at the provider, per
// the ResourceDoesNotExist contract documented on
// ResourceDeleterLimited.Delete.
type testResourceDeleterAlreadyGone struct {
	ResourceSyncer
}

func newTestResourceDeleterAlreadyGone(resourceType string) ResourceDeleter {
	return &testResourceDeleterAlreadyGone{newTestResourceSyncer(resourceType)}
}

func (t *testResourceDeleterAlreadyGone) Delete(ctx context.Context, resourceId *v2.ResourceId) (annotations.Annotations, error) {
	return rs.ResourceDoesNotExistAnnotations(), nil
}

// testResourceDeleterV2AlreadyGone is the ResourceDeleterV2 analog of
// testResourceDeleterAlreadyGone.
type testResourceDeleterV2AlreadyGone struct {
	ResourceSyncerV2
}

func newTestResourceDeleterV2AlreadyGone(resourceType string) ResourceDeleterV2 {
	return &testResourceDeleterV2AlreadyGone{newTestResourceSyncerV2(resourceType)}
}

func (t *testResourceDeleterV2AlreadyGone) Delete(ctx context.Context, resourceId *v2.ResourceId, parentResourceID *v2.ResourceId) (annotations.Annotations, error) {
	return rs.ResourceDoesNotExistAnnotations(), nil
}

// testResourceDeleterV2Failing always fails, proving a genuine delete error
// (e.g. a mis-addressed resource) still surfaces as an error from the RPC
// handler instead of being swallowed by the already-gone contract.
type testResourceDeleterV2Failing struct {
	ResourceSyncerV2
}

func newTestResourceDeleterV2Failing(resourceType string) ResourceDeleterV2 {
	return &testResourceDeleterV2Failing{newTestResourceSyncerV2(resourceType)}
}

var errTestResourceDeleterV2Failing = errors.New("delete failed: provider rejected request")

func (t *testResourceDeleterV2Failing) Delete(ctx context.Context, resourceId *v2.ResourceId, parentResourceID *v2.ResourceId) (annotations.Annotations, error) {
	return nil, errTestResourceDeleterV2Failing
}

// TestDeleteResourceAlreadyGoneAnnotation proves that a V1 ResourceDeleter
// returning the ResourceDoesNotExist annotation with a nil error has that
// annotation preserved end-to-end through the builder's DeleteResource (V1)
// RPC handler.
func TestDeleteResourceAlreadyGoneAnnotation(t *testing.T) {
	ctx := context.Background()

	rsDeleter := newTestResourceDeleterAlreadyGone("test-resource")
	connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{rsDeleter}))
	require.NoError(t, err)

	deleteResp, err := connector.DeleteResource(ctx, v2.DeleteResourceRequest_builder{
		ResourceId: v2.ResourceId_builder{
			ResourceType: "test-resource",
			Resource:     "test-resource-1",
		}.Build(),
	}.Build())
	require.NoError(t, err)
	require.NotNil(t, deleteResp)

	annos := annotations.Annotations(deleteResp.GetAnnotations())
	require.True(t, annos.Contains(&v2.ResourceDoesNotExist{}))
}

// TestDeleteResourceV2AlreadyGoneAnnotation is the V2 analog of
// TestDeleteResourceAlreadyGoneAnnotation, invoked through the builder's
// DeleteResourceV2 RPC handler.
func TestDeleteResourceV2AlreadyGoneAnnotation(t *testing.T) {
	ctx := context.Background()

	rsDeleterV2 := newTestResourceDeleterV2AlreadyGone("test-resource")
	connector, err := NewConnector(ctx, newTestConnectorV2([]ResourceSyncerV2{rsDeleterV2}))
	require.NoError(t, err)

	deleteResp, err := connector.DeleteResourceV2(ctx, v2.DeleteResourceV2Request_builder{
		ResourceId: v2.ResourceId_builder{
			ResourceType: "test-resource",
			Resource:     "test-resource-1",
		}.Build(),
		ParentResourceId: v2.ResourceId_builder{
			ResourceType: "parent-resource",
			Resource:     "parent-1",
		}.Build(),
	}.Build())
	require.NoError(t, err)
	require.NotNil(t, deleteResp)

	annos := annotations.Annotations(deleteResp.GetAnnotations())
	require.True(t, annos.Contains(&v2.ResourceDoesNotExist{}))
}

// TestDeleteResourceNormalSuccessHasNoAlreadyGoneAnnotation proves that an
// ordinary successful delete (nil annotations, nil error) does NOT carry a
// ResourceDoesNotExist annotation, through both the V1 and V2 RPC handlers.
func TestDeleteResourceNormalSuccessHasNoAlreadyGoneAnnotation(t *testing.T) {
	ctx := context.Background()

	// V1 handler.
	rsDeleter := newTestResourceDeleter("test-resource")
	connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{rsDeleter}))
	require.NoError(t, err)

	deleteResp, err := connector.DeleteResource(ctx, v2.DeleteResourceRequest_builder{
		ResourceId: v2.ResourceId_builder{
			ResourceType: "test-resource",
			Resource:     "test-resource-1",
		}.Build(),
	}.Build())
	require.NoError(t, err)
	require.NotNil(t, deleteResp)
	annos := annotations.Annotations(deleteResp.GetAnnotations())
	require.False(t, annos.Contains(&v2.ResourceDoesNotExist{}))

	// V2 handler.
	rsDeleterV2 := newTestResourceDeleterV2("test-resource")
	connectorV2, err := NewConnector(ctx, newTestConnectorV2([]ResourceSyncerV2{rsDeleterV2}))
	require.NoError(t, err)

	deleteRespV2, err := connectorV2.DeleteResourceV2(ctx, v2.DeleteResourceV2Request_builder{
		ResourceId: v2.ResourceId_builder{
			ResourceType: "test-resource",
			Resource:     "test-resource-1",
		}.Build(),
		ParentResourceId: v2.ResourceId_builder{
			ResourceType: "parent-resource",
			Resource:     "parent-1",
		}.Build(),
	}.Build())
	require.NoError(t, err)
	require.NotNil(t, deleteRespV2)
	annosV2 := annotations.Annotations(deleteRespV2.GetAnnotations())
	require.False(t, annosV2.Contains(&v2.ResourceDoesNotExist{}))
}

// TestDeleteResourceV2ErrorSurfaces proves a non-nil error from Delete still
// surfaces as an error from the DeleteResourceV2 RPC handler rather than
// being masked by the already-gone contract.
func TestDeleteResourceV2ErrorSurfaces(t *testing.T) {
	ctx := context.Background()

	rsDeleterV2 := newTestResourceDeleterV2Failing("test-resource")
	connector, err := NewConnector(ctx, newTestConnectorV2([]ResourceSyncerV2{rsDeleterV2}))
	require.NoError(t, err)

	_, err = connector.DeleteResourceV2(ctx, v2.DeleteResourceV2Request_builder{
		ResourceId: v2.ResourceId_builder{
			ResourceType: "test-resource",
			Resource:     "test-resource-1",
		}.Build(),
		ParentResourceId: v2.ResourceId_builder{
			ResourceType: "parent-resource",
			Resource:     "parent-1",
		}.Build(),
	}.Build())
	require.Error(t, err)
	require.ErrorIs(t, err, errTestResourceDeleterV2Failing)
}

// TestResourceDoesNotExistAnnotations proves the pkg/types/resource
// discoverability helper returns annotations carrying the ResourceDoesNotExist
// marker connectors are expected to hand-roll otherwise.
func TestResourceDoesNotExistAnnotations(t *testing.T) {
	annos := rs.ResourceDoesNotExistAnnotations()
	require.True(t, annos.Contains(&v2.ResourceDoesNotExist{}))
}
