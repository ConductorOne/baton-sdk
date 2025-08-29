package connectorbuilder

import (
	"context"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	"github.com/stretchr/testify/require"
)

type testConnector struct {
	resourceSyncers []ResourceSyncer
}

func newTestConnector(resourceSyncers []ResourceSyncer) *testConnector {
	return &testConnector{resourceSyncers: resourceSyncers}
}

func (t *testConnector) Metadata(ctx context.Context) (*v2.ConnectorMetadata, error) {
	return &v2.ConnectorMetadata{
		DisplayName: "test-connector",
		Description: "A test connector",
	}, nil
}

func (t *testConnector) Validate(ctx context.Context) (annotations.Annotations, error) {
	return annotations.Annotations{}, nil
}

func (t *testConnector) ResourceSyncers(ctx context.Context) []ResourceSyncer {
	return t.resourceSyncers
}

type testResourceSyncer struct {
}

func (t *testResourceSyncer) Create(ctx context.Context, resource *v2.Resource) (*v2.Resource, annotations.Annotations, error) {
	return nil, nil, nil
}

func (t *testResourceSyncer) Delete(ctx context.Context, resourceId *v2.ResourceId, parentResourceID *v2.ResourceId) (annotations.Annotations, error) {
	return nil, nil
}

func (t *testResourceSyncer) ResourceType(ctx context.Context) *v2.ResourceType {
	return &v2.ResourceType{
		Id:          "test_resource",
		DisplayName: "Test Resource",
	}
}

func (t *testResourceSyncer) List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	return nil, "", nil, nil
}

func (t *testResourceSyncer) Entitlements(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	return nil, "", nil, nil
}

func (t *testResourceSyncer) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	return nil, "", nil, nil
}

func TestDeleteResourceV2(t *testing.T) {
	ctx := context.Background()

	rsSyncer := &testResourceSyncer{}
	rsID := &v2.ResourceId{
		ResourceType: rsSyncer.ResourceType(ctx).Id,
	}

	connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{rsSyncer}))
	require.NoError(t, err)

	_, err = connector.DeleteResource(ctx, &v2.DeleteResourceRequest{
		ResourceId: rsID,
	})
	require.NoError(t, err)

	_, err = connector.DeleteResourceV2(ctx, &v2.DeleteResourceV2Request{
		ResourceId: rsID,
	})
	require.NoError(t, err)
}
