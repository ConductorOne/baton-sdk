package test

import (
	"context"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	"github.com/stretchr/testify/require"
)

var mockResourceType = &v2.ResourceType{
	Id:          "mock_resource_type",
	Description: "mock resource type",
	DisplayName: "mock resource type",
	Traits: []v2.ResourceType_Trait{
		v2.ResourceType_TRAIT_UNSPECIFIED,
	},
}

type mockConnector struct{}

func (m mockConnector) Metadata(ctx context.Context) (*v2.ConnectorMetadata, error) {
	return &v2.ConnectorMetadata{}, nil
}

func (m mockConnector) Validate(ctx context.Context) (annotations.Annotations, error) {
	return annotations.Annotations{}, nil
}

func (m mockConnector) ResourceSyncers(ctx context.Context) []connectorbuilder.ResourceSyncer {
	return []connectorbuilder.ResourceSyncer{
		&mockResource{
			resourceType: mockResourceType,
			resources: []*v2.Resource{
				{
					Id: &v2.ResourceId{
						Resource:     "1",
						ResourceType: mockResourceType.Id,
					},
					DisplayName: "Mock Resource 1",
				},
			},
		},
	}
}

type mockResource struct {
	resourceType *v2.ResourceType
	resources    []*v2.Resource
}

func (m *mockResource) ResourceType(ctx context.Context) *v2.ResourceType {
	return m.resourceType
}

func (m *mockResource) List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	return m.resources, "", nil, nil
}

func (m *mockResource) Entitlements(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	return []*v2.Entitlement{}, "", nil, nil
}

func (m *mockResource) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	return []*v2.Grant{}, "", nil, nil
}

func TestIntegrationTestWrapper_Sync(t *testing.T) {
	ctx := context.Background()

	wrapper := NewIntegrationTestWrapper(ctx, t, &mockConnector{})
	require.NotNil(t, wrapper)

	err := wrapper.Sync(ctx)
	require.NoError(t, err)

	z := wrapper.LoadC1Z(ctx, t)

	t.Cleanup(func() {
		err := z.Close()
		require.NoError(t, err)
	})

	resources, err := z.ListResources(ctx, &v2.ResourcesServiceListResourcesRequest{})
	require.NoError(t, err)

	require.Len(t, resources.List, 1)
}
