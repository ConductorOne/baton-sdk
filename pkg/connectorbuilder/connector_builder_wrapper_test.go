package connectorbuilder

import (
	"context"
	"errors"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	"github.com/conductorone/baton-sdk/pkg/session"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockResourceSyncer2 is a mock implementation of ResourceSyncer2 for testing
type mockResourceSyncer2 struct {
	resourceType     *v2.ResourceType
	listFunc         func(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token, opts types.ResourceSyncerOptions) ([]*v2.Resource, string, annotations.Annotations, error)
	entitlementsFunc func(ctx context.Context, resource *v2.Resource, pToken *pagination.Token, opts types.ResourceSyncerOptions) ([]*v2.Entitlement, string, annotations.Annotations, error)
	grantsFunc       func(ctx context.Context, resource *v2.Resource, pToken *pagination.Token, opts types.ResourceSyncerOptions) ([]*v2.Grant, string, annotations.Annotations, error)
}

func (m *mockResourceSyncer2) ResourceType(ctx context.Context) *v2.ResourceType {
	return m.resourceType
}

func (m *mockResourceSyncer2) List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token, opts types.ResourceSyncerOptions) ([]*v2.Resource, string, annotations.Annotations, error) {
	if m.listFunc != nil {
		return m.listFunc(ctx, parentResourceID, pToken, opts)
	}
	return []*v2.Resource{}, "", nil, nil
}

func (m *mockResourceSyncer2) Entitlements(ctx context.Context, resource *v2.Resource, pToken *pagination.Token, opts types.ResourceSyncerOptions) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	if m.entitlementsFunc != nil {
		return m.entitlementsFunc(ctx, resource, pToken, opts)
	}
	return []*v2.Entitlement{}, "", nil, nil
}

func (m *mockResourceSyncer2) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token, opts types.ResourceSyncerOptions) ([]*v2.Grant, string, annotations.Annotations, error) {
	if m.grantsFunc != nil {
		return m.grantsFunc(ctx, resource, pToken, opts)
	}
	return []*v2.Grant{}, "", nil, nil
}

// mockConnectorBuilder2 is a mock implementation of ConnectorBuilder2 for testing
type mockConnectorBuilder2 struct {
	metadataFunc    func(ctx context.Context) (*v2.ConnectorMetadata, error)
	validateFunc    func(ctx context.Context) (annotations.Annotations, error)
	resourceSyncers []ResourceSyncer2
}

func (m *mockConnectorBuilder2) Metadata(ctx context.Context) (*v2.ConnectorMetadata, error) {
	if m.metadataFunc != nil {
		return m.metadataFunc(ctx)
	}
	return &v2.ConnectorMetadata{
		DisplayName: "Test Connector",
		Description: "Test Connector Description",
	}, nil
}

func (m *mockConnectorBuilder2) Validate(ctx context.Context) (annotations.Annotations, error) {
	if m.validateFunc != nil {
		return m.validateFunc(ctx)
	}
	return annotations.Annotations{}, nil
}

func (m *mockConnectorBuilder2) ResourceSyncers(ctx context.Context) []ResourceSyncer2 {
	return m.resourceSyncers
}

func TestConnectorBuilder2Wrapper(t *testing.T) {
	t.Run("Metadata passes through", func(t *testing.T) {
		expectedMetadata := &v2.ConnectorMetadata{
			DisplayName: "Test Connector",
			Description: "Test Connector Description",
		}

		mockBuilder2 := &mockConnectorBuilder2{
			metadataFunc: func(ctx context.Context) (*v2.ConnectorMetadata, error) {
				return expectedMetadata, nil
			},
		}

		wrapper := NewConnectorBuilder2Wrapper(mockBuilder2, nil)

		result, err := wrapper.Metadata(context.Background())
		require.NoError(t, err)
		assert.Equal(t, expectedMetadata, result)
	})

	t.Run("Validate passes through", func(t *testing.T) {
		expectedAnnotations := annotations.Annotations{}

		mockBuilder2 := &mockConnectorBuilder2{
			validateFunc: func(ctx context.Context) (annotations.Annotations, error) {
				return expectedAnnotations, nil
			},
		}

		wrapper := NewConnectorBuilder2Wrapper(mockBuilder2, nil)

		result, err := wrapper.Validate(context.Background())
		require.NoError(t, err)
		assert.Equal(t, expectedAnnotations, result)
	})

	t.Run("ResourceSyncers wraps ResourceSyncer2 instances", func(t *testing.T) {
		// Create mock ResourceSyncer2 instances
		mockSyncer1 := &mockResourceSyncer2{
			resourceType: &v2.ResourceType{Id: "type1"},
		}
		mockSyncer2 := &mockResourceSyncer2{
			resourceType: &v2.ResourceType{Id: "type2"},
		}

		mockBuilder2 := &mockConnectorBuilder2{
			resourceSyncers: []ResourceSyncer2{mockSyncer1, mockSyncer2},
		}

		wrapper := NewConnectorBuilder2Wrapper(mockBuilder2, nil)

		syncers := wrapper.ResourceSyncers(context.Background())
		require.Len(t, syncers, 2)

		// Verify that the returned syncers are ResourceSyncer2Wrapper instances
		wrapper1, ok := syncers[0].(*ResourceSyncer2Wrapper)
		require.True(t, ok)
		assert.Equal(t, mockSyncer1, wrapper1.syncer2)

		wrapper2, ok := syncers[1].(*ResourceSyncer2Wrapper)
		require.True(t, ok)
		assert.Equal(t, mockSyncer2, wrapper2.syncer2)
	})

	t.Run("ResourceSyncers handles empty list", func(t *testing.T) {
		mockBuilder2 := &mockConnectorBuilder2{
			resourceSyncers: []ResourceSyncer2{},
		}

		wrapper := NewConnectorBuilder2Wrapper(mockBuilder2, nil)

		syncers := wrapper.ResourceSyncers(context.Background())
		assert.Empty(t, syncers)
	})

	t.Run("passes through errors from Metadata", func(t *testing.T) {
		expectedError := errors.New("metadata error")
		mockBuilder2 := &mockConnectorBuilder2{
			metadataFunc: func(ctx context.Context) (*v2.ConnectorMetadata, error) {
				return nil, expectedError
			},
		}

		wrapper := NewConnectorBuilder2Wrapper(mockBuilder2, nil)

		_, err := wrapper.Metadata(context.Background())
		assert.Equal(t, expectedError, err)
	})

	t.Run("passes through errors from Validate", func(t *testing.T) {
		expectedError := errors.New("validate error")
		mockBuilder2 := &mockConnectorBuilder2{
			validateFunc: func(ctx context.Context) (annotations.Annotations, error) {
				return nil, expectedError
			},
		}

		wrapper := NewConnectorBuilder2Wrapper(mockBuilder2, nil)

		_, err := wrapper.Validate(context.Background())
		assert.Equal(t, expectedError, err)
	})

	t.Run("wrapped ResourceSyncer uses provided session store", func(t *testing.T) {
		var capturedSession types.SessionStore
		mockSyncer2 := &mockResourceSyncer2{
			resourceType: &v2.ResourceType{Id: "test"},
			listFunc: func(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token, opts types.ResourceSyncerOptions) ([]*v2.Resource, string, annotations.Annotations, error) {
				capturedSession = opts.Session
				return []*v2.Resource{}, "", nil, nil
			},
		}

		mockBuilder2 := &mockConnectorBuilder2{
			resourceSyncers: []ResourceSyncer2{mockSyncer2},
		}

		// Create a session store to pass to the wrapper
		mockSession, err := session.NewMemorySessionCache(context.Background())
		require.NoError(t, err)

		wrapper := NewConnectorBuilder2Wrapper(mockBuilder2, mockSession)

		syncers := wrapper.ResourceSyncers(context.Background())
		require.Len(t, syncers, 1)

		// Call List on the wrapped syncer
		_, _, _, listErr := syncers[0].List(context.Background(), nil, &pagination.Token{})
		require.NoError(t, listErr)
		assert.Equal(t, mockSession, capturedSession)
	})
}
