package pebble

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
)

func TestPebbleEngine_PutResources(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Start a sync to have an active sync ID
	syncID, err := engine.StartNewSync(ctx)
	require.NoError(t, err)

	// Create test resources with parent-child relationships
	resources := []*v2.Resource{
		{
			Id: &v2.ResourceId{
				ResourceType: "user",
				Resource:     "user1",
			},
			DisplayName: "User 1",
			Description: "Test user 1",
		},
		{
			Id: &v2.ResourceId{
				ResourceType: "group",
				Resource:     "group1",
			},
			DisplayName: "Group 1",
			Description: "Test group 1",
		},
		{
			Id: &v2.ResourceId{
				ResourceType: "group",
				Resource:     "subgroup1",
			},
			ParentResourceId: &v2.ResourceId{
				ResourceType: "group",
				Resource:     "group1",
			},
			DisplayName: "Subgroup 1",
			Description: "Test subgroup 1",
		},
	}

	// Test PutResources
	err = engine.PutResources(ctx, resources...)
	require.NoError(t, err)

	// Verify the resources were stored
	for _, res := range resources {
		req := &reader_v2.ResourcesReaderServiceGetResourceRequest{
			ResourceId:  res.Id,
			Annotations: createAnnotations(syncID),
		}

		resp, err := engine.GetResource(ctx, req)
		require.NoError(t, err)
		assert.Equal(t, res.Id.ResourceType, resp.Resource.Id.ResourceType)
		assert.Equal(t, res.Id.Resource, resp.Resource.Id.Resource)
		assert.Equal(t, res.DisplayName, resp.Resource.DisplayName)
		assert.Equal(t, res.Description, resp.Resource.Description)

		// Check parent-child relationship
		if res.ParentResourceId != nil {
			assert.Equal(t, res.ParentResourceId.ResourceType, resp.Resource.ParentResourceId.ResourceType)
			assert.Equal(t, res.ParentResourceId.Resource, resp.Resource.ParentResourceId.Resource)
		}
	}
}

func TestPebbleEngine_PutResourcesIfNewer(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Start a sync
	syncID, err := engine.StartNewSync(ctx)
	require.NoError(t, err)

	// Create initial resource
	initialResource := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "user",
			Resource:     "user1",
		},
		DisplayName: "Initial User",
		Description: "Initial description",
	}

	// Put initial resource
	err = engine.PutResources(ctx, initialResource)
	require.NoError(t, err)

	// Try to update with newer resource
	newerResource := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "user",
			Resource:     "user1",
		},
		DisplayName: "Updated User",
		Description: "Updated description",
	}

	err = engine.PutResourcesIfNewer(ctx, newerResource)
	require.NoError(t, err)

	// Verify the resource was updated
	req := &reader_v2.ResourcesReaderServiceGetResourceRequest{
		ResourceId: &v2.ResourceId{
			ResourceType: "user",
			Resource:     "user1",
		},
		Annotations: createAnnotations(syncID),
	}

	resp, err := engine.GetResource(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, "Updated User", resp.Resource.DisplayName)
	assert.Equal(t, "Updated description", resp.Resource.Description)
}

func TestPebbleEngine_PutResourcesIfNewer_UpdatesWhenNewer(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Start a sync
	syncID, err := engine.StartNewSync(ctx)
	require.NoError(t, err)

	// Create initial resource
	initialResource := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "user",
			Resource:     "user1",
		},
		DisplayName: "Initial User",
		Description: "Initial description",
	}

	// Put initial resource using IfNewer (will create since it doesn't exist)
	err = engine.PutResourcesIfNewer(ctx, initialResource)
	require.NoError(t, err)

	// Try to update with newer data using IfNewer (should update since it's newer)
	newerResource := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "user",
			Resource:     "user1",
		},
		DisplayName: "Updated User",
		Description: "Updated description",
	}

	err = engine.PutResourcesIfNewer(ctx, newerResource)
	require.NoError(t, err)

	// Verify the resource WAS updated
	req := &reader_v2.ResourcesReaderServiceGetResourceRequest{
		ResourceId: &v2.ResourceId{
			ResourceType: "user",
			Resource:     "user1",
		},
		Annotations: createAnnotations(syncID),
	}

	resp, err := engine.GetResource(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, "Updated User", resp.Resource.DisplayName)
	assert.Equal(t, "Updated description", resp.Resource.Description)
}

func TestPebbleEngine_GetResource(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Start a sync
	syncID, err := engine.StartNewSync(ctx)
	require.NoError(t, err)

	// Create and store a resource
	resource := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "user",
			Resource:     "user1",
		},
		DisplayName: "Test User",
		Description: "Test description",
	}

	err = engine.PutResources(ctx, resource)
	require.NoError(t, err)

	// Test GetResource
	req := &reader_v2.ResourcesReaderServiceGetResourceRequest{
		ResourceId: &v2.ResourceId{
			ResourceType: "user",
			Resource:     "user1",
		},
		Annotations: createAnnotations(syncID),
	}

	resp, err := engine.GetResource(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, resource.Id.ResourceType, resp.Resource.Id.ResourceType)
	assert.Equal(t, resource.Id.Resource, resp.Resource.Id.Resource)
	assert.Equal(t, resource.DisplayName, resp.Resource.DisplayName)
	assert.Equal(t, resource.Description, resp.Resource.Description)
}

func TestPebbleEngine_GetResource_NotFound(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Start a sync
	syncID, err := engine.StartNewSync(ctx)
	require.NoError(t, err)

	// Test GetResource for non-existent resource
	req := &reader_v2.ResourcesReaderServiceGetResourceRequest{
		ResourceId: &v2.ResourceId{
			ResourceType: "user",
			Resource:     "nonexistent",
		},
		Annotations: createAnnotations(syncID),
	}

	_, err = engine.GetResource(ctx, req)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "resource not found")
}

func TestPebbleEngine_ListResources(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Start a sync
	syncID, err := engine.StartNewSync(ctx)
	require.NoError(t, err)

	// Create multiple resources of different types
	resources := []*v2.Resource{
		{
			Id: &v2.ResourceId{
				ResourceType: "user",
				Resource:     "user1",
			},
			DisplayName: "User 1",
		},
		{
			Id: &v2.ResourceId{
				ResourceType: "user",
				Resource:     "user2",
			},
			DisplayName: "User 2",
		},
		{
			Id: &v2.ResourceId{
				ResourceType: "group",
				Resource:     "group1",
			},
			DisplayName: "Group 1",
		},
		{
			Id: &v2.ResourceId{
				ResourceType: "group",
				Resource:     "group2",
			},
			DisplayName: "Group 2",
		},
		{
			Id: &v2.ResourceId{
				ResourceType: "role",
				Resource:     "role1",
			},
			DisplayName: "Role 1",
		},
	}

	// Store all resources
	err = engine.PutResources(ctx, resources...)
	require.NoError(t, err)

	// Test ListResources without filtering
	req := &v2.ResourcesServiceListResourcesRequest{
		Annotations: createAnnotations(syncID),
		PageSize:    10,
	}

	resp, err := engine.ListResources(ctx, req)
	require.NoError(t, err)
	assert.Len(t, resp.List, 5)
	assert.Empty(t, resp.NextPageToken)

	// Verify all resources are present
	resourceMap := make(map[string]*v2.Resource)
	for _, res := range resp.List {
		key := res.Id.ResourceType + "/" + res.Id.Resource
		resourceMap[key] = res
	}

	for _, expectedRes := range resources {
		key := expectedRes.Id.ResourceType + "/" + expectedRes.Id.Resource
		actualRes, exists := resourceMap[key]
		assert.True(t, exists, "Resource %s not found", key)
		assert.Equal(t, expectedRes.DisplayName, actualRes.DisplayName)
	}
}

func TestPebbleEngine_ListResources_FilterByResourceType(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Start a sync
	syncID, err := engine.StartNewSync(ctx)
	require.NoError(t, err)

	// Create multiple resources of different types
	resources := []*v2.Resource{
		{
			Id: &v2.ResourceId{
				ResourceType: "user",
				Resource:     "user1",
			},
			DisplayName: "User 1",
		},
		{
			Id: &v2.ResourceId{
				ResourceType: "user",
				Resource:     "user2",
			},
			DisplayName: "User 2",
		},
		{
			Id: &v2.ResourceId{
				ResourceType: "group",
				Resource:     "group1",
			},
			DisplayName: "Group 1",
		},
		{
			Id: &v2.ResourceId{
				ResourceType: "group",
				Resource:     "group2",
			},
			DisplayName: "Group 2",
		},
	}

	// Store all resources
	err = engine.PutResources(ctx, resources...)
	require.NoError(t, err)

	// Test ListResources filtering by resource type "user"
	req := &v2.ResourcesServiceListResourcesRequest{
		ResourceTypeId: "user",
		Annotations:    createAnnotations(syncID),
		PageSize:       10,
	}

	resp, err := engine.ListResources(ctx, req)
	require.NoError(t, err)
	assert.Len(t, resp.List, 2)
	assert.Empty(t, resp.NextPageToken)

	// Verify only user resources are returned
	for _, res := range resp.List {
		assert.Equal(t, "user", res.Id.ResourceType)
	}

	// Test ListResources filtering by resource type "group"
	req.ResourceTypeId = "group"
	resp, err = engine.ListResources(ctx, req)
	require.NoError(t, err)
	assert.Len(t, resp.List, 2)

	// Verify only group resources are returned
	for _, res := range resp.List {
		assert.Equal(t, "group", res.Id.ResourceType)
	}
}

func TestPebbleEngine_ListResources_Pagination(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Start a sync
	syncID, err := engine.StartNewSync(ctx)
	require.NoError(t, err)

	// Create multiple resources
	var resources []*v2.Resource
	for i := 0; i < 10; i++ {
		resources = append(resources, &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: "user",
				Resource:     fmt.Sprintf("user%d", i),
			},
			DisplayName: fmt.Sprintf("User %d", i),
		})
	}

	// Store all resources
	err = engine.PutResources(ctx, resources...)
	require.NoError(t, err)

	// Test pagination with page size 3
	var allResults []*v2.Resource
	pageToken := ""
	pageCount := 0

	for {
		pageCount++
		req := &v2.ResourcesServiceListResourcesRequest{
			ResourceTypeId: "user",
			Annotations:    createAnnotations(syncID),
			PageSize:       3,
			PageToken:      pageToken,
		}

		resp, err := engine.ListResources(ctx, req)
		require.NoError(t, err)

		allResults = append(allResults, resp.List...)

		if resp.NextPageToken == "" {
			break
		}

		pageToken = resp.NextPageToken

		// Prevent infinite loops in tests
		if pageCount > 10 {
			t.Fatal("Too many pages, possible infinite loop")
		}
	}

	// Verify we got all resources
	assert.Len(t, allResults, 10)
	assert.LessOrEqual(t, pageCount, 4) // Should take at most 4 pages (3+3+3+1)

	// Verify all resources are present
	resourceMap := make(map[string]*v2.Resource)
	for _, res := range allResults {
		key := res.Id.ResourceType + "/" + res.Id.Resource
		resourceMap[key] = res
	}

	for _, expectedRes := range resources {
		key := expectedRes.Id.ResourceType + "/" + expectedRes.Id.Resource
		actualRes, exists := resourceMap[key]
		assert.True(t, exists, "Resource %s not found", key)
		assert.Equal(t, expectedRes.DisplayName, actualRes.DisplayName)
	}
}

func TestPebbleEngine_ListResources_EmptyResult(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Start a sync
	syncID, err := engine.StartNewSync(ctx)
	require.NoError(t, err)

	// Test ListResources with no data
	req := &v2.ResourcesServiceListResourcesRequest{
		Annotations: createAnnotations(syncID),
		PageSize:    10,
	}

	resp, err := engine.ListResources(ctx, req)
	require.NoError(t, err)
	assert.Empty(t, resp.List)
	assert.Empty(t, resp.NextPageToken)
}

func TestPebbleEngine_Resources_NoActiveSync(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Try to put resources without an active sync
	resource := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "user",
			Resource:     "user1",
		},
		DisplayName: "Test User",
	}

	err := engine.PutResources(ctx, resource)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no active sync")
}

func TestPebbleEngine_Resources_ParentChildRelationships(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Start a sync
	syncID, err := engine.StartNewSync(ctx)
	require.NoError(t, err)

	// Create resources with parent-child relationships
	parentResource := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "organization",
			Resource:     "org1",
		},
		DisplayName: "Organization 1",
		Description: "Parent organization",
	}

	childResource := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "team",
			Resource:     "team1",
		},
		ParentResourceId: &v2.ResourceId{
			ResourceType: "organization",
			Resource:     "org1",
		},
		DisplayName: "Team 1",
		Description: "Child team",
	}

	grandchildResource := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "project",
			Resource:     "project1",
		},
		ParentResourceId: &v2.ResourceId{
			ResourceType: "team",
			Resource:     "team1",
		},
		DisplayName: "Project 1",
		Description: "Grandchild project",
	}

	// Store all resources
	err = engine.PutResources(ctx, parentResource, childResource, grandchildResource)
	require.NoError(t, err)

	// Verify parent resource
	req := &reader_v2.ResourcesReaderServiceGetResourceRequest{
		ResourceId:  parentResource.Id,
		Annotations: createAnnotations(syncID),
	}

	resp, err := engine.GetResource(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, parentResource.DisplayName, resp.Resource.DisplayName)
	assert.Nil(t, resp.Resource.ParentResourceId) // No parent

	// Verify child resource and its parent relationship
	req.ResourceId = childResource.Id
	resp, err = engine.GetResource(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, childResource.DisplayName, resp.Resource.DisplayName)
	require.NotNil(t, resp.Resource.ParentResourceId)
	assert.Equal(t, parentResource.Id.ResourceType, resp.Resource.ParentResourceId.ResourceType)
	assert.Equal(t, parentResource.Id.Resource, resp.Resource.ParentResourceId.Resource)

	// Verify grandchild resource and its parent relationship
	req.ResourceId = grandchildResource.Id
	resp, err = engine.GetResource(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, grandchildResource.DisplayName, resp.Resource.DisplayName)
	require.NotNil(t, resp.Resource.ParentResourceId)
	assert.Equal(t, childResource.Id.ResourceType, resp.Resource.ParentResourceId.ResourceType)
	assert.Equal(t, childResource.Id.Resource, resp.Resource.ParentResourceId.Resource)
}

func TestPebbleEngine_Resources_BatchOperations(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Start a sync
	syncID, err := engine.StartNewSync(ctx)
	require.NoError(t, err)

	// Create a large batch of resources
	var resources []*v2.Resource
	for i := 0; i < 100; i++ {
		resources = append(resources, &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: "user",
				Resource:     fmt.Sprintf("user%d", i),
			},
			DisplayName: fmt.Sprintf("User %d", i),
		})
	}

	// Store all resources in one batch
	err = engine.PutResources(ctx, resources...)
	require.NoError(t, err)

	// Verify all were stored
	req := &v2.ResourcesServiceListResourcesRequest{
		ResourceTypeId: "user",
		Annotations:    createAnnotations(syncID),
		PageSize:       1000,
	}

	resp, err := engine.ListResources(ctx, req)
	require.NoError(t, err)
	assert.Len(t, resp.List, 100)
}

func TestPebbleEngine_Resources_KeyEncoding(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Start a sync
	syncID, err := engine.StartNewSync(ctx)
	require.NoError(t, err)

	// Test resources with special characters in IDs
	resources := []*v2.Resource{
		{
			Id: &v2.ResourceId{
				ResourceType: "user/type",
				Resource:     "user/with/slashes",
			},
			DisplayName: "User with slashes",
		},
		{
			Id: &v2.ResourceId{
				ResourceType: "user type",
				Resource:     "user with spaces",
			},
			DisplayName: "User with spaces",
		},
		{
			Id: &v2.ResourceId{
				ResourceType: "user\x00type",
				Resource:     "user\x00with\x01nulls",
			},
			DisplayName: "User with null bytes",
		},
		{
			Id: &v2.ResourceId{
				ResourceType: "user|type",
				Resource:     "user|with|pipes",
			},
			DisplayName: "User with pipes",
		},
	}

	// Store resources
	err = engine.PutResources(ctx, resources...)
	require.NoError(t, err)

	// Verify all can be retrieved
	for _, res := range resources {
		req := &reader_v2.ResourcesReaderServiceGetResourceRequest{
			ResourceId:  res.Id,
			Annotations: createAnnotations(syncID),
		}

		resp, err := engine.GetResource(ctx, req)
		require.NoError(t, err, "Failed to get resource with ID: %s/%s", res.Id.ResourceType, res.Id.Resource)
		assert.Equal(t, res.Id.ResourceType, resp.Resource.Id.ResourceType)
		assert.Equal(t, res.Id.Resource, resp.Resource.Id.Resource)
		assert.Equal(t, res.DisplayName, resp.Resource.DisplayName)
	}
}

func TestPebbleEngine_Resources_ValidationErrors(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Start a sync
	_, err := engine.StartNewSync(ctx)
	require.NoError(t, err)

	// Test with nil resource
	err = engine.PutResources(ctx, nil)
	require.NoError(t, err) // Should not error, just skip nil resources

	// Test with resource missing ID
	resourceWithoutID := &v2.Resource{
		DisplayName: "Resource without ID",
	}

	err = engine.PutResources(ctx, resourceWithoutID)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "resource missing ID")

	// Test with resource missing resource type
	resourceWithoutType := &v2.Resource{
		Id: &v2.ResourceId{
			Resource: "resource1",
		},
		DisplayName: "Resource without type",
	}

	err = engine.PutResources(ctx, resourceWithoutType)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "resource missing resource type or resource ID")

	// Test with resource missing resource ID
	resourceWithoutResourceID := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "user",
		},
		DisplayName: "Resource without resource ID",
	}

	err = engine.PutResources(ctx, resourceWithoutResourceID)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "resource missing resource type or resource ID")
}
