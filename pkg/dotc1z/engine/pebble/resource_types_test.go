package pebble

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"

	c1zpb "github.com/conductorone/baton-sdk/pb/c1/c1z/v1"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
)

func TestPebbleEngine_PutResourceTypes(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Start a sync to have an active sync ID
	syncID, err := engine.StartNewSync(ctx)
	require.NoError(t, err)

	// Create test resource types
	resourceTypes := []*v2.ResourceType{
		{
			Id:          "rt1",
			DisplayName: "Resource Type 1",
		},
		{
			Id:          "rt2",
			DisplayName: "Resource Type 2",
		},
	}

	// Test PutResourceTypes
	err = engine.PutResourceTypes(ctx, resourceTypes...)
	require.NoError(t, err)

	// Verify the resource types were stored
	for _, rt := range resourceTypes {
		req := &reader_v2.ResourceTypesReaderServiceGetResourceTypeRequest{
			ResourceTypeId: rt.Id,
			Annotations:    createAnnotations(syncID),
		}

		resp, err := engine.GetResourceType(ctx, req)
		require.NoError(t, err)
		assert.Equal(t, rt.Id, resp.ResourceType.Id)
		assert.Equal(t, rt.DisplayName, resp.ResourceType.DisplayName)
	}
}

func TestPebbleEngine_PutResourceTypesIfNewer(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Start a sync
	syncID, err := engine.StartNewSync(ctx)
	require.NoError(t, err)

	// Create initial resource type
	initialRT := &v2.ResourceType{
		Id:          "rt1",
		DisplayName: "Initial Resource Type",
	}

	// Put initial resource type
	err = engine.PutResourceTypes(ctx, initialRT)
	require.NoError(t, err)

	// Try to update with newer resource type
	newerRT := &v2.ResourceType{
		Id:          "rt1",
		DisplayName: "Updated Resource Type",
	}

	err = engine.PutResourceTypesIfNewer(ctx, newerRT)
	require.NoError(t, err)

	// Verify the resource type was updated
	req := &reader_v2.ResourceTypesReaderServiceGetResourceTypeRequest{
		ResourceTypeId: "rt1",
		Annotations:    createAnnotations(syncID),
	}

	resp, err := engine.GetResourceType(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, "Updated Resource Type", resp.ResourceType.DisplayName)
}

func TestPebbleEngine_PutResourceTypesIfNewer_UpdatesWhenNewer(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Start a sync
	syncID, err := engine.StartNewSync(ctx)
	require.NoError(t, err)

	// Create initial resource type
	initialRT := &v2.ResourceType{
		Id:          "rt1",
		DisplayName: "Initial Resource Type",
	}

	// Put initial resource type using IfNewer (will create since it doesn't exist)
	err = engine.PutResourceTypesIfNewer(ctx, initialRT)
	require.NoError(t, err)

	// Try to update with newer data using IfNewer (should update since it's newer)
	newerRT := &v2.ResourceType{
		Id:          "rt1",
		DisplayName: "Updated Resource Type",
	}

	err = engine.PutResourceTypesIfNewer(ctx, newerRT)
	require.NoError(t, err)

	// Verify the resource type WAS updated
	req := &reader_v2.ResourceTypesReaderServiceGetResourceTypeRequest{
		ResourceTypeId: "rt1",
		Annotations:    createAnnotations(syncID),
	}

	resp, err := engine.GetResourceType(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, "Updated Resource Type", resp.ResourceType.DisplayName)
}

func TestPebbleEngine_GetResourceType(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Start a sync
	syncID, err := engine.StartNewSync(ctx)
	require.NoError(t, err)

	// Create and store a resource type
	resourceType := &v2.ResourceType{
		Id:          "rt1",
		DisplayName: "Test Resource Type",
	}

	err = engine.PutResourceTypes(ctx, resourceType)
	require.NoError(t, err)

	// Test GetResourceType
	req := &reader_v2.ResourceTypesReaderServiceGetResourceTypeRequest{
		ResourceTypeId: "rt1",
		Annotations:    createAnnotations(syncID),
	}

	resp, err := engine.GetResourceType(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, resourceType.Id, resp.ResourceType.Id)
	assert.Equal(t, resourceType.DisplayName, resp.ResourceType.DisplayName)
}

func TestPebbleEngine_GetResourceType_NotFound(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Start a sync
	syncID, err := engine.StartNewSync(ctx)
	require.NoError(t, err)

	// Test GetResourceType for non-existent resource type
	req := &reader_v2.ResourceTypesReaderServiceGetResourceTypeRequest{
		ResourceTypeId: "nonexistent",
		Annotations:    createAnnotations(syncID),
	}

	_, err = engine.GetResourceType(ctx, req)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "resource type not found")
}

func TestPebbleEngine_ListResourceTypes(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Start a sync
	syncID, err := engine.StartNewSync(ctx)
	require.NoError(t, err)

	// Create multiple resource types
	resourceTypes := []*v2.ResourceType{
		{Id: "rt1", DisplayName: "Resource Type 1"},
		{Id: "rt2", DisplayName: "Resource Type 2"},
		{Id: "rt3", DisplayName: "Resource Type 3"},
		{Id: "rt4", DisplayName: "Resource Type 4"},
		{Id: "rt5", DisplayName: "Resource Type 5"},
	}

	// Store all resource types
	err = engine.PutResourceTypes(ctx, resourceTypes...)
	require.NoError(t, err)

	// Test ListResourceTypes without pagination
	req := &v2.ResourceTypesServiceListResourceTypesRequest{
		Annotations: createAnnotations(syncID),
		PageSize:    10,
	}

	resp, err := engine.ListResourceTypes(ctx, req)
	require.NoError(t, err)
	assert.Len(t, resp.List, 5)
	assert.Empty(t, resp.NextPageToken)

	// Verify all resource types are present
	rtMap := make(map[string]*v2.ResourceType)
	for _, rt := range resp.List {
		rtMap[rt.Id] = rt
	}

	for _, expectedRT := range resourceTypes {
		actualRT, exists := rtMap[expectedRT.Id]
		assert.True(t, exists, "Resource type %s not found", expectedRT.Id)
		assert.Equal(t, expectedRT.DisplayName, actualRT.DisplayName)
	}
}

func TestPebbleEngine_ListResourceTypes_Pagination(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Start a sync
	syncID, err := engine.StartNewSync(ctx)
	require.NoError(t, err)

	// Create multiple resource types
	resourceTypes := []*v2.ResourceType{
		{Id: "rt1", DisplayName: "Resource Type 1"},
		{Id: "rt2", DisplayName: "Resource Type 2"},
		{Id: "rt3", DisplayName: "Resource Type 3"},
		{Id: "rt4", DisplayName: "Resource Type 4"},
		{Id: "rt5", DisplayName: "Resource Type 5"},
	}

	// Store all resource types
	err = engine.PutResourceTypes(ctx, resourceTypes...)
	require.NoError(t, err)

	// Test pagination with page size 2
	var allResults []*v2.ResourceType
	pageToken := ""
	pageCount := 0

	for {
		pageCount++
		req := &v2.ResourceTypesServiceListResourceTypesRequest{
			Annotations: createAnnotations(syncID),
			PageSize:    2,
			PageToken:   pageToken,
		}

		resp, err := engine.ListResourceTypes(ctx, req)
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

	// Verify we got all resource types
	assert.Len(t, allResults, 5)
	assert.LessOrEqual(t, pageCount, 3) // Should take at most 3 pages (2+2+1)

	// Verify all resource types are present
	rtMap := make(map[string]*v2.ResourceType)
	for _, rt := range allResults {
		rtMap[rt.Id] = rt
	}

	for _, expectedRT := range resourceTypes {
		actualRT, exists := rtMap[expectedRT.Id]
		assert.True(t, exists, "Resource type %s not found", expectedRT.Id)
		assert.Equal(t, expectedRT.DisplayName, actualRT.DisplayName)
	}
}

func TestPebbleEngine_ListResourceTypes_EmptyResult(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Start a sync
	syncID, err := engine.StartNewSync(ctx)
	require.NoError(t, err)

	// Test ListResourceTypes with no data
	req := &v2.ResourceTypesServiceListResourceTypesRequest{
		Annotations: createAnnotations(syncID),
		PageSize:    10,
	}

	resp, err := engine.ListResourceTypes(ctx, req)
	require.NoError(t, err)
	assert.Empty(t, resp.List)
	assert.Empty(t, resp.NextPageToken)
}

func TestPebbleEngine_ResourceTypes_NoActiveSync(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Try to put resource types without an active sync
	resourceType := &v2.ResourceType{
		Id:          "rt1",
		DisplayName: "Test Resource Type",
	}

	err := engine.PutResourceTypes(ctx, resourceType)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no active sync")
}

func TestPebbleEngine_ResourceTypes_BatchOperations(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Start a sync
	syncID, err := engine.StartNewSync(ctx)
	require.NoError(t, err)

	// Create a large batch of resource types
	var resourceTypes []*v2.ResourceType
	for i := 0; i < 100; i++ {
		resourceTypes = append(resourceTypes, &v2.ResourceType{
			Id:          fmt.Sprintf("rt%d", i),
			DisplayName: fmt.Sprintf("Resource Type %d", i),
		})
	}

	// Store all resource types in one batch
	err = engine.PutResourceTypes(ctx, resourceTypes...)
	require.NoError(t, err)

	// Verify all were stored
	req := &v2.ResourceTypesServiceListResourceTypesRequest{
		Annotations: createAnnotations(syncID),
		PageSize:    1000,
	}

	resp, err := engine.ListResourceTypes(ctx, req)
	require.NoError(t, err)
	assert.Len(t, resp.List, 100)
}

func TestPebbleEngine_ResourceTypes_KeyEncoding(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Start a sync
	syncID, err := engine.StartNewSync(ctx)
	require.NoError(t, err)

	// Test resource types with special characters in IDs
	resourceTypes := []*v2.ResourceType{
		{Id: "rt/with/slashes", DisplayName: "RT with slashes"},
		{Id: "rt with spaces", DisplayName: "RT with spaces"},
		{Id: "rt\x00with\x01nulls", DisplayName: "RT with null bytes"},
		{Id: "rt|with|pipes", DisplayName: "RT with pipes"},
	}

	// Store resource types
	err = engine.PutResourceTypes(ctx, resourceTypes...)
	require.NoError(t, err)

	// Verify all can be retrieved
	for _, rt := range resourceTypes {
		req := &reader_v2.ResourceTypesReaderServiceGetResourceTypeRequest{
			ResourceTypeId: rt.Id,
			Annotations:    createAnnotations(syncID),
		}

		resp, err := engine.GetResourceType(ctx, req)
		require.NoError(t, err, "Failed to get resource type with ID: %s", rt.Id)
		assert.Equal(t, rt.Id, resp.ResourceType.Id)
		assert.Equal(t, rt.DisplayName, resp.ResourceType.DisplayName)
	}
}

// Helper function to create annotations with sync ID
func createAnnotations(syncID string) []*anypb.Any {
	syncDetails := &c1zpb.SyncDetails{
		Id: syncID,
	}

	syncDetailsAny, err := anypb.New(syncDetails)
	if err != nil {
		panic(fmt.Errorf("failed to create sync details annotation: %w", err))
	}

	return []*anypb.Any{syncDetailsAny}
}

// Helper function to set up a test engine
func setupTestEngine(t *testing.T) (*PebbleEngine, func()) {
	tempDir := t.TempDir()

	engine, err := NewPebbleEngine(context.Background(), tempDir)
	require.NoError(t, err)

	cleanup := func() {
		engine.Close()
	}

	return engine, cleanup
}
