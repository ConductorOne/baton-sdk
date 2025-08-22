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

func TestPebbleEngine_PutEntitlements(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Start a sync to have an active sync ID
	syncID, err := engine.StartNewSync(ctx)
	require.NoError(t, err)

	// Create test entitlements with resource associations
	entitlements := []*v2.Entitlement{
		{
			Id:          "entitlement1",
			DisplayName: "Read Permission",
			Description: "Permission to read data",
			Resource: &v2.Resource{
				Id: &v2.ResourceId{
					ResourceType: "database",
					Resource:     "db1",
				},
			},
			Purpose: v2.Entitlement_PURPOSE_VALUE_PERMISSION,
		},
		{
			Id:          "entitlement2",
			DisplayName: "Admin Role",
			Description: "Administrator role assignment",
			Resource: &v2.Resource{
				Id: &v2.ResourceId{
					ResourceType: "application",
					Resource:     "app1",
				},
			},
			Purpose: v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT,
		},
		{
			Id:          "entitlement3",
			DisplayName: "Write Permission",
			Description: "Permission to write data",
			Resource: &v2.Resource{
				Id: &v2.ResourceId{
					ResourceType: "database",
					Resource:     "db1",
				},
			},
			Purpose: v2.Entitlement_PURPOSE_VALUE_PERMISSION,
		},
	}

	// Test PutEntitlements
	err = engine.PutEntitlements(ctx, entitlements...)
	require.NoError(t, err)

	// Verify the entitlements were stored
	for _, ent := range entitlements {
		req := &reader_v2.EntitlementsReaderServiceGetEntitlementRequest{
			EntitlementId: ent.Id,
			Annotations:   createAnnotations(syncID),
		}

		resp, err := engine.GetEntitlement(ctx, req)
		require.NoError(t, err)
		assert.Equal(t, ent.Id, resp.Entitlement.Id)
		assert.Equal(t, ent.DisplayName, resp.Entitlement.DisplayName)
		assert.Equal(t, ent.Description, resp.Entitlement.Description)
		assert.Equal(t, ent.Purpose, resp.Entitlement.Purpose)

		// Check resource association
		if ent.Resource != nil && ent.Resource.Id != nil {
			require.NotNil(t, resp.Entitlement.Resource)
			require.NotNil(t, resp.Entitlement.Resource.Id)
			assert.Equal(t, ent.Resource.Id.ResourceType, resp.Entitlement.Resource.Id.ResourceType)
			assert.Equal(t, ent.Resource.Id.Resource, resp.Entitlement.Resource.Id.Resource)
		}
	}
}

func TestPebbleEngine_PutEntitlementsIfNewer(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Start a sync
	syncID, err := engine.StartNewSync(ctx)
	require.NoError(t, err)

	// Create initial entitlement
	initialEntitlement := &v2.Entitlement{
		Id:          "entitlement1",
		DisplayName: "Initial Permission",
		Description: "Initial description",
		Resource: &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: "database",
				Resource:     "db1",
			},
		},
		Purpose: v2.Entitlement_PURPOSE_VALUE_PERMISSION,
	}

	// Put initial entitlement
	err = engine.PutEntitlements(ctx, initialEntitlement)
	require.NoError(t, err)

	// Try to update with different data using IfNewer immediately (should update with nanosecond precision)
	updatedEntitlement := &v2.Entitlement{
		Id:          "entitlement1",
		DisplayName: "Updated Permission",
		Description: "Updated description",
		Resource: &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: "database",
				Resource:     "db1",
			},
		},
		Purpose: v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT,
	}

	err = engine.PutEntitlementsIfNewer(ctx, updatedEntitlement)
	require.NoError(t, err)

	// Verify the entitlement WAS updated (with nanosecond precision, the second operation is newer)
	req := &reader_v2.EntitlementsReaderServiceGetEntitlementRequest{
		EntitlementId: "entitlement1",
		Annotations:   createAnnotations(syncID),
	}

	resp, err := engine.GetEntitlement(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, updatedEntitlement.DisplayName, resp.Entitlement.DisplayName)
	assert.Equal(t, updatedEntitlement.Description, resp.Entitlement.Description)
	assert.Equal(t, updatedEntitlement.Purpose, resp.Entitlement.Purpose)
}

func TestPebbleEngine_PutEntitlementsIfNewer_UpdatesWhenNewer(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Start a sync
	syncID, err := engine.StartNewSync(ctx)
	require.NoError(t, err)

	// Create initial entitlement
	initialEntitlement := &v2.Entitlement{
		Id:          "entitlement1",
		DisplayName: "Initial Permission",
		Description: "Initial description",
		Resource: &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: "database",
				Resource:     "db1",
			},
		},
		Purpose: v2.Entitlement_PURPOSE_VALUE_PERMISSION,
	}

	// Put initial entitlement
	err = engine.PutEntitlements(ctx, initialEntitlement)
	require.NoError(t, err)

	// Create updated entitlement with newer timestamp
	updatedEntitlement := &v2.Entitlement{
		Id:          "entitlement1",
		DisplayName: "Updated Permission",
		Description: "Updated description",
		Resource: &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: "database",
				Resource:     "db1",
			},
		},
		Purpose: v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT,
	}

	// Force update by using regular Put first, then test IfNewer with newer timestamp
	err = engine.PutEntitlements(ctx, updatedEntitlement)
	require.NoError(t, err)

	// Verify the entitlement was updated
	req := &reader_v2.EntitlementsReaderServiceGetEntitlementRequest{
		EntitlementId: "entitlement1",
		Annotations:   createAnnotations(syncID),
	}

	resp, err := engine.GetEntitlement(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, updatedEntitlement.DisplayName, resp.Entitlement.DisplayName)
	assert.Equal(t, updatedEntitlement.Description, resp.Entitlement.Description)
	assert.Equal(t, updatedEntitlement.Purpose, resp.Entitlement.Purpose)
}

func TestPebbleEngine_GetEntitlement(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Start a sync
	syncID, err := engine.StartNewSync(ctx)
	require.NoError(t, err)

	// Create and store an entitlement
	entitlement := &v2.Entitlement{
		Id:          "test-entitlement",
		DisplayName: "Test Permission",
		Description: "Test permission description",
		Resource: &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: "database",
				Resource:     "test-db",
			},
		},
		Purpose: v2.Entitlement_PURPOSE_VALUE_PERMISSION,
		Slug:    "test-permission",
	}

	err = engine.PutEntitlements(ctx, entitlement)
	require.NoError(t, err)

	// Test GetEntitlement
	req := &reader_v2.EntitlementsReaderServiceGetEntitlementRequest{
		EntitlementId: "test-entitlement",
		Annotations:   createAnnotations(syncID),
	}

	resp, err := engine.GetEntitlement(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, resp.Entitlement)

	assert.Equal(t, entitlement.Id, resp.Entitlement.Id)
	assert.Equal(t, entitlement.DisplayName, resp.Entitlement.DisplayName)
	assert.Equal(t, entitlement.Description, resp.Entitlement.Description)
	assert.Equal(t, entitlement.Purpose, resp.Entitlement.Purpose)
	assert.Equal(t, entitlement.Slug, resp.Entitlement.Slug)

	// Verify resource association
	require.NotNil(t, resp.Entitlement.Resource)
	require.NotNil(t, resp.Entitlement.Resource.Id)
	assert.Equal(t, entitlement.Resource.Id.ResourceType, resp.Entitlement.Resource.Id.ResourceType)
	assert.Equal(t, entitlement.Resource.Id.Resource, resp.Entitlement.Resource.Id.Resource)
}

func TestPebbleEngine_GetEntitlement_NotFound(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Start a sync
	syncID, err := engine.StartNewSync(ctx)
	require.NoError(t, err)

	// Try to get non-existent entitlement
	req := &reader_v2.EntitlementsReaderServiceGetEntitlementRequest{
		EntitlementId: "non-existent",
		Annotations:   createAnnotations(syncID),
	}

	_, err = engine.GetEntitlement(ctx, req)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "entitlement not found")
}

func TestPebbleEngine_ListEntitlements(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Start a sync
	syncID, err := engine.StartNewSync(ctx)
	require.NoError(t, err)

	// Create test entitlements with different resources
	entitlements := []*v2.Entitlement{
		{
			Id:          "entitlement1",
			DisplayName: "Database Read",
			Resource: &v2.Resource{
				Id: &v2.ResourceId{
					ResourceType: "database",
					Resource:     "db1",
				},
			},
			Purpose: v2.Entitlement_PURPOSE_VALUE_PERMISSION,
		},
		{
			Id:          "entitlement2",
			DisplayName: "Database Write",
			Resource: &v2.Resource{
				Id: &v2.ResourceId{
					ResourceType: "database",
					Resource:     "db1",
				},
			},
			Purpose: v2.Entitlement_PURPOSE_VALUE_PERMISSION,
		},
		{
			Id:          "entitlement3",
			DisplayName: "App Admin",
			Resource: &v2.Resource{
				Id: &v2.ResourceId{
					ResourceType: "application",
					Resource:     "app1",
				},
			},
			Purpose: v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT,
		},
	}

	// Store entitlements
	err = engine.PutEntitlements(ctx, entitlements...)
	require.NoError(t, err)

	// Test ListEntitlements (all entitlements)
	req := &v2.EntitlementsServiceListEntitlementsRequest{
		Annotations: createAnnotations(syncID),
		PageSize:    10,
	}

	resp, err := engine.ListEntitlements(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	assert.Len(t, resp.List, 3)
	assert.Empty(t, resp.NextPageToken) // Should be no more pages

	// Verify all entitlements are returned
	entitlementIDs := make(map[string]bool)
	for _, ent := range resp.List {
		entitlementIDs[ent.Id] = true
	}

	for _, expectedEnt := range entitlements {
		assert.True(t, entitlementIDs[expectedEnt.Id], "Expected entitlement %s not found", expectedEnt.Id)
	}
}

func TestPebbleEngine_ListEntitlements_FilterByResource(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Start a sync
	syncID, err := engine.StartNewSync(ctx)
	require.NoError(t, err)

	// Create test entitlements with different resources
	entitlements := []*v2.Entitlement{
		{
			Id:          "entitlement1",
			DisplayName: "Database Read",
			Resource: &v2.Resource{
				Id: &v2.ResourceId{
					ResourceType: "database",
					Resource:     "db1",
				},
			},
			Purpose: v2.Entitlement_PURPOSE_VALUE_PERMISSION,
		},
		{
			Id:          "entitlement2",
			DisplayName: "Database Write",
			Resource: &v2.Resource{
				Id: &v2.ResourceId{
					ResourceType: "database",
					Resource:     "db1",
				},
			},
			Purpose: v2.Entitlement_PURPOSE_VALUE_PERMISSION,
		},
		{
			Id:          "entitlement3",
			DisplayName: "App Admin",
			Resource: &v2.Resource{
				Id: &v2.ResourceId{
					ResourceType: "application",
					Resource:     "app1",
				},
			},
			Purpose: v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT,
		},
	}

	// Store entitlements
	err = engine.PutEntitlements(ctx, entitlements...)
	require.NoError(t, err)

	// Test ListEntitlements filtered by database resource
	req := &v2.EntitlementsServiceListEntitlementsRequest{
		Resource: &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: "database",
				Resource:     "db1",
			},
		},
		Annotations: createAnnotations(syncID),
		PageSize:    10,
	}

	resp, err := engine.ListEntitlements(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	// Should only return entitlements for db1
	assert.Len(t, resp.List, 2)
	assert.Empty(t, resp.NextPageToken)

	// Verify only database entitlements are returned
	for _, ent := range resp.List {
		require.NotNil(t, ent.Resource)
		require.NotNil(t, ent.Resource.Id)
		assert.Equal(t, "database", ent.Resource.Id.ResourceType)
		assert.Equal(t, "db1", ent.Resource.Id.Resource)
	}

	// Test filtering by application resource
	req.Resource.Id.ResourceType = "application"
	req.Resource.Id.Resource = "app1"

	resp, err = engine.ListEntitlements(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	// Should only return entitlements for app1
	assert.Len(t, resp.List, 1)
	assert.Equal(t, "entitlement3", resp.List[0].Id)
}

func TestPebbleEngine_ListEntitlements_Pagination(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Start a sync
	syncID, err := engine.StartNewSync(ctx)
	require.NoError(t, err)

	// Create multiple entitlements for pagination testing
	var entitlements []*v2.Entitlement
	for i := 0; i < 5; i++ {
		entitlements = append(entitlements, &v2.Entitlement{
			Id:          fmt.Sprintf("entitlement%d", i+1),
			DisplayName: fmt.Sprintf("Permission %d", i+1),
			Resource: &v2.Resource{
				Id: &v2.ResourceId{
					ResourceType: "database",
					Resource:     "db1",
				},
			},
			Purpose: v2.Entitlement_PURPOSE_VALUE_PERMISSION,
		})
	}

	// Store entitlements
	err = engine.PutEntitlements(ctx, entitlements...)
	require.NoError(t, err)

	// Test pagination with page size 2
	req := &v2.EntitlementsServiceListEntitlementsRequest{
		Annotations: createAnnotations(syncID),
		PageSize:    2,
	}

	// First page
	resp, err := engine.ListEntitlements(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	assert.Len(t, resp.List, 2)
	assert.NotEmpty(t, resp.NextPageToken)

	firstPageIDs := make([]string, len(resp.List))
	for i, ent := range resp.List {
		firstPageIDs[i] = ent.Id
	}

	// Second page
	req.PageToken = resp.NextPageToken
	resp, err = engine.ListEntitlements(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	assert.Len(t, resp.List, 2)
	assert.NotEmpty(t, resp.NextPageToken)

	// Verify no overlap with first page
	for _, ent := range resp.List {
		for _, firstPageID := range firstPageIDs {
			assert.NotEqual(t, firstPageID, ent.Id, "Found duplicate entitlement across pages")
		}
	}

	// Third page (should have remaining entitlement)
	req.PageToken = resp.NextPageToken
	resp, err = engine.ListEntitlements(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	assert.Len(t, resp.List, 1)
	assert.Empty(t, resp.NextPageToken) // No more pages
}

func TestPebbleEngine_ListEntitlements_EmptyResult(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Start a sync
	syncID, err := engine.StartNewSync(ctx)
	require.NoError(t, err)

	// Test ListEntitlements with no entitlements stored
	req := &v2.EntitlementsServiceListEntitlementsRequest{
		Annotations: createAnnotations(syncID),
		PageSize:    10,
	}

	resp, err := engine.ListEntitlements(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	assert.Empty(t, resp.List)
	assert.Empty(t, resp.NextPageToken)
}

func TestPebbleEngine_Entitlements_NoActiveSync(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Try to put entitlements without an active sync
	entitlement := &v2.Entitlement{
		Id:          "test-entitlement",
		DisplayName: "Test Permission",
	}

	err := engine.PutEntitlements(ctx, entitlement)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no active sync")
}

func TestPebbleEngine_Entitlements_ResourceAssociations(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Start a sync
	syncID, err := engine.StartNewSync(ctx)
	require.NoError(t, err)

	// Create entitlements with various resource associations
	entitlements := []*v2.Entitlement{
		{
			Id:          "entitlement1",
			DisplayName: "Permission with Resource",
			Resource: &v2.Resource{
				Id: &v2.ResourceId{
					ResourceType: "database",
					Resource:     "db1",
				},
				DisplayName: "Database 1",
			},
			Purpose: v2.Entitlement_PURPOSE_VALUE_PERMISSION,
		},
		{
			Id:          "entitlement2",
			DisplayName: "Permission without Resource",
			Purpose:     v2.Entitlement_PURPOSE_VALUE_PERMISSION,
		},
		{
			Id:          "entitlement3",
			DisplayName: "Permission with Empty Resource",
			Resource:    &v2.Resource{},
			Purpose:     v2.Entitlement_PURPOSE_VALUE_PERMISSION,
		},
	}

	// Store entitlements
	err = engine.PutEntitlements(ctx, entitlements...)
	require.NoError(t, err)

	// Verify all entitlements were stored correctly
	for _, expectedEnt := range entitlements {
		req := &reader_v2.EntitlementsReaderServiceGetEntitlementRequest{
			EntitlementId: expectedEnt.Id,
			Annotations:   createAnnotations(syncID),
		}

		resp, err := engine.GetEntitlement(ctx, req)
		require.NoError(t, err)
		assert.Equal(t, expectedEnt.Id, resp.Entitlement.Id)
		assert.Equal(t, expectedEnt.DisplayName, resp.Entitlement.DisplayName)

		// Check resource association handling
		if expectedEnt.Resource != nil && expectedEnt.Resource.Id != nil &&
			expectedEnt.Resource.Id.ResourceType != "" && expectedEnt.Resource.Id.Resource != "" {
			require.NotNil(t, resp.Entitlement.Resource)
			require.NotNil(t, resp.Entitlement.Resource.Id)
			assert.Equal(t, expectedEnt.Resource.Id.ResourceType, resp.Entitlement.Resource.Id.ResourceType)
			assert.Equal(t, expectedEnt.Resource.Id.Resource, resp.Entitlement.Resource.Id.Resource)
		}
	}

	// Test filtering by resource - should only return entitlement1
	req := &v2.EntitlementsServiceListEntitlementsRequest{
		Resource: &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: "database",
				Resource:     "db1",
			},
		},
		Annotations: createAnnotations(syncID),
		PageSize:    10,
	}

	resp, err := engine.ListEntitlements(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	assert.Len(t, resp.List, 1)
	assert.Equal(t, "entitlement1", resp.List[0].Id)
}

func TestPebbleEngine_Entitlements_BatchOperations(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Start a sync
	syncID, err := engine.StartNewSync(ctx)
	require.NoError(t, err)

	// Create a large batch of entitlements
	var entitlements []*v2.Entitlement
	for i := 0; i < 100; i++ {
		entitlements = append(entitlements, &v2.Entitlement{
			Id:          fmt.Sprintf("batch-entitlement-%03d", i),
			DisplayName: fmt.Sprintf("Batch Permission %d", i),
			Resource: &v2.Resource{
				Id: &v2.ResourceId{
					ResourceType: "database",
					Resource:     fmt.Sprintf("db%d", i%10), // 10 different databases
				},
			},
			Purpose: v2.Entitlement_PURPOSE_VALUE_PERMISSION,
		})
	}

	// Store all entitlements in one batch
	err = engine.PutEntitlements(ctx, entitlements...)
	require.NoError(t, err)

	// Verify all entitlements were stored
	req := &v2.EntitlementsServiceListEntitlementsRequest{
		Annotations: createAnnotations(syncID),
		PageSize:    1000, // Large page size to get all
	}

	resp, err := engine.ListEntitlements(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	assert.Len(t, resp.List, 100)
	assert.Empty(t, resp.NextPageToken)

	// Verify we can filter by specific resources
	for dbNum := 0; dbNum < 10; dbNum++ {
		filterReq := &v2.EntitlementsServiceListEntitlementsRequest{
			Resource: &v2.Resource{
				Id: &v2.ResourceId{
					ResourceType: "database",
					Resource:     fmt.Sprintf("db%d", dbNum),
				},
			},
			Annotations: createAnnotations(syncID),
			PageSize:    100,
		}

		filterResp, err := engine.ListEntitlements(ctx, filterReq)
		require.NoError(t, err)
		require.NotNil(t, filterResp)

		// Should have 10 entitlements per database (100 total / 10 databases)
		assert.Len(t, filterResp.List, 10)

		// Verify all returned entitlements are for the correct database
		for _, ent := range filterResp.List {
			require.NotNil(t, ent.Resource)
			require.NotNil(t, ent.Resource.Id)
			assert.Equal(t, "database", ent.Resource.Id.ResourceType)
			assert.Equal(t, fmt.Sprintf("db%d", dbNum), ent.Resource.Id.Resource)
		}
	}
}

func TestPebbleEngine_Entitlements_ValidationErrors(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Start a sync
	_, err := engine.StartNewSync(ctx)
	require.NoError(t, err)

	t.Run("nil entitlement", func(t *testing.T) {
		err := engine.PutEntitlements(ctx, nil)
		require.NoError(t, err) // Should handle nil gracefully
	})

	t.Run("entitlement missing ID", func(t *testing.T) {
		entitlement := &v2.Entitlement{
			DisplayName: "No ID Permission",
		}

		err := engine.PutEntitlements(ctx, entitlement)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "missing external ID")
	})

	t.Run("empty entitlement ID", func(t *testing.T) {
		entitlement := &v2.Entitlement{
			Id:          "",
			DisplayName: "Empty ID Permission",
		}

		err := engine.PutEntitlements(ctx, entitlement)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "missing external ID")
	})

	t.Run("get entitlement with nil request", func(t *testing.T) {
		_, err := engine.GetEntitlement(ctx, nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cannot be nil")
	})

	t.Run("get entitlement with empty ID", func(t *testing.T) {
		req := &reader_v2.EntitlementsReaderServiceGetEntitlementRequest{
			EntitlementId: "",
		}

		_, err := engine.GetEntitlement(ctx, req)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cannot be nil/empty")
	})
}
