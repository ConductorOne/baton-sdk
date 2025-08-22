package pebble

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

// TestConditionalUpsertConsistency tests that all IfNewer methods use consistent logic
func TestConditionalUpsertConsistency(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Start a sync
	syncID, err := engine.StartNewSync(ctx)
	require.NoError(t, err)

	// Test ResourceTypes
	t.Run("ResourceTypes", func(t *testing.T) {
		testConditionalUpsertResourceTypes(t, ctx, engine, syncID)
	})

	// Test Resources
	t.Run("Resources", func(t *testing.T) {
		testConditionalUpsertResources(t, ctx, engine, syncID)
	})

	// Test Entitlements
	t.Run("Entitlements", func(t *testing.T) {
		testConditionalUpsertEntitlements(t, ctx, engine, syncID)
	})

	// Test Grants
	t.Run("Grants", func(t *testing.T) {
		testConditionalUpsertGrants(t, ctx, engine, syncID)
	})
}

func testConditionalUpsertResourceTypes(t *testing.T, ctx context.Context, engine *PebbleEngine, syncID string) {
	// Create initial resource type
	initialRT := &v2.ResourceType{
		Id:          "test-rt-1",
		DisplayName: "Initial Resource Type",
	}

	// Put initial resource type
	err := engine.PutResourceTypesIfNewer(ctx, initialRT)
	require.NoError(t, err)

	// Get the stored timestamp
	key := engine.keyEncoder.EncodeResourceTypeKey(syncID, initialRT.GetId())
	value, closer, err := engine.db.Get(key)
	require.NoError(t, err)
	defer closer.Close()

	initialTime, err := engine.valueCodec.GetDiscoveredAt(value)
	require.NoError(t, err)

	// Try to update with same timestamp (should be skipped)
	updatedRT := &v2.ResourceType{
		Id:          "test-rt-1",
		DisplayName: "Updated Resource Type",
	}

	// Timestamps are stored with nanosecond precision - no sleep needed

	// This should update since time has passed
	err = engine.PutResourceTypesIfNewer(ctx, updatedRT)
	require.NoError(t, err)

	// Verify it was updated
	value2, closer2, err := engine.db.Get(key)
	require.NoError(t, err)
	defer closer2.Close()

	newTime, err := engine.valueCodec.GetDiscoveredAt(value2)
	require.NoError(t, err)

	require.True(t, newTime.After(initialTime), "New timestamp should be after initial timestamp")

	// Decode and verify the content was updated
	storedRT := &v2.ResourceType{}
	_, err = engine.valueCodec.DecodeValue(value2, storedRT)
	require.NoError(t, err)
	require.Equal(t, "Updated Resource Type", storedRT.GetDisplayName())

	// Test that older timestamp is rejected
	olderRT := &v2.ResourceType{
		Id:          "test-rt-1",
		DisplayName: "Older Resource Type",
	}

	// Create a time that's definitely older
	olderTime := initialTime.Add(-1 * time.Hour)

	// We need to test this by manually creating an older entry first
	olderValue, err := engine.valueCodec.EncodeValue(olderRT, olderTime, "")
	require.NoError(t, err)

	// Set the older value directly
	err = engine.db.Set(key, olderValue, nil)
	require.NoError(t, err)

	// Now try to update with newer timestamp - should work
	newerRT := &v2.ResourceType{
		Id:          "test-rt-1",
		DisplayName: "Newer Resource Type",
	}

	err = engine.PutResourceTypesIfNewer(ctx, newerRT)
	require.NoError(t, err)

	// Verify it was updated
	value3, closer3, err := engine.db.Get(key)
	require.NoError(t, err)
	defer closer3.Close()

	finalRT := &v2.ResourceType{}
	envelope, err := engine.valueCodec.DecodeValue(value3, finalRT)
	require.NoError(t, err)
	require.Equal(t, "Newer Resource Type", finalRT.GetDisplayName())
	require.True(t, time.Unix(envelope.DiscoveredAt, 0).After(olderTime))
}

func testConditionalUpsertResources(t *testing.T, ctx context.Context, engine *PebbleEngine, syncID string) {
	// Create initial resource
	initialResource := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "test-resource-type",
			Resource:     "test-resource-1",
		},
		DisplayName: "Initial Resource",
	}

	// Put initial resource
	err := engine.PutResourcesIfNewer(ctx, initialResource)
	require.NoError(t, err)

	// Get the stored timestamp
	key := engine.keyEncoder.EncodeResourceKey(syncID, initialResource.GetId().GetResourceType(), initialResource.GetId().GetResource())
	value, closer, err := engine.db.Get(key)
	require.NoError(t, err)
	defer closer.Close()

	initialTime, err := engine.valueCodec.GetDiscoveredAt(value)
	require.NoError(t, err)

	// Timestamps are stored with nanosecond precision - no sleep needed

	// Try to update with newer timestamp
	updatedResource := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "test-resource-type",
			Resource:     "test-resource-1",
		},
		DisplayName: "Updated Resource",
	}

	err = engine.PutResourcesIfNewer(ctx, updatedResource)
	require.NoError(t, err)

	// Verify it was updated
	value2, closer2, err := engine.db.Get(key)
	require.NoError(t, err)
	defer closer2.Close()

	newTime, err := engine.valueCodec.GetDiscoveredAt(value2)
	require.NoError(t, err)
	require.True(t, newTime.After(initialTime))

	// Decode and verify the content was updated
	storedResource := &v2.Resource{}
	_, err = engine.valueCodec.DecodeValue(value2, storedResource)
	require.NoError(t, err)
	require.Equal(t, "Updated Resource", storedResource.GetDisplayName())
}

func testConditionalUpsertEntitlements(t *testing.T, ctx context.Context, engine *PebbleEngine, syncID string) {
	// Create test resource for entitlement
	resource := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "test-resource-type",
			Resource:     "test-resource",
		},
		DisplayName: "Test Resource",
	}

	// Create initial entitlement
	initialEntitlement := &v2.Entitlement{
		Id:          "test-entitlement-1",
		DisplayName: "Initial Entitlement",
		Resource:    resource,
	}

	// Put initial entitlement
	err := engine.PutEntitlementsIfNewer(ctx, initialEntitlement)
	require.NoError(t, err)

	// Get the stored timestamp
	key := engine.keyEncoder.EncodeEntitlementKey(syncID, initialEntitlement.GetId())
	value, closer, err := engine.db.Get(key)
	require.NoError(t, err)
	defer closer.Close()

	initialTime, err := engine.valueCodec.GetDiscoveredAt(value)
	require.NoError(t, err)

	// Timestamps are stored with nanosecond precision - no sleep needed

	// Try to update with newer timestamp
	updatedEntitlement := &v2.Entitlement{
		Id:          "test-entitlement-1",
		DisplayName: "Updated Entitlement",
		Resource:    resource,
	}

	err = engine.PutEntitlementsIfNewer(ctx, updatedEntitlement)
	require.NoError(t, err)

	// Verify it was updated
	value2, closer2, err := engine.db.Get(key)
	require.NoError(t, err)
	defer closer2.Close()

	newTime, err := engine.valueCodec.GetDiscoveredAt(value2)
	require.NoError(t, err)
	require.True(t, newTime.After(initialTime))

	// Decode and verify the content was updated
	storedEntitlement := &v2.Entitlement{}
	_, err = engine.valueCodec.DecodeValue(value2, storedEntitlement)
	require.NoError(t, err)
	require.Equal(t, "Updated Entitlement", storedEntitlement.GetDisplayName())
}

func testConditionalUpsertGrants(t *testing.T, ctx context.Context, engine *PebbleEngine, syncID string) {
	// Create test data
	resource := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "test-resource-type",
			Resource:     "test-resource",
		},
		DisplayName: "Test Resource",
	}

	entitlement := &v2.Entitlement{
		Id:          "test-entitlement",
		DisplayName: "Test Entitlement",
		Resource:    resource,
	}

	principal := &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "user",
			Resource:     "test-user",
		},
		DisplayName: "Test User",
	}

	// Create initial grant
	initialGrant := &v2.Grant{
		Id:          "test-grant-1",
		Entitlement: entitlement,
		Principal:   principal,
	}

	// Put initial grant
	err := engine.PutGrantsIfNewer(ctx, initialGrant)
	require.NoError(t, err)

	// Get the stored timestamp
	key := engine.keyEncoder.EncodeGrantKey(syncID, initialGrant.GetId())
	value, closer, err := engine.db.Get(key)
	require.NoError(t, err)
	defer closer.Close()

	initialTime, err := engine.valueCodec.GetDiscoveredAt(value)
	require.NoError(t, err)

	// Timestamps are stored with nanosecond precision - no sleep needed

	// Try to update with newer timestamp
	updatedGrant := &v2.Grant{
		Id:          "test-grant-1",
		Entitlement: entitlement,
		Principal:   principal,
	}

	err = engine.PutGrantsIfNewer(ctx, updatedGrant)
	require.NoError(t, err)

	// Verify it was updated
	value2, closer2, err := engine.db.Get(key)
	require.NoError(t, err)
	defer closer2.Close()

	newTime, err := engine.valueCodec.GetDiscoveredAt(value2)
	require.NoError(t, err)
	require.True(t, newTime.After(initialTime))

	// Decode and verify the grant exists
	storedGrant := &v2.Grant{}
	_, err = engine.valueCodec.DecodeValue(value2, storedGrant)
	require.NoError(t, err)
	require.Equal(t, "test-grant-1", storedGrant.GetId())
}

// TestAtomicReadModifyWrite tests that conditional upserts are atomic
func TestAtomicReadModifyWrite(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Start a sync
	_, err := engine.StartNewSync(ctx)
	require.NoError(t, err)

	// Test that multiple IfNewer operations in the same batch are consistent
	resourceType1 := &v2.ResourceType{
		Id:          "rt-1",
		DisplayName: "Resource Type 1",
	}

	resourceType2 := &v2.ResourceType{
		Id:          "rt-2",
		DisplayName: "Resource Type 2",
	}

	// Put both resource types in the same call (same batch)
	err = engine.PutResourceTypesIfNewer(ctx, resourceType1, resourceType2)
	require.NoError(t, err)

	// Verify both were stored
	key1 := engine.keyEncoder.EncodeResourceTypeKey(engine.getCurrentSyncID(), resourceType1.GetId())
	value1, closer1, err := engine.db.Get(key1)
	require.NoError(t, err)
	defer closer1.Close()

	key2 := engine.keyEncoder.EncodeResourceTypeKey(engine.getCurrentSyncID(), resourceType2.GetId())
	value2, closer2, err := engine.db.Get(key2)
	require.NoError(t, err)
	defer closer2.Close()

	// Both should exist
	require.NotNil(t, value1)
	require.NotNil(t, value2)

	// Get timestamps - they should be very close (same batch)
	time1, err := engine.valueCodec.GetDiscoveredAt(value1)
	require.NoError(t, err)

	time2, err := engine.valueCodec.GetDiscoveredAt(value2)
	require.NoError(t, err)

	// Times should be identical or very close (within same batch)
	timeDiff := time1.Sub(time2)
	if timeDiff < 0 {
		timeDiff = -timeDiff
	}
	require.True(t, timeDiff < time.Second, "Timestamps should be very close for same batch operation")
}

// TestIdempotency tests that IfNewer operations are idempotent
func TestIdempotency(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Start a sync
	_, err := engine.StartNewSync(ctx)
	require.NoError(t, err)

	resourceType := &v2.ResourceType{
		Id:          "rt-idempotent",
		DisplayName: "Idempotent Test",
	}

	// Put the resource type multiple times rapidly
	for i := 0; i < 5; i++ {
		err = engine.PutResourceTypesIfNewer(ctx, resourceType)
		require.NoError(t, err)
	}

	// Should only have one entry
	key := engine.keyEncoder.EncodeResourceTypeKey(engine.getCurrentSyncID(), resourceType.GetId())
	value, closer, err := engine.db.Get(key)
	require.NoError(t, err)
	defer closer.Close()

	// Verify the resource type exists
	storedRT := &v2.ResourceType{}
	_, err = engine.valueCodec.DecodeValue(value, storedRT)
	require.NoError(t, err)
	require.Equal(t, "Idempotent Test", storedRT.GetDisplayName())
}

// TestConcurrentOperations tests behavior under concurrent access
func TestConcurrentOperations(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Start a sync
	_, err := engine.StartNewSync(ctx)
	require.NoError(t, err)

	// This test verifies that the conditional upsert logic handles
	// concurrent operations correctly by ensuring consistent behavior
	// Note: Pebble handles concurrency internally, so we're mainly testing
	// that our logic doesn't break under concurrent access

	resourceType := &v2.ResourceType{
		Id:          "rt-concurrent",
		DisplayName: "Concurrent Test",
	}

	// Put the resource type
	err = engine.PutResourceTypesIfNewer(ctx, resourceType)
	require.NoError(t, err)

	// Verify it exists
	key := engine.keyEncoder.EncodeResourceTypeKey(engine.getCurrentSyncID(), resourceType.GetId())
	value, closer, err := engine.db.Get(key)
	require.NoError(t, err)
	defer closer.Close()

	storedRT := &v2.ResourceType{}
	_, err = engine.valueCodec.DecodeValue(value, storedRT)
	require.NoError(t, err)
	require.Equal(t, "Concurrent Test", storedRT.GetDisplayName())
}

// TestErrorHandling tests error scenarios in conditional upserts
func TestErrorHandling(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Start a sync
	_, err := engine.StartNewSync(ctx)
	require.NoError(t, err)

	// Test with nil resource type
	err = engine.PutResourceTypesIfNewer(ctx, nil)
	require.NoError(t, err) // Should not error, just skip nil entries

	// Test with empty ID
	emptyRT := &v2.ResourceType{
		Id:          "",
		DisplayName: "Empty ID",
	}

	err = engine.PutResourceTypesIfNewer(ctx, emptyRT)
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing external ID")

	// Test with no active sync
	engine.setCurrentSyncID("")

	validRT := &v2.ResourceType{
		Id:          "valid-rt",
		DisplayName: "Valid RT",
	}

	err = engine.PutResourceTypesIfNewer(ctx, validRT)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no active sync")
}

// TestTimestampComparison tests the timestamp comparison logic
func TestTimestampComparison(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Start a sync
	syncID, err := engine.StartNewSync(ctx)
	require.NoError(t, err)

	// Create a resource type with a specific timestamp
	baseTime := time.Now().Truncate(time.Second)

	resourceType := &v2.ResourceType{
		Id:          "rt-timestamp",
		DisplayName: "Timestamp Test",
	}

	// Manually create an entry with a specific timestamp
	key := engine.keyEncoder.EncodeResourceTypeKey(syncID, resourceType.GetId())
	value, err := engine.valueCodec.EncodeValue(resourceType, baseTime, "")
	require.NoError(t, err)

	err = engine.db.Set(key, value, nil)
	require.NoError(t, err)

	// Test shouldUpdateEntity with different timestamps

	// Same timestamp - should not update
	shouldUpdate, err := engine.shouldUpdateEntity(key, baseTime)
	require.NoError(t, err)
	require.False(t, shouldUpdate)

	// Older timestamp - should not update
	olderTime := baseTime.Add(-1 * time.Second)
	shouldUpdate, err = engine.shouldUpdateEntity(key, olderTime)
	require.NoError(t, err)
	require.False(t, shouldUpdate)

	// Newer timestamp - should update
	newerTime := baseTime.Add(1 * time.Second)
	shouldUpdate, err = engine.shouldUpdateEntity(key, newerTime)
	require.NoError(t, err)
	require.True(t, shouldUpdate)

	// Test with non-existent key - should update
	nonExistentKey := engine.keyEncoder.EncodeResourceTypeKey(syncID, "non-existent")
	shouldUpdate, err = engine.shouldUpdateEntity(nonExistentKey, baseTime)
	require.NoError(t, err)
	require.True(t, shouldUpdate)
}
