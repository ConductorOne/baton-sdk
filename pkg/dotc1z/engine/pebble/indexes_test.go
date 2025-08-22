package pebble

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

func TestIndexManager_CreateEntitlementsByResourceIndex(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	engine, err := NewPebbleEngine(ctx, tempDir)
	require.NoError(t, err)
	defer engine.Close()

	// Start a sync to have a valid sync ID
	syncID := "test-sync-1"
	engine.setCurrentSyncID(syncID)

	batch := engine.db.NewBatch()
	defer batch.Close()

	tests := []struct {
		name        string
		entitlement *v2.Entitlement
		expectIndex bool
		expectError bool
	}{
		{
			name: "valid entitlement with resource",
			entitlement: &v2.Entitlement{
				Id: "ent-1",
				Resource: &v2.Resource{
					Id: &v2.ResourceId{
						ResourceType: "user",
						Resource:     "user-123",
					},
				},
			},
			expectIndex: true,
			expectError: false,
		},
		{
			name: "entitlement without resource",
			entitlement: &v2.Entitlement{
				Id: "ent-2",
			},
			expectIndex: false,
			expectError: false,
		},
		{
			name: "entitlement with incomplete resource",
			entitlement: &v2.Entitlement{
				Id: "ent-3",
				Resource: &v2.Resource{
					Id: &v2.ResourceId{
						ResourceType: "user",
						// Missing Resource field
					},
				},
			},
			expectIndex: false,
			expectError: false,
		},
		{
			name:        "nil entitlement",
			entitlement: nil,
			expectIndex: false,
			expectError: true,
		},
		{
			name: "entitlement without ID",
			entitlement: &v2.Entitlement{
				Resource: &v2.Resource{
					Id: &v2.ResourceId{
						ResourceType: "user",
						Resource:     "user-123",
					},
				},
			},
			expectIndex: false,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := engine.indexManager.CreateEntitlementsByResourceIndex(batch, syncID, tt.entitlement)

			if tt.expectError {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)

			if tt.expectIndex && tt.entitlement != nil {
				// Verify the index key was created
				resourceID := tt.entitlement.GetResource().GetId()
				expectedKey := engine.keyEncoder.EncodeEntitlementsByResourceIndexKey(
					syncID,
					resourceID.GetResourceType(),
					resourceID.GetResource(),
					tt.entitlement.GetId(),
				)

				// Check if the key exists in the batch (we can't easily verify this without committing)
				// So we'll commit and check
				err := batch.Commit(engine.getSyncOptions())
				require.NoError(t, err)

				// Create a new batch for the next test
				batch = engine.db.NewBatch()

				// Verify the index exists
				_, closer, err := engine.db.Get(expectedKey)
				assert.NoError(t, err)
				if closer != nil {
					closer.Close()
				}
			}
		})
	}
}

func TestIndexManager_DeleteEntitlementsByResourceIndex(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	engine, err := NewPebbleEngine(ctx, tempDir)
	require.NoError(t, err)
	defer engine.Close()

	syncID := "test-sync-1"
	engine.setCurrentSyncID(syncID)

	entitlement := &v2.Entitlement{
		Id: "ent-1",
		Resource: &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: "user",
				Resource:     "user-123",
			},
		},
	}

	// First create the index
	batch := engine.db.NewBatch()
	err = engine.indexManager.CreateEntitlementsByResourceIndex(batch, syncID, entitlement)
	require.NoError(t, err)
	err = batch.Commit(engine.getSyncOptions())
	require.NoError(t, err)
	batch.Close()

	// Verify index exists
	resourceID := entitlement.GetResource().GetId()
	indexKey := engine.keyEncoder.EncodeEntitlementsByResourceIndexKey(
		syncID,
		resourceID.GetResourceType(),
		resourceID.GetResource(),
		entitlement.GetId(),
	)

	_, closer, err := engine.db.Get(indexKey)
	require.NoError(t, err)
	closer.Close()

	// Now delete the index
	batch = engine.db.NewBatch()
	err = engine.indexManager.DeleteEntitlementsByResourceIndex(batch, syncID, entitlement)
	require.NoError(t, err)
	err = batch.Commit(engine.getSyncOptions())
	require.NoError(t, err)
	batch.Close()

	// Verify index no longer exists
	_, closer, err = engine.db.Get(indexKey)
	assert.Equal(t, pebble.ErrNotFound, err)
	if closer != nil {
		closer.Close()
	}
}

func TestIndexManager_CreateGrantIndexes(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	engine, err := NewPebbleEngine(ctx, tempDir)
	require.NoError(t, err)
	defer engine.Close()

	syncID := "test-sync-1"
	engine.setCurrentSyncID(syncID)

	grant := &v2.Grant{
		Id: "grant-1",
		Principal: &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: "user",
				Resource:     "user-456",
			},
		},
		Entitlement: &v2.Entitlement{
			Id: "ent-789",
			Resource: &v2.Resource{
				Id: &v2.ResourceId{
					ResourceType: "group",
					Resource:     "group-123",
				},
			},
		},
	}

	batch := engine.db.NewBatch()
	defer batch.Close()

	// Test grants-by-resource index
	t.Run("grants-by-resource", func(t *testing.T) {
		err := engine.indexManager.CreateGrantsByResourceIndex(batch, syncID, grant)
		assert.NoError(t, err)
	})

	// Test grants-by-principal index
	t.Run("grants-by-principal", func(t *testing.T) {
		err := engine.indexManager.CreateGrantsByPrincipalIndex(batch, syncID, grant)
		assert.NoError(t, err)
	})

	// Test grants-by-entitlement index
	t.Run("grants-by-entitlement", func(t *testing.T) {
		err := engine.indexManager.CreateGrantsByEntitlementIndex(batch, syncID, grant)
		assert.NoError(t, err)
	})

	// Test creating all indexes at once
	t.Run("create-all-indexes", func(t *testing.T) {
		err := engine.indexManager.CreateAllGrantIndexes(batch, syncID, grant)
		assert.NoError(t, err)
	})

	// Commit and verify indexes exist
	err = batch.Commit(engine.getSyncOptions())
	require.NoError(t, err)

	// Verify grants-by-resource index
	resourceIndexKey := engine.keyEncoder.EncodeGrantsByResourceIndexKey(
		syncID,
		grant.GetEntitlement().GetResource().GetId().GetResourceType(),
		grant.GetEntitlement().GetResource().GetId().GetResource(),
		grant.GetId(),
	)
	_, closer, err := engine.db.Get(resourceIndexKey)
	assert.NoError(t, err)
	if closer != nil {
		closer.Close()
	}

	// Verify grants-by-principal index
	principalIndexKey := engine.keyEncoder.EncodeGrantsByPrincipalIndexKey(
		syncID,
		grant.GetPrincipal().GetId().GetResourceType(),
		grant.GetPrincipal().GetId().GetResource(),
		grant.GetId(),
	)
	_, closer, err = engine.db.Get(principalIndexKey)
	assert.NoError(t, err)
	if closer != nil {
		closer.Close()
	}

	// Verify grants-by-entitlement index
	entitlementIndexKey := engine.keyEncoder.EncodeGrantsByEntitlementIndexKey(
		syncID,
		grant.GetEntitlement().GetId(),
		grant.GetPrincipal().GetId().GetResourceType(),
		grant.GetPrincipal().GetId().GetResource(),
		grant.GetId(),
	)
	_, closer, err = engine.db.Get(entitlementIndexKey)
	assert.NoError(t, err)
	if closer != nil {
		closer.Close()
	}
}

func TestIndexManager_DeleteGrantIndexes(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	engine, err := NewPebbleEngine(ctx, tempDir)
	require.NoError(t, err)
	defer engine.Close()

	syncID := "test-sync-1"
	engine.setCurrentSyncID(syncID)

	grant := &v2.Grant{
		Id: "grant-1",
		Principal: &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: "user",
				Resource:     "user-456",
			},
		},
		Entitlement: &v2.Entitlement{
			Id: "ent-789",
			Resource: &v2.Resource{
				Id: &v2.ResourceId{
					ResourceType: "group",
					Resource:     "group-123",
				},
			},
		},
	}

	// First create all indexes
	batch := engine.db.NewBatch()
	err = engine.indexManager.CreateAllGrantIndexes(batch, syncID, grant)
	require.NoError(t, err)
	err = batch.Commit(engine.getSyncOptions())
	require.NoError(t, err)
	batch.Close()

	// Now delete all indexes
	batch = engine.db.NewBatch()
	err = engine.indexManager.DeleteAllGrantIndexes(batch, syncID, grant)
	require.NoError(t, err)
	err = batch.Commit(engine.getSyncOptions())
	require.NoError(t, err)
	batch.Close()

	// Verify all indexes are deleted
	resourceIndexKey := engine.keyEncoder.EncodeGrantsByResourceIndexKey(
		syncID,
		grant.GetEntitlement().GetResource().GetId().GetResourceType(),
		grant.GetEntitlement().GetResource().GetId().GetResource(),
		grant.GetId(),
	)
	_, closer, err := engine.db.Get(resourceIndexKey)
	assert.Equal(t, pebble.ErrNotFound, err)
	if closer != nil {
		closer.Close()
	}

	principalIndexKey := engine.keyEncoder.EncodeGrantsByPrincipalIndexKey(
		syncID,
		grant.GetPrincipal().GetId().GetResourceType(),
		grant.GetPrincipal().GetId().GetResource(),
		grant.GetId(),
	)
	_, closer, err = engine.db.Get(principalIndexKey)
	assert.Equal(t, pebble.ErrNotFound, err)
	if closer != nil {
		closer.Close()
	}

	entitlementIndexKey := engine.keyEncoder.EncodeGrantsByEntitlementIndexKey(
		syncID,
		grant.GetEntitlement().GetId(),
		grant.GetPrincipal().GetId().GetResourceType(),
		grant.GetPrincipal().GetId().GetResource(),
		grant.GetId(),
	)
	_, closer, err = engine.db.Get(entitlementIndexKey)
	assert.Equal(t, pebble.ErrNotFound, err)
	if closer != nil {
		closer.Close()
	}
}

func TestIndexManager_ValidateIndexConsistency(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	engine, err := NewPebbleEngine(ctx, tempDir)
	require.NoError(t, err)
	defer engine.Close()

	syncID := "test-sync-1"
	engine.setCurrentSyncID(syncID)

	// Create an entitlement with proper primary data
	entitlement := &v2.Entitlement{
		Id: "ent-1",
		Resource: &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: "user",
				Resource:     "user-123",
			},
		},
	}

	// Store the entitlement in primary storage
	err = engine.PutEntitlements(ctx, entitlement)
	require.NoError(t, err)

	// Validate consistency - should pass
	err = engine.indexManager.ValidateIndexConsistency(ctx, syncID)
	assert.NoError(t, err)

	// Create an orphaned index entry (index without primary data)
	batch := engine.db.NewBatch()
	orphanedEntitlement := &v2.Entitlement{
		Id: "ent-orphaned",
		Resource: &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: "user",
				Resource:     "user-orphaned",
			},
		},
	}
	err = engine.indexManager.CreateEntitlementsByResourceIndex(batch, syncID, orphanedEntitlement)
	require.NoError(t, err)
	err = batch.Commit(engine.getSyncOptions())
	require.NoError(t, err)
	batch.Close()

	// Validate consistency - should fail due to orphaned index
	err = engine.indexManager.ValidateIndexConsistency(ctx, syncID)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "index inconsistency")
}

func TestIndexManager_RebuildIndexes(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	engine, err := NewPebbleEngine(ctx, tempDir)
	require.NoError(t, err)
	defer engine.Close()

	syncID := "test-sync-1"
	engine.setCurrentSyncID(syncID)

	// Create some entitlements
	entitlements := []*v2.Entitlement{
		{
			Id: "ent-1",
			Resource: &v2.Resource{
				Id: &v2.ResourceId{
					ResourceType: "user",
					Resource:     "user-123",
				},
			},
		},
		{
			Id: "ent-2",
			Resource: &v2.Resource{
				Id: &v2.ResourceId{
					ResourceType: "group",
					Resource:     "group-456",
				},
			},
		},
		{
			Id: "ent-3", // No resource, should not have index
		},
	}

	// Store entitlements
	err = engine.PutEntitlements(ctx, entitlements...)
	require.NoError(t, err)

	// Manually corrupt indexes by creating an orphaned index entry
	batch := engine.db.NewBatch()
	orphanedEntitlement := &v2.Entitlement{
		Id: "ent-orphaned",
		Resource: &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: "user",
				Resource:     "user-orphaned",
			},
		},
	}
	err = engine.indexManager.CreateEntitlementsByResourceIndex(batch, syncID, orphanedEntitlement)
	require.NoError(t, err)
	err = batch.Commit(engine.getSyncOptions())
	require.NoError(t, err)
	batch.Close()

	// Validate consistency - should fail due to orphaned index
	err = engine.indexManager.ValidateIndexConsistency(ctx, syncID)
	assert.Error(t, err)

	// Rebuild indexes
	err = engine.indexManager.RebuildIndexes(ctx, syncID)
	require.NoError(t, err)

	// Validate consistency - should pass now
	err = engine.indexManager.ValidateIndexConsistency(ctx, syncID)
	assert.NoError(t, err)

	// Verify specific indexes exist
	indexKey1 := engine.keyEncoder.EncodeEntitlementsByResourceIndexKey(
		syncID,
		"user",
		"user-123",
		"ent-1",
	)
	_, closer, err := engine.db.Get(indexKey1)
	assert.NoError(t, err)
	if closer != nil {
		closer.Close()
	}

	indexKey2 := engine.keyEncoder.EncodeEntitlementsByResourceIndexKey(
		syncID,
		"group",
		"group-456",
		"ent-2",
	)
	_, closer, err = engine.db.Get(indexKey2)
	assert.NoError(t, err)
	if closer != nil {
		closer.Close()
	}

	// Verify ent-3 has no index (since it has no resource)
	// We can't easily test this without knowing all possible resource combinations
	// But the rebuild should only create indexes for entitlements with resources
}

func TestIndexManager_GetIndexStats(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	engine, err := NewPebbleEngine(ctx, tempDir)
	require.NoError(t, err)
	defer engine.Close()

	syncID := "test-sync-1"
	engine.setCurrentSyncID(syncID)

	// Initially should have no index entries
	stats, err := engine.indexManager.GetIndexStats(ctx, syncID)
	require.NoError(t, err)
	assert.Equal(t, int64(0), stats["entitlements_by_resource_index_entries"])

	// Create some entitlements with resources
	entitlements := []*v2.Entitlement{
		{
			Id: "ent-1",
			Resource: &v2.Resource{
				Id: &v2.ResourceId{
					ResourceType: "user",
					Resource:     "user-123",
				},
			},
		},
		{
			Id: "ent-2",
			Resource: &v2.Resource{
				Id: &v2.ResourceId{
					ResourceType: "group",
					Resource:     "group-456",
				},
			},
		},
		{
			Id: "ent-3", // No resource, should not create index
		},
	}

	err = engine.PutEntitlements(ctx, entitlements...)
	require.NoError(t, err)

	// Should now have 2 index entries (ent-1 and ent-2 have resources)
	stats, err = engine.indexManager.GetIndexStats(ctx, syncID)
	require.NoError(t, err)
	assert.Equal(t, int64(2), stats["entitlements_by_resource_index_entries"])
}

func TestIndexManager_PerformanceWithLargeDataset(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	ctx := context.Background()
	tempDir := t.TempDir()

	engine, err := NewPebbleEngine(ctx, tempDir)
	require.NoError(t, err)
	defer engine.Close()

	syncID := "test-sync-1"
	engine.setCurrentSyncID(syncID)

	// Create a large number of entitlements
	const numEntitlements = 10000
	entitlements := make([]*v2.Entitlement, numEntitlements)

	for i := 0; i < numEntitlements; i++ {
		entitlements[i] = &v2.Entitlement{
			Id: fmt.Sprintf("ent-%d", i),
			Resource: &v2.Resource{
				Id: &v2.ResourceId{
					ResourceType: "user",
					Resource:     fmt.Sprintf("user-%d", i%100), // 100 different users
				},
			},
		}
	}

	// Measure time to create entitlements with indexes
	start := time.Now()
	err = engine.PutEntitlements(ctx, entitlements...)
	require.NoError(t, err)
	createDuration := time.Since(start)

	t.Logf("Created %d entitlements with indexes in %v", numEntitlements, createDuration)

	// Measure time to validate index consistency
	start = time.Now()
	err = engine.indexManager.ValidateIndexConsistency(ctx, syncID)
	require.NoError(t, err)
	validateDuration := time.Since(start)

	t.Logf("Validated index consistency in %v", validateDuration)

	// Measure time to rebuild indexes
	start = time.Now()
	err = engine.indexManager.RebuildIndexes(ctx, syncID)
	require.NoError(t, err)
	rebuildDuration := time.Since(start)

	t.Logf("Rebuilt indexes in %v", rebuildDuration)

	// Get stats
	stats, err := engine.indexManager.GetIndexStats(ctx, syncID)
	require.NoError(t, err)
	t.Logf("Index stats: %+v", stats)

	// Verify we have the expected number of index entries
	assert.Equal(t, int64(numEntitlements), stats["entitlements_by_resource_index_entries"])

	// Performance assertions (these are rough guidelines)
	assert.Less(t, createDuration, 30*time.Second, "Creating entitlements with indexes should be reasonably fast")
	assert.Less(t, validateDuration, 10*time.Second, "Index validation should be reasonably fast")
	assert.Less(t, rebuildDuration, 20*time.Second, "Index rebuilding should be reasonably fast")
}

func TestIndexManager_ConcurrentAccess(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	engine, err := NewPebbleEngine(ctx, tempDir)
	require.NoError(t, err)
	defer engine.Close()

	syncID := "test-sync-1"
	engine.setCurrentSyncID(syncID)

	// Test concurrent index operations
	const numGoroutines = 10
	const numOperationsPerGoroutine = 100

	// Channel to collect errors
	errChan := make(chan error, numGoroutines)

	// Start multiple goroutines performing index operations
	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer func() {
				errChan <- nil // Signal completion
			}()

			for j := 0; j < numOperationsPerGoroutine; j++ {
				entitlement := &v2.Entitlement{
					Id: fmt.Sprintf("ent-%d-%d", goroutineID, j),
					Resource: &v2.Resource{
						Id: &v2.ResourceId{
							ResourceType: "user",
							Resource:     fmt.Sprintf("user-%d", goroutineID),
						},
					},
				}

				// Create entitlement (which creates indexes)
				err := engine.PutEntitlements(ctx, entitlement)
				if err != nil {
					errChan <- fmt.Errorf("goroutine %d: failed to put entitlement: %w", goroutineID, err)
					return
				}
			}
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < numGoroutines; i++ {
		err := <-errChan
		if err != nil {
			t.Errorf("Concurrent operation failed: %v", err)
		}
	}

	// Validate final consistency
	err = engine.indexManager.ValidateIndexConsistency(ctx, syncID)
	assert.NoError(t, err)

	// Verify expected number of index entries
	stats, err := engine.indexManager.GetIndexStats(ctx, syncID)
	require.NoError(t, err)
	expectedEntries := int64(numGoroutines * numOperationsPerGoroutine)
	assert.Equal(t, expectedEntries, stats["entitlements_by_resource_index_entries"])
}

// Helper function to create a test engine for index tests
func createIndexTestEngine(t testing.TB) (*PebbleEngine, string) {
	ctx := context.Background()
	tempDir := t.TempDir()

	engine, err := NewPebbleEngine(ctx, tempDir)
	require.NoError(t, err)

	syncID := "test-sync-1"
	engine.setCurrentSyncID(syncID)

	return engine, syncID
}

// Benchmark index creation performance
func BenchmarkIndexManager_CreateEntitlementsByResourceIndex(b *testing.B) {
	engine, syncID := createIndexTestEngine(b)
	defer engine.Close()

	entitlement := &v2.Entitlement{
		Id: "ent-1",
		Resource: &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: "user",
				Resource:     "user-123",
			},
		},
	}

	batch := engine.db.NewBatch()
	defer batch.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := engine.indexManager.CreateEntitlementsByResourceIndex(batch, syncID, entitlement)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark index validation performance
func BenchmarkIndexManager_ValidateIndexConsistency(b *testing.B) {
	engine, syncID := createIndexTestEngine(b)
	defer engine.Close()

	ctx := context.Background()

	// Create some test data
	entitlements := make([]*v2.Entitlement, 1000)
	for i := 0; i < 1000; i++ {
		entitlements[i] = &v2.Entitlement{
			Id: fmt.Sprintf("ent-%d", i),
			Resource: &v2.Resource{
				Id: &v2.ResourceId{
					ResourceType: "user",
					Resource:     fmt.Sprintf("user-%d", i%10),
				},
			},
		}
	}

	err := engine.PutEntitlements(ctx, entitlements...)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := engine.indexManager.ValidateIndexConsistency(ctx, syncID)
		if err != nil {
			b.Fatal(err)
		}
	}
}
