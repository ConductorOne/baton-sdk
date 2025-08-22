package pebble

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

func TestIntegrityChecker_CheckIndexConsistency(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	pe, err := NewPebbleEngine(ctx, tempDir)
	require.NoError(t, err)
	defer pe.Close()

	// Create a sync with data
	_, err = pe.StartNewSync(ctx)
	require.NoError(t, err)

	// Add some entitlements
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
	}

	err = pe.PutEntitlements(ctx, entitlements...)
	require.NoError(t, err)

	// End the sync
	err = pe.EndSync(ctx)
	require.NoError(t, err)

	// Check index consistency - should pass
	err = pe.IntegrityChecker().CheckIndexConsistency(ctx)
	assert.NoError(t, err)
}

func TestIntegrityChecker_RebuildIndexes(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	pe, err := NewPebbleEngine(ctx, tempDir)
	require.NoError(t, err)
	defer pe.Close()

	// Create a sync with data
	_, err = pe.StartNewSync(ctx)
	require.NoError(t, err)

	// Add some entitlements
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
	}

	err = pe.PutEntitlements(ctx, entitlements...)
	require.NoError(t, err)

	// End the sync
	err = pe.EndSync(ctx)
	require.NoError(t, err)

	// Rebuild indexes - should succeed
	err = pe.IntegrityChecker().RebuildIndexes(ctx)
	assert.NoError(t, err)

	// Check that indexes are still consistent after rebuild
	err = pe.IntegrityChecker().CheckIndexConsistency(ctx)
	assert.NoError(t, err)
}

func TestIntegrityChecker_ValidateReferentialIntegrity(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	pe, err := NewPebbleEngine(ctx, tempDir)
	require.NoError(t, err)
	defer pe.Close()

	// Create a sync with data
	_, err = pe.StartNewSync(ctx)
	require.NoError(t, err)

	// Add resource types
	err = pe.PutResourceTypes(ctx, &v2.ResourceType{
		Id:          "user",
		DisplayName: "User",
	})
	require.NoError(t, err)

	// Add resources
	err = pe.PutResources(ctx, &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "user",
			Resource:     "user-123",
		},
		DisplayName: "User 123",
	})
	require.NoError(t, err)

	// Add entitlements
	err = pe.PutEntitlements(ctx, &v2.Entitlement{
		Id: "ent-1",
		Resource: &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: "user",
				Resource:     "user-123",
			},
		},
	})
	require.NoError(t, err)

	// End the sync
	err = pe.EndSync(ctx)
	require.NoError(t, err)

	// Validate referential integrity - should pass
	err = pe.IntegrityChecker().ValidateReferentialIntegrity(ctx)
	assert.NoError(t, err)
}

func TestCompactionManager_TriggerManualCompaction(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	pe, err := NewPebbleEngine(ctx, tempDir)
	require.NoError(t, err)
	defer pe.Close()

	// Trigger manual compaction - should succeed
	err = pe.CompactionManager().TriggerManualCompaction(ctx)
	assert.NoError(t, err)
}

func TestCompactionManager_EstimateSpaceReclamation(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	pe, err := NewPebbleEngine(ctx, tempDir)
	require.NoError(t, err)
	defer pe.Close()

	// Estimate space reclamation - should return a non-negative value
	estimatedSpace, err := pe.CompactionManager().EstimateSpaceReclamation(ctx)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, estimatedSpace, int64(0))
}

func TestCompactionManager_GetCompactionStats(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	pe, err := NewPebbleEngine(ctx, tempDir)
	require.NoError(t, err)
	defer pe.Close()

	// Get compaction stats - should succeed and return non-empty map
	stats, err := pe.CompactionManager().GetCompactionStats(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, stats)
	assert.Greater(t, len(stats), 0)

	// Check that expected keys exist
	expectedKeys := []string{
		"block_cache_size",
		"block_cache_count",
		"table_cache_size",
		"table_cache_count",
		"memtable_size",
		"memtable_count",
	}

	for _, key := range expectedKeys {
		_, exists := stats[key]
		assert.True(t, exists, "Expected key %s in compaction stats", key)
	}
}

func TestCompactionManager_TriggerFullCompaction(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	pe, err := NewPebbleEngine(ctx, tempDir)
	require.NoError(t, err)
	defer pe.Close()

	// Trigger full compaction - should succeed
	err = pe.CompactionManager().TriggerFullCompaction(ctx)
	assert.NoError(t, err)
}

func TestCompactionManager_ConfigureCompactionPolicy(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	pe, err := NewPebbleEngine(ctx, tempDir)
	require.NoError(t, err)
	defer pe.Close()

	// Test all compaction policies
	policies := []CompactionPolicy{
		CompactionPolicyDefault,
		CompactionPolicyAggressive,
		CompactionPolicyConservative,
		CompactionPolicyManual,
	}

	for _, policy := range policies {
		err := pe.CompactionManager().ConfigureCompactionPolicy(policy)
		assert.NoError(t, err, "Failed to configure policy: %s", policy.String())
	}
}

func TestCompactionPolicy_String(t *testing.T) {
	// Test string representation of compaction policies
	testCases := []struct {
		policy   CompactionPolicy
		expected string
	}{
		{CompactionPolicyDefault, "default"},
		{CompactionPolicyAggressive, "aggressive"},
		{CompactionPolicyConservative, "conservative"},
		{CompactionPolicyManual, "manual"},
		{CompactionPolicy(999), "unknown"},
	}

	for _, tc := range testCases {
		result := tc.policy.String()
		assert.Equal(t, tc.expected, result)
	}
}

func TestMaintenanceOperations_Integration(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	pe, err := NewPebbleEngine(ctx, tempDir)
	require.NoError(t, err)
	defer pe.Close()

	// Create multiple syncs with data
	for i := 0; i < 3; i++ {
		_, err = pe.StartNewSync(ctx)
		require.NoError(t, err)

		// Add some data
		err = pe.PutResourceTypes(ctx, &v2.ResourceType{
			Id:          fmt.Sprintf("rt-%d", i),
			DisplayName: fmt.Sprintf("Resource Type %d", i),
		})
		require.NoError(t, err)

		err = pe.PutEntitlements(ctx, &v2.Entitlement{
			Id: fmt.Sprintf("ent-%d", i),
			Resource: &v2.Resource{
				Id: &v2.ResourceId{
					ResourceType: fmt.Sprintf("rt-%d", i),
					Resource:     fmt.Sprintf("r-%d", i),
				},
			},
		})
		require.NoError(t, err)

		err = pe.EndSync(ctx)
		require.NoError(t, err)
	}

	// Test integrity checker operations
	err = pe.IntegrityChecker().CheckIndexConsistency(ctx)
	assert.NoError(t, err)

	err = pe.IntegrityChecker().ValidateReferentialIntegrity(ctx)
	assert.NoError(t, err)

	// Test compaction manager operations
	err = pe.CompactionManager().TriggerManualCompaction(ctx)
	assert.NoError(t, err)

	estimatedSpace, err := pe.CompactionManager().EstimateSpaceReclamation(ctx)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, estimatedSpace, int64(0))

	stats, err := pe.CompactionManager().GetCompactionStats(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, stats)

	// Test cleanup operation
	err = pe.Cleanup(ctx)
	assert.NoError(t, err)

	// Verify that cleanup worked and only latest syncs remain
	// (This depends on the default cleanup behavior keeping 2 syncs)
}

func TestMaintenanceOperations_ErrorHandling(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	pe, err := NewPebbleEngine(ctx, tempDir)
	require.NoError(t, err)
	defer pe.Close()

	// Test operations on closed engine
	pe.Close()

	// These operations should fail on closed engine
	err = pe.IntegrityChecker().CheckIndexConsistency(ctx)
	assert.Error(t, err)

	err = pe.CompactionManager().TriggerManualCompaction(ctx)
	assert.Error(t, err)

	_, err = pe.CompactionManager().EstimateSpaceReclamation(ctx)
	assert.Error(t, err)

	_, err = pe.CompactionManager().GetCompactionStats(ctx)
	assert.Error(t, err)
}

func TestMaintenanceOperations_Performance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	ctx := context.Background()
	tempDir := t.TempDir()

	pe, err := NewPebbleEngine(ctx, tempDir)
	require.NoError(t, err)
	defer pe.Close()

	// Create a large dataset
	const numSyncs = 10
	const numEntitlementsPerSync = 1000

	for i := 0; i < numSyncs; i++ {
		_, err = pe.StartNewSync(ctx)
		require.NoError(t, err)

		// Create many entitlements
		entitlements := make([]*v2.Entitlement, numEntitlementsPerSync)
		for j := 0; j < numEntitlementsPerSync; j++ {
			entitlements[j] = &v2.Entitlement{
				Id: fmt.Sprintf("ent-%d-%d", i, j),
				Resource: &v2.Resource{
					Id: &v2.ResourceId{
						ResourceType: fmt.Sprintf("rt-%d", i),
						Resource:     fmt.Sprintf("r-%d", j),
					},
				},
			}
		}

		err = pe.PutEntitlements(ctx, entitlements...)
		require.NoError(t, err)

		err = pe.EndSync(ctx)
		require.NoError(t, err)
	}

	// Measure performance of maintenance operations
	start := time.Now()
	err = pe.IntegrityChecker().CheckIndexConsistency(ctx)
	consistencyDuration := time.Since(start)
	require.NoError(t, err)

	start = time.Now()
	err = pe.CompactionManager().TriggerManualCompaction(ctx)
	compactionDuration := time.Since(start)
	require.NoError(t, err)

	start = time.Now()
	_, err = pe.CompactionManager().GetCompactionStats(ctx)
	statsDuration := time.Since(start)
	require.NoError(t, err)

	// Log performance metrics
	t.Logf("Index consistency check: %v", consistencyDuration)
	t.Logf("Manual compaction: %v", compactionDuration)
	t.Logf("Stats collection: %v", statsDuration)

	// Performance assertions (these are rough guidelines)
	assert.Less(t, consistencyDuration, 30*time.Second, "Index consistency check should complete in reasonable time")
	assert.Less(t, compactionDuration, 60*time.Second, "Manual compaction should complete in reasonable time")
	assert.Less(t, statsDuration, 1*time.Second, "Stats collection should be very fast")
}
