package pebble

import (
	"context"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine"
)

func TestPebbleEngine_Cleanup(t *testing.T) {
	ctx := context.Background()

	t.Run("cleanup with no syncs does nothing", func(t *testing.T) {
		tempDir := t.TempDir()
		pe, err := NewPebbleEngine(ctx, tempDir)
		require.NoError(t, err)
		defer pe.Close()

		err = pe.Cleanup(ctx)
		require.NoError(t, err)
	})

	t.Run("cleanup preserves latest N full syncs", func(t *testing.T) {
		tempDir := t.TempDir()
		pe, err := NewPebbleEngine(ctx, tempDir)
		require.NoError(t, err)
		defer pe.Close()

		// Create 5 full syncs
		syncIDs := make([]string, 5)
		for i := 0; i < 5; i++ {
			syncID, err := pe.StartNewSync(ctx)
			require.NoError(t, err)
			syncIDs[i] = syncID

			// Add some data to each sync
			err = pe.PutResourceTypes(ctx, &v2.ResourceType{
				Id:          "rt" + strconv.Itoa(i),
				DisplayName: "Resource Type " + strconv.Itoa(i),
			})
			require.NoError(t, err)

			// End the sync
			err = pe.EndSync(ctx)
			require.NoError(t, err)

			// Timestamps use nanosecond precision - no sleep needed
		}

		// Verify all syncs exist before cleanup
		for _, syncID := range syncIDs {
			_, err := pe.getSyncRun(ctx, syncID)
			require.NoError(t, err)
		}

		// Run cleanup (default keeps 2 syncs)
		pe.setCurrentSyncID("") // Clear current sync for cleanup
		err = pe.Cleanup(ctx)
		require.NoError(t, err)

		// Verify that cleanup worked correctly
		// Should have exactly 2 syncs remaining (default limit)
		remainingSyncs := 0
		for _, syncID := range syncIDs {
			_, err := pe.getSyncRun(ctx, syncID)
			if err == nil {
				remainingSyncs++
			}
		}
		assert.Equal(t, 2, remainingSyncs, "Expected exactly 2 syncs to remain after cleanup")

		// With proper timestamps, cleanup should keep the 2 most recent syncs (indices 3 and 4)
		// and delete the first 3 syncs (indices 0, 1, and 2)
		for i, syncID := range syncIDs {
			_, err := pe.getSyncRun(ctx, syncID)
			if i < 3 {
				// First 3 syncs should be deleted
				assert.Error(t, err, "Expected sync %d (%s) to be deleted", i, syncID)
			} else {
				// Last 2 syncs should remain
				assert.NoError(t, err, "Expected sync %d (%s) to be kept", i, syncID)
			}
		}
	})

	t.Run("cleanup respects BATON_KEEP_SYNC_COUNT environment variable", func(t *testing.T) {
		tempDir := t.TempDir()
		pe, err := NewPebbleEngine(ctx, tempDir)
		require.NoError(t, err)
		defer pe.Close()

		// Set environment variable to keep 3 syncs
		os.Setenv("BATON_KEEP_SYNC_COUNT", "3")
		defer os.Unsetenv("BATON_KEEP_SYNC_COUNT")

		// Create 5 full syncs
		syncIDs := make([]string, 5)
		for i := 0; i < 5; i++ {
			syncID, err := pe.StartNewSync(ctx)
			require.NoError(t, err)
			syncIDs[i] = syncID

			err = pe.PutResourceTypes(ctx, &v2.ResourceType{
				Id:          "rt" + strconv.Itoa(i),
				DisplayName: "Resource Type " + strconv.Itoa(i),
			})
			require.NoError(t, err)

			err = pe.EndSync(ctx)
			require.NoError(t, err)

			// Timestamps use nanosecond precision - no sleep needed
		}

		// Run cleanup
		pe.setCurrentSyncID("") // Clear current sync for cleanup
		err = pe.Cleanup(ctx)
		require.NoError(t, err)

		// Verify only the latest 3 syncs remain
		for i, syncID := range syncIDs {
			_, err := pe.getSyncRun(ctx, syncID)
			if i < 2 {
				// First 2 syncs should be deleted
				assert.Error(t, err)
			} else {
				// Last 3 syncs should remain
				assert.NoError(t, err)
			}
		}
	})

	t.Run("cleanup skips when BATON_SKIP_CLEANUP is set", func(t *testing.T) {
		tempDir := t.TempDir()
		pe, err := NewPebbleEngine(ctx, tempDir)
		require.NoError(t, err)
		defer pe.Close()

		// Set skip cleanup environment variable
		os.Setenv("BATON_SKIP_CLEANUP", "true")
		defer os.Unsetenv("BATON_SKIP_CLEANUP")

		// Create 5 full syncs
		syncIDs := make([]string, 5)
		for i := 0; i < 5; i++ {
			syncID, err := pe.StartNewSync(ctx)
			require.NoError(t, err)
			syncIDs[i] = syncID

			err = pe.PutResourceTypes(ctx, &v2.ResourceType{
				Id:          "rt" + strconv.Itoa(i),
				DisplayName: "Resource Type " + strconv.Itoa(i),
			})
			require.NoError(t, err)

			err = pe.EndSync(ctx)
			require.NoError(t, err)

			// Timestamps use nanosecond precision - no sleep needed
		}

		// Run cleanup
		err = pe.Cleanup(ctx)
		require.NoError(t, err)

		// Verify all syncs still exist
		for _, syncID := range syncIDs {
			_, err := pe.getSyncRun(ctx, syncID)
			assert.NoError(t, err)
		}
	})

	t.Run("cleanup skips when current sync is active", func(t *testing.T) {
		tempDir := t.TempDir()
		pe, err := NewPebbleEngine(ctx, tempDir)
		require.NoError(t, err)
		defer pe.Close()

		// Create and end 3 syncs
		for i := 0; i < 3; i++ {
			_, err := pe.StartNewSync(ctx)
			require.NoError(t, err)

			err = pe.PutResourceTypes(ctx, &v2.ResourceType{
				Id:          "rt" + strconv.Itoa(i),
				DisplayName: "Resource Type " + strconv.Itoa(i),
			})
			require.NoError(t, err)

			err = pe.EndSync(ctx)
			require.NoError(t, err)

			// Timestamps use nanosecond precision - no sleep needed
		}

		// Start a new sync but don't end it
		_, err = pe.StartNewSync(ctx)
		require.NoError(t, err)

		// Run cleanup - should skip because there's an active sync
		err = pe.Cleanup(ctx)
		require.NoError(t, err)

		// All syncs should still exist since cleanup was skipped
		runs, _, err := pe.ListSyncRuns(ctx, "", 100)
		require.NoError(t, err)
		assert.Len(t, runs, 4) // 3 finished + 1 active

		// End the active sync
		err = pe.EndSync(ctx)
		require.NoError(t, err)

		// Now cleanup should work
		err = pe.Cleanup(ctx)
		require.NoError(t, err)

		// Should have only 2 syncs remaining (latest 2)
		runs, _, err = pe.ListSyncRuns(ctx, "", 100)
		require.NoError(t, err)
		assert.Len(t, runs, 2)
	})

	t.Run("cleanup removes partial syncs that ended before earliest kept full sync", func(t *testing.T) {
		tempDir := t.TempDir()
		pe, err := NewPebbleEngine(ctx, tempDir)
		require.NoError(t, err)
		defer pe.Close()

		// Create first full sync
		fullSyncIDs := make([]string, 3)
		syncID, err := pe.StartNewSync(ctx)
		require.NoError(t, err)
		fullSyncIDs[0] = syncID

		err = pe.PutResourceTypes(ctx, &v2.ResourceType{
			Id:          "rt0",
			DisplayName: "Resource Type 0",
		})
		require.NoError(t, err)

		err = pe.EndSync(ctx)
		require.NoError(t, err)
		// Timestamps use nanosecond precision - no sleep needed

		// Create 2 partial syncs based on the first full sync
		// These will end before the remaining full syncs start
		partialSyncIDs := make([]string, 2)
		for i := 0; i < 2; i++ {
			syncID, err := pe.StartNewSyncV2(ctx, string(engine.SyncTypePartial), fullSyncIDs[0])
			require.NoError(t, err)
			partialSyncIDs[i] = syncID

			err = pe.PutResourceTypes(ctx, &v2.ResourceType{
				Id:          "partial_rt" + strconv.Itoa(i),
				DisplayName: "Partial Resource Type " + strconv.Itoa(i),
			})
			require.NoError(t, err)

			err = pe.EndSync(ctx)
			require.NoError(t, err)

			// Timestamps use nanosecond precision - no sleep needed
		}

		// Now create the remaining 2 full syncs
		// These will be kept by cleanup (since limit is 2)
		for i := 1; i < 3; i++ {
			syncID, err := pe.StartNewSync(ctx)
			require.NoError(t, err)
			fullSyncIDs[i] = syncID

			err = pe.PutResourceTypes(ctx, &v2.ResourceType{
				Id:          "rt" + strconv.Itoa(i),
				DisplayName: "Resource Type " + strconv.Itoa(i),
			})
			require.NoError(t, err)

			err = pe.EndSync(ctx)
			require.NoError(t, err)

			// Timestamps use nanosecond precision - no sleep needed
		}

		// Run cleanup (keeps 2 full syncs, so first full sync should be deleted)
		pe.setCurrentSyncID("") // Clear current sync for cleanup
		err = pe.Cleanup(ctx)
		require.NoError(t, err)

		// First full sync should be deleted
		_, err = pe.getSyncRun(ctx, fullSyncIDs[0])
		assert.Error(t, err)

		// Last 2 full syncs should remain
		_, err = pe.getSyncRun(ctx, fullSyncIDs[1])
		assert.NoError(t, err)
		_, err = pe.getSyncRun(ctx, fullSyncIDs[2])
		assert.NoError(t, err)

		// Partial syncs should be deleted since they ended before the earliest kept full sync
		for _, partialSyncID := range partialSyncIDs {
			_, err = pe.getSyncRun(ctx, partialSyncID)
			assert.Error(t, err)
		}
	})

	t.Run("cleanup removes all entity data for deleted syncs", func(t *testing.T) {
		tempDir := t.TempDir()
		pe, err := NewPebbleEngine(ctx, tempDir)
		require.NoError(t, err)
		defer pe.Close()

		// Create 3 syncs with different types of data
		syncIDs := make([]string, 3)
		for i := 0; i < 3; i++ {
			syncID, err := pe.StartNewSync(ctx)
			require.NoError(t, err)
			syncIDs[i] = syncID

			// Add resource types
			err = pe.PutResourceTypes(ctx, &v2.ResourceType{
				Id:          "rt" + strconv.Itoa(i),
				DisplayName: "Resource Type " + strconv.Itoa(i),
			})
			require.NoError(t, err)

			// Add resources
			err = pe.PutResources(ctx, &v2.Resource{
				Id: &v2.ResourceId{
					ResourceType: "rt" + strconv.Itoa(i),
					Resource:     "r" + strconv.Itoa(i),
				},
				DisplayName: "Resource " + strconv.Itoa(i),
			})
			require.NoError(t, err)

			// Add entitlements
			entitlement := &v2.Entitlement{
				Id:          "ent" + strconv.Itoa(i),
				DisplayName: "Entitlement " + strconv.Itoa(i),
				Resource: &v2.Resource{
					Id: &v2.ResourceId{
						ResourceType: "rt" + strconv.Itoa(i),
						Resource:     "r" + strconv.Itoa(i),
					},
					DisplayName: "Resource " + strconv.Itoa(i),
				},
			}
			err = pe.PutEntitlements(ctx, entitlement)
			require.NoError(t, err)

			// Add grants
			err = pe.PutGrants(ctx, &v2.Grant{
				Id: "grant" + strconv.Itoa(i),
				Principal: &v2.Resource{
					Id: &v2.ResourceId{
						ResourceType: "user",
						Resource:     "user" + strconv.Itoa(i),
					},
					DisplayName: "User " + strconv.Itoa(i),
				},
				Entitlement: entitlement,
			})
			require.NoError(t, err)

			// Add assets
			err = pe.PutAsset(ctx, &v2.AssetRef{Id: "asset" + strconv.Itoa(i)}, "text/plain", []byte("asset data "+strconv.Itoa(i)))
			require.NoError(t, err)

			err = pe.EndSync(ctx)
			require.NoError(t, err)

			// Timestamps use nanosecond precision - no sleep needed
		}

		// Verify all data exists before cleanup
		for i := 0; i < 3; i++ {
			pe.setCurrentSyncID(syncIDs[i])

			// Check resource types
			rtResp, err := pe.ListResourceTypes(ctx, &v2.ResourceTypesServiceListResourceTypesRequest{})
			require.NoError(t, err)
			assert.Len(t, rtResp.List, 1)

			// Check resources
			rResp, err := pe.ListResources(ctx, &v2.ResourcesServiceListResourcesRequest{})
			require.NoError(t, err)
			assert.Len(t, rResp.List, 1)

			// Check entitlements
			entResp, err := pe.ListEntitlements(ctx, &v2.EntitlementsServiceListEntitlementsRequest{})
			require.NoError(t, err)
			assert.Len(t, entResp.List, 1)

			// Check grants
			gResp, err := pe.ListGrants(ctx, &v2.GrantsServiceListGrantsRequest{})
			require.NoError(t, err)
			assert.Len(t, gResp.List, 1)

			// Check assets
			_, reader, err := pe.GetAsset(ctx, &v2.AssetServiceGetAssetRequest{
				Asset: &v2.AssetRef{Id: "asset" + strconv.Itoa(i)},
			})
			require.NoError(t, err)
			assert.NotNil(t, reader)
		}

		pe.setCurrentSyncID("") // Clear current sync for cleanup

		// Run cleanup (keeps 2 syncs, so first sync should be deleted)
		err = pe.Cleanup(ctx)
		require.NoError(t, err)

		// First sync data should be completely removed
		pe.setCurrentSyncID(syncIDs[0])

		rtResp, err := pe.ListResourceTypes(ctx, &v2.ResourceTypesServiceListResourceTypesRequest{})
		require.NoError(t, err)
		assert.Len(t, rtResp.List, 0)

		rResp, err := pe.ListResources(ctx, &v2.ResourcesServiceListResourcesRequest{})
		require.NoError(t, err)
		assert.Len(t, rResp.List, 0)

		entResp, err := pe.ListEntitlements(ctx, &v2.EntitlementsServiceListEntitlementsRequest{})
		require.NoError(t, err)
		assert.Len(t, entResp.List, 0)

		gResp, err := pe.ListGrants(ctx, &v2.GrantsServiceListGrantsRequest{})
		require.NoError(t, err)
		assert.Len(t, gResp.List, 0)

		_, _, err = pe.GetAsset(ctx, &v2.AssetServiceGetAssetRequest{
			Asset: &v2.AssetRef{Id: "asset0"},
		})
		assert.Error(t, err)

		// Last 2 syncs should still have their data
		for i := 1; i < 3; i++ {
			pe.setCurrentSyncID(syncIDs[i])

			rtResp, err := pe.ListResourceTypes(ctx, &v2.ResourceTypesServiceListResourceTypesRequest{})
			require.NoError(t, err)
			assert.Len(t, rtResp.List, 1)

			rResp, err := pe.ListResources(ctx, &v2.ResourcesServiceListResourcesRequest{})
			require.NoError(t, err)
			assert.Len(t, rResp.List, 1)

			entResp, err := pe.ListEntitlements(ctx, &v2.EntitlementsServiceListEntitlementsRequest{})
			require.NoError(t, err)
			assert.Len(t, entResp.List, 1)

			gResp, err := pe.ListGrants(ctx, &v2.GrantsServiceListGrantsRequest{})
			require.NoError(t, err)
			assert.Len(t, gResp.List, 1)

			_, reader, err := pe.GetAsset(ctx, &v2.AssetServiceGetAssetRequest{
				Asset: &v2.AssetRef{Id: "asset" + strconv.Itoa(i)},
			})
			require.NoError(t, err)
			assert.NotNil(t, reader)
		}
	})
}

func TestPebbleEngine_deleteSyncData(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()
	pe, err := NewPebbleEngine(ctx, tempDir)
	require.NoError(t, err)
	defer pe.Close()

	// Create a sync with data
	syncID, err := pe.StartNewSync(ctx)
	require.NoError(t, err)

	// Add various types of data
	err = pe.PutResourceTypes(ctx, &v2.ResourceType{
		Id:          "rt1",
		DisplayName: "Resource Type 1",
	})
	require.NoError(t, err)

	err = pe.PutResources(ctx, &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "rt1",
			Resource:     "r1",
		},
		DisplayName: "Resource 1",
	})
	require.NoError(t, err)

	entitlement := &v2.Entitlement{
		Id:          "ent1",
		DisplayName: "Entitlement 1",
		Resource: &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: "rt1",
				Resource:     "r1",
			},
			DisplayName: "Resource 1",
		},
	}
	err = pe.PutEntitlements(ctx, entitlement)
	require.NoError(t, err)

	err = pe.PutGrants(ctx, &v2.Grant{
		Id: "grant1",
		Principal: &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: "user",
				Resource:     "user1",
			},
			DisplayName: "User 1",
		},
		Entitlement: entitlement,
	})
	require.NoError(t, err)

	err = pe.PutAsset(ctx, &v2.AssetRef{Id: "asset1"}, "text/plain", []byte("asset data"))
	require.NoError(t, err)

	err = pe.EndSync(ctx)
	require.NoError(t, err)

	// Set the sync as current to verify data exists
	err = pe.SetCurrentSync(ctx, syncID)
	require.NoError(t, err)

	// Verify data exists
	rtResp, err := pe.ListResourceTypes(ctx, &v2.ResourceTypesServiceListResourceTypesRequest{})
	require.NoError(t, err)
	assert.Len(t, rtResp.List, 1)

	// Delete the sync data
	err = pe.deleteSyncData(ctx, syncID)
	require.NoError(t, err)

	// Verify sync run is deleted
	_, err = pe.getSyncRun(ctx, syncID)
	assert.Error(t, err)

	// Try to set the deleted sync as current - should fail
	err = pe.SetCurrentSync(ctx, syncID)
	assert.Error(t, err)
}

func TestPebbleEngine_triggerManualCompaction(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()
	pe, err := NewPebbleEngine(ctx, tempDir)
	require.NoError(t, err)
	defer pe.Close()

	// This test mainly verifies that the compaction call doesn't error
	err = pe.triggerManualCompaction(ctx)
	assert.NoError(t, err)
}

func TestPebbleEngine_getAllFinishedSyncs(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()
	pe, err := NewPebbleEngine(ctx, tempDir)
	require.NoError(t, err)
	defer pe.Close()

	// Create 2 full syncs and 1 partial sync
	fullSyncID1, err := pe.StartNewSync(ctx)
	require.NoError(t, err)
	err = pe.EndSync(ctx)
	require.NoError(t, err)

	fullSyncID2, err := pe.StartNewSync(ctx)
	require.NoError(t, err)
	err = pe.EndSync(ctx)
	require.NoError(t, err)

	partialSyncID, err := pe.StartNewSyncV2(ctx, string(engine.SyncTypePartial), fullSyncID1)
	require.NoError(t, err)
	err = pe.EndSync(ctx)
	require.NoError(t, err)

	// Create an unfinished sync
	unfinishedSyncID, err := pe.StartNewSync(ctx)
	require.NoError(t, err)
	// Don't end this sync

	fullSyncs, partialSyncs, err := pe.getAllFinishedSyncs(ctx)
	require.NoError(t, err)

	// Should have 2 full syncs and 1 partial sync
	assert.Len(t, fullSyncs, 2)
	assert.Len(t, partialSyncs, 1)

	// Verify the sync IDs
	fullSyncIDsFound := make(map[string]bool)
	for _, sync := range fullSyncs {
		fullSyncIDsFound[sync.ID] = true
	}
	assert.True(t, fullSyncIDsFound[fullSyncID1])
	assert.True(t, fullSyncIDsFound[fullSyncID2])

	assert.Equal(t, partialSyncID, partialSyncs[0].ID)

	// Unfinished sync should not be included
	for _, sync := range fullSyncs {
		assert.NotEqual(t, unfinishedSyncID, sync.ID)
	}
	for _, sync := range partialSyncs {
		assert.NotEqual(t, unfinishedSyncID, sync.ID)
	}
}
