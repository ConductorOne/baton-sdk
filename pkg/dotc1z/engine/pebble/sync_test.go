package pebble

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine"
)

func TestSyncRun_Conversion(t *testing.T) {
	now := time.Now()

	// Test engine.SyncRun to Pebble SyncRun conversion
	engineSyncRun := &engine.SyncRun{
		ID:           "test-sync-id",
		StartedAt:    &now,
		EndedAt:      nil,
		SyncToken:    "test-token",
		Type:         engine.SyncTypeFull,
		ParentSyncID: "parent-sync-id",
	}

	pebbleSyncRun := fromEngineSyncRun(engineSyncRun)
	assert.Equal(t, "test-sync-id", pebbleSyncRun.ID)
	assert.Equal(t, now.Unix(), pebbleSyncRun.StartedAt)
	assert.Equal(t, int64(0), pebbleSyncRun.EndedAt)
	assert.Equal(t, "test-token", pebbleSyncRun.SyncToken)
	assert.Equal(t, "full", pebbleSyncRun.Type)
	assert.Equal(t, "parent-sync-id", pebbleSyncRun.ParentSyncID)

	// Test Pebble SyncRun to engine.SyncRun conversion
	convertedBack := pebbleSyncRun.toEngineSyncRun()
	assert.Equal(t, engineSyncRun.ID, convertedBack.ID)
	assert.Equal(t, engineSyncRun.StartedAt.Unix(), convertedBack.StartedAt.Unix())
	assert.Nil(t, convertedBack.EndedAt)
	assert.Equal(t, engineSyncRun.SyncToken, convertedBack.SyncToken)
	assert.Equal(t, engineSyncRun.Type, convertedBack.Type)
	assert.Equal(t, engineSyncRun.ParentSyncID, convertedBack.ParentSyncID)
}

func TestSyncRun_ConversionWithEndTime(t *testing.T) {
	startTime := time.Now().Add(-time.Hour)
	endTime := time.Now()

	engineSyncRun := &engine.SyncRun{
		ID:           "test-sync-id",
		StartedAt:    &startTime,
		EndedAt:      &endTime,
		SyncToken:    "test-token",
		Type:         engine.SyncTypePartial,
		ParentSyncID: "",
	}

	pebbleSyncRun := fromEngineSyncRun(engineSyncRun)
	assert.Equal(t, startTime.Unix(), pebbleSyncRun.StartedAt)
	assert.Equal(t, endTime.Unix(), pebbleSyncRun.EndedAt)

	convertedBack := pebbleSyncRun.toEngineSyncRun()
	assert.Equal(t, startTime.Unix(), convertedBack.StartedAt.Unix())
	assert.Equal(t, endTime.Unix(), convertedBack.EndedAt.Unix())
}

func TestPebbleEngine_StartSync_NewSync(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	engine, err := NewPebbleEngine(ctx, tempDir)
	require.NoError(t, err)
	defer engine.Close()

	// First call should create a new sync
	syncID, isNew, err := engine.StartSync(ctx)
	require.NoError(t, err)
	assert.True(t, isNew)
	assert.NotEmpty(t, syncID)
	assert.Equal(t, syncID, engine.getCurrentSyncID())

	// Second call should return the same sync
	syncID2, isNew2, err := engine.StartSync(ctx)
	require.NoError(t, err)
	assert.False(t, isNew2)
	assert.Equal(t, syncID, syncID2)
}

func TestPebbleEngine_StartNewSync(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	engine, err := NewPebbleEngine(ctx, tempDir)
	require.NoError(t, err)
	defer engine.Close()

	syncID, err := engine.StartNewSync(ctx)
	require.NoError(t, err)
	assert.NotEmpty(t, syncID)
	assert.Equal(t, syncID, engine.getCurrentSyncID())

	// Verify the sync run was stored
	syncRun, err := engine.getSyncRun(ctx, syncID)
	require.NoError(t, err)
	assert.Equal(t, syncID, syncRun.ID)
	assert.Greater(t, syncRun.StartedAt, int64(0))
	assert.Equal(t, int64(0), syncRun.EndedAt)
	assert.Equal(t, "", syncRun.SyncToken)
	assert.Equal(t, "full", syncRun.Type)
	assert.Equal(t, "", syncRun.ParentSyncID)
}

func TestPebbleEngine_StartNewSyncV2(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	engine, err := NewPebbleEngine(ctx, tempDir)
	require.NoError(t, err)
	defer engine.Close()

	// Test full sync
	syncID1, err := engine.StartNewSyncV2(ctx, "full", "")
	require.NoError(t, err)
	assert.NotEmpty(t, syncID1)

	// End the first sync
	err = engine.EndSync(ctx)
	require.NoError(t, err)

	// Test partial sync with parent
	syncID2, err := engine.StartNewSyncV2(ctx, "partial", syncID1)
	require.NoError(t, err)
	assert.NotEmpty(t, syncID2)
	assert.NotEqual(t, syncID1, syncID2)

	// Verify the partial sync run
	syncRun, err := engine.getSyncRun(ctx, syncID2)
	require.NoError(t, err)
	assert.Equal(t, "partial", syncRun.Type)
	assert.Equal(t, syncID1, syncRun.ParentSyncID)
}

func TestPebbleEngine_StartNewSyncV2_InvalidType(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	engine, err := NewPebbleEngine(ctx, tempDir)
	require.NoError(t, err)
	defer engine.Close()

	_, err = engine.StartNewSyncV2(ctx, "invalid", "")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid sync type")
}

func TestPebbleEngine_SetCurrentSync(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	engine, err := NewPebbleEngine(ctx, tempDir)
	require.NoError(t, err)
	defer engine.Close()

	// Create a sync
	syncID, err := engine.StartNewSync(ctx)
	require.NoError(t, err)

	// End the sync
	err = engine.EndSync(ctx)
	require.NoError(t, err)

	// Set it as current again
	err = engine.SetCurrentSync(ctx, syncID)
	require.NoError(t, err)
	assert.Equal(t, syncID, engine.getCurrentSyncID())
}

func TestPebbleEngine_SetCurrentSync_NotFound(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	engine, err := NewPebbleEngine(ctx, tempDir)
	require.NoError(t, err)
	defer engine.Close()

	err = engine.SetCurrentSync(ctx, "non-existent-sync")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestPebbleEngine_CurrentSyncStep(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	engine, err := NewPebbleEngine(ctx, tempDir)
	require.NoError(t, err)
	defer engine.Close()

	// No current sync
	_, err = engine.CurrentSyncStep(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no current sync running")

	// Start a sync
	_, err = engine.StartNewSync(ctx)
	require.NoError(t, err)

	// Should return empty token initially
	token, err := engine.CurrentSyncStep(ctx)
	require.NoError(t, err)
	assert.Equal(t, "", token)

	// Checkpoint with a token
	err = engine.CheckpointSync(ctx, "test-token")
	require.NoError(t, err)

	// Should return the token
	token, err = engine.CurrentSyncStep(ctx)
	require.NoError(t, err)
	assert.Equal(t, "test-token", token)
}

func TestPebbleEngine_CheckpointSync(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	engine, err := NewPebbleEngine(ctx, tempDir)
	require.NoError(t, err)
	defer engine.Close()

	// No current sync
	err = engine.CheckpointSync(ctx, "test-token")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no current sync running")

	// Start a sync
	syncID, err := engine.StartNewSync(ctx)
	require.NoError(t, err)

	// Checkpoint
	err = engine.CheckpointSync(ctx, "test-token")
	require.NoError(t, err)

	// Verify the token was stored
	syncRun, err := engine.getSyncRun(ctx, syncID)
	require.NoError(t, err)
	assert.Equal(t, "test-token", syncRun.SyncToken)

	// Update the token
	err = engine.CheckpointSync(ctx, "updated-token")
	require.NoError(t, err)

	// Verify the token was updated
	syncRun, err = engine.getSyncRun(ctx, syncID)
	require.NoError(t, err)
	assert.Equal(t, "updated-token", syncRun.SyncToken)
}

func TestPebbleEngine_EndSync(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	engine, err := NewPebbleEngine(ctx, tempDir)
	require.NoError(t, err)
	defer engine.Close()

	// No current sync
	err = engine.EndSync(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no current sync running")

	// Start a sync
	syncID, err := engine.StartNewSync(ctx)
	require.NoError(t, err)

	// Checkpoint
	err = engine.CheckpointSync(ctx, "final-token")
	require.NoError(t, err)

	// End the sync
	beforeEnd := time.Now()
	err = engine.EndSync(ctx)
	require.NoError(t, err)
	afterEnd := time.Now()

	// Verify current sync is cleared
	assert.Equal(t, "", engine.getCurrentSyncID())

	// Verify the sync run was updated
	syncRun, err := engine.getSyncRun(ctx, syncID)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, syncRun.EndedAt, beforeEnd.UnixNano())
	assert.LessOrEqual(t, syncRun.EndedAt, afterEnd.UnixNano())
	assert.Equal(t, "final-token", syncRun.SyncToken)
}

func TestPebbleEngine_SyncLifecycle_Complete(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	engine, err := NewPebbleEngine(ctx, tempDir)
	require.NoError(t, err)
	defer engine.Close()

	// Start sync
	syncID, isNew, err := engine.StartSync(ctx)
	require.NoError(t, err)
	assert.True(t, isNew)
	assert.NotEmpty(t, syncID)

	// Checkpoint multiple times
	err = engine.CheckpointSync(ctx, "step1")
	require.NoError(t, err)

	err = engine.CheckpointSync(ctx, "step2")
	require.NoError(t, err)

	err = engine.CheckpointSync(ctx, "final")
	require.NoError(t, err)

	// Verify current step
	step, err := engine.CurrentSyncStep(ctx)
	require.NoError(t, err)
	assert.Equal(t, "final", step)

	// End sync
	err = engine.EndSync(ctx)
	require.NoError(t, err)

	// Verify sync is complete
	syncRun, err := engine.getSyncRun(ctx, syncID)
	require.NoError(t, err)
	assert.Greater(t, syncRun.EndedAt, int64(0))
	assert.Equal(t, "final", syncRun.SyncToken)
}

func TestPebbleEngine_ResumeUnfinishedSync(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	// Create engine and start sync
	engine1, err := NewPebbleEngine(ctx, tempDir)
	require.NoError(t, err)

	syncID1, err := engine1.StartNewSync(ctx)
	require.NoError(t, err)

	err = engine1.CheckpointSync(ctx, "checkpoint1")
	require.NoError(t, err)

	// Close engine without ending sync
	err = engine1.Close()
	require.NoError(t, err)

	// Create new engine instance
	engine2, err := NewPebbleEngine(ctx, tempDir)
	require.NoError(t, err)
	defer engine2.Close()

	// StartSync should resume the unfinished sync
	syncID2, isNew, err := engine2.StartSync(ctx)
	require.NoError(t, err)
	assert.False(t, isNew)
	assert.Equal(t, syncID1, syncID2)

	// Verify the checkpoint was preserved
	step, err := engine2.CurrentSyncStep(ctx)
	require.NoError(t, err)
	assert.Equal(t, "checkpoint1", step)
}

func TestPebbleEngine_IgnoreOldUnfinishedSync(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	engine, err := NewPebbleEngine(ctx, tempDir)
	require.NoError(t, err)
	defer engine.Close()

	// Create an old unfinished sync by manipulating the stored data
	oldSyncID := "old-sync-id"
	oldTime := time.Now().AddDate(0, 0, -8) // 8 days ago

	oldSyncRun := &SyncRun{
		ID:           oldSyncID,
		StartedAt:    oldTime.Unix(),
		EndedAt:      0, // Unfinished
		SyncToken:    "old-token",
		Type:         "full",
		ParentSyncID: "",
	}

	// Store the old sync run
	err = engine.putSyncRun(ctx, oldSyncRun)
	require.NoError(t, err)

	// No need to create sync index - we scan sync runs directly

	// StartSync should create a new sync instead of resuming the old one
	syncID, isNew, err := engine.StartSync(ctx)
	require.NoError(t, err)
	assert.True(t, isNew)
	assert.NotEqual(t, oldSyncID, syncID)
}

func TestPebbleEngine_SyncStateTransitions(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	engine, err := NewPebbleEngine(ctx, tempDir)
	require.NoError(t, err)
	defer engine.Close()

	// Test state: No sync running
	assert.Equal(t, "", engine.getCurrentSyncID())

	_, err = engine.CurrentSyncStep(ctx)
	assert.Error(t, err)

	err = engine.CheckpointSync(ctx, "token")
	assert.Error(t, err)

	err = engine.EndSync(ctx)
	assert.Error(t, err)

	// Test state: Sync running
	syncID, err := engine.StartNewSync(ctx)
	require.NoError(t, err)
	assert.Equal(t, syncID, engine.getCurrentSyncID())

	_, err = engine.CurrentSyncStep(ctx)
	require.NoError(t, err)

	err = engine.CheckpointSync(ctx, "token")
	require.NoError(t, err)

	// Test state: Sync ended
	err = engine.EndSync(ctx)
	require.NoError(t, err)
	assert.Equal(t, "", engine.getCurrentSyncID())

	_, err = engine.CurrentSyncStep(ctx)
	assert.Error(t, err)

	err = engine.CheckpointSync(ctx, "token")
	assert.Error(t, err)

	err = engine.EndSync(ctx)
	assert.Error(t, err)
}

func TestPebbleEngine_SyncMetadataPersistence(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	engine, err := NewPebbleEngine(ctx, tempDir)
	require.NoError(t, err)
	defer engine.Close()

	// Create a full sync
	fullSyncID, err := engine.StartNewSyncV2(ctx, "full", "")
	require.NoError(t, err)

	err = engine.CheckpointSync(ctx, "full-sync-token")
	require.NoError(t, err)

	err = engine.EndSync(ctx)
	require.NoError(t, err)

	// Create a partial sync
	partialSyncID, err := engine.StartNewSyncV2(ctx, "partial", fullSyncID)
	require.NoError(t, err)

	err = engine.CheckpointSync(ctx, "partial-sync-token")
	require.NoError(t, err)

	err = engine.EndSync(ctx)
	require.NoError(t, err)

	// Verify full sync metadata
	fullSync, err := engine.getSyncRun(ctx, fullSyncID)
	require.NoError(t, err)
	assert.Equal(t, "full", fullSync.Type)
	assert.Equal(t, "", fullSync.ParentSyncID)
	assert.Equal(t, "full-sync-token", fullSync.SyncToken)
	assert.Greater(t, fullSync.StartedAt, int64(0))
	assert.Greater(t, fullSync.EndedAt, int64(0))
	assert.GreaterOrEqual(t, fullSync.EndedAt, fullSync.StartedAt)

	// Verify partial sync metadata
	partialSync, err := engine.getSyncRun(ctx, partialSyncID)
	require.NoError(t, err)
	assert.Equal(t, "partial", partialSync.Type)
	assert.Equal(t, fullSyncID, partialSync.ParentSyncID)
	assert.Equal(t, "partial-sync-token", partialSync.SyncToken)
	assert.Greater(t, partialSync.StartedAt, int64(0))
	assert.Greater(t, partialSync.EndedAt, int64(0))
	assert.GreaterOrEqual(t, partialSync.StartedAt, fullSync.EndedAt)
}

func TestPebbleEngine_ConcurrentSyncOperations(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	engine, err := NewPebbleEngine(ctx, tempDir)
	require.NoError(t, err)
	defer engine.Close()

	// Start a sync
	_, err = engine.StartNewSync(ctx)
	require.NoError(t, err)

	// Multiple checkpoint operations should work
	for i := 0; i < 10; i++ {
		token := fmt.Sprintf("token-%d", i)
		err = engine.CheckpointSync(ctx, token)
		require.NoError(t, err)

		step, err := engine.CurrentSyncStep(ctx)
		require.NoError(t, err)
		assert.Equal(t, token, step)
	}

	// End sync
	err = engine.EndSync(ctx)
	require.NoError(t, err)
}

func TestPebbleEngine_DatabaseClosed(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	engine, err := NewPebbleEngine(ctx, tempDir)
	require.NoError(t, err)

	// Close the engine
	err = engine.Close()
	require.NoError(t, err)

	// All sync operations should fail
	_, _, err = engine.StartSync(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not open")

	_, err = engine.StartNewSync(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not open")

	_, err = engine.StartNewSyncV2(ctx, "full", "")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not open")

	err = engine.SetCurrentSync(ctx, "test")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not open")

	_, err = engine.CurrentSyncStep(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not open")

	err = engine.CheckpointSync(ctx, "token")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not open")

	err = engine.EndSync(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not open")
}

// Helper function to create a test engine
func createTestEngine(t *testing.T) (*PebbleEngine, string) {
	tempDir := t.TempDir()
	engine, err := NewPebbleEngine(context.Background(), tempDir)
	require.NoError(t, err)
	return engine, tempDir
}

// Helper function to create and complete a sync
func createCompletedSync(t *testing.T, engine *PebbleEngine, syncType string, parentID string) string {
	ctx := context.Background()

	syncID, err := engine.StartNewSyncV2(ctx, syncType, parentID)
	require.NoError(t, err)

	err = engine.CheckpointSync(ctx, "completed")
	require.NoError(t, err)

	err = engine.EndSync(ctx)
	require.NoError(t, err)

	return syncID
}
