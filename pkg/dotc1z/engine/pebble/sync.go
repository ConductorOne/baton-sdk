package pebble

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/segmentio/ksuid"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine"
)

// SyncRun represents a sync run stored in Pebble.
type SyncRun struct {
	ID           string
	StartedAt    int64
	EndedAt      int64
	SyncToken    string
	Type         string
	ParentSyncID string
}

// String returns a string representation of the SyncRun.
func (sr *SyncRun) String() string {
	return fmt.Sprintf("SyncRun{ID: %s, StartedAt: %d, EndedAt: %d, SyncToken: %s, Type: %s, ParentSyncID: %s}",
		sr.ID, sr.StartedAt, sr.EndedAt, sr.SyncToken, sr.Type, sr.ParentSyncID)
}

// toEngineSyncRun converts a Pebble SyncRun to an engine.SyncRun.
func (sr *SyncRun) toEngineSyncRun() *engine.SyncRun {
	var startedAt, endedAt *time.Time

	if sr.StartedAt > 0 {
		t := time.Unix(sr.StartedAt, 0)
		startedAt = &t
	}

	if sr.EndedAt > 0 {
		t := time.Unix(sr.EndedAt, 0)
		endedAt = &t
	}

	return &engine.SyncRun{
		ID:           sr.ID,
		StartedAt:    startedAt,
		EndedAt:      endedAt,
		SyncToken:    sr.SyncToken,
		Type:         engine.SyncType(sr.Type),
		ParentSyncID: sr.ParentSyncID,
	}
}

// fromEngineSyncRun converts an engine.SyncRun to a Pebble SyncRun.
func fromEngineSyncRun(sr *engine.SyncRun) *SyncRun {
	psr := &SyncRun{
		ID:           sr.ID,
		SyncToken:    sr.SyncToken,
		Type:         string(sr.Type),
		ParentSyncID: sr.ParentSyncID,
	}

	if sr.StartedAt != nil {
		psr.StartedAt = sr.StartedAt.Unix()
	}

	if sr.EndedAt != nil {
		psr.EndedAt = sr.EndedAt.Unix()
	}

	return psr
}

// StartSync generates a sync ID to be associated with all objects discovered during this run.
// Returns the sync ID, whether it's a new sync, and any error.
func (pe *PebbleEngine) StartSync(ctx context.Context) (string, bool, error) {
	ctx, span := tracer.Start(ctx, "PebbleEngine.StartSync")
	defer span.End()

	if err := pe.validateOpen(); err != nil {
		return "", false, err
	}

	currentSyncID := pe.getCurrentSyncID()
	if currentSyncID != "" {
		return currentSyncID, false, nil
	}

	newSync := false

	sync, err := pe.getLatestUnfinishedSync(ctx)
	if err != nil {
		return "", false, err
	}

	var syncID string
	if sync != nil && sync.EndedAt == 0 {
		syncID = sync.ID
	} else {
		syncID, err = pe.StartNewSync(ctx)
		if err != nil {
			return "", false, err
		}
		newSync = true
	}

	pe.setCurrentSyncID(syncID)

	return syncID, newSync, nil
}

// StartNewSync creates a new full sync run.
func (pe *PebbleEngine) StartNewSync(ctx context.Context) (string, error) {
	ctx, span := tracer.Start(ctx, "PebbleEngine.StartNewSync")
	defer span.End()

	return pe.startNewSyncInternal(ctx, string(engine.SyncTypeFull), "")
}

// StartNewSyncV2 creates a new sync run with the specified type and parent.
func (pe *PebbleEngine) StartNewSyncV2(ctx context.Context, syncType string, parentSyncID string) (string, error) {
	ctx, span := tracer.Start(ctx, "PebbleEngine.StartNewSyncV2")
	defer span.End()

	// Validate sync type
	switch syncType {
	case string(engine.SyncTypeFull), string(engine.SyncTypePartial):
		// Valid types
	default:
		return "", fmt.Errorf("pebble: invalid sync type: %s", syncType)
	}

	return pe.startNewSyncInternal(ctx, syncType, parentSyncID)
}

// startNewSyncInternal creates a new sync run with the specified parameters.
func (pe *PebbleEngine) startNewSyncInternal(ctx context.Context, syncType string, parentSyncID string) (string, error) {
	if err := pe.validateOpen(); err != nil {
		return "", err
	}

	// Check if there's already a current sync
	currentSyncID := pe.getCurrentSyncID()
	if currentSyncID != "" {
		return currentSyncID, nil
	}

	// Generate new sync ID
	syncID := ksuid.New().String()

	// Create sync run
	syncRun := &SyncRun{
		ID:           syncID,
		StartedAt:    time.Now().UnixNano(),
		EndedAt:      0, // Not finished yet
		SyncToken:    "",
		Type:         syncType,
		ParentSyncID: parentSyncID,
	}

	// Store the sync run
	if err := pe.putSyncRun(ctx, syncRun); err != nil {
		return "", fmt.Errorf("pebble: failed to store sync run: %w", err)
	}

	// No need to create sync index - we scan sync runs directly

	pe.setCurrentSyncID(syncID)
	pe.markDirty()

	return syncID, nil
}

// SetCurrentSync sets the current sync ID to an existing sync.
func (pe *PebbleEngine) SetCurrentSync(ctx context.Context, syncID string) error {
	ctx, span := tracer.Start(ctx, "PebbleEngine.SetCurrentSync")
	defer span.End()

	if err := pe.validateOpen(); err != nil {
		return err
	}

	// Verify the sync exists
	_, err := pe.getSyncRun(ctx, syncID)
	if err != nil {
		return fmt.Errorf("pebble: sync %s not found: %w", syncID, err)
	}

	pe.setCurrentSyncID(syncID)
	return nil
}

// CurrentSyncStep returns the current sync token.
func (pe *PebbleEngine) CurrentSyncStep(ctx context.Context) (string, error) {
	ctx, span := tracer.Start(ctx, "PebbleEngine.CurrentSyncStep")
	defer span.End()

	if err := pe.validateOpen(); err != nil {
		return "", err
	}

	currentSyncID := pe.getCurrentSyncID()
	if currentSyncID == "" {
		return "", fmt.Errorf("pebble: no current sync running")
	}

	syncRun, err := pe.getSyncRun(ctx, currentSyncID)
	if err != nil {
		return "", fmt.Errorf("pebble: failed to get current sync: %w", err)
	}

	return syncRun.SyncToken, nil
}

// CheckpointSync updates the sync token for the current sync.
func (pe *PebbleEngine) CheckpointSync(ctx context.Context, syncToken string) error {
	ctx, span := tracer.Start(ctx, "PebbleEngine.CheckpointSync")
	defer span.End()

	if err := pe.validateOpen(); err != nil {
		return err
	}

	currentSyncID := pe.getCurrentSyncID()
	if currentSyncID == "" {
		return fmt.Errorf("pebble: no current sync running")
	}

	// Get the current sync run
	syncRun, err := pe.getSyncRun(ctx, currentSyncID)
	if err != nil {
		return fmt.Errorf("pebble: failed to get current sync: %w", err)
	}

	// Update the sync token
	syncRun.SyncToken = syncToken

	// Store the updated sync run
	if err := pe.putSyncRun(ctx, syncRun); err != nil {
		return fmt.Errorf("pebble: failed to update sync run: %w", err)
	}

	pe.markDirty()
	return nil
}

// EndSync marks the current sync as completed.
func (pe *PebbleEngine) EndSync(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "PebbleEngine.EndSync")
	defer span.End()

	if err := pe.validateOpen(); err != nil {
		return err
	}

	currentSyncID := pe.getCurrentSyncID()
	if currentSyncID == "" {
		return fmt.Errorf("pebble: no current sync running")
	}

	// Get the current sync run
	syncRun, err := pe.getSyncRun(ctx, currentSyncID)
	if err != nil {
		return fmt.Errorf("pebble: failed to get current sync: %w", err)
	}

	// Mark as ended
	now := time.Now()
	syncRun.EndedAt = now.UnixNano()

	// Use a batch for atomic operations
	batch := pe.db.NewBatch()
	defer batch.Close()

	// Store the updated sync run
	if err := pe.putSyncRunBatch(batch, syncRun); err != nil {
		return fmt.Errorf("pebble: failed to update sync run: %w", err)
	}

	// No need to manage sync indexes - we scan sync runs directly

	// Commit the batch
	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("pebble: failed to commit end sync batch: %w", err)
	}

	// Clear current sync
	pe.setCurrentSyncID("")
	pe.markDirty()

	return nil
}

// putSyncRun stores a sync run in the database.
func (pe *PebbleEngine) putSyncRun(ctx context.Context, syncRun *SyncRun) error {
	// Encode the key
	key := pe.keyEncoder.EncodeSyncRunKey(syncRun.ID)

	// Serialize to JSON
	data, err := json.Marshal(syncRun)
	if err != nil {
		return fmt.Errorf("failed to marshal sync run: %w", err)
	}

	// Encode the value with envelope
	envelope, err := pe.valueCodec.EncodeAsset(data, time.Now(), "application/json")
	if err != nil {
		return fmt.Errorf("failed to encode sync run value: %w", err)
	}

	// Store in database
	if err := pe.db.Set(key, envelope, pebble.Sync); err != nil {
		return fmt.Errorf("failed to store sync run: %w", err)
	}

	return nil
}

// putSyncRunBatch stores a sync run in a batch.
func (pe *PebbleEngine) putSyncRunBatch(batch *pebble.Batch, syncRun *SyncRun) error {
	// Encode the key
	key := pe.keyEncoder.EncodeSyncRunKey(syncRun.ID)

	// Serialize to JSON
	data, err := json.Marshal(syncRun)
	if err != nil {
		return fmt.Errorf("failed to marshal sync run: %w", err)
	}

	// Encode the value with envelope
	envelope, err := pe.valueCodec.EncodeAsset(data, time.Now(), "application/json")
	if err != nil {
		return fmt.Errorf("failed to encode sync run value: %w", err)
	}

	// Store in batch
	if err := batch.Set(key, envelope, nil); err != nil {
		return fmt.Errorf("failed to store sync run in batch: %w", err)
	}

	return nil
}

// getSyncRun retrieves a sync run by ID.
func (pe *PebbleEngine) getSyncRun(ctx context.Context, syncID string) (*SyncRun, error) {
	// Encode the key
	key := pe.keyEncoder.EncodeSyncRunKey(syncID)

	// Get from database
	value, closer, err := pe.db.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, fmt.Errorf("sync run %s not found", syncID)
		}
		return nil, fmt.Errorf("failed to get sync run: %w", err)
	}
	defer closer.Close()

	// Decode the asset envelope
	_, data, err := pe.valueCodec.DecodeAsset(value)
	if err != nil {
		return nil, fmt.Errorf("failed to decode sync run envelope: %w", err)
	}

	// Unmarshal from JSON
	syncRun := &SyncRun{}
	if err := json.Unmarshal(data, syncRun); err != nil {
		return nil, fmt.Errorf("failed to unmarshal sync run: %w", err)
	}

	return syncRun, nil
}

// getLatestUnfinishedSync returns the most recent unfinished sync.
func (pe *PebbleEngine) getLatestUnfinishedSync(ctx context.Context) (*SyncRun, error) {
	// Get all sync runs and find the most recent unfinished one
	syncs, _, err := pe.ListSyncRuns(ctx, "", 1000) // Get up to 1000 syncs (way more than needed)
	if err != nil {
		return nil, fmt.Errorf("failed to list sync runs: %w", err)
	}

	// Don't resume syncs that started over a week ago
	oneWeekAgo := time.Now().AddDate(0, 0, -7)

	var latestUnfinishedSync *engine.SyncRun
	var latestStartedAt time.Time

	// Find the most recent unfinished sync
	for _, syncRun := range syncs {
		// Skip finished syncs
		if syncRun.EndedAt != nil {
			continue
		}

		// Skip syncs older than a week
		if syncRun.StartedAt != nil && syncRun.StartedAt.Before(oneWeekAgo) {
			continue
		}

		// Check if this is the most recent unfinished sync
		if latestUnfinishedSync == nil || (syncRun.StartedAt != nil && syncRun.StartedAt.After(latestStartedAt)) {
			latestUnfinishedSync = syncRun
			if syncRun.StartedAt != nil {
				latestStartedAt = *syncRun.StartedAt
			}
		}
	}

	// If we found an engine.SyncRun, we need to get the internal SyncRun
	if latestUnfinishedSync != nil {
		return pe.getSyncRun(ctx, latestUnfinishedSync.ID)
	}

	return nil, nil
}

// No index functions needed - we scan sync runs directly since there are only tens of them

// keyUpperBound returns the upper bound for a prefix key.
func keyUpperBound(prefix []byte) []byte {
	end := make([]byte, len(prefix))
	copy(end, prefix)
	for i := len(end) - 1; i >= 0; i-- {
		end[i]++
		if end[i] != 0 {
			return end
		}
	}
	return nil // No upper bound
}
