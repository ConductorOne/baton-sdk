package pebble

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/cockroachdb/pebble"
	"go.opentelemetry.io/otel"

	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine"
)

var syncRunTracer = otel.Tracer("baton-sdk/pkg.dotc1z.engine.pebble.sync-runs")

// Stats returns statistics about the current sync or view.
func (pe *PebbleEngine) Stats(ctx context.Context) (map[string]int64, error) {
	ctx, span := syncRunTracer.Start(ctx, "PebbleEngine.Stats")
	defer span.End()

	if err := pe.validateOpen(); err != nil {
		return nil, err
	}

	// Get the sync ID to query (current sync or view sync)
	var syncID string
	if currentSyncID := pe.getCurrentSyncID(); currentSyncID != "" {
		syncID = currentSyncID
	} else if viewSyncID := pe.getViewSyncID(); viewSyncID != "" {
		syncID = viewSyncID
	} else {
		// Get the latest finished sync (most recent first from getAllFinishedSyncs)
		fullSyncs, _, err := pe.getAllFinishedSyncs(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get finished syncs: %w", err)
		}
		if len(fullSyncs) == 0 {
			// No syncs available, return empty stats
			return map[string]int64{
				"resource_types": 0,
				"resources":      0,
				"entitlements":   0,
				"grants":         0,
				"assets":         0,
			}, nil
		}
		// Use the most recent full sync (first in the sorted list)
		syncID = fullSyncs[0].ID
	}

	stats := make(map[string]int64)

	// Count resource types
	rtPrefix := pe.keyEncoder.EncodePrefixKey(KeyTypeResourceType, syncID)
	rtCount, err := pe.countKeysWithPrefix(rtPrefix)
	if err != nil {
		return nil, fmt.Errorf("failed to count resource types: %w", err)
	}
	stats["resource_types"] = rtCount

	// Count resources
	rsPrefix := pe.keyEncoder.EncodePrefixKey(KeyTypeResource, syncID)
	rsCount, err := pe.countKeysWithPrefix(rsPrefix)
	if err != nil {
		return nil, fmt.Errorf("failed to count resources: %w", err)
	}
	stats["resources"] = rsCount

	// Count entitlements
	enPrefix := pe.keyEncoder.EncodePrefixKey(KeyTypeEntitlement, syncID)
	enCount, err := pe.countKeysWithPrefix(enPrefix)
	if err != nil {
		return nil, fmt.Errorf("failed to count entitlements: %w", err)
	}
	stats["entitlements"] = enCount

	// Count grants
	grPrefix := pe.keyEncoder.EncodePrefixKey(KeyTypeGrant, syncID)
	grCount, err := pe.countKeysWithPrefix(grPrefix)
	if err != nil {
		return nil, fmt.Errorf("failed to count grants: %w", err)
	}
	stats["grants"] = grCount

	// Count assets
	asPrefix := pe.keyEncoder.EncodePrefixKey(KeyTypeAsset, syncID)
	asCount, err := pe.countKeysWithPrefix(asPrefix)
	if err != nil {
		return nil, fmt.Errorf("failed to count assets: %w", err)
	}
	stats["assets"] = asCount

	return stats, nil
}

// CloneSync creates a clone of a finished sync in a separate directory.
func (pe *PebbleEngine) CloneSync(ctx context.Context, syncID string) (string, error) {
	ctx, span := syncRunTracer.Start(ctx, "PebbleEngine.CloneSync")
	defer span.End()

	if err := pe.validateOpen(); err != nil {
		return "", err
	}

	// Validate the sync exists and is finished
	syncRun, err := pe.getSyncRun(ctx, syncID)
	if err != nil {
		return "", fmt.Errorf("failed to get sync run: %w", err)
	}

	if syncRun.EndedAt == 0 {
		return "", fmt.Errorf("cannot clone unfinished sync")
	}

	// Create a temporary directory for the clone
	cloneDir, err := os.MkdirTemp(pe.workingDir, fmt.Sprintf("clone-%s-*", syncID))
	if err != nil {
		return "", fmt.Errorf("failed to create clone directory: %w", err)
	}

	// Create a new Pebble engine for the clone
	cloneEngine, err := NewPebbleEngine(ctx, cloneDir)
	if err != nil {
		os.RemoveAll(cloneDir)
		return "", fmt.Errorf("failed to create clone engine: %w", err)
	}
	defer cloneEngine.Close()

	// Start a sync in the clone engine
	_, err = cloneEngine.StartNewSync(ctx)
	if err != nil {
		os.RemoveAll(cloneDir)
		return "", fmt.Errorf("failed to start sync in clone: %w", err)
	}

	// Copy all data from the source sync
	err = pe.copySyncDataToClone(ctx, syncID, cloneEngine)
	if err != nil {
		os.RemoveAll(cloneDir)
		return "", fmt.Errorf("failed to copy sync data to clone: %w", err)
	}

	// End the sync in the clone engine
	err = cloneEngine.EndSync(ctx)
	if err != nil {
		os.RemoveAll(cloneDir)
		return "", fmt.Errorf("failed to end sync in clone: %w", err)
	}

	return cloneDir, nil
}

// GenerateSyncDiff generates a diff between two syncs and returns the diff sync ID.
func (pe *PebbleEngine) GenerateSyncDiff(ctx context.Context, baseSyncID string, appliedSyncID string) (string, error) {
	ctx, span := syncRunTracer.Start(ctx, "PebbleEngine.GenerateSyncDiff")
	defer span.End()

	if err := pe.validateOpen(); err != nil {
		return "", err
	}

	// Validate both syncs exist and are finished
	_, err := pe.getSyncRun(ctx, baseSyncID)
	if err != nil {
		return "", fmt.Errorf("failed to get base sync run: %w", err)
	}

	_, err = pe.getSyncRun(ctx, appliedSyncID)
	if err != nil {
		return "", fmt.Errorf("failed to get applied sync run: %w", err)
	}

	// Start a new sync for the diff
	diffSyncID, err := pe.StartNewSync(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to start diff sync: %w", err)
	}

	// Generate diff data for each entity type into current diff sync
	if err := pe.generateDiffForResourceTypes(ctx, baseSyncID, appliedSyncID); err != nil {
		return "", fmt.Errorf("failed to generate diff for resource types: %w", err)
	}

	if err := pe.generateDiffForResources(ctx, baseSyncID, appliedSyncID); err != nil {
		return "", fmt.Errorf("failed to generate diff for resources: %w", err)
	}

	if err := pe.generateDiffForEntitlements(ctx, baseSyncID, appliedSyncID); err != nil {
		return "", fmt.Errorf("failed to generate diff for entitlements: %w", err)
	}

	if err := pe.generateDiffForGrants(ctx, baseSyncID, appliedSyncID); err != nil {
		return "", fmt.Errorf("failed to generate diff for grants: %w", err)
	}

	if err := pe.generateDiffForAssets(ctx, baseSyncID, appliedSyncID); err != nil {
		return "", fmt.Errorf("failed to generate diff for assets: %w", err)
	}

	// End the diff sync
	err = pe.EndSync(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to end diff sync: %w", err)
	}

	return diffSyncID, nil
}

// ViewSync sets the view to a specific sync.
func (pe *PebbleEngine) ViewSync(ctx context.Context, syncID string) error {
	ctx, span := syncRunTracer.Start(ctx, "PebbleEngine.ViewSync")
	defer span.End()

	if err := pe.validateOpen(); err != nil {
		return err
	}

	if syncID == "" {
		// Reset view to use latest finished sync
		pe.setViewSyncID("")
		return nil
	}

	// Validate the sync exists and is finished
	syncRun, err := pe.getSyncRun(ctx, syncID)
	if err != nil {
		return fmt.Errorf("failed to get sync run: %w", err)
	}

	if syncRun.EndedAt == 0 {
		return fmt.Errorf("cannot view unfinished sync")
	}

	// Set the view sync ID
	pe.setViewSyncID(syncID)
	return nil
}

// Cleanup performs maintenance operations on the database.
func (pe *PebbleEngine) Cleanup(ctx context.Context) error {
	ctx, span := syncRunTracer.Start(ctx, "PebbleEngine.Cleanup")
	defer span.End()

	if err := pe.validateOpen(); err != nil {
		return err
	}

	// Check if cleanup is disabled via environment variable
	if os.Getenv("BATON_SKIP_CLEANUP") == "true" {
		return nil // Skip cleanup entirely
	}

	// Skip cleanup if there's an active sync
	if pe.getCurrentSyncID() != "" {
		return nil // Skip cleanup when sync is active
	}

	// Get all finished syncs to determine what can be cleaned up
	fullSyncs, partialSyncs, err := pe.getAllFinishedSyncs(ctx)
	if err != nil {
		return fmt.Errorf("failed to get finished syncs: %w", err)
	}

	// Keep only the most recent N full syncs (configurable via BATON_KEEP_SYNC_COUNT, default 2)
	maxFullSyncs := 2
	if keepCountStr := os.Getenv("BATON_KEEP_SYNC_COUNT"); keepCountStr != "" {
		if keepCount, err := strconv.Atoi(keepCountStr); err == nil && keepCount > 0 {
			maxFullSyncs = keepCount
		}
	}

	if len(fullSyncs) > maxFullSyncs {
		// Sort by started time (most recent first)
		sort.Slice(fullSyncs, func(i, j int) bool {
			return fullSyncs[i].StartedAt > fullSyncs[j].StartedAt
		})

		// Keep track of deleted full sync IDs
		deletedFullSyncIDs := make(map[string]bool)

		// Delete old full syncs beyond the limit
		for i := maxFullSyncs; i < len(fullSyncs); i++ {
			deletedFullSyncIDs[fullSyncs[i].ID] = true
			if err := pe.deleteSyncData(ctx, fullSyncs[i].ID); err != nil {
				return fmt.Errorf("failed to delete old sync %s: %w", fullSyncs[i].ID, err)
			}
		}

		// Also delete partial syncs whose parent sync was deleted
		for _, partialSync := range partialSyncs {
			if partialSync.ParentSyncID != "" && deletedFullSyncIDs[partialSync.ParentSyncID] {
				if err := pe.deleteSyncData(ctx, partialSync.ID); err != nil {
					return fmt.Errorf("failed to delete partial sync %s (parent deleted): %w", partialSync.ID, err)
				}
			}
		}
	}

	// Clean up partial syncs older than a certain age (configurable, default 7 days)
	const maxPartialAge = 7 * 24 * time.Hour
	cutoff := time.Now().Add(-maxPartialAge).Unix()

	for _, partialSync := range partialSyncs {
		if partialSync.StartedAt < cutoff {
			if err := pe.deleteSyncData(ctx, partialSync.ID); err != nil {
				return fmt.Errorf("failed to delete old partial sync %s: %w", partialSync.ID, err)
			}
		}
	}

	// Trigger manual compaction to reclaim space
	if err := pe.triggerManualCompaction(ctx); err != nil {
		return fmt.Errorf("failed to trigger compaction: %w", err)
	}

	return nil
}

// ListSyncRuns lists all sync runs with pagination.
func (pe *PebbleEngine) ListSyncRuns(ctx context.Context, pageToken string, pageSize uint32) ([]*engine.SyncRun, string, error) {
	ctx, span := syncRunTracer.Start(ctx, "PebbleEngine.ListSyncRuns")
	defer span.End()

	if err := pe.validateOpen(); err != nil {
		return nil, "", err
	}

	if pageSize == 0 || pageSize > 1000 {
		pageSize = 1000
	}

	// Parse the page token to get the starting timestamp
	var startTimestamp int64
	var hasPageToken bool
	if pageToken != "" {
		timestamp, err := strconv.ParseInt(pageToken, 10, 64)
		if err != nil {
			return nil, "", fmt.Errorf("invalid page token: %w", err)
		}
		startTimestamp = timestamp
		hasPageToken = true
	}

	// Create iterator options for sync runs
	prefix := pe.keyEncoder.EncodePrefixKey(KeyTypeSyncRun, "")
	iterOpts := &pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: pe.keyEncoder.EncodePrefixKey(KeyTypeSyncRun, "~"),
	}

	iter, err := pe.db.NewIter(iterOpts)
	if err != nil {
		return nil, "", fmt.Errorf("failed to create iterator: %w", err)
	}
	defer iter.Close()

	var syncRuns []*engine.SyncRun
	var nextPageToken string
	count := uint32(0)

	// Collect all sync runs first, then sort and paginate
	var allSyncRuns []*SyncRun
	for iter.First(); iter.Valid(); iter.Next() {
		// Decode the asset envelope
		_, data, err := pe.valueCodec.DecodeAsset(iter.Value())
		if err != nil {
			continue // Skip invalid entries
		}

		// Unmarshal from JSON
		syncRun := &SyncRun{}
		if err := json.Unmarshal(data, syncRun); err != nil {
			continue // Skip invalid entries
		}
		allSyncRuns = append(allSyncRuns, syncRun)
	}

	// Sort by creation time (most recent first)
	sort.Slice(allSyncRuns, func(i, j int) bool {
		return allSyncRuns[i].StartedAt > allSyncRuns[j].StartedAt
	})

	// Apply pagination and convert to engine.SyncRun
	for _, syncRun := range allSyncRuns {
		// If no page token, include all. If page token provided, only include syncs older than the token timestamp
		if (!hasPageToken || syncRun.StartedAt < startTimestamp) && count < pageSize {
			syncRuns = append(syncRuns, syncRun.toEngineSyncRun())
			count++
		}
	}

	// Generate next page token if there are more results
	if count == pageSize && len(allSyncRuns) > int(count) {
		lastSync := allSyncRuns[count-1]
		nextPageToken = strconv.FormatInt(lastSync.StartedAt, 10)
	}

	return syncRuns, nextPageToken, nil
}

// ListSyncsReader implements the reader interface for listing syncs.
func (pe *PebbleEngine) ListSyncsReader(ctx context.Context, req *reader_v2.SyncsReaderServiceListSyncsRequest) (*reader_v2.SyncsReaderServiceListSyncsResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

// GetLatestFinishedSync gets the latest finished sync.
func (pe *PebbleEngine) GetLatestFinishedSync(ctx context.Context, req *reader_v2.SyncsReaderServiceGetLatestFinishedSyncRequest) (*reader_v2.SyncsReaderServiceGetLatestFinishedSyncResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

// GetSync gets a specific sync.
func (pe *PebbleEngine) GetSync(ctx context.Context, req *reader_v2.SyncsReaderServiceGetSyncRequest) (*reader_v2.SyncsReaderServiceGetSyncResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

// ListSyncs lists syncs using the reader interface.
func (pe *PebbleEngine) ListSyncs(ctx context.Context, req *reader_v2.SyncsReaderServiceListSyncsRequest) (*reader_v2.SyncsReaderServiceListSyncsResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

// copySyncDataToClone copies all data from a source sync to a clone engine
func (pe *PebbleEngine) copySyncDataToClone(ctx context.Context, sourceSyncID string, cloneEngine *PebbleEngine) error {
	// Copy resource types
	if err := pe.copyResourceTypesToClone(ctx, sourceSyncID, cloneEngine); err != nil {
		return fmt.Errorf("failed to copy resource types: %w", err)
	}
	// Copy resources
	if err := pe.copyResourcesToClone(ctx, sourceSyncID, cloneEngine); err != nil {
		return fmt.Errorf("failed to copy resources: %w", err)
	}
	// Copy entitlements
	if err := pe.copyEntitlementsToClone(ctx, sourceSyncID, cloneEngine); err != nil {
		return fmt.Errorf("failed to copy entitlements: %w", err)
	}
	// Copy grants
	if err := pe.copyGrantsToClone(ctx, sourceSyncID, cloneEngine); err != nil {
		return fmt.Errorf("failed to copy grants: %w", err)
	}
	// Copy assets
	if err := pe.copyAssetsToClone(ctx, sourceSyncID, cloneEngine); err != nil {
		return fmt.Errorf("failed to copy assets: %w", err)
	}
	return nil
}
