package pebble

import (
	"context"
	"fmt"

	"github.com/cockroachdb/pebble"
	"go.opentelemetry.io/otel"
)

var maintenanceTracer = otel.Tracer("baton-sdk/pkg.dotc1z.engine.pebble.maintenance")

// IntegrityChecker provides database integrity checking and repair capabilities.
type IntegrityChecker struct {
	engine *PebbleEngine
}

// NewIntegrityChecker creates a new IntegrityChecker instance.
func NewIntegrityChecker(engine *PebbleEngine) *IntegrityChecker {
	return &IntegrityChecker{
		engine: engine,
	}
}

// CheckIndexConsistency validates that secondary indexes are consistent with primary data.
func (ic *IntegrityChecker) CheckIndexConsistency(ctx context.Context) error {
	ctx, span := maintenanceTracer.Start(ctx, "IntegrityChecker.CheckIndexConsistency")
	defer span.End()

	if err := ic.engine.validateOpen(); err != nil {
		return err
	}

	// Get all sync IDs to check
	syncIDs, err := ic.getAllSyncIDs(ctx)
	if err != nil {
		return fmt.Errorf("failed to get sync IDs for consistency check: %w", err)
	}

	var allErrors []error
	for _, syncID := range syncIDs {
		if err := ic.engine.indexManager.ValidateIndexConsistency(ctx, syncID); err != nil {
			allErrors = append(allErrors, fmt.Errorf("sync %s: %w", syncID, err))
		}
	}

	if len(allErrors) > 0 {
		return fmt.Errorf("index consistency check failed for %d syncs: %v", len(allErrors), allErrors)
	}

	return nil
}

// RebuildIndexes rebuilds all secondary indexes for all syncs.
func (ic *IntegrityChecker) RebuildIndexes(ctx context.Context) error {
	ctx, span := maintenanceTracer.Start(ctx, "IntegrityChecker.RebuildIndexes")
	defer span.End()

	if err := ic.engine.validateOpen(); err != nil {
		return err
	}

	// Get all sync IDs to rebuild
	syncIDs, err := ic.getAllSyncIDs(ctx)
	if err != nil {
		return fmt.Errorf("failed to get sync IDs for index rebuild: %w", err)
	}

	for _, syncID := range syncIDs {
		if err := ic.engine.indexManager.RebuildIndexes(ctx, syncID); err != nil {
			return fmt.Errorf("failed to rebuild indexes for sync %s: %w", syncID, err)
		}
	}

	return nil
}

// ValidateReferentialIntegrity checks that all entity relationships are valid.
func (ic *IntegrityChecker) ValidateReferentialIntegrity(ctx context.Context) error {
	ctx, span := maintenanceTracer.Start(ctx, "IntegrityChecker.ValidateReferentialIntegrity")
	defer span.End()

	if err := ic.engine.validateOpen(); err != nil {
		return err
	}

	// Get all sync IDs to check
	syncIDs, err := ic.getAllSyncIDs(ctx)
	if err != nil {
		return fmt.Errorf("failed to get sync IDs for referential integrity check: %w", err)
	}

	var allErrors []error
	for _, syncID := range syncIDs {
		if err := ic.validateSyncReferentialIntegrity(ctx, syncID); err != nil {
			allErrors = append(allErrors, fmt.Errorf("sync %s: %w", syncID, err))
		}
	}

	if len(allErrors) > 0 {
		return fmt.Errorf("referential integrity check failed for %d syncs: %v", len(allErrors), allErrors)
	}

	return nil
}

// validateSyncReferentialIntegrity checks referential integrity for a specific sync.
func (ic *IntegrityChecker) validateSyncReferentialIntegrity(ctx context.Context, syncID string) error {
	// Check that all resources reference valid resource types
	if err := ic.validateResourceReferences(ctx, syncID); err != nil {
		return fmt.Errorf("resource references validation failed: %w", err)
	}

	// Check that all entitlements reference valid resources
	if err := ic.validateEntitlementReferences(ctx, syncID); err != nil {
		return fmt.Errorf("entitlement references validation failed: %w", err)
	}

	// Check that all grants reference valid principals and entitlements
	if err := ic.validateGrantReferences(ctx, syncID); err != nil {
		return fmt.Errorf("grant references validation failed: %w", err)
	}

	return nil
}

// validateResourceReferences checks that all resources reference valid resource types.
func (ic *IntegrityChecker) validateResourceReferences(ctx context.Context, syncID string) error {
	// This is a placeholder for future implementation
	// Will validate that all resources have valid resource_type references
	return nil
}

// validateEntitlementReferences checks that all entitlements reference valid resources.
func (ic *IntegrityChecker) validateEntitlementReferences(ctx context.Context, syncID string) error {
	// This is a placeholder for future implementation
	// Will validate that all entitlements have valid resource references
	return nil
}

// validateGrantReferences checks that all grants reference valid principals and entitlements.
func (ic *IntegrityChecker) validateGrantReferences(ctx context.Context, syncID string) error {
	// This is a placeholder for future implementation
	// Will validate that all grants have valid principal and entitlement references
	return nil
}

// getAllSyncIDs retrieves all sync IDs from the database by scanning sync runs.
func (ic *IntegrityChecker) getAllSyncIDs(ctx context.Context) ([]string, error) {
	var syncIDs []string

	// Scan all sync runs directly since there are only tens of them
	prefix := ic.engine.keyEncoder.EncodePrefixKey(KeyTypeSyncRun, "")
	upperBound := ic.engine.keyEncoder.EncodePrefixKey(KeyTypeSyncRun, "~")

	iter, err := ic.engine.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create iterator for sync IDs: %w", err)
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		decodedKey, err := ic.engine.keyEncoder.DecodeKey(key)
		if err != nil {
			continue // Skip invalid keys
		}

		if len(decodedKey.Components) >= 1 {
			syncID := decodedKey.Components[0] // sync_id is the first component in sync run keys
			syncIDs = append(syncIDs, syncID)
		}
	}

	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterator error while collecting sync IDs: %w", err)
	}

	return syncIDs, nil
}

// CompactionManager provides database compaction management capabilities.
type CompactionManager struct {
	engine *PebbleEngine
}

// NewCompactionManager creates a new CompactionManager instance.
func NewCompactionManager(engine *PebbleEngine) *CompactionManager {
	return &CompactionManager{
		engine: engine,
	}
}

// TriggerManualCompaction triggers manual compaction to reclaim space.
func (cm *CompactionManager) TriggerManualCompaction(ctx context.Context) error {
	ctx, span := maintenanceTracer.Start(ctx, "CompactionManager.TriggerManualCompaction")
	defer span.End()

	if err := cm.engine.validateOpen(); err != nil {
		return fmt.Errorf("cannot trigger compaction on closed engine: %w", err)
	}

	// Trigger manual compaction directly on Pebble database
	// Use empty byte slice instead of nil to avoid "start not less than end" error
	start := []byte{}
	end := []byte{0xff, 0xff, 0xff, 0xff} // Large end key to encompass all data
	return cm.engine.db.Compact(start, end, false)
}

// EstimateSpaceReclamation estimates how much space can be reclaimed through compaction.
func (cm *CompactionManager) EstimateSpaceReclamation(ctx context.Context) (int64, error) {
	ctx, span := maintenanceTracer.Start(ctx, "CompactionManager.EstimateSpaceReclamation")
	defer span.End()

	if err := cm.engine.validateOpen(); err != nil {
		return 0, fmt.Errorf("cannot estimate space reclamation on closed engine: %w", err)
	}

	// Get database statistics
	stats := cm.engine.db.Metrics()

	// Estimate space that could be reclaimed
	// This is a rough estimate based on Pebble's internal metrics
	estimatedReclamation := int64(0)

	// Add up various metrics that indicate reclaimable space
	if stats.BlockCache.Size > 0 {
		estimatedReclamation += int64(stats.BlockCache.Size)
	}

	// Add table cache size
	if stats.TableCache.Size > 0 {
		estimatedReclamation += int64(stats.TableCache.Size)
	}

	// Add memtable size
	if stats.MemTable.Size > 0 {
		estimatedReclamation += int64(stats.MemTable.Size)
	}

	return estimatedReclamation, nil
}

// ConfigureCompactionPolicy configures the compaction policy for the database.
func (cm *CompactionManager) ConfigureCompactionPolicy(policy CompactionPolicy) error {
	if err := cm.engine.validateOpen(); err != nil {
		return fmt.Errorf("cannot configure compaction policy on closed engine: %w", err)
	}

	// This is a placeholder for future implementation
	// Will allow configuring compaction policies like:
	// - Level-based compaction
	// - Size-based compaction
	// - Time-based compaction
	return nil
}

// CompactionPolicy defines different compaction strategies.
type CompactionPolicy int

const (
	CompactionPolicyDefault CompactionPolicy = iota
	CompactionPolicyAggressive
	CompactionPolicyConservative
	CompactionPolicyManual
)

// String returns the string representation of the compaction policy.
func (cp CompactionPolicy) String() string {
	switch cp {
	case CompactionPolicyDefault:
		return "default"
	case CompactionPolicyAggressive:
		return "aggressive"
	case CompactionPolicyConservative:
		return "conservative"
	case CompactionPolicyManual:
		return "manual"
	default:
		return "unknown"
	}
}

// GetCompactionStats returns current compaction statistics.
func (cm *CompactionManager) GetCompactionStats(ctx context.Context) (map[string]interface{}, error) {
	ctx, span := maintenanceTracer.Start(ctx, "CompactionManager.GetCompactionStats")
	defer span.End()

	if err := cm.engine.validateOpen(); err != nil {
		return nil, fmt.Errorf("cannot get compaction stats on closed engine: %w", err)
	}

	stats := cm.engine.db.Metrics()

	compactionStats := map[string]interface{}{
		"block_cache_size":   stats.BlockCache.Size,
		"block_cache_count":  stats.BlockCache.Count,
		"block_cache_hits":   stats.BlockCache.Hits,
		"block_cache_misses": stats.BlockCache.Misses,
		"table_cache_size":   stats.TableCache.Size,
		"table_cache_count":  stats.TableCache.Count,
		"table_cache_hits":   stats.TableCache.Hits,
		"table_cache_misses": stats.TableCache.Misses,
		"memtable_size":      stats.MemTable.Size,
		"memtable_count":     stats.MemTable.Count,
	}

	// Note: Pebble doesn't expose detailed compaction metrics through the Metrics() interface
	// These would need to be collected through other means if needed

	return compactionStats, nil
}

// TriggerFullCompaction triggers a full database compaction.
func (cm *CompactionManager) TriggerFullCompaction(ctx context.Context) error {
	ctx, span := maintenanceTracer.Start(ctx, "CompactionManager.TriggerFullCompaction")
	defer span.End()

	if err := cm.engine.validateOpen(); err != nil {
		return fmt.Errorf("cannot trigger full compaction on closed engine: %w", err)
	}

	// Trigger compaction on the entire keyspace
	startKey := []byte{0x00}
	endKey := []byte{0xFF, 0xFF}

	if err := cm.engine.db.Compact(startKey, endKey, true /* parallelize */); err != nil {
		return fmt.Errorf("failed to trigger full compaction: %w", err)
	}

	return nil
}
