package pebble

import (
	"context"
	"fmt"

	"github.com/cockroachdb/pebble"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

// IndexManager manages secondary indexes for efficient querying.
type IndexManager struct {
	engine *PebbleEngine
}

// NewIndexManager creates a new IndexManager instance.
func NewIndexManager(engine *PebbleEngine) *IndexManager {
	return &IndexManager{
		engine: engine,
	}
}

// CreateEntitlementsByResourceIndex creates an index entry for entitlements-by-resource.
// This allows efficient querying of entitlements filtered by resource.
func (im *IndexManager) CreateEntitlementsByResourceIndex(batch *pebble.Batch, syncID string, entitlement *v2.Entitlement) error {
	if entitlement == nil {
		return fmt.Errorf("entitlement cannot be nil")
	}

	externalID := entitlement.GetId()
	if externalID == "" {
		return fmt.Errorf("entitlement missing external ID")
	}

	// Only create index if entitlement has a resource
	if entitlement.GetResource() == nil || entitlement.GetResource().GetId() == nil {
		return nil // No resource, no index needed
	}

	resourceID := entitlement.GetResource().GetId()
	if resourceID.GetResourceType() == "" || resourceID.GetResource() == "" {
		return nil // Incomplete resource ID, no index needed
	}

	// Create index key: v1|ix|en_by_res|{sync_id}|{resource_type_id}|{resource_id}|{external_id}
	indexKey := im.engine.keyEncoder.EncodeEntitlementsByResourceIndexKey(
		syncID,
		resourceID.GetResourceType(),
		resourceID.GetResource(),
		externalID,
	)

	// The index value can be empty since we only need the key for lookups
	// We store the entitlement ID as the value for potential future use
	indexValue := []byte(externalID)

	err := batch.Set(indexKey, indexValue, nil)
	if err != nil {
		return fmt.Errorf("failed to create entitlements-by-resource index: %w", err)
	}

	return nil
}

// DeleteEntitlementsByResourceIndex removes an index entry for entitlements-by-resource.
func (im *IndexManager) DeleteEntitlementsByResourceIndex(batch *pebble.Batch, syncID string, entitlement *v2.Entitlement) error {
	if entitlement == nil {
		return fmt.Errorf("entitlement cannot be nil")
	}

	externalID := entitlement.GetId()
	if externalID == "" {
		return fmt.Errorf("entitlement missing external ID")
	}

	// Only delete index if entitlement has a resource
	if entitlement.GetResource() == nil || entitlement.GetResource().GetId() == nil {
		return nil // No resource, no index to delete
	}

	resourceID := entitlement.GetResource().GetId()
	if resourceID.GetResourceType() == "" || resourceID.GetResource() == "" {
		return nil // Incomplete resource ID, no index to delete
	}

	// Create index key to delete
	indexKey := im.engine.keyEncoder.EncodeEntitlementsByResourceIndexKey(
		syncID,
		resourceID.GetResourceType(),
		resourceID.GetResource(),
		externalID,
	)

	err := batch.Delete(indexKey, nil)
	if err != nil {
		return fmt.Errorf("failed to delete entitlements-by-resource index: %w", err)
	}

	return nil
}

// CreateGrantsByResourceIndex creates an index entry for grants-by-resource.
func (im *IndexManager) CreateGrantsByResourceIndex(batch *pebble.Batch, syncID string, grant *v2.Grant) error {
	if grant == nil {
		return fmt.Errorf("grant cannot be nil")
	}

	externalID := grant.GetId()
	if externalID == "" {
		return fmt.Errorf("grant missing external ID")
	}

	// Only create index if grant has an entitlement with a resource
	if grant.GetEntitlement() == nil || grant.GetEntitlement().GetResource() == nil || grant.GetEntitlement().GetResource().GetId() == nil {
		return nil // No entitlement resource, no index needed
	}

	resourceID := grant.GetEntitlement().GetResource().GetId()
	if resourceID.GetResourceType() == "" || resourceID.GetResource() == "" {
		return nil // Incomplete resource ID, no index needed
	}

	// Create index key: v1|ix|gr_by_res|{sync_id}|{resource_type_id}|{resource_id}|{external_id}
	indexKey := im.engine.keyEncoder.EncodeGrantsByResourceIndexKey(
		syncID,
		resourceID.GetResourceType(),
		resourceID.GetResource(),
		externalID,
	)

	// Store the grant ID as the value
	indexValue := []byte(externalID)

	err := batch.Set(indexKey, indexValue, nil)
	if err != nil {
		return fmt.Errorf("failed to create grants-by-resource index: %w", err)
	}

	return nil
}

// DeleteGrantsByResourceIndex removes an index entry for grants-by-resource.
func (im *IndexManager) DeleteGrantsByResourceIndex(batch *pebble.Batch, syncID string, grant *v2.Grant) error {
	if grant == nil {
		return fmt.Errorf("grant cannot be nil")
	}

	externalID := grant.GetId()
	if externalID == "" {
		return fmt.Errorf("grant missing external ID")
	}

	// Only delete index if grant has an entitlement with a resource
	if grant.GetEntitlement() == nil || grant.GetEntitlement().GetResource() == nil || grant.GetEntitlement().GetResource().GetId() == nil {
		return nil // No entitlement resource, no index to delete
	}

	resourceID := grant.GetEntitlement().GetResource().GetId()
	if resourceID.GetResourceType() == "" || resourceID.GetResource() == "" {
		return nil // Incomplete resource ID, no index to delete
	}

	// Create index key to delete
	indexKey := im.engine.keyEncoder.EncodeGrantsByResourceIndexKey(
		syncID,
		resourceID.GetResourceType(),
		resourceID.GetResource(),
		externalID,
	)

	err := batch.Delete(indexKey, nil)
	if err != nil {
		return fmt.Errorf("failed to delete grants-by-resource index: %w", err)
	}

	return nil
}

// CreateGrantsByPrincipalIndex creates an index entry for grants-by-principal.
func (im *IndexManager) CreateGrantsByPrincipalIndex(batch *pebble.Batch, syncID string, grant *v2.Grant) error {
	if grant == nil {
		return fmt.Errorf("grant cannot be nil")
	}

	externalID := grant.GetId()
	if externalID == "" {
		return fmt.Errorf("grant missing external ID")
	}

	// Only create index if grant has a principal
	if grant.GetPrincipal() == nil || grant.GetPrincipal().GetId() == nil {
		return nil // No principal, no index needed
	}

	principalID := grant.GetPrincipal().GetId()
	if principalID.GetResourceType() == "" || principalID.GetResource() == "" {
		return nil // Incomplete principal ID, no index needed
	}

	// Create index key: v1|ix|gr_by_prn|{sync_id}|{principal_type}|{principal_id}|{external_id}
	indexKey := im.engine.keyEncoder.EncodeGrantsByPrincipalIndexKey(
		syncID,
		principalID.GetResourceType(),
		principalID.GetResource(),
		externalID,
	)

	// Store the grant ID as the value
	indexValue := []byte(externalID)

	err := batch.Set(indexKey, indexValue, nil)
	if err != nil {
		return fmt.Errorf("failed to create grants-by-principal index: %w", err)
	}

	return nil
}

// DeleteGrantsByPrincipalIndex removes an index entry for grants-by-principal.
func (im *IndexManager) DeleteGrantsByPrincipalIndex(batch *pebble.Batch, syncID string, grant *v2.Grant) error {
	if grant == nil {
		return fmt.Errorf("grant cannot be nil")
	}

	externalID := grant.GetId()
	if externalID == "" {
		return fmt.Errorf("grant missing external ID")
	}

	// Only delete index if grant has a principal
	if grant.GetPrincipal() == nil || grant.GetPrincipal().GetId() == nil {
		return nil // No principal, no index to delete
	}

	principalID := grant.GetPrincipal().GetId()
	if principalID.GetResourceType() == "" || principalID.GetResource() == "" {
		return nil // Incomplete principal ID, no index to delete
	}

	// Create index key to delete
	indexKey := im.engine.keyEncoder.EncodeGrantsByPrincipalIndexKey(
		syncID,
		principalID.GetResourceType(),
		principalID.GetResource(),
		externalID,
	)

	err := batch.Delete(indexKey, nil)
	if err != nil {
		return fmt.Errorf("failed to delete grants-by-principal index: %w", err)
	}

	return nil
}

// CreateGrantsByEntitlementIndex creates an index entry for grants-by-entitlement.
func (im *IndexManager) CreateGrantsByEntitlementIndex(batch *pebble.Batch, syncID string, grant *v2.Grant) error {
	if grant == nil {
		return fmt.Errorf("grant cannot be nil")
	}

	externalID := grant.GetId()
	if externalID == "" {
		return fmt.Errorf("grant missing external ID")
	}

	// Only create index if grant has an entitlement
	if grant.GetEntitlement() == nil {
		return nil // No entitlement, no index needed
	}

	entitlementID := grant.GetEntitlement().GetId()
	if entitlementID == "" {
		return nil // No entitlement ID, no index needed
	}

	// Also need principal for the index key
	if grant.GetPrincipal() == nil || grant.GetPrincipal().GetId() == nil {
		return nil // No principal, no index needed
	}

	principalID := grant.GetPrincipal().GetId()
	if principalID.GetResourceType() == "" || principalID.GetResource() == "" {
		return nil // Incomplete principal ID, no index needed
	}

	// Create index key: v1|ix|gr_by_ent|{sync_id}|{entitlement_id}|{principal_type}|{principal_id}|{external_id}
	indexKey := im.engine.keyEncoder.EncodeGrantsByEntitlementIndexKey(
		syncID,
		entitlementID,
		principalID.GetResourceType(),
		principalID.GetResource(),
		externalID,
	)

	// Store the grant ID as the value
	indexValue := []byte(externalID)

	err := batch.Set(indexKey, indexValue, nil)
	if err != nil {
		return fmt.Errorf("failed to create grants-by-entitlement index: %w", err)
	}

	return nil
}

// DeleteGrantsByEntitlementIndex removes an index entry for grants-by-entitlement.
func (im *IndexManager) DeleteGrantsByEntitlementIndex(batch *pebble.Batch, syncID string, grant *v2.Grant) error {
	if grant == nil {
		return fmt.Errorf("grant cannot be nil")
	}

	externalID := grant.GetId()
	if externalID == "" {
		return fmt.Errorf("grant missing external ID")
	}

	// Only delete index if grant has an entitlement
	if grant.GetEntitlement() == nil {
		return nil // No entitlement, no index to delete
	}

	entitlementID := grant.GetEntitlement().GetId()
	if entitlementID == "" {
		return nil // No entitlement ID, no index to delete
	}

	// Also need principal for the index key
	if grant.GetPrincipal() == nil || grant.GetPrincipal().GetId() == nil {
		return nil // No principal, no index to delete
	}

	principalID := grant.GetPrincipal().GetId()
	if principalID.GetResourceType() == "" || principalID.GetResource() == "" {
		return nil // Incomplete principal ID, no index to delete
	}

	// Create index key to delete
	indexKey := im.engine.keyEncoder.EncodeGrantsByEntitlementIndexKey(
		syncID,
		entitlementID,
		principalID.GetResourceType(),
		principalID.GetResource(),
		externalID,
	)

	err := batch.Delete(indexKey, nil)
	if err != nil {
		return fmt.Errorf("failed to delete grants-by-entitlement index: %w", err)
	}

	return nil
}

// CreateAllGrantIndexes creates all applicable indexes for a grant.
func (im *IndexManager) CreateAllGrantIndexes(batch *pebble.Batch, syncID string, grant *v2.Grant) error {
	// Create grants-by-resource index
	if err := im.CreateGrantsByResourceIndex(batch, syncID, grant); err != nil {
		return fmt.Errorf("failed to create grants-by-resource index: %w", err)
	}

	// Create grants-by-principal index
	if err := im.CreateGrantsByPrincipalIndex(batch, syncID, grant); err != nil {
		return fmt.Errorf("failed to create grants-by-principal index: %w", err)
	}

	// Create grants-by-entitlement index
	if err := im.CreateGrantsByEntitlementIndex(batch, syncID, grant); err != nil {
		return fmt.Errorf("failed to create grants-by-entitlement index: %w", err)
	}

	return nil
}

// DeleteAllGrantIndexes removes all applicable indexes for a grant.
func (im *IndexManager) DeleteAllGrantIndexes(batch *pebble.Batch, syncID string, grant *v2.Grant) error {
	// Delete grants-by-resource index
	if err := im.DeleteGrantsByResourceIndex(batch, syncID, grant); err != nil {
		return fmt.Errorf("failed to delete grants-by-resource index: %w", err)
	}

	// Delete grants-by-principal index
	if err := im.DeleteGrantsByPrincipalIndex(batch, syncID, grant); err != nil {
		return fmt.Errorf("failed to delete grants-by-principal index: %w", err)
	}

	// Delete grants-by-entitlement index
	if err := im.DeleteGrantsByEntitlementIndex(batch, syncID, grant); err != nil {
		return fmt.Errorf("failed to delete grants-by-entitlement index: %w", err)
	}

	return nil
}

// ValidateIndexConsistency validates that secondary indexes are consistent with primary data.
func (im *IndexManager) ValidateIndexConsistency(ctx context.Context, syncID string) error {
	if err := im.engine.validateOpen(); err != nil {
		return err
	}

	// Validate entitlements-by-resource index
	if err := im.validateEntitlementsByResourceIndex(ctx, syncID); err != nil {
		return fmt.Errorf("entitlements-by-resource index validation failed: %w", err)
	}

	// Note: Grant index validation will be implemented when grant operations are added
	// This is a placeholder for future implementation

	return nil
}

// validateEntitlementsByResourceIndex validates the entitlements-by-resource index.
func (im *IndexManager) validateEntitlementsByResourceIndex(ctx context.Context, syncID string) error {
	// Create prefix for entitlements-by-resource index
	indexPrefix := im.engine.keyEncoder.EncodeIndexPrefixKey(IndexEntitlementsByResource, syncID)
	indexUpperBound := im.engine.keyEncoder.EncodeIndexPrefixKey(IndexEntitlementsByResource, syncID+"~")

	// Iterate through index entries
	iterOpts := &pebble.IterOptions{
		LowerBound: indexPrefix,
		UpperBound: indexUpperBound,
	}

	iter, err := im.engine.db.NewIter(iterOpts)
	if err != nil {
		return fmt.Errorf("failed to create index iterator: %w", err)
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		indexKey := iter.Key()

		// Decode the index key to get the entitlement ID
		decodedKey, err := im.engine.keyEncoder.DecodeKey(indexKey)
		if err != nil {
			return fmt.Errorf("failed to decode index key: %w", err)
		}

		// Index key format: v1|ix|en_by_res|{sync_id}|{resource_type_id}|{resource_id}|{external_id}
		// Components: [sync_id, resource_type_id, resource_id, external_id]
		if len(decodedKey.Components) < 4 {
			return fmt.Errorf("invalid index key format: expected at least 4 components, got %d", len(decodedKey.Components))
		}

		entitlementID := decodedKey.Components[3] // The external_id is the 4th component (0-indexed)

		// Check if the corresponding entitlement exists
		entitlementKey := im.engine.keyEncoder.EncodeEntitlementKey(syncID, entitlementID)
		_, closer, err := im.engine.db.Get(entitlementKey)
		if err != nil {
			if err == pebble.ErrNotFound {
				return fmt.Errorf("index inconsistency: entitlement %s referenced in index but not found in primary data", entitlementID)
			}
			return fmt.Errorf("failed to check entitlement existence: %w", err)
		}
		closer.Close()
	}

	if err := iter.Error(); err != nil {
		return fmt.Errorf("iterator error during index validation: %w", err)
	}

	return nil
}

// RebuildIndexes rebuilds all secondary indexes for a given sync.
func (im *IndexManager) RebuildIndexes(ctx context.Context, syncID string) error {
	if err := im.engine.validateOpen(); err != nil {
		return err
	}

	// Create a batch for atomic index rebuilding
	batch := im.engine.db.NewBatch()
	defer batch.Close()

	// First, delete all existing indexes for this sync
	if err := im.deleteAllIndexesForSync(batch, syncID); err != nil {
		return fmt.Errorf("failed to delete existing indexes: %w", err)
	}

	// Rebuild entitlements-by-resource index
	if err := im.rebuildEntitlementsByResourceIndex(batch, syncID); err != nil {
		return fmt.Errorf("failed to rebuild entitlements-by-resource index: %w", err)
	}

	// Note: Grant index rebuilding will be implemented when grant operations are added

	// Commit the batch
	err := batch.Commit(im.engine.getSyncOptions())
	if err != nil {
		return fmt.Errorf("failed to commit index rebuild: %w", err)
	}

	return nil
}

// deleteAllIndexesForSync deletes all secondary indexes for a given sync.
func (im *IndexManager) deleteAllIndexesForSync(batch *pebble.Batch, syncID string) error {
	// Delete entitlements-by-resource indexes
	indexPrefix := im.engine.keyEncoder.EncodeIndexPrefixKey(IndexEntitlementsByResource, syncID)
	indexUpperBound := im.engine.keyEncoder.EncodeIndexPrefixKey(IndexEntitlementsByResource, syncID+"~")

	err := batch.DeleteRange(indexPrefix, indexUpperBound, nil)
	if err != nil {
		return fmt.Errorf("failed to delete entitlements-by-resource indexes: %w", err)
	}

	// Note: Additional index types will be added here when implemented

	return nil
}

// rebuildEntitlementsByResourceIndex rebuilds the entitlements-by-resource index.
func (im *IndexManager) rebuildEntitlementsByResourceIndex(batch *pebble.Batch, syncID string) error {
	// Create prefix for entitlements in this sync
	entitlementPrefix := im.engine.keyEncoder.EncodePrefixKey(KeyTypeEntitlement, syncID)
	entitlementUpperBound := im.engine.keyEncoder.EncodePrefixKey(KeyTypeEntitlement, syncID+"~")

	// Iterate through all entitlements
	iterOpts := &pebble.IterOptions{
		LowerBound: entitlementPrefix,
		UpperBound: entitlementUpperBound,
	}

	iter, err := im.engine.db.NewIter(iterOpts)
	if err != nil {
		return fmt.Errorf("failed to create entitlement iterator: %w", err)
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		value := iter.Value()

		// Decode the entitlement
		entitlement := &v2.Entitlement{}
		_, err := im.engine.valueCodec.DecodeValue(value, entitlement)
		if err != nil {
			return fmt.Errorf("failed to decode entitlement during index rebuild: %w", err)
		}

		// Create the index entry
		if err := im.CreateEntitlementsByResourceIndex(batch, syncID, entitlement); err != nil {
			return fmt.Errorf("failed to create index entry during rebuild: %w", err)
		}
	}

	if err := iter.Error(); err != nil {
		return fmt.Errorf("iterator error during index rebuild: %w", err)
	}

	return nil
}

// GetIndexStats returns statistics about the indexes.
func (im *IndexManager) GetIndexStats(ctx context.Context, syncID string) (map[string]int64, error) {
	if err := im.engine.validateOpen(); err != nil {
		return nil, err
	}

	stats := make(map[string]int64)

	// Count entitlements-by-resource index entries
	entitlementsByResourceCount, err := im.countIndexEntries(IndexEntitlementsByResource, syncID)
	if err != nil {
		return nil, fmt.Errorf("failed to count entitlements-by-resource index entries: %w", err)
	}
	stats["entitlements_by_resource_index_entries"] = entitlementsByResourceCount

	// Note: Additional index stats will be added when more indexes are implemented

	return stats, nil
}

// countIndexEntries counts the number of entries in a specific index for a sync.
func (im *IndexManager) countIndexEntries(indexType IndexType, syncID string) (int64, error) {
	indexPrefix := im.engine.keyEncoder.EncodeIndexPrefixKey(indexType, syncID)
	indexUpperBound := im.engine.keyEncoder.EncodeIndexPrefixKey(indexType, syncID+"~")

	iterOpts := &pebble.IterOptions{
		LowerBound: indexPrefix,
		UpperBound: indexUpperBound,
	}

	iter, err := im.engine.db.NewIter(iterOpts)
	if err != nil {
		return 0, fmt.Errorf("failed to create iterator: %w", err)
	}
	defer iter.Close()

	var count int64
	for iter.First(); iter.Valid(); iter.Next() {
		count++
	}

	if err := iter.Error(); err != nil {
		return 0, fmt.Errorf("iterator error: %w", err)
	}

	return count, nil
}
