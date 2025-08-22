package pebble

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/pebble"
	"go.opentelemetry.io/otel"
	"google.golang.org/protobuf/proto"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
)

var entitlementTracer = otel.Tracer("baton-sdk/pkg.dotc1z.engine.pebble.entitlements")

// PutEntitlements stores entitlements in the database.
func (pe *PebbleEngine) PutEntitlements(ctx context.Context, entitlements ...*v2.Entitlement) error {
	ctx, span := entitlementTracer.Start(ctx, "PebbleEngine.PutEntitlements")
	defer span.End()

	return pe.putEntitlementsInternal(ctx, false, entitlements...)
}

// PutEntitlementsIfNewer stores entitlements only if they are newer than existing ones.
func (pe *PebbleEngine) PutEntitlementsIfNewer(ctx context.Context, entitlementObjs ...*v2.Entitlement) error {
	ctx, span := entitlementTracer.Start(ctx, "PebbleEngine.PutEntitlementsIfNewer")
	defer span.End()

	return pe.putEntitlementsInternal(ctx, true, entitlementObjs...)
}

// ListEntitlements lists entitlements with pagination.
func (pe *PebbleEngine) ListEntitlements(ctx context.Context, req *v2.EntitlementsServiceListEntitlementsRequest) (*v2.EntitlementsServiceListEntitlementsResponse, error) {
	ctx, span := entitlementTracer.Start(ctx, "PebbleEngine.ListEntitlements")
	defer span.End()

	if err := pe.validateOpen(); err != nil {
		return nil, err
	}

	// Get the sync ID to use for the query
	syncID, err := pe.getSyncIDForQuery(ctx, req.GetAnnotations())
	if err != nil {
		return nil, fmt.Errorf("failed to get sync ID: %w", err)
	}

	// Check if we need to filter by resource
	var prefixKey []byte
	var upperBound []byte

	if req.GetResource() != nil && req.GetResource().GetId() != nil {
		// Filter by resource using secondary index
		resourceID := req.GetResource().GetId()
		if resourceID.GetResourceType() == "" || resourceID.GetResource() == "" {
			return nil, fmt.Errorf("resource type and resource ID are required for filtering")
		}

		// Use entitlements-by-resource index
		prefixKey = pe.keyEncoder.EncodeIndexPrefixKey(IndexEntitlementsByResource, syncID, resourceID.GetResourceType(), resourceID.GetResource())
		upperBound = pe.keyEncoder.EncodeIndexPrefixKey(IndexEntitlementsByResource, syncID, resourceID.GetResourceType(), resourceID.GetResource()+"~")
	} else {
		// List all entitlements
		prefixKey = pe.keyEncoder.EncodePrefixKey(KeyTypeEntitlement, syncID)
		upperBound = pe.keyEncoder.EncodePrefixKey(KeyTypeEntitlement, syncID+"~")
	}

	// Parse page token if provided
	var startKey []byte
	if req.GetPageToken() != "" {
		token, err := pe.decodePageToken(req.GetPageToken())
		if err != nil {
			return nil, fmt.Errorf("invalid page token: %w", err)
		}
		startKey = token.LastKey
	} else {
		startKey = prefixKey
	}

	// Determine page size
	pageSize := req.GetPageSize()
	if pageSize == 0 || pageSize > 1000 {
		pageSize = 1000
	}

	// Create iterator for range scan
	iterOpts := &pebble.IterOptions{
		LowerBound: startKey,
		UpperBound: upperBound,
	}

	iter, err := pe.db.NewIter(iterOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to create iterator: %w", err)
	}
	defer iter.Close()

	var entitlements []*v2.Entitlement
	count := uint32(0)

	// Iterate through entitlements
	for iter.First(); iter.Valid() && count < pageSize; iter.Next() {
		key := iter.Key()
		value := iter.Value()

		var entitlement *v2.Entitlement
		var entitlementKey []byte

		// Check if we're using an index or direct keys
		if req.GetResource() != nil && req.GetResource().GetId() != nil {
			// We're using the index, need to decode the index key to get the entitlement ID
			decodedKey, err := pe.keyEncoder.DecodeKey(key)
			if err != nil {
				return nil, fmt.Errorf("failed to decode index key: %w", err)
			}

			// Index key format: v1|ix|en_by_res|{sync_id}|{resource_type_id}|{resource_id}|{external_id}
			// Components: [sync_id, resource_type_id, resource_id, external_id]
			if len(decodedKey.Components) < 4 {
				return nil, fmt.Errorf("invalid index key format: expected at least 4 components, got %d", len(decodedKey.Components))
			}

			entitlementID := decodedKey.Components[3] // The external_id is the 4th component (0-indexed)
			entitlementKey = pe.keyEncoder.EncodeEntitlementKey(syncID, entitlementID)

			// Get the actual entitlement data
			entitlementValue, closer, err := pe.db.Get(entitlementKey)
			if err != nil {
				if err == pebble.ErrNotFound {
					// Index is inconsistent, skip this entry
					continue
				}
				return nil, fmt.Errorf("failed to get entitlement from index: %w", err)
			}
			defer closer.Close()

			// Decode the entitlement
			entitlement = &v2.Entitlement{}
			_, err = pe.valueCodec.DecodeValue(entitlementValue, entitlement)
			if err != nil {
				return nil, fmt.Errorf("failed to decode entitlement: %w", err)
			}
		} else {
			// Direct entitlement key, decode directly
			entitlement = &v2.Entitlement{}
			_, err = pe.valueCodec.DecodeValue(value, entitlement)
			if err != nil {
				return nil, fmt.Errorf("failed to decode entitlement: %w", err)
			}
		}

		entitlements = append(entitlements, entitlement)
		count++
	}

	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterator error: %w", err)
	}

	// Generate next page token if there are more results
	var nextPageToken string
	if iter.Valid() {
		// There are more results, create a page token from the next key
		nextKey := iter.Key()
		nextPageToken, err = pe.encodePageToken(nextKey)
		if err != nil {
			return nil, fmt.Errorf("failed to encode page token: %w", err)
		}
	}

	return &v2.EntitlementsServiceListEntitlementsResponse{
		List:          entitlements,
		NextPageToken: nextPageToken,
	}, nil
}

// GetEntitlement retrieves a specific entitlement by ID.
func (pe *PebbleEngine) GetEntitlement(ctx context.Context, req *reader_v2.EntitlementsReaderServiceGetEntitlementRequest) (*reader_v2.EntitlementsReaderServiceGetEntitlementResponse, error) {
	ctx, span := entitlementTracer.Start(ctx, "PebbleEngine.GetEntitlement")
	defer span.End()

	if err := pe.validateOpen(); err != nil {
		return nil, err
	}

	if req == nil || req.GetEntitlementId() == "" {
		return nil, fmt.Errorf("entitlement request or entitlement ID cannot be nil/empty")
	}

	// Get the sync ID to use for the query
	syncID, err := pe.getSyncIDForQuery(ctx, req.GetAnnotations())
	if err != nil {
		return nil, fmt.Errorf("failed to get sync ID: %w", err)
	}

	// Create the key for this entitlement
	key := pe.keyEncoder.EncodeEntitlementKey(syncID, req.GetEntitlementId())

	// Get the value from the database
	value, closer, err := pe.db.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, fmt.Errorf("entitlement not found: %s", req.GetEntitlementId())
		}
		return nil, fmt.Errorf("failed to get entitlement: %w", err)
	}
	defer closer.Close()

	// Decode the entitlement
	entitlement := &v2.Entitlement{}
	_, err = pe.valueCodec.DecodeValue(value, entitlement)
	if err != nil {
		return nil, fmt.Errorf("failed to decode entitlement: %w", err)
	}

	return &reader_v2.EntitlementsReaderServiceGetEntitlementResponse{
		Entitlement: entitlement,
	}, nil
}

// putEntitlementsInternal handles the internal logic for putting entitlements.
func (pe *PebbleEngine) putEntitlementsInternal(ctx context.Context, ifNewer bool, entitlements ...*v2.Entitlement) error {
	if err := pe.validateOpen(); err != nil {
		return err
	}

	if len(entitlements) == 0 {
		return nil
	}

	// Get current sync ID
	syncID := pe.getCurrentSyncID()
	if syncID == "" {
		return fmt.Errorf("no active sync")
	}

	// Create a batch for atomic operations
	batch := pe.db.NewBatch()
	defer batch.Close()

	currentTime := time.Now()

	for _, entitlement := range entitlements {
		if entitlement == nil {
			continue
		}

		externalID := entitlement.GetId()
		if externalID == "" {
			return fmt.Errorf("entitlement missing external ID")
		}

		key := pe.keyEncoder.EncodeEntitlementKey(syncID, externalID)

		// Handle conditional upsert if ifNewer is true
		if ifNewer {
			shouldUpdate, err := pe.shouldUpdateEntity(key, currentTime)
			if err != nil {
				return fmt.Errorf("failed to check if entitlement should be updated: %w", err)
			}
			if !shouldUpdate {
				continue
			}
		}

		// Encode the value with metadata
		value, err := pe.valueCodec.EncodeValue(entitlement, currentTime, "")
		if err != nil {
			return fmt.Errorf("failed to encode entitlement: %w", err)
		}

		// Add to batch
		err = batch.Set(key, value, nil)
		if err != nil {
			return fmt.Errorf("failed to add entitlement to batch: %w", err)
		}

		// Create secondary index for entitlements-by-resource using IndexManager
		err = pe.indexManager.CreateEntitlementsByResourceIndex(batch, syncID, entitlement)
		if err != nil {
			return fmt.Errorf("failed to create entitlement index: %w", err)
		}
	}

	// Commit the batch
	err := batch.Commit(pe.getSyncOptions())
	if err != nil {
		return fmt.Errorf("failed to commit entitlements batch: %w", err)
	}

	pe.markDirty()
	return nil
}

// getAllEntitlementsForSync retrieves all entitlements for a specific sync
func (pe *PebbleEngine) getAllEntitlementsForSync(ctx context.Context, syncID string) (map[string]*v2.Entitlement, error) {
	m := make(map[string]*v2.Entitlement)
	prefix := pe.keyEncoder.EncodePrefixKey(KeyTypeEntitlement, syncID)
	upper := pe.keyEncoder.EncodePrefixKey(KeyTypeEntitlement, syncID+"~")
	iter, err := pe.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: upper})
	if err != nil {
		return nil, fmt.Errorf("failed to create iterator: %w", err)
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		var e v2.Entitlement
		_, err := pe.valueCodec.DecodeValue(iter.Value(), &e)
		if err != nil {
			continue
		}
		m[e.GetId()] = &e
	}
	return m, nil
}

// copyEntitlementsToClone copies entitlements from source to clone
func (pe *PebbleEngine) copyEntitlementsToClone(ctx context.Context, sourceSyncID string, cloneEngine *PebbleEngine) error {
	entitlements, err := pe.getAllEntitlementsForSync(ctx, sourceSyncID)
	if err != nil {
		return err
	}
	for _, ent := range entitlements {
		if err := cloneEngine.PutEntitlementsIfNewer(ctx, ent); err != nil {
			return fmt.Errorf("failed to copy entitlement %s: %w", ent.GetId(), err)
		}
	}
	return nil
}

// generateDiffForEntitlements generates diff data for entitlements
func (pe *PebbleEngine) generateDiffForEntitlements(ctx context.Context, baseSyncID, appliedSyncID string) error {
	applied, err := pe.getAllEntitlementsForSync(ctx, appliedSyncID)
	if err != nil {
		return err
	}
	base, err := pe.getAllEntitlementsForSync(ctx, baseSyncID)
	if err != nil {
		return err
	}
	for _, ae := range applied {
		be := base[ae.GetId()]
		if be == nil || !pe.entitlementsEqual(be, ae) {
			if err := pe.PutEntitlementsIfNewer(ctx, ae); err != nil {
				return fmt.Errorf("failed to add entitlement to diff: %w", err)
			}
		}
	}
	return nil
}

// entitlementsEqual compares two entitlements for equality
func (pe *PebbleEngine) entitlementsEqual(a, b *v2.Entitlement) bool {
	return proto.Equal(a, b)
}