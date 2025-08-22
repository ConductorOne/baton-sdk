package pebble

import (
	"context"
	"fmt"

	"github.com/cockroachdb/pebble"
	"go.opentelemetry.io/otel"
	"google.golang.org/protobuf/proto"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
)

var resourceTypeTracer = otel.Tracer("baton-sdk/pkg.dotc1z.engine.pebble.resource-types")

// PutResourceTypes stores resource types in the database.
func (pe *PebbleEngine) PutResourceTypes(ctx context.Context, resourceTypes ...*v2.ResourceType) error {
	ctx, span := resourceTypeTracer.Start(ctx, "PebbleEngine.PutResourceTypes")
	defer span.End()

	return pe.putResourceTypesInternal(ctx, false, resourceTypes...)
}

// PutResourceTypesIfNewer stores resource types only if they are newer than existing ones.
func (pe *PebbleEngine) PutResourceTypesIfNewer(ctx context.Context, resourceTypesObjs ...*v2.ResourceType) error {
	ctx, span := resourceTypeTracer.Start(ctx, "PebbleEngine.PutResourceTypesIfNewer")
	defer span.End()

	return pe.putResourceTypesInternal(ctx, true, resourceTypesObjs...)
}

// ListResourceTypes lists resource types with pagination.
func (pe *PebbleEngine) ListResourceTypes(ctx context.Context, req *v2.ResourceTypesServiceListResourceTypesRequest) (*v2.ResourceTypesServiceListResourceTypesResponse, error) {
	ctx, span := resourceTypeTracer.Start(ctx, "PebbleEngine.ListResourceTypes")
	defer span.End()

	if err := pe.validateOpen(); err != nil {
		return nil, err
	}

	// Get the sync ID to use for the query
	syncID, err := pe.getSyncIDForQuery(ctx, req.GetAnnotations())
	if err != nil {
		return nil, fmt.Errorf("failed to get sync ID: %w", err)
	}

	// Create prefix key for resource types in this sync
	prefixKey := pe.keyEncoder.EncodePrefixKey(KeyTypeResourceType, syncID)

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
		UpperBound: pe.keyEncoder.EncodePrefixKey(KeyTypeResourceType, syncID+"~"), // Use tilde for upper bound
	}

	iter, err := pe.db.NewIter(iterOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to create iterator: %w", err)
	}
	defer iter.Close()

	var resourceTypes []*v2.ResourceType
	var lastKey []byte
	count := uint32(0)

	// Iterate through resource types
	for iter.First(); iter.Valid() && count < pageSize; iter.Next() {
		key := iter.Key()
		value := iter.Value()

		// Decode the resource type
		resourceType := &v2.ResourceType{}
		_, err := pe.valueCodec.DecodeValue(value, resourceType)
		if err != nil {
			return nil, fmt.Errorf("failed to decode resource type: %w", err)
		}

		resourceTypes = append(resourceTypes, resourceType)
		lastKey = make([]byte, len(key))
		copy(lastKey, key)
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
		nextPageToken, err = pe.encodePageTokenWithSize(nextKey, pageSize)
		if err != nil {
			return nil, fmt.Errorf("failed to encode page token: %w", err)
		}
	}

	return &v2.ResourceTypesServiceListResourceTypesResponse{
		List:          resourceTypes,
		NextPageToken: nextPageToken,
	}, nil
}

// GetResourceType retrieves a specific resource type by ID.
func (pe *PebbleEngine) GetResourceType(ctx context.Context, req *reader_v2.ResourceTypesReaderServiceGetResourceTypeRequest) (*reader_v2.ResourceTypesReaderServiceGetResourceTypeResponse, error) {
	ctx, span := resourceTypeTracer.Start(ctx, "PebbleEngine.GetResourceType")
	defer span.End()

	if err := pe.validateOpen(); err != nil {
		return nil, err
	}

	// Get the sync ID to use for the query
	syncID, err := pe.getSyncIDForQuery(ctx, req.GetAnnotations())
	if err != nil {
		return nil, fmt.Errorf("failed to get sync ID: %w", err)
	}

	// Create the key for this resource type
	key := pe.keyEncoder.EncodeResourceTypeKey(syncID, req.GetResourceTypeId())

	// Get the value from the database
	value, closer, err := pe.db.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, fmt.Errorf("resource type not found: %s", req.GetResourceTypeId())
		}
		return nil, fmt.Errorf("failed to get resource type: %w", err)
	}
	defer closer.Close()

	// Decode the resource type
	resourceType := &v2.ResourceType{}
	_, err = pe.valueCodec.DecodeValue(value, resourceType)
	if err != nil {
		return nil, fmt.Errorf("failed to decode resource type: %w", err)
	}

	return &reader_v2.ResourceTypesReaderServiceGetResourceTypeResponse{
		ResourceType: resourceType,
	}, nil
}

// putResourceTypesInternal handles the internal logic for putting resource types.
func (pe *PebbleEngine) putResourceTypesInternal(ctx context.Context, ifNewer bool, resourceTypes ...*v2.ResourceType) error {
	if err := pe.validateOpen(); err != nil {
		return err
	}

	if len(resourceTypes) == 0 {
		return nil
	}

	pe.markDirty()

	syncID := pe.getCurrentSyncID()
	if syncID == "" {
		return fmt.Errorf("no active sync")
	}

	batch := pe.db.NewBatch()
	defer batch.Close()

	currentTime := pe.currentSyncTime()

	for _, resourceType := range resourceTypes {
		if resourceType == nil {
			continue
		}

		externalID := resourceType.GetId()
		if externalID == "" {
			return fmt.Errorf("resource type missing external ID")
		}

		key := pe.keyEncoder.EncodeResourceTypeKey(syncID, externalID)

		if ifNewer {
			shouldUpdate, err := pe.shouldUpdateEntityWithBatch(batch, key, currentTime)
			if err != nil {
				return fmt.Errorf("failed to check if resource type should be updated: %w", err)
			}
			if !shouldUpdate {
				continue
			}
		}

		value, err := pe.valueCodec.EncodeValue(resourceType, currentTime, "application/x-protobuf")
		if err != nil {
			return fmt.Errorf("failed to encode resource type %s: %w", externalID, err)
		}

		if err := batch.Set(key, value, pe.getSyncOptions()); err != nil {
			return fmt.Errorf("failed to set resource type %s in batch: %w", externalID, err)
		}
	}

	if err := batch.Commit(pe.getSyncOptions()); err != nil {
		return fmt.Errorf("failed to commit batch: %w", err)
	}

	return nil
}

// getAllResourceTypesForSync retrieves all resource types for a specific sync
func (pe *PebbleEngine) getAllResourceTypesForSync(ctx context.Context, syncID string) (map[string]*v2.ResourceType, error) {
	m := make(map[string]*v2.ResourceType)
	prefix := pe.keyEncoder.EncodePrefixKey(KeyTypeResourceType, syncID)
	upper := pe.keyEncoder.EncodePrefixKey(KeyTypeResourceType, syncID+"~")
	iter, err := pe.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: upper})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		var rt v2.ResourceType
		_, err := pe.valueCodec.DecodeValue(iter.Value(), &rt)
		if err != nil {
			return nil, fmt.Errorf("failed to decode resource type: %w", err)
		}
		m[rt.GetId()] = &rt
	}
	return m, iter.Error()
}

// copyResourceTypesToClone copies resource types from source to clone
func (pe *PebbleEngine) copyResourceTypesToClone(ctx context.Context, sourceSyncID string, cloneEngine *PebbleEngine) error {
	resourceTypes, err := pe.getAllResourceTypesForSync(ctx, sourceSyncID)
	if err != nil {
		return fmt.Errorf("failed to get resource types for sync %s: %w", sourceSyncID, err)
	}
	for _, rt := range resourceTypes {
		if err := cloneEngine.PutResourceTypesIfNewer(ctx, rt); err != nil {
			return fmt.Errorf("failed to copy resource type %s: %w", rt.GetId(), err)
		}
	}
	return nil
}

// generateDiffForResourceTypes generates diff data for resource types
func (pe *PebbleEngine) generateDiffForResourceTypes(ctx context.Context, baseSyncID, appliedSyncID string) error {
	applied, err := pe.getAllResourceTypesForSync(ctx, appliedSyncID)
	if err != nil {
		return fmt.Errorf("failed to get applied resource types: %w", err)
	}
	base, err := pe.getAllResourceTypesForSync(ctx, baseSyncID)
	if err != nil {
		return fmt.Errorf("failed to get base resource types: %w", err)
	}

	for id, art := range applied {
		if brt := base[id]; brt == nil || !pe.resourceTypesEqual(brt, art) {
			if err := pe.PutResourceTypesIfNewer(ctx, art); err != nil {
				return fmt.Errorf("failed to add resource type to diff: %w", err)
			}
		}
	}
	return nil
}

// resourceTypesEqual compares two resource types for equality
func (pe *PebbleEngine) resourceTypesEqual(a, b *v2.ResourceType) bool {
	return proto.Equal(a, b)
}