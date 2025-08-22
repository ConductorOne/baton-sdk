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

var grantTracer = otel.Tracer("baton-sdk/pkg.dotc1z.engine.pebble.grants")

// PutGrants stores grants in the database.
func (pe *PebbleEngine) PutGrants(ctx context.Context, grants ...*v2.Grant) error {
	ctx, span := grantTracer.Start(ctx, "PebbleEngine.PutGrants")
	defer span.End()

	return pe.putGrantsInternal(ctx, false, grants...)
}

// PutGrantsIfNewer stores grants only if they are newer than existing ones.
func (pe *PebbleEngine) PutGrantsIfNewer(ctx context.Context, bulkGrants ...*v2.Grant) error {
	ctx, span := grantTracer.Start(ctx, "PebbleEngine.PutGrantsIfNewer")
	defer span.End()

	return pe.putGrantsInternal(ctx, true, bulkGrants...)
}

// DeleteGrant deletes a grant by ID.
func (pe *PebbleEngine) DeleteGrant(ctx context.Context, grantId string) error {
	ctx, span := grantTracer.Start(ctx, "PebbleEngine.DeleteGrant")
	defer span.End()

	if err := pe.validateOpen(); err != nil {
		return err
	}

	if grantId == "" {
		return fmt.Errorf("grant ID cannot be empty")
	}

	syncID := pe.getCurrentSyncID()
	if syncID == "" {
		return fmt.Errorf("no active sync")
	}

	// Get the current grant to find its associated data for index cleanup
	grantKey := pe.keyEncoder.EncodeGrantKey(syncID, grantId)
	grantValue, closer, err := pe.db.Get(grantKey)
	if err != nil {
		if err == pebble.ErrNotFound {
			return fmt.Errorf("grant not found: %s", grantId)
		}
		return fmt.Errorf("failed to get grant for deletion: %w", err)
	}
	defer closer.Close()

	// Decode the grant to clean up indexes
	grant := &v2.Grant{}
	_, err = pe.valueCodec.DecodeValue(grantValue, grant)
	if err != nil {
		return fmt.Errorf("failed to decode grant for deletion: %w", err)
	}

	// Create a batch for atomic deletion
	batch := pe.db.NewBatch()
	defer batch.Close()

	// Delete the main grant record
	err = batch.Delete(grantKey, nil)
	if err != nil {
		return fmt.Errorf("failed to delete grant from batch: %w", err)
	}

	// Clean up secondary indexes
	err = pe.indexManager.DeleteAllGrantIndexes(batch, syncID, grant)
	if err != nil {
		return fmt.Errorf("failed to delete grant indexes: %w", err)
	}

	// Commit the batch
	err = batch.Commit(pe.getSyncOptions())
	if err != nil {
		return fmt.Errorf("failed to commit grant deletion: %w", err)
	}

	pe.markDirty()
	return nil
}

// ListGrants lists grants with pagination and optional filtering.
func (pe *PebbleEngine) ListGrants(ctx context.Context, req *v2.GrantsServiceListGrantsRequest) (*v2.GrantsServiceListGrantsResponse, error) {
	ctx, span := grantTracer.Start(ctx, "PebbleEngine.ListGrants")
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

		// Use grants-by-resource index
		prefixKey = pe.keyEncoder.EncodeIndexPrefixKey(IndexGrantsByResource, syncID, resourceID.GetResourceType(), resourceID.GetResource())
		upperBound = pe.keyEncoder.EncodeIndexPrefixKey(IndexGrantsByResource, syncID, resourceID.GetResourceType(), resourceID.GetResource()+"~")
	} else {
		// List all grants
		prefixKey = pe.keyEncoder.EncodePrefixKey(KeyTypeGrant, syncID)
		upperBound = pe.keyEncoder.EncodePrefixKey(KeyTypeGrant, syncID+"~")
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

	var grants []*v2.Grant
	count := uint32(0)

	// Iterate through grants
	for iter.First(); iter.Valid() && count < pageSize; iter.Next() {
		key := iter.Key()
		value := iter.Value()

		var grant *v2.Grant
		var grantKey []byte

		// Check if we're using an index or direct keys
		if req.GetResource() != nil && req.GetResource().GetId() != nil {
			// We're using the index, need to decode the index key to get the grant ID
			decodedKey, err := pe.keyEncoder.DecodeKey(key)
			if err != nil {
				return nil, fmt.Errorf("failed to decode index key: %w", err)
			}

			// Index key format: v1|ix|gr_by_res|{sync_id}|{resource_type_id}|{resource_id}|{external_id}
			if len(decodedKey.Components) < 4 {
				return nil, fmt.Errorf("invalid index key format: expected at least 4 components, got %d", len(decodedKey.Components))
			}

			grantID := decodedKey.Components[3]
			grantKey = pe.keyEncoder.EncodeGrantKey(syncID, grantID)

			// Get the actual grant data
			grantValue, closer, err := pe.db.Get(grantKey)
			if err != nil {
				if err == pebble.ErrNotFound {
					continue
				}
				return nil, fmt.Errorf("failed to get grant from index: %w", err)
			}
			defer closer.Close()

			grant = &v2.Grant{}
			_, err = pe.valueCodec.DecodeValue(grantValue, grant)
			if err != nil {
				return nil, fmt.Errorf("failed to decode grant: %w", err)
			}
		} else {
			// Direct grant key, decode directly
			grant = &v2.Grant{}
			_, err = pe.valueCodec.DecodeValue(value, grant)
			if err != nil {
				return nil, fmt.Errorf("failed to decode grant: %w", err)
			}
		}

		grants = append(grants, grant)
		count++
	}

	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterator error: %w", err)
	}

	// Generate next page token if there are more results
	var nextPageToken string
	if iter.Valid() {
		nextKey := iter.Key()
		nextPageToken, err = pe.encodePageToken(nextKey)
		if err != nil {
			return nil, fmt.Errorf("failed to encode page token: %w", err)
		}
	}

	return &v2.GrantsServiceListGrantsResponse{
		List:          grants,
		NextPageToken: nextPageToken,
	}, nil
}

// ListGrantsForPrincipal lists grants for a specific principal.
func (pe *PebbleEngine) ListGrantsForPrincipal(ctx context.Context, req *reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest) (*reader_v2.GrantsReaderServiceListGrantsForEntitlementResponse, error) {
	ctx, span := grantTracer.Start(ctx, "PebbleEngine.ListGrantsForPrincipal")
	defer span.End()

	if err := pe.validateOpen(); err != nil {
		return nil, err
	}

	if req == nil || req.GetPrincipalId() == nil {
		return nil, fmt.Errorf("principal request or principal ID cannot be nil")
	}

	principalID := req.GetPrincipalId()
	if principalID.GetResourceType() == "" || principalID.GetResource() == "" {
		return nil, fmt.Errorf("principal resource type and resource ID cannot be empty")
	}

	// Get the sync ID to use for the query
	syncID, err := pe.getSyncIDForQuery(ctx, req.GetAnnotations())
	if err != nil {
		return nil, fmt.Errorf("failed to get sync ID: %w", err)
	}

	// Use grants-by-principal index
	prefixKey := pe.keyEncoder.EncodeIndexPrefixKey(IndexGrantsByPrincipal, syncID, principalID.GetResourceType(), principalID.GetResource())
	upperBound := pe.keyEncoder.EncodeIndexPrefixKey(IndexGrantsByPrincipal, syncID, principalID.GetResourceType(), principalID.GetResource()+"~")

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

	var grants []*v2.Grant
	count := uint32(0)

	// Iterate through grants using the index
	for iter.First(); iter.Valid() && count < pageSize; iter.Next() {
		key := iter.Key()

		// Decode the index key to get the grant ID
		decodedKey, err := pe.keyEncoder.DecodeKey(key)
		if err != nil {
			return nil, fmt.Errorf("failed to decode index key: %w", err)
		}

		if len(decodedKey.Components) < 4 {
			return nil, fmt.Errorf("invalid index key format: expected at least 4 components, got %d", len(decodedKey.Components))
		}

		grantID := decodedKey.Components[3]
		grantKey := pe.keyEncoder.EncodeGrantKey(syncID, grantID)

		// Get the actual grant data
		grantValue, closer, err := pe.db.Get(grantKey)
		if err != nil {
			if err == pebble.ErrNotFound {
				continue
			}
			return nil, fmt.Errorf("failed to get grant from index: %w", err)
		}
		defer closer.Close()

		// Decode the grant
		grant := &v2.Grant{}
		_, err = pe.valueCodec.DecodeValue(grantValue, grant)
		if err != nil {
			return nil, fmt.Errorf("failed to decode grant: %w", err)
		}

		grants = append(grants, grant)
		count++
	}

	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterator error: %w", err)
	}

	// Generate next page token if there are more results
	var nextPageToken string
	if iter.Valid() {
		nextKey := iter.Key()
		nextPageToken, err = pe.encodePageToken(nextKey)
		if err != nil {
			return nil, fmt.Errorf("failed to encode page token: %w", err)
		}
	}

	return &reader_v2.GrantsReaderServiceListGrantsForEntitlementResponse{
		List:          grants,
		NextPageToken: nextPageToken,
	}, nil
}

// ListGrantsForEntitlement lists grants for a specific entitlement.
func (pe *PebbleEngine) ListGrantsForEntitlement(ctx context.Context, req *reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest) (*reader_v2.GrantsReaderServiceListGrantsForEntitlementResponse, error) {
	ctx, span := grantTracer.Start(ctx, "PebbleEngine.ListGrantsForEntitlement")
	defer span.End()

	if err := pe.validateOpen(); err != nil {
		return nil, err
	}

	if req == nil || req.GetEntitlement() == nil {
		return nil, fmt.Errorf("entitlement request or entitlement cannot be nil")
	}

	entitlementID := req.GetEntitlement().GetId()
	if entitlementID == "" {
		return nil, fmt.Errorf("entitlement ID cannot be empty")
	}

	// Get the sync ID to use for the query
	syncID, err := pe.getSyncIDForQuery(ctx, req.GetAnnotations())
	if err != nil {
		return nil, fmt.Errorf("failed to get sync ID: %w", err)
	}

	// Check if we need to filter by principal
	var prefixKey []byte
	var upperBound []byte

	if req.GetPrincipalId() != nil {
		// Filter by both entitlement and principal using grants-by-entitlement index
		principalID := req.GetPrincipalId()
		if principalID.GetResourceType() == "" || principalID.GetResource() == "" {
			return nil, fmt.Errorf("principal resource type and resource ID are required for filtering")
		}

		// Use grants-by-entitlement index
		prefixKey = pe.keyEncoder.EncodeIndexPrefixKey(IndexGrantsByEntitlement, syncID, entitlementID, principalID.GetResourceType(), principalID.GetResource())
		upperBound = pe.keyEncoder.EncodeIndexPrefixKey(IndexGrantsByEntitlement, syncID, entitlementID, principalID.GetResourceType(), principalID.GetResource()+"~")
	} else {
		// Filter by entitlement only using grants-by-entitlement index
		prefixKey = pe.keyEncoder.EncodeIndexPrefixKey(IndexGrantsByEntitlement, syncID, entitlementID)
		upperBound = pe.keyEncoder.EncodeIndexPrefixKey(IndexGrantsByEntitlement, syncID, entitlementID+"~")
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

	var grants []*v2.Grant
	count := uint32(0)

	// Iterate through grants using the index
	for iter.First(); iter.Valid() && count < pageSize; iter.Next() {
		key := iter.Key()

		// Decode the index key to get the grant ID
		decodedKey, err := pe.keyEncoder.DecodeKey(key)
		if err != nil {
			return nil, fmt.Errorf("failed to decode index key: %w", err)
		}

		if len(decodedKey.Components) < 5 {
			return nil, fmt.Errorf("invalid index key format: expected at least 5 components, got %d", len(decodedKey.Components))
		}

		grantID := decodedKey.Components[4]
		grantKey := pe.keyEncoder.EncodeGrantKey(syncID, grantID)

		// Get the actual grant data
		grantValue, closer, err := pe.db.Get(grantKey)
		if err != nil {
			if err == pebble.ErrNotFound {
				continue
			}
			return nil, fmt.Errorf("failed to get grant from index: %w", err)
		}
		defer closer.Close()

		// Decode the grant
		grant := &v2.Grant{}
		_, err = pe.valueCodec.DecodeValue(grantValue, grant)
		if err != nil {
			return nil, fmt.Errorf("failed to decode grant: %w", err)
		}

		grants = append(grants, grant)
		count++
	}

	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterator error: %w", err)
	}

	// Generate next page token if there are more results
	var nextPageToken string
	if iter.Valid() {
		nextKey := iter.Key()
		nextPageToken, err = pe.encodePageToken(nextKey)
		if err != nil {
			return nil, fmt.Errorf("failed to encode page token: %w", err)
		}
	}

	return &reader_v2.GrantsReaderServiceListGrantsForEntitlementResponse{
		List:          grants,
		NextPageToken: nextPageToken,
	}, nil
}

// ListGrantsForResourceType lists grants for a specific resource type.
func (pe *PebbleEngine) ListGrantsForResourceType(ctx context.Context, req *reader_v2.GrantsReaderServiceListGrantsForResourceTypeRequest) (*reader_v2.GrantsReaderServiceListGrantsForResourceTypeResponse, error) {
	ctx, span := grantTracer.Start(ctx, "PebbleEngine.ListGrantsForResourceType")
	defer span.End()

	if err := pe.validateOpen(); err != nil {
		return nil, err
	}

	if req == nil || req.GetResourceTypeId() == "" {
		return nil, fmt.Errorf("resource type ID cannot be empty")
	}

	// Get the sync ID to use for the query
	syncID, err := pe.getSyncIDForQuery(ctx, req.GetAnnotations())
	if err != nil {
		return nil, fmt.Errorf("failed to get sync ID: %w", err)
	}

	// Use grants-by-resource index to find grants for this resource type
	prefixKey := pe.keyEncoder.EncodeIndexPrefixKey(IndexGrantsByResource, syncID, req.GetResourceTypeId())
	upperBound := pe.keyEncoder.EncodeIndexPrefixKey(IndexGrantsByResource, syncID, req.GetResourceTypeId()+"~")

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

	var grants []*v2.Grant
	count := uint32(0)

	// Iterate through grants using the index
	for iter.First(); iter.Valid() && count < pageSize; iter.Next() {
		key := iter.Key()

		// Decode the index key to get the grant ID
		decodedKey, err := pe.keyEncoder.DecodeKey(key)
		if err != nil {
			return nil, fmt.Errorf("failed to decode index key: %w", err)
		}

		if len(decodedKey.Components) < 4 {
			return nil, fmt.Errorf("invalid index key format: expected at least 4 components, got %d", len(decodedKey.Components))
		}

		grantID := decodedKey.Components[3]
		grantKey := pe.keyEncoder.EncodeGrantKey(syncID, grantID)

		// Get the actual grant data
		grantValue, closer, err := pe.db.Get(grantKey)
		if err != nil {
			if err == pebble.ErrNotFound {
				continue
			}
			return nil, fmt.Errorf("failed to get grant from index: %w", err)
		}
		defer closer.Close()

		// Decode the grant
		grant := &v2.Grant{}
		_, err = pe.valueCodec.DecodeValue(grantValue, grant)
		if err != nil {
			return nil, fmt.Errorf("failed to decode grant: %w", err)
		}

		grants = append(grants, grant)
		count++
	}

	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterator error: %w", err)
	}

	// Generate next page token if there are more results
	var nextPageToken string
	if iter.Valid() {
		nextKey := iter.Key()
		nextPageToken, err = pe.encodePageToken(nextKey)
		if err != nil {
			return nil, fmt.Errorf("failed to encode page token: %w", err)
		}
	}

	return &reader_v2.GrantsReaderServiceListGrantsForResourceTypeResponse{
		List:          grants,
		NextPageToken: nextPageToken,
	}, nil
}

// GetGrant retrieves a specific grant by ID.
func (pe *PebbleEngine) GetGrant(ctx context.Context, req *reader_v2.GrantsReaderServiceGetGrantRequest) (*reader_v2.GrantsReaderServiceGetGrantResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

// putGrantsInternal handles the internal logic for putting grants.
func (pe *PebbleEngine) putGrantsInternal(ctx context.Context, ifNewer bool, grants ...*v2.Grant) error {
	if err := pe.validateOpen(); err != nil {
		return err
	}

	if len(grants) == 0 {
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

	for _, grant := range grants {
		if grant == nil {
			continue
		}

		if grant.GetId() == "" {
			return fmt.Errorf("grant missing ID")
		}

		if grant.GetEntitlement() == nil {
			return fmt.Errorf("grant missing entitlement")
		}

		if grant.GetPrincipal() == nil {
			return fmt.Errorf("grant missing principal")
		}

		// Create the key for this grant
		key := pe.keyEncoder.EncodeGrantKey(syncID, grant.GetId())

		// Handle conditional upsert if ifNewer is true
		if ifNewer {
			shouldUpdate, err := pe.shouldUpdateEntity(key, currentTime)
			if err != nil {
				return fmt.Errorf("failed to check if grant should be updated: %w", err)
			}
			if !shouldUpdate {
				continue
			}
		}

		// Encode the grant with metadata envelope
		value, err := pe.valueCodec.EncodeValue(grant, currentTime, "")
		if err != nil {
			return fmt.Errorf("failed to encode grant: %w", err)
		}

		// Store the grant
		err = batch.Set(key, value, nil)
		if err != nil {
			return fmt.Errorf("failed to set grant in batch: %w", err)
		}

		// Create secondary indexes for this grant
		err = pe.indexManager.CreateAllGrantIndexes(batch, syncID, grant)
		if err != nil {
			return fmt.Errorf("failed to create grant indexes: %w", err)
		}
	}

	// Commit the batch
	err := batch.Commit(pe.getSyncOptions())
	if err != nil {
		return fmt.Errorf("failed to commit grants batch: %w", err)
	}

	pe.markDirty()
	return nil
}

// getAllGrantsForSync retrieves all grants for a specific sync
func (pe *PebbleEngine) getAllGrantsForSync(ctx context.Context, syncID string) (map[string]*v2.Grant, error) {
	m := make(map[string]*v2.Grant)
	prefix := pe.keyEncoder.EncodePrefixKey(KeyTypeGrant, syncID)
	upper := pe.keyEncoder.EncodePrefixKey(KeyTypeGrant, syncID+"~")
	iter, err := pe.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: upper})
	if err != nil {
		return nil, fmt.Errorf("failed to create iterator: %w", err)
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		var g v2.Grant
		_, err := pe.valueCodec.DecodeValue(iter.Value(), &g)
		if err != nil {
			continue
		}
		m[g.GetId()] = &g
	}
	return m, nil
}

// copyGrantsToClone copies grants from source to clone
func (pe *PebbleEngine) copyGrantsToClone(ctx context.Context, sourceSyncID string, cloneEngine *PebbleEngine) error {
	grants, err := pe.getAllGrantsForSync(ctx, sourceSyncID)
	if err != nil {
		return err
	}
	for _, grant := range grants {
		if err := cloneEngine.PutGrantsIfNewer(ctx, grant); err != nil {
			return fmt.Errorf("failed to copy grant %s: %w", grant.GetId(), err)
		}
	}
	return nil
}

// generateDiffForGrants generates diff data for grants
func (pe *PebbleEngine) generateDiffForGrants(ctx context.Context, baseSyncID, appliedSyncID string) error {
	applied, err := pe.getAllGrantsForSync(ctx, appliedSyncID)
	if err != nil {
		return err
	}
	base, err := pe.getAllGrantsForSync(ctx, baseSyncID)
	if err != nil {
		return err
	}
	for _, ag := range applied {
		bg := base[ag.GetId()]
		if bg == nil || !pe.grantsEqual(bg, ag) {
			if err := pe.PutGrantsIfNewer(ctx, ag); err != nil {
				return fmt.Errorf("failed to add grant to diff: %w", err)
			}
		}
	}
	return nil
}

// grantsEqual compares two grants for equality
func (pe *PebbleEngine) grantsEqual(a, b *v2.Grant) bool {
	return proto.Equal(a, b)
}