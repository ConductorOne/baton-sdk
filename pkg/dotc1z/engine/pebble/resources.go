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

var resourceTracer = otel.Tracer("baton-sdk/pkg.dotc1z.engine.pebble.resources")

// PutResources stores resources in the database.
func (pe *PebbleEngine) PutResources(ctx context.Context, resources ...*v2.Resource) error {
	ctx, span := resourceTracer.Start(ctx, "PebbleEngine.PutResources")
	defer span.End()

	return pe.putResourcesInternal(ctx, false, resources...)
}

// PutResourcesIfNewer stores resources only if they are newer than existing ones.
func (pe *PebbleEngine) PutResourcesIfNewer(ctx context.Context, resourceObjs ...*v2.Resource) error {
	ctx, span := resourceTracer.Start(ctx, "PebbleEngine.PutResourcesIfNewer")
	defer span.End()

	return pe.putResourcesInternal(ctx, true, resourceObjs...)
}

// ListResources lists resources with pagination.
func (pe *PebbleEngine) ListResources(ctx context.Context, req *v2.ResourcesServiceListResourcesRequest) (*v2.ResourcesServiceListResourcesResponse, error) {
	ctx, span := resourceTracer.Start(ctx, "PebbleEngine.ListResources")
	defer span.End()

	if err := pe.validateOpen(); err != nil {
		return nil, err
	}

	// Get the sync ID to use for the query
	syncID, err := pe.getSyncIDForQuery(ctx, req.GetAnnotations())
	if err != nil {
		return nil, fmt.Errorf("failed to get sync ID: %w", err)
	}

	// Determine the key prefix based on whether we're filtering by resource type
	var prefixKey []byte
	if req.GetResourceTypeId() != "" {
		// Filter by resource type
		prefixKey = pe.keyEncoder.EncodeResourcePrefixKey(syncID, req.GetResourceTypeId())
	} else {
		// List all resources
		prefixKey = pe.keyEncoder.EncodePrefixKey(KeyTypeResource, syncID)
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

	// Create upper bound for the range scan
	var upperBound []byte
	if req.GetResourceTypeId() != "" {
		// Use resource type specific upper bound
		upperBound = pe.keyEncoder.EncodeResourcePrefixKey(syncID, req.GetResourceTypeId()+"~")
	} else {
		// Use general resource upper bound
		upperBound = pe.keyEncoder.EncodePrefixKey(KeyTypeResource, syncID+"~")
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

	var resources []*v2.Resource
	var lastKey []byte
	count := uint32(0)

	// Iterate through resources
	for iter.First(); iter.Valid() && count < pageSize; iter.Next() {
		key := iter.Key()
		value := iter.Value()

		// Decode the resource
		resource := &v2.Resource{}
		_, err := pe.valueCodec.DecodeValue(value, resource)
		if err != nil {
			return nil, fmt.Errorf("failed to decode resource: %w", err)
		}

		resources = append(resources, resource)
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
		nextPageToken, err = pe.encodePageToken(nextKey)
		if err != nil {
			return nil, fmt.Errorf("failed to encode page token: %w", err)
		}
	}

	return &v2.ResourcesServiceListResourcesResponse{
		List:          resources,
		NextPageToken: nextPageToken,
	}, nil
}

// GetResource retrieves a specific resource by ID.
func (pe *PebbleEngine) GetResource(ctx context.Context, req *reader_v2.ResourcesReaderServiceGetResourceRequest) (*reader_v2.ResourcesReaderServiceGetResourceResponse, error) {
	ctx, span := resourceTracer.Start(ctx, "PebbleEngine.GetResource")
	defer span.End()

	if err := pe.validateOpen(); err != nil {
		return nil, err
	}

	if req == nil || req.GetResourceId() == nil {
		return nil, fmt.Errorf("resource request or resource ID cannot be nil")
	}

	resourceID := req.GetResourceId()
	if resourceID.GetResourceType() == "" || resourceID.GetResource() == "" {
		return nil, fmt.Errorf("resource type and resource ID are required")
	}

	// Get the sync ID to use for the query
	syncID, err := pe.getSyncIDForQuery(ctx, req.GetAnnotations())
	if err != nil {
		return nil, fmt.Errorf("failed to get sync ID: %w", err)
	}

	// Create the key for this resource
	key := pe.keyEncoder.EncodeResourceKey(syncID, resourceID.GetResourceType(), resourceID.GetResource())

	// Get the value from the database
	value, closer, err := pe.db.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, fmt.Errorf("resource not found: %s/%s", resourceID.GetResourceType(), resourceID.GetResource())
		}
		return nil, fmt.Errorf("failed to get resource: %w", err)
	}
	defer closer.Close()

	// Decode the resource
	resource := &v2.Resource{}
	_, err = pe.valueCodec.DecodeValue(value, resource)
	if err != nil {
		return nil, fmt.Errorf("failed to decode resource: %w", err)
	}

	return &reader_v2.ResourcesReaderServiceGetResourceResponse{
		Resource: resource,
	}, nil
}

// putResourcesInternal handles the internal logic for putting resources.
func (pe *PebbleEngine) putResourcesInternal(ctx context.Context, ifNewer bool, resources ...*v2.Resource) error {
	if err := pe.validateOpen(); err != nil {
		return err
	}

	if len(resources) == 0 {
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

	for _, resource := range resources {
		if resource == nil {
			continue
		}

		// Validate resource structure
		if resource.GetId() == nil {
			return fmt.Errorf("resource missing ID")
		}

		resourceID := resource.GetId()
		if resourceID.GetResourceType() == "" || resourceID.GetResource() == "" {
			return fmt.Errorf("resource missing resource type or resource ID")
		}

		key := pe.keyEncoder.EncodeResourceKey(syncID, resourceID.GetResourceType(), resourceID.GetResource())

		// Handle conditional upsert if ifNewer is true
		if ifNewer {
			shouldUpdate, err := pe.shouldUpdateEntity(key, currentTime)
			if err != nil {
				return fmt.Errorf("failed to check if resource should be updated: %w", err)
			}
			if !shouldUpdate {
				continue
			}
		}

		// Encode the value with metadata
		value, err := pe.valueCodec.EncodeValue(resource, currentTime, "")
		if err != nil {
			return fmt.Errorf("failed to encode resource: %w", err)
		}

		// Add to batch
		err = batch.Set(key, value, nil)
		if err != nil {
			return fmt.Errorf("failed to add resource to batch: %w", err)
		}
	}

	// Commit the batch
	err := batch.Commit(pe.getSyncOptions())
	if err != nil {
		return fmt.Errorf("failed to commit resources batch: %w", err)
	}

	pe.markDirty()
	return nil
}

// getAllResourcesForSync retrieves all resources for a specific sync
func (pe *PebbleEngine) getAllResourcesForSync(ctx context.Context, syncID string) (map[string]*v2.Resource, error) {
	m := make(map[string]*v2.Resource)
	prefix := pe.keyEncoder.EncodePrefixKey(KeyTypeResource, syncID)
	upper := pe.keyEncoder.EncodePrefixKey(KeyTypeResource, syncID+"~")
	iter, err := pe.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: upper})
	if err != nil {
		return nil, fmt.Errorf("failed to create iterator: %w", err)
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		var r v2.Resource
		_, err := pe.valueCodec.DecodeValue(iter.Value(), &r)
		if err != nil {
			continue
		}
		if r.GetId() != nil {
			k := fmt.Sprintf("%s:%s", r.GetId().GetResourceType(), r.GetId().GetResource())
			m[k] = &r
		}
	}
	return m, nil
}

// copyResourcesToClone copies resources from source to clone
func (pe *PebbleEngine) copyResourcesToClone(ctx context.Context, sourceSyncID string, cloneEngine *PebbleEngine) error {
	resources, err := pe.getAllResourcesForSync(ctx, sourceSyncID)
	if err != nil {
		return err
	}
	for _, res := range resources {
		if err := cloneEngine.PutResourcesIfNewer(ctx, res); err != nil {
			id := ""
			if res.GetId() != nil {
				id = fmt.Sprintf("%s/%s", res.GetId().GetResourceType(), res.GetId().GetResource())
			}
			return fmt.Errorf("failed to copy resource %s: %w", id, err)
		}
	}
	return nil
}

// generateDiffForResources generates diff data for resources
func (pe *PebbleEngine) generateDiffForResources(ctx context.Context, baseSyncID, appliedSyncID string) error {
	applied, err := pe.getAllResourcesForSync(ctx, appliedSyncID)
	if err != nil {
		return err
	}
	base, err := pe.getAllResourcesForSync(ctx, baseSyncID)
	if err != nil {
		return err
	}
	for _, ar := range applied {
		key := ""
		if ar.GetId() != nil {
			key = fmt.Sprintf("%s:%s", ar.GetId().GetResourceType(), ar.GetId().GetResource())
		}
		br := base[key]
		if br == nil || !pe.resourcesEqual(br, ar) {
			if err := pe.PutResourcesIfNewer(ctx, ar); err != nil {
				return fmt.Errorf("failed to add resource to diff: %w", err)
			}
		}
	}
	return nil
}

// resourcesEqual compares two resources for equality
func (pe *PebbleEngine) resourcesEqual(a, b *v2.Resource) bool {
	return proto.Equal(a, b)
}