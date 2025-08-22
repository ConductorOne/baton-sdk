package pebble

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/cockroachdb/pebble"
	"go.opentelemetry.io/otel"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

var assetTracer = otel.Tracer("baton-sdk/pkg.dotc1z.engine.pebble.assets")

// PutAsset stores an asset in the database.
func (pe *PebbleEngine) PutAsset(ctx context.Context, assetRef *v2.AssetRef, contentType string, data []byte) error {
	ctx, span := assetTracer.Start(ctx, "PebbleEngine.PutAsset")
	defer span.End()

	if err := pe.validateOpen(); err != nil {
		return err
	}

	if assetRef == nil {
		return fmt.Errorf("asset reference cannot be nil")
	}

	externalID := assetRef.GetId()
	if externalID == "" {
		return fmt.Errorf("asset reference missing ID")
	}

	if contentType == "" {
		return fmt.Errorf("content type cannot be empty")
	}

	// Check asset size limits (default 100MB)
	const maxAssetSize = 100 * 1024 * 1024 // 100MB
	if len(data) > maxAssetSize {
		return fmt.Errorf("asset size %d bytes exceeds maximum allowed size %d bytes", len(data), maxAssetSize)
	}

	// Get current sync ID
	syncID := pe.getCurrentSyncID()
	if syncID == "" {
		return fmt.Errorf("no active sync")
	}

	// Create the key for this asset
	key := pe.keyEncoder.EncodeAssetKey(syncID, externalID)

	// Encode the asset data with metadata envelope
	currentTime := time.Now()
	value, err := pe.valueCodec.EncodeAsset(data, currentTime, contentType)
	if err != nil {
		return fmt.Errorf("failed to encode asset: %w", err)
	}

	// Store the asset
	err = pe.db.Set(key, value, pe.getSyncOptions())
	if err != nil {
		return fmt.Errorf("failed to store asset: %w", err)
	}

	pe.markDirty()
	return nil
}

// GetAsset retrieves an asset from the database.
func (pe *PebbleEngine) GetAsset(ctx context.Context, req *v2.AssetServiceGetAssetRequest) (string, io.Reader, error) {
	ctx, span := assetTracer.Start(ctx, "PebbleEngine.GetAsset")
	defer span.End()

	if err := pe.validateOpen(); err != nil {
		return "", nil, err
	}

	if req == nil || req.GetAsset() == nil {
		return "", nil, fmt.Errorf("asset request or asset reference cannot be nil")
	}

	externalID := req.GetAsset().GetId()
	if externalID == "" {
		return "", nil, fmt.Errorf("asset reference missing ID")
	}

	// Get the sync ID to use for the query
	// Assets don't have annotations, so we use the current sync ID
	syncID := pe.getCurrentSyncID()
	if syncID == "" {
		// If no current sync, try to get the view sync ID
		syncID = pe.getViewSyncID()
		if syncID == "" {
			// Fall back to latest finished sync
			fullSyncs, _, err := pe.getAllFinishedSyncs(ctx)
			if err != nil {
				return "", nil, fmt.Errorf("failed to get finished syncs for asset query: %w", err)
			}
			
			if len(fullSyncs) == 0 {
				return "", nil, fmt.Errorf("no sync ID available for asset query")
			}
			
			// Use the most recent full sync
			syncID = fullSyncs[len(fullSyncs)-1].ID
		}
	}

	// Create the key for this asset
	key := pe.keyEncoder.EncodeAssetKey(syncID, externalID)

	// Get the value from the database
	value, closer, err := pe.db.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return "", nil, fmt.Errorf("asset not found: %s", externalID)
		}
		return "", nil, fmt.Errorf("failed to get asset: %w", err)
	}
	defer closer.Close()

	// Decode the asset data and metadata
	envelope, assetData, err := pe.valueCodec.DecodeAsset(value)
	if err != nil {
		return "", nil, fmt.Errorf("failed to decode asset: %w", err)
	}

	// Create a reader for the asset data
	reader := bytes.NewReader(assetData)

	return envelope.ContentType, reader, nil
}

// copyAssetsToClone copies assets from source to clone
func (pe *PebbleEngine) copyAssetsToClone(ctx context.Context, sourceSyncID string, cloneEngine *PebbleEngine) error {
	prefix := pe.keyEncoder.EncodePrefixKey(KeyTypeAsset, sourceSyncID)
	upper := pe.keyEncoder.EncodePrefixKey(KeyTypeAsset, sourceSyncID+"~")

	iter, err := pe.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: upper})
	if err != nil {
		return fmt.Errorf("failed to create iterator for assets: %w", err)
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		decodedKey, err := pe.keyEncoder.DecodeKey(key)
		if err != nil {
			return fmt.Errorf("failed to decode asset key: %w", err)
		}
		externalID := decodedKey.GetExternalID()

		value := iter.Value()
		env, data, err := pe.valueCodec.DecodeAsset(value)
		if err != nil {
			return fmt.Errorf("failed to decode asset: %w", err)
		}

		if err := cloneEngine.PutAsset(ctx, &v2.AssetRef{Id: externalID}, env.ContentType, data); err != nil {
			return fmt.Errorf("failed to put asset %s: %w", externalID, err)
		}
	}
	return nil
}

// generateDiffForAssets generates diff data for assets
func (pe *PebbleEngine) generateDiffForAssets(ctx context.Context, baseSyncID, appliedSyncID string) error {
	// For each asset in applied, include if not present in base or data differs
	appliedPrefix := pe.keyEncoder.EncodePrefixKey(KeyTypeAsset, appliedSyncID)
	appliedUpper := pe.keyEncoder.EncodePrefixKey(KeyTypeAsset, appliedSyncID+"~")
	basePrefix := pe.keyEncoder.EncodePrefixKey(KeyTypeAsset, baseSyncID)
	baseUpper := pe.keyEncoder.EncodePrefixKey(KeyTypeAsset, baseSyncID+"~")

	// Build a map of base asset id -> data
	baseAssets := make(map[string][]byte)
	biter, err := pe.db.NewIter(&pebble.IterOptions{LowerBound: basePrefix, UpperBound: baseUpper})
	if err != nil {
		return fmt.Errorf("failed to create iterator for base assets: %w", err)
	}
	for biter.First(); biter.Valid(); biter.Next() {
		key := biter.Key()
		decoded, err := pe.keyEncoder.DecodeKey(key)
		if err != nil {
			biter.Close()
			return fmt.Errorf("failed to decode base asset key: %w", err)
		}
		_, data, err := pe.valueCodec.DecodeAsset(biter.Value())
		if err != nil {
			biter.Close()
			return fmt.Errorf("failed to decode base asset: %w", err)
		}
		baseAssets[decoded.GetExternalID()] = data
	}
	biter.Close()

	// Iterate applied and write differences
	aiter, err := pe.db.NewIter(&pebble.IterOptions{LowerBound: appliedPrefix, UpperBound: appliedUpper})
	if err != nil {
		return fmt.Errorf("failed to create iterator for applied assets: %w", err)
	}
	defer aiter.Close()
	for aiter.First(); aiter.Valid(); aiter.Next() {
		key := aiter.Key()
		decoded, err := pe.keyEncoder.DecodeKey(key)
		if err != nil {
			return fmt.Errorf("failed to decode applied asset key: %w", err)
		}
		externalID := decoded.GetExternalID()
		env, data, err := pe.valueCodec.DecodeAsset(aiter.Value())
		if err != nil {
			return fmt.Errorf("failed to decode applied asset: %w", err)
		}
		if baseData, ok := baseAssets[externalID]; !ok || !bytes.Equal(baseData, data) {
			if err := pe.PutAsset(ctx, &v2.AssetRef{Id: externalID}, env.ContentType, data); err != nil {
				return fmt.Errorf("failed to add asset to diff: %w", err)
			}
		}
	}
	return nil
}