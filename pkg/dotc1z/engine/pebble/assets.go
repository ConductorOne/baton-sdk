package pebble

import (
	"context"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
)

// PutAssetRecord — assets have no secondary indexes.
func (e *Engine) PutAssetRecord(ctx context.Context, r *v3.AssetRecord) error {
	if r == nil {
		return errors.New("PutAssetRecord: nil record")
	}
	return e.withWrite(func() error {
		idBytes, err := e.resolveSyncBytes(r.GetSyncId())
		if err != nil {
			return err
		}
		val, err := marshalRecord(r)
		if err != nil {
			return err
		}
		return e.db.Set(encodeAssetKey(idBytes, r.GetExternalId()), val, writeOpts(e.opts.durability))
	})
}

func (e *Engine) GetAssetRecord(ctx context.Context, syncID, externalID string) (*v3.AssetRecord, error) {
	idBytes, err := e.resolveSyncBytes(syncID)
	if err != nil {
		return nil, err
	}
	val, closer, err := e.db.Get(encodeAssetKey(idBytes, externalID))
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	r := &v3.AssetRecord{}
	if err := unmarshalRecord(val, r); err != nil {
		return nil, fmt.Errorf("GetAssetRecord: unmarshal: %w", err)
	}
	return r, nil
}

func (e *Engine) DeleteAssetRecord(ctx context.Context, syncID, externalID string) error {
	return e.withWrite(func() error {
		idBytes, err := e.resolveSyncBytes(syncID)
		if err != nil {
			return err
		}
		return e.db.Delete(encodeAssetKey(idBytes, externalID), writeOpts(e.opts.durability))
	})
}

func (e *Engine) IterateAssetsBySync(ctx context.Context, syncID string, yield func(*v3.AssetRecord) bool) error {
	idBytes, err := e.resolveSyncBytes(syncID)
	if err != nil {
		return err
	}
	prefix := encodeAssetPrefix(idBytes)
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upperBoundOf(prefix),
	})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		r := &v3.AssetRecord{}
		if err := unmarshalRecord(iter.Value(), r); err != nil {
			return fmt.Errorf("iterate assets: %w", err)
		}
		if !yield(r) {
			return nil
		}
	}
	return iter.Error()
}
