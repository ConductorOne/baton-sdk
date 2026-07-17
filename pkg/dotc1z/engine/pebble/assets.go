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
		if err := e.requireCurrentSync(); err != nil {
			return err
		}
		val, err := marshalRecord(r)
		if err != nil {
			return err
		}
		return e.db.MetaSet(encodeAssetKey(r.GetExternalId()), val, writeOpts(e.opts.durability))
	})
}

func (e *Engine) GetAssetRecord(ctx context.Context, externalID string) (*v3.AssetRecord, error) {
	val, closer, err := e.db.Get(encodeAssetKey(externalID))
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

func (e *Engine) DeleteAssetRecord(ctx context.Context, externalID string) error {
	return e.withWrite(func() error {
		return e.db.MetaDelete(encodeAssetKey(externalID), writeOpts(e.opts.durability))
	})
}

func (e *Engine) IterateAssets(ctx context.Context, yield func(*v3.AssetRecord) bool) error {
	prefix := encodeAssetPrefix()
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
