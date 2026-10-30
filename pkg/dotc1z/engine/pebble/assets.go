package pebble

import (
	"context"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
)

// PutAssetRecord — assets have no secondary indexes.
func (r *Records) PutAssetRecord(ctx context.Context, rec *v3.AssetRecord) error {
	if rec == nil {
		return errors.New("PutAssetRecord: nil record")
	}
	return r.e.withWrite(func() error {
		if err := r.e.requireCurrentSync(); err != nil {
			return err
		}
		val, err := marshalRecord(rec)
		if err != nil {
			return err
		}
		return r.e.db.MetaSet(encodeAssetKey(rec.GetExternalId()), val, writeOpts(r.e.opts.durability))
	})
}

func (r *Records) GetAssetRecord(ctx context.Context, externalID string) (*v3.AssetRecord, error) {
	val, closer, err := r.e.db.Get(encodeAssetKey(externalID))
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	rec := &v3.AssetRecord{}
	if err := unmarshalRecord(val, rec); err != nil {
		return nil, fmt.Errorf("GetAssetRecord: unmarshal: %w", err)
	}
	return rec, nil
}

func (r *Records) DeleteAssetRecord(ctx context.Context, externalID string) error {
	return r.e.withWrite(func() error {
		return r.e.db.MetaDelete(encodeAssetKey(externalID), writeOpts(r.e.opts.durability))
	})
}

func (r *Records) IterateAssets(ctx context.Context, yield func(*v3.AssetRecord) bool) error {
	prefix := encodeAssetPrefix()
	iter, err := r.e.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upperBoundOf(prefix),
	})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		rec := &v3.AssetRecord{}
		if err := unmarshalRecord(iter.Value(), rec); err != nil {
			return fmt.Errorf("iterate assets: %w", err)
		}
		if !yield(rec) {
			return nil
		}
	}
	return iter.Error()
}
