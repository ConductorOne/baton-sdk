package pebble

import (
	"context"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
)

// PutResourceTypeRecord writes a resource_type record. No secondary
// indexes, so this is a single-key write.
func (e *Engine) PutResourceTypeRecord(ctx context.Context, r *v3.ResourceTypeRecord) error {
	if r == nil {
		return errors.New("PutResourceTypeRecord: nil record")
	}
	return e.PutResourceTypeRecords(ctx, r)
}

// PutResourceTypeRecords writes N resource_types in one batch.
// Fresh-sync uses pebble.NoSync (one fsync at EndFreshSync).
func (e *Engine) PutResourceTypeRecords(ctx context.Context, records ...*v3.ResourceTypeRecord) error {
	if len(records) == 0 {
		return nil
	}
	return e.withWrite(func() error {
		if err := e.requireCurrentSync(); err != nil {
			return err
		}
		batch := e.db.NewBatch()
		defer batch.Close()
		fresh := e.IsFreshSync()
		for _, r := range records {
			if r == nil {
				continue
			}
			key := encodeResourceTypeKey(r.GetExternalId())
			val, err := marshalRecord(r)
			if err != nil {
				return err
			}
			if err := batch.Set(key, val, nil); err != nil {
				return err
			}
		}
		opts := writeOpts(e.opts.durability)
		if fresh {
			opts = pebble.NoSync
		}
		return batch.Commit(opts)
	})
}

func (e *Engine) GetResourceTypeRecord(ctx context.Context, externalID string) (*v3.ResourceTypeRecord, error) {
	key := encodeResourceTypeKey(externalID)
	val, closer, err := e.db.Get(key)
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	r := &v3.ResourceTypeRecord{}
	if err := unmarshalRecord(val, r); err != nil {
		return nil, fmt.Errorf("GetResourceTypeRecord: unmarshal: %w", err)
	}
	return r, nil
}

func (e *Engine) DeleteResourceTypeRecord(ctx context.Context, externalID string) error {
	return e.withWrite(func() error {
		return e.db.Delete(encodeResourceTypeKey(externalID), writeOpts(e.opts.durability))
	})
}

func (e *Engine) IterateResourceTypes(ctx context.Context, yield func(*v3.ResourceTypeRecord) bool) error {
	prefix := encodeResourceTypePrefix()
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upperBoundOf(prefix),
	})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		r := &v3.ResourceTypeRecord{}
		if err := unmarshalRecord(iter.Value(), r); err != nil {
			return fmt.Errorf("iterate resource_types: %w", err)
		}
		if !yield(r) {
			return nil
		}
	}
	return iter.Error()
}
