package pebble

import (
	"context"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/internal/rawdb"
)

// PutResourceTypeRecord writes a resource_type record. No secondary
// indexes, so this is a single-key write.
func (r *Records) PutResourceTypeRecord(ctx context.Context, rec *v3.ResourceTypeRecord) error {
	if rec == nil {
		return errors.New("PutResourceTypeRecord: nil record")
	}
	return r.PutResourceTypeRecords(ctx, rec)
}

// PutResourceTypeRecords writes N resource_types in one batch.
func (r *Records) PutResourceTypeRecords(ctx context.Context, records ...*v3.ResourceTypeRecord) error {
	if len(records) == 0 {
		return nil
	}
	return r.e.withWrite(func() error {
		if err := r.e.requireCurrentSync(); err != nil {
			return err
		}
		batch := r.e.db.NewRecordBatch()
		defer batch.Close()
		if _, err := stageResourceTypeRecords(batch, records); err != nil {
			return err
		}
		opts := recordWriteOpts
		return batch.Commit(opts)
	})
}

func stageResourceTypeRecords(batch *rawdb.RecordBatch, records []*v3.ResourceTypeRecord) (uint64, error) {
	var last map[string]int
	if len(records) > 1 {
		last = make(map[string]int, len(records))
		for i, r := range records {
			if r != nil {
				last[r.GetExternalId()] = i
			}
		}
	}
	var staged uint64
	for i, r := range records {
		if r == nil {
			continue
		}
		if last != nil && last[r.GetExternalId()] != i {
			continue
		}
		key := encodeResourceTypeKey(r.GetExternalId())
		val, err := marshalRecord(r)
		if err != nil {
			return 0, err
		}
		if err := batch.StageResourceTypePut(key, val); err != nil {
			return 0, err
		}
		staged++
	}
	return staged, nil
}

func (r *Records) GetResourceTypeRecord(ctx context.Context, externalID string) (*v3.ResourceTypeRecord, error) {
	key := encodeResourceTypeKey(externalID)
	val, closer, err := r.e.db.Get(key)
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	rec := &v3.ResourceTypeRecord{}
	if err := unmarshalRecord(val, rec); err != nil {
		return nil, fmt.Errorf("GetResourceTypeRecord: unmarshal: %w", err)
	}
	return rec, nil
}

func (r *Records) DeleteResourceTypeRecord(ctx context.Context, externalID string) error {
	return r.e.withWrite(func() error {
		// Record family, not engine-meta: resource types are record
		// rows. A one-op RecordBatch commit is durability-identical to
		// the old single Delete (the commit carries the write options).
		batch := r.e.db.NewRecordBatch()
		defer batch.Close()
		if err := batch.StageResourceTypeDelete(encodeResourceTypeKey(externalID)); err != nil {
			return err
		}
		return batch.Commit(writeOpts(r.e.opts.durability))
	})
}

func (r *Records) IterateResourceTypes(ctx context.Context, yield func(*v3.ResourceTypeRecord) bool) error {
	prefix := encodeResourceTypePrefix()
	iter, err := r.e.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upperBoundOf(prefix),
	})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		rec := &v3.ResourceTypeRecord{}
		if err := unmarshalRecord(iter.Value(), rec); err != nil {
			return fmt.Errorf("iterate resource_types: %w", err)
		}
		if !yield(rec) {
			return nil
		}
	}
	return iter.Error()
}
