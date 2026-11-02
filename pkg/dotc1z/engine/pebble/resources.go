package pebble

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/cockroachdb/pebble/v2"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/internal/rawdb"
)

// PutResourceRecord writes a resource record + its by_parent index
// entry (if the resource has a parent). Read-before-write index
// cleanup follows the canonical pattern from grants.go.
func (r *Records) PutResourceRecord(ctx context.Context, rec *v3.ResourceRecord) error {
	if rec == nil {
		return errors.New("PutResourceRecord: nil record")
	}
	return r.PutResourceRecords(ctx, rec)
}

// PutResourceRecords writes N resources through a single RecordBatch —
// StageResourcePut derives the by_parent index obligations inside
// rawdb, so primary and index land in one atomic commit. Mirrors
// the PutGrantRecords pattern (RFC §3a Tier-B/C):
//   - within-call dedup pre-pass keyed by (rt, res_id) drops earlier
//     occurrences of repeated resources;
//   - the first PutResourceRecords call of a fresh sync skips the
//     read-before-write Get (keyspace provably empty);
//   - subsequent calls fall back to read-before-write so cross-call
//     duplicates can clean up index entries the prior call wrote.
func (r *Records) PutResourceRecords(ctx context.Context, records ...*v3.ResourceRecord) error {
	if len(records) == 0 {
		return nil
	}
	return r.e.withWrite(func() error {
		if err := r.e.requireCurrentSync(); err != nil {
			return err
		}
		batch := r.e.db.NewRecordBatch()
		defer batch.Close()

		if _, err := r.stageResourceRecords(batch, records); err != nil {
			return err
		}
		opts := recordWriteOpts
		// One atomic commit: rows and their index obligations ride the
		// same batch, so a primary commit landing without its index
		// entries is unexpressible.
		return batch.Commit(opts)
	})
}

// Caller holds the write barrier and commits the batch.
func (r *Records) stageResourceRecords(batch *rawdb.RecordBatch, records []*v3.ResourceRecord) (uint64, error) {
	if len(records) == 0 {
		return 0, nil
	}
	skipGet := r.e.takeFreshResourcesEmpty()

	type dedupKey struct {
		rtID, resID string
	}
	var dedup map[dedupKey]int
	if len(records) > 1 {
		dedup = make(map[dedupKey]int, len(records))
		for i, rec := range records {
			if rec == nil {
				continue
			}
			dedup[dedupKey{rec.GetResourceTypeId(), rec.GetResourceId()}] = i
		}
	}

	var staged uint64
	for i, rec := range records {
		if rec == nil {
			continue
		}
		if dedup != nil {
			if dedup[dedupKey{rec.GetResourceTypeId(), rec.GetResourceId()}] != i {
				continue
			}
		}
		staged++
		key := encodeResourceKey(rec.GetResourceTypeId(), rec.GetResourceId())
		val, err := marshalRecord(rec)
		if err != nil {
			return 0, err
		}
		var oldVal []byte
		var oldCloser io.Closer
		if !skipGet {
			got, closer, getErr := r.e.db.Get(key)
			switch {
			case getErr == nil:
				// The prior VALUE is genuinely needed here (unlike
				// grants): the by_parent cleanup key depends on the old
				// parent ref, which only the old bytes carry.
				oldVal, oldCloser = got, closer
			case errors.Is(getErr, pebble.ErrNotFound):
				// no prior — write unconditionally
			default:
				return 0, fmt.Errorf("PutResourceRecords: get old: %w", getErr)
			}
		}
		// One typed op stages the row and its by_parent obligations
		// (old entry cleanup from oldVal, new entry from val).
		err = batch.StageResourcePut(key, val, oldVal, rec.GetResourceTypeId(), rec.GetResourceId())
		if oldCloser != nil {
			_ = oldCloser.Close()
		}
		if err != nil {
			return 0, err
		}
	}
	return staged, nil
}

func (r *Records) GetResourceRecord(ctx context.Context, resourceTypeID, resourceID string) (*v3.ResourceRecord, error) {
	key := encodeResourceKey(resourceTypeID, resourceID)
	val, closer, err := r.e.db.Get(key)
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	rec := &v3.ResourceRecord{}
	if err := unmarshalRecord(val, rec); err != nil {
		return nil, fmt.Errorf("GetResourceRecord: unmarshal: %w", err)
	}
	return rec, nil
}

func (r *Records) DeleteResourceRecord(ctx context.Context, resourceTypeID, resourceID string) error {
	return r.e.withWrite(func() error {
		key := encodeResourceKey(resourceTypeID, resourceID)

		batch := r.e.db.NewRecordBatch()
		defer batch.Close()

		oldVal, closer, err := r.e.db.Get(key)
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				return nil
			}
			return err
		}
		// The typed op stages the row's removal plus its by_parent
		// cleanup derived from the prior value.
		stageErr := batch.StageResourceDelete(key, oldVal, resourceTypeID, resourceID)
		closer.Close()
		if stageErr != nil {
			return stageErr
		}
		return batch.Commit(writeOpts(r.e.opts.durability))
	})
}

func (r *Records) IterateResources(ctx context.Context, yield func(*v3.ResourceRecord) bool) error {
	prefix := encodeResourcePrefix()
	iter, err := r.e.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upperBoundOf(prefix),
	})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		rec := &v3.ResourceRecord{}
		if err := unmarshalRecord(iter.Value(), rec); err != nil {
			return fmt.Errorf("iterate resources: %w", err)
		}
		if !yield(rec) {
			return nil
		}
	}
	return iter.Error()
}

func (r *Records) IterateResourcesByParent(ctx context.Context, parentRT, parentID string, yield func(*v3.ResourceRecord) bool) error {
	indexPrefix := encodeResourceByParentPrefix(parentRT, parentID)
	iter, err := r.e.db.NewIter(&pebble.IterOptions{
		LowerBound: indexPrefix,
		UpperBound: upperBoundOf(indexPrefix),
	})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		// Decode (childRT, childID) from the tail.
		childRT, childID, ok := decodeTwoTupleComponents(iter.Key(), indexPrefix)
		if !ok {
			continue
		}
		val, closer, err := r.e.db.Get(encodeResourceKey(childRT, childID))
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				continue
			}
			return err
		}
		rec := &v3.ResourceRecord{}
		err = unmarshalRecord(val, rec)
		closer.Close()
		if err != nil {
			return err
		}
		if !yield(rec) {
			return nil
		}
	}
	return iter.Error()
}
