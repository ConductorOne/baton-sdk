package pebble

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/cockroachdb/pebble/v2"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
)

// PutResourceRecord writes a resource record + its by_parent index
// entry (if the resource has a parent). Read-before-write index
// cleanup follows the canonical pattern from grants.go.
func (e *Engine) PutResourceRecord(ctx context.Context, r *v3.ResourceRecord) error {
	if r == nil {
		return errors.New("PutResourceRecord: nil record")
	}
	return e.PutResourceRecords(ctx, r)
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
func (e *Engine) PutResourceRecords(ctx context.Context, records ...*v3.ResourceRecord) error {
	if len(records) == 0 {
		return nil
	}
	return e.withWrite(func() error {
		if err := e.requireCurrentSync(); err != nil {
			return err
		}
		batch := e.db.NewRecordBatch()
		defer batch.Close()

		fresh := e.IsFreshSync()
		skipGet := e.takeFreshResourcesEmpty()

		type dedupKey struct {
			rtID, resID string
		}
		var dedup map[dedupKey]int
		if len(records) > 1 {
			dedup = make(map[dedupKey]int, len(records))
			for i, r := range records {
				if r == nil {
					continue
				}
				dedup[dedupKey{r.GetResourceTypeId(), r.GetResourceId()}] = i
			}
		}

		for i, r := range records {
			if r == nil {
				continue
			}
			if dedup != nil {
				if dedup[dedupKey{r.GetResourceTypeId(), r.GetResourceId()}] != i {
					continue
				}
			}
			key := encodeResourceKey(r.GetResourceTypeId(), r.GetResourceId())
			val, err := marshalRecord(r)
			if err != nil {
				return err
			}
			var oldVal []byte
			var oldCloser io.Closer
			if !skipGet {
				got, closer, getErr := e.db.Get(key)
				switch {
				case getErr == nil:
					// The prior VALUE is genuinely needed here (unlike
					// grants): the by_parent cleanup key depends on the old
					// parent ref, which only the old bytes carry.
					oldVal, oldCloser = got, closer
				case errors.Is(getErr, pebble.ErrNotFound):
					// no prior — write unconditionally
				default:
					return fmt.Errorf("PutResourceRecords: get old: %w", getErr)
				}
			}
			// One typed op stages the row and its by_parent obligations
			// (old entry cleanup from oldVal, new entry from val).
			err = batch.StageResourcePut(key, val, oldVal, r.GetResourceTypeId(), r.GetResourceId())
			if oldCloser != nil {
				_ = oldCloser.Close()
			}
			if err != nil {
				return err
			}
		}
		opts := writeOpts(e.opts.durability)
		if fresh {
			opts = pebble.NoSync
		}
		// One atomic commit: rows and their index obligations ride the
		// same batch, so a primary commit landing without its index
		// entries is unexpressible.
		return batch.Commit(opts)
	})
}

func (e *Engine) GetResourceRecord(ctx context.Context, resourceTypeID, resourceID string) (*v3.ResourceRecord, error) {
	key := encodeResourceKey(resourceTypeID, resourceID)
	val, closer, err := e.db.Get(key)
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	r := &v3.ResourceRecord{}
	if err := unmarshalRecord(val, r); err != nil {
		return nil, fmt.Errorf("GetResourceRecord: unmarshal: %w", err)
	}
	return r, nil
}

func (e *Engine) DeleteResourceRecord(ctx context.Context, resourceTypeID, resourceID string) error {
	return e.withWrite(func() error {
		key := encodeResourceKey(resourceTypeID, resourceID)

		batch := e.db.NewRecordBatch()
		defer batch.Close()

		oldVal, closer, err := e.db.Get(key)
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
		return batch.Commit(writeOpts(e.opts.durability))
	})
}

func (e *Engine) IterateResources(ctx context.Context, yield func(*v3.ResourceRecord) bool) error {
	prefix := encodeResourcePrefix()
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upperBoundOf(prefix),
	})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		r := &v3.ResourceRecord{}
		if err := unmarshalRecord(iter.Value(), r); err != nil {
			return fmt.Errorf("iterate resources: %w", err)
		}
		if !yield(r) {
			return nil
		}
	}
	return iter.Error()
}

func (e *Engine) IterateResourcesByParent(ctx context.Context, parentRT, parentID string, yield func(*v3.ResourceRecord) bool) error {
	indexPrefix := encodeResourceByParentPrefix(parentRT, parentID)
	iter, err := e.db.NewIter(&pebble.IterOptions{
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
		val, closer, err := e.db.Get(encodeResourceKey(childRT, childID))
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				continue
			}
			return err
		}
		r := &v3.ResourceRecord{}
		err = unmarshalRecord(val, r)
		closer.Close()
		if err != nil {
			return err
		}
		if !yield(r) {
			return nil
		}
	}
	return iter.Error()
}
