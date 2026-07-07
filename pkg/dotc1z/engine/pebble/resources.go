package pebble

import (
	"context"
	"errors"
	"fmt"

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

// PutResourceRecords writes N resources in two pebble.Batches —
// primary keys in one, by_parent index keys in the other. Mirrors
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
		priBatch := e.db.NewBatch()
		defer priBatch.Close()
		idxBatch := e.db.NewBatch()
		defer idxBatch.Close()

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
			if !skipGet {
				oldVal, closer, getErr := e.db.Get(key)
				switch {
				case getErr == nil:
					if err := e.deleteResourceIndexesRaw(idxBatch, r.GetResourceTypeId(), r.GetResourceId(), oldVal); err != nil {
						closer.Close()
						return err
					}
					closer.Close()
				case errors.Is(getErr, pebble.ErrNotFound):
					// no prior — write unconditionally
				default:
					return fmt.Errorf("PutResourceRecords: get old: %w", getErr)
				}
			}
			if err := priBatch.Set(key, val, nil); err != nil {
				return err
			}
			if err := e.writeResourceIndexes(idxBatch, r); err != nil {
				return err
			}
		}
		opts := writeOpts(e.opts.durability)
		if fresh {
			opts = pebble.NoSync
		}
		// One atomic commit: folding the index batch into the primary batch
		// closes the durability gap where the primary commit landed but the
		// index commit failed — a divergence that would persist in the saved
		// artifact if the caller shipped it anyway.
		if err := priBatch.Apply(idxBatch, nil); err != nil {
			return err
		}
		return priBatch.Commit(opts)
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

		batch := e.db.NewBatch()
		defer batch.Close()

		oldVal, closer, err := e.db.Get(key)
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				return nil
			}
			return err
		}
		if err := e.deleteResourceIndexesRaw(batch, resourceTypeID, resourceID, oldVal); err != nil {
			closer.Close()
			return err
		}
		closer.Close()
		if err := batch.Delete(key, nil); err != nil {
			return err
		}
		return batch.Commit(writeOpts(e.opts.durability))
	})
}

func (e *Engine) writeResourceIndexes(batch *pebble.Batch, r *v3.ResourceRecord) error {
	parent := r.GetParent()
	if parent == nil || parent.GetResourceId() == "" {
		return nil
	}
	k := encodeResourceByParentIndexKey(
		parent.GetResourceTypeId(), parent.GetResourceId(),
		r.GetResourceTypeId(), r.GetResourceId(),
	)
	return batch.Set(k, nil, nil)
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
