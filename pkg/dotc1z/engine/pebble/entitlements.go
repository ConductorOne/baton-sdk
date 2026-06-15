package pebble

import (
	"context"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
)

// PutEntitlementRecord writes an entitlement + its by_resource index.
func (e *Engine) PutEntitlementRecord(ctx context.Context, r *v3.EntitlementRecord) error {
	if r == nil {
		return errors.New("PutEntitlementRecord: nil record")
	}
	return e.PutEntitlementRecords(ctx, r)
}

// PutEntitlementRecords writes N entitlements in two pebble.Batches
// — primary keys in one, by_resource index keys in the other.
// Mirrors the PutGrantRecords pattern (RFC §3a Tier-B/C):
//   - within-call dedup pre-pass keyed by external_id drops earlier
//     occurrences;
//   - the first PutEntitlementRecords call of a fresh sync skips
//     the read-before-write Get (keyspace provably empty);
//   - subsequent calls fall back to read-before-write so cross-call
//     duplicates can clean up the prior call's index entries.
func (e *Engine) PutEntitlementRecords(ctx context.Context, records ...*v3.EntitlementRecord) error {
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
		skipGet := e.takeFreshEntitlementsEmpty()

		type dedupKey struct {
			extID string
		}
		var dedup map[dedupKey]int
		if len(records) > 1 {
			dedup = make(map[dedupKey]int, len(records))
			for i, r := range records {
				if r == nil {
					continue
				}
				dedup[dedupKey{r.GetExternalId()}] = i
			}
		}

		for i, r := range records {
			if r == nil {
				continue
			}
			if dedup != nil {
				if dedup[dedupKey{r.GetExternalId()}] != i {
					continue
				}
			}
			key := encodeEntitlementKey(r.GetExternalId())
			val, err := marshalRecord(r)
			if err != nil {
				return err
			}
			if !skipGet {
				oldVal, closer, getErr := e.db.Get(key)
				switch {
				case getErr == nil:
					if err := e.deleteEntitlementIndexesRaw(idxBatch, r.GetExternalId(), oldVal); err != nil {
						closer.Close()
						return err
					}
					closer.Close()
				case errors.Is(getErr, pebble.ErrNotFound):
					// no prior — write unconditionally
				default:
					return fmt.Errorf("PutEntitlementRecords: get old: %w", getErr)
				}
			}
			if err := priBatch.Set(key, val, nil); err != nil {
				return err
			}
			if err := e.writeEntitlementIndexes(idxBatch, r); err != nil {
				return err
			}
		}
		opts := writeOpts(e.opts.durability)
		if fresh {
			opts = pebble.NoSync
		}
		if err := priBatch.Commit(opts); err != nil {
			return err
		}
		return idxBatch.Commit(opts)
	})
}

func (e *Engine) GetEntitlementRecord(ctx context.Context, externalID string) (*v3.EntitlementRecord, error) {
	val, closer, err := e.db.Get(encodeEntitlementKey(externalID))
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	r := &v3.EntitlementRecord{}
	if err := unmarshalRecord(val, r); err != nil {
		return nil, fmt.Errorf("GetEntitlementRecord: unmarshal: %w", err)
	}
	return r, nil
}

func (e *Engine) DeleteEntitlementRecord(ctx context.Context, externalID string) error {
	return e.withWrite(func() error {
		key := encodeEntitlementKey(externalID)
		batch := e.db.NewBatch()
		defer batch.Close()
		oldVal, closer, err := e.db.Get(key)
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				return nil
			}
			return err
		}
		if err := e.deleteEntitlementIndexesRaw(batch, externalID, oldVal); err != nil {
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

func (e *Engine) writeEntitlementIndexes(batch *pebble.Batch, r *v3.EntitlementRecord) error {
	res := r.GetResource()
	if res == nil || res.GetResourceId() == "" {
		return nil
	}
	k := encodeEntitlementByResourceIndexKey(
		res.GetResourceTypeId(), res.GetResourceId(),
		r.GetExternalId(),
	)
	return batch.Set(k, nil, nil)
}

func (e *Engine) IterateEntitlements(ctx context.Context, yield func(*v3.EntitlementRecord) bool) error {
	prefix := encodeEntitlementPrefix()
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upperBoundOf(prefix),
	})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		r := &v3.EntitlementRecord{}
		if err := unmarshalRecord(iter.Value(), r); err != nil {
			return fmt.Errorf("iterate entitlements: %w", err)
		}
		if !yield(r) {
			return nil
		}
	}
	return iter.Error()
}

func (e *Engine) IterateEntitlementsByResource(ctx context.Context, resourceTypeID, resourceID string, yield func(*v3.EntitlementRecord) bool) error {
	indexPrefix := encodeEntitlementByResourcePrefix(resourceTypeID, resourceID)
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: indexPrefix,
		UpperBound: upperBoundOf(indexPrefix),
	})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		externalID := lastTupleComponent(iter.Key(), indexPrefix)
		if externalID == "" {
			continue
		}
		val, closer, err := e.db.Get(encodeEntitlementKey(externalID))
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				continue
			}
			return err
		}
		r := &v3.EntitlementRecord{}
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
