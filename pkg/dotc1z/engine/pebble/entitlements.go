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

// PutEntitlementRecords writes N entitlements by structured primary key.
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

		fresh := e.IsFreshSync()
		skipGet := e.takeFreshEntitlementsEmpty()

		type dedupKey struct {
			id entitlementIdentity
		}
		var dedup map[dedupKey]int
		if len(records) > 1 {
			dedup = make(map[dedupKey]int, len(records))
			for i, r := range records {
				if r == nil {
					continue
				}
				id, err := entitlementIdentityFromRecord(r)
				if err != nil {
					return err
				}
				dedup[dedupKey{id}] = i
			}
		}

		for i, r := range records {
			if r == nil {
				continue
			}
			id, err := entitlementIdentityFromRecord(r)
			if err != nil {
				return err
			}
			if dedup != nil {
				if dedup[dedupKey{id}] != i {
					continue
				}
			}
			key := encodeEntitlementIdentityKey(id)
			val, err := marshalRecord(r)
			if err != nil {
				return err
			}
			if !skipGet {
				_, closer, getErr := e.db.Get(key)
				switch {
				case getErr == nil:
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
		}
		opts := writeOpts(e.opts.durability)
		if fresh {
			opts = pebble.NoSync
		}
		return priBatch.Commit(opts)
	})
}

func (e *Engine) GetEntitlementRecord(ctx context.Context, externalID string) (*v3.EntitlementRecord, error) {
	id, err := entitlementIdentityFromID(externalID)
	if err != nil {
		return nil, pebble.ErrNotFound
	}
	val, closer, err := e.db.Get(encodeEntitlementIdentityKey(id))
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
		id, err := entitlementIdentityFromID(externalID)
		if err != nil {
			return nil
		}
		key := encodeEntitlementIdentityKey(id)
		batch := e.db.NewBatch()
		defer batch.Close()
		_, closer, err := e.db.Get(key)
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				return nil
			}
			return err
		}
		closer.Close()
		if err := batch.Delete(key, nil); err != nil {
			return err
		}
		return batch.Commit(writeOpts(e.opts.durability))
	})
}

func (e *Engine) writeEntitlementIndexes(_ *pebble.Batch, _ *v3.EntitlementRecord) error {
	return nil
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
	indexPrefix := encodeEntitlementPrimaryResourcePrefix(resourceTypeID, resourceID)
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: indexPrefix,
		UpperBound: upperBoundOf(indexPrefix),
	})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		r := &v3.EntitlementRecord{}
		if err := unmarshalRecord(iter.Value(), r); err != nil {
			return err
		}
		if !yield(r) {
			return nil
		}
	}
	return iter.Error()
}
