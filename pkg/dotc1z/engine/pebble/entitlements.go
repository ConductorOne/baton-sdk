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
// The identity key is a pure function of the record (it contains the raw
// external id), so overwrites are idempotent and no read-before-write or
// index cleanup is needed; a within-call dedup pre-pass keeps last-wins
// semantics for same-identity duplicates in one batch.
func (e *Engine) PutEntitlementRecords(ctx context.Context, records ...*v3.EntitlementRecord) error {
	if len(records) == 0 {
		return nil
	}
	return e.withWrite(func() error {
		if err := e.requireCurrentSync(); err != nil {
			return err
		}
		priBatch := e.db.NewRecordBatch()
		defer priBatch.Close()

		fresh := e.IsFreshSync()

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
			if err := priBatch.StageEntitlementPut(key, val); err != nil {
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
		e.noteEntitlementKeyspaceWrite()
		return nil
	})
}

// GetEntitlementRecord fetches an entitlement by its raw public id via the
// bare-id lookup (exact string-match, exactly-one rule — see lookup.go).
func (e *Engine) GetEntitlementRecord(ctx context.Context, externalID string) (*v3.EntitlementRecord, error) {
	id, err := e.resolveEntitlementIdentityByExternalID(ctx, externalID)
	if err != nil {
		return nil, err
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

// DeleteEntitlementRecord deletes by raw public id. A missing id is a
// no-op; an ambiguous id is an error (a lossy string must never guess a
// delete).
func (e *Engine) DeleteEntitlementRecord(ctx context.Context, externalID string) error {
	return e.withWrite(func() error {
		id, err := e.resolveEntitlementIdentityByExternalID(ctx, externalID)
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				return nil
			}
			return err
		}
		key := encodeEntitlementIdentityKey(id)
		batch := e.db.NewRecordBatch()
		defer batch.Close()
		if err := batch.StageEntitlementDelete(key); err != nil {
			return err
		}
		if err := batch.Commit(writeOpts(e.opts.durability)); err != nil {
			return err
		}
		e.noteEntitlementKeyspaceWrite()
		return nil
	})
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
