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
// external id), so overwrites are idempotent; a within-call dedup
// pre-pass keeps last-wins semantics for same-identity duplicates in one
// batch.
//
// Read-before-write exists ONLY for the by_source_scope obligation: an
// overwrite that changes a row's scope stamp must clean the prior
// entry, and entitlement scope entries are value-derived. The Get is
// therefore skipped while rawdb's sourceScopeMayExist gate is unarmed:
// the unarmed gate certifies no index entry exists to clean (the only
// thing the prior value is fetched for), so an ordinary unscoped sync
// pays no per-row read at all — the pre-scope write cost, exactly. A
// stamped record still gets its index entry (stageSourceScopeChange
// always scans the NEW value and arms the gate AT STAGING, before the
// batch commits), so later records in the same call — and every call
// after — take the Get path. Rows staged Get-free earlier in the
// arming call are sound: the gate was unarmed when they staged, so no
// committed index entry existed for their identities (db.Get cannot
// see in-batch writes either way).
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
			if skipGet || !e.db.SourceScopeMayExist() {
				if err := priBatch.StageEntitlementPut(key, val, nil); err != nil {
					return err
				}
				continue
			}
			oldVal, closer, getErr := e.db.Get(key)
			switch {
			case getErr == nil:
				err = priBatch.StageEntitlementPut(key, val, oldVal)
				closer.Close()
			case errors.Is(getErr, pebble.ErrNotFound):
				err = priBatch.StageEntitlementPut(key, val, nil)
			default:
				return fmt.Errorf("PutEntitlementRecords: get old: %w", getErr)
			}
			if err != nil {
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
		oldVal, closer, getErr := e.db.Get(key)
		if errors.Is(getErr, pebble.ErrNotFound) {
			return nil
		}
		if getErr != nil {
			return getErr
		}
		batch := e.db.NewRecordBatch()
		defer batch.Close()
		if err := batch.StageEntitlementDelete(key, oldVal); err != nil {
			closer.Close()
			return err
		}
		closer.Close()
		if err := batch.Commit(writeOpts(e.opts.durability)); err != nil {
			return err
		}
		e.noteEntitlementKeyspaceWrite()
		return nil
	})
}

// DeleteEntitlementRecordByIdentity deletes one exact structured identity.
// Unlike the compatibility bare-ID delete, it remains unambiguous when two
// resources expose the same entitlement ID and requires no global lookup.
func (e *Engine) DeleteEntitlementRecordByIdentity(
	ctx context.Context,
	resourceTypeID string,
	resourceID string,
	externalID string,
) error {
	return e.withWrite(func() error {
		key := encodeEntitlementIdentityKey(
			entitlementIdentityFromParts(resourceTypeID, resourceID, externalID),
		)
		// A missing row stages nothing, matching the bare-ID delete above and the
		// grant path's rule: staging unconditionally would emit index and digest
		// obligations for identities that never existed.
		oldVal, closer, err := e.db.Get(key)
		if errors.Is(err, pebble.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		defer closer.Close()
		batch := e.db.NewRecordBatch()
		defer batch.Close()
		if err := batch.StageEntitlementDelete(key, oldVal); err != nil {
			return err
		}
		if err := batch.Commit(writeOpts(e.opts.durability)); err != nil {
			return err
		}
		e.noteEntitlementKeyspaceWrite()
		return nil
	})
}

// DeleteEntitlementRecords validates every public id before staging any
// tombstone, then commits the resolved deletes in bounded chunks (deletion
// is idempotent; a mid-way error retries convergently). Missing ids are
// no-ops; an ambiguous id rejects the entire request. actingScope is the
// scope on whose behalf the tombstones act: deleting that scope's own rows
// stages no poison, while deleting a row stamped with any OTHER scope
// poisons it (CO-015); "" acts unscoped and poisons any stamped delete.
func (e *Engine) DeleteEntitlementRecords(ctx context.Context, externalIDs []string, actingScope string) error {
	return e.withWrite(func() error {
		identities := make([]entitlementIdentity, 0, len(externalIDs))
		seen := make(map[entitlementIdentity]struct{}, len(externalIDs))
		for _, externalID := range externalIDs {
			if err := ctx.Err(); err != nil {
				return err
			}
			id, err := e.resolveEntitlementIdentityByExternalID(ctx, externalID)
			if err != nil {
				if errors.Is(err, pebble.ErrNotFound) {
					continue
				}
				return err
			}
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			identities = append(identities, id)
		}
		if len(identities) == 0 {
			return nil
		}

		deletes := newSourceCacheDeleteBatch(e, "entitlements-canonical", actingScope, writeOpts(e.opts.durability))
		defer deletes.close()
		// The bare-id lookup map must observe every chunk AS it lands,
		// not on function exit: entitlementIdentitiesForExternalID takes
		// only entIDLookupMu, so a concurrent lookup between a mid-loop
		// chunk commit and this function's return would serve a cached
		// map listing rows already deleted on disk. The per-commit hook
		// (an atomic add) closes that window and covers the
		// error-after-intermediate-commit case for free.
		deletes.onCommit = e.noteEntitlementKeyspaceWrite
		for _, id := range identities {
			key := encodeEntitlementIdentityKey(id)
			oldVal, closer, err := e.db.Get(key)
			if errors.Is(err, pebble.ErrNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			if err := deletes.batch.StageEntitlementDelete(key, oldVal); err != nil {
				_ = closer.Close()
				return err
			}
			_ = closer.Close()
			if err := deletes.staged(true); err != nil {
				return err
			}
		}
		return deletes.commit(true)
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
