package pebble

import (
	"context"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/internal/rawdb"
)

// PutEntitlementRecord writes an entitlement + its by_resource index.
func (r *Records) PutEntitlementRecord(ctx context.Context, rec *v3.EntitlementRecord) error {
	if rec == nil {
		return errors.New("PutEntitlementRecord: nil record")
	}
	return r.PutEntitlementRecords(ctx, rec)
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
func (r *Records) PutEntitlementRecords(ctx context.Context, records ...*v3.EntitlementRecord) error {
	if len(records) == 0 {
		return nil
	}
	return r.e.withWrite(func() error {
		if err := r.e.requireCurrentSync(); err != nil {
			return err
		}
		priBatch := r.e.db.NewRecordBatch()
		defer priBatch.Close()

		if _, err := r.stageEntitlementRecords(priBatch, records); err != nil {
			return err
		}
		opts := recordWriteOpts
		if err := priBatch.Commit(opts); err != nil {
			return err
		}
		r.noteEntitlementKeyspaceWrite()
		return nil
	})
}

// Caller holds the write barrier, commits the batch, and then calls
// noteEntitlementKeyspaceWrite.
func (r *Records) stageEntitlementRecords(batch *rawdb.RecordBatch, records []*v3.EntitlementRecord) (uint64, error) {
	if len(records) == 0 {
		return 0, nil
	}
	skipGet := r.e.takeFreshEntitlementsEmpty()

	type dedupKey struct {
		id entitlementIdentity
	}
	var dedup map[dedupKey]int
	if len(records) > 1 {
		dedup = make(map[dedupKey]int, len(records))
		for i, rec := range records {
			if rec == nil {
				continue
			}
			id, err := entitlementIdentityFromRecord(rec)
			if err != nil {
				return 0, err
			}
			dedup[dedupKey{id}] = i
		}
	}

	var staged uint64
	for i, rec := range records {
		if rec == nil {
			continue
		}
		id, err := entitlementIdentityFromRecord(rec)
		if err != nil {
			return 0, err
		}
		if dedup != nil {
			if dedup[dedupKey{id}] != i {
				continue
			}
		}
		staged++
		key := encodeEntitlementIdentityKey(id)
		val, err := marshalRecord(rec)
		if err != nil {
			return 0, err
		}
		if skipGet || !r.e.db.SourceScopeMayExist() {
			if err := batch.StageEntitlementPut(key, val, nil); err != nil {
				return 0, err
			}
			continue
		}
		oldVal, closer, getErr := r.e.db.Get(key)
		switch {
		case getErr == nil:
			err = batch.StageEntitlementPut(key, val, oldVal)
			closer.Close()
		case errors.Is(getErr, pebble.ErrNotFound):
			err = batch.StageEntitlementPut(key, val, nil)
		default:
			return 0, fmt.Errorf("PutEntitlementRecords: get old: %w", getErr)
		}
		if err != nil {
			return 0, err
		}
	}
	return staged, nil
}

// GetEntitlementRecord fetches an entitlement by its raw public id via the
// bare-id lookup (exact string-match, exactly-one rule — see lookup.go).
func (r *Records) GetEntitlementRecord(ctx context.Context, externalID string) (*v3.EntitlementRecord, error) {
	id, err := r.resolveEntitlementIdentityByExternalID(ctx, externalID)
	if err != nil {
		return nil, err
	}
	val, closer, err := r.e.db.Get(encodeEntitlementIdentityKey(id))
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	rec := &v3.EntitlementRecord{}
	if err := unmarshalRecord(val, rec); err != nil {
		return nil, fmt.Errorf("GetEntitlementRecord: unmarshal: %w", err)
	}
	return rec, nil
}

// DeleteEntitlementRecord deletes by raw public id. A missing id is a
// no-op; an ambiguous id is an error (a lossy string must never guess a
// delete).
func (r *Records) DeleteEntitlementRecord(ctx context.Context, externalID string) error {
	return r.e.withWrite(func() error {
		id, err := r.resolveEntitlementIdentityByExternalID(ctx, externalID)
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				return nil
			}
			return err
		}
		key := encodeEntitlementIdentityKey(id)
		oldVal, closer, getErr := r.e.db.Get(key)
		if errors.Is(getErr, pebble.ErrNotFound) {
			return nil
		}
		if getErr != nil {
			return getErr
		}
		batch := r.e.db.NewRecordBatch()
		defer batch.Close()
		if err := batch.StageEntitlementDelete(key, oldVal); err != nil {
			closer.Close()
			return err
		}
		closer.Close()
		if err := batch.Commit(writeOpts(r.e.opts.durability)); err != nil {
			return err
		}
		r.noteEntitlementKeyspaceWrite()
		return nil
	})
}

// DeleteEntitlementRecordByIdentity deletes one exact structured identity.
// Unlike the compatibility bare-ID delete, it remains unambiguous when two
// resources expose the same entitlement ID and requires no global lookup.
func (r *Records) DeleteEntitlementRecordByIdentity(
	ctx context.Context,
	resourceTypeID string,
	resourceID string,
	externalID string,
) error {
	return r.e.withWrite(func() error {
		key := encodeEntitlementIdentityKey(
			entitlementIdentityFromParts(resourceTypeID, resourceID, externalID),
		)
		// A missing row stages nothing, matching the bare-ID delete above and the
		// grant path's rule: staging unconditionally would emit index and digest
		// obligations for identities that never existed.
		oldVal, closer, err := r.e.db.Get(key)
		if errors.Is(err, pebble.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		defer closer.Close()
		batch := r.e.db.NewRecordBatch()
		defer batch.Close()
		if err := batch.StageEntitlementDelete(key, oldVal); err != nil {
			return err
		}
		if err := batch.Commit(writeOpts(r.e.opts.durability)); err != nil {
			return err
		}
		r.noteEntitlementKeyspaceWrite()
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
func (r *Records) DeleteEntitlementRecords(ctx context.Context, externalIDs []string, actingScope string) error {
	return r.e.withWrite(func() error {
		identities := make([]entitlementIdentity, 0, len(externalIDs))
		seen := make(map[entitlementIdentity]struct{}, len(externalIDs))
		for _, externalID := range externalIDs {
			if err := ctx.Err(); err != nil {
				return err
			}
			id, err := r.resolveEntitlementIdentityByExternalID(ctx, externalID)
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

		deletes := newSourceCacheDeleteBatch(r.e, "entitlements-canonical", actingScope, writeOpts(r.e.opts.durability))
		defer deletes.close()
		// The bare-id lookup map must observe every chunk AS it lands,
		// not on function exit: entitlementIdentitiesForExternalID takes
		// only entIDLookupMu, so a concurrent lookup between a mid-loop
		// chunk commit and this function's return would serve a cached
		// map listing rows already deleted on disk. The per-commit hook
		// (an atomic add) closes that window and covers the
		// error-after-intermediate-commit case for free.
		deletes.onCommit = r.noteEntitlementKeyspaceWrite
		for _, id := range identities {
			key := encodeEntitlementIdentityKey(id)
			oldVal, closer, err := r.e.db.Get(key)
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

func (r *Records) IterateEntitlements(ctx context.Context, yield func(*v3.EntitlementRecord) bool) error {
	prefix := encodeEntitlementPrefix()
	iter, err := r.e.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upperBoundOf(prefix),
	})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		rec := &v3.EntitlementRecord{}
		if err := unmarshalRecord(iter.Value(), rec); err != nil {
			return fmt.Errorf("iterate entitlements: %w", err)
		}
		if !yield(rec) {
			return nil
		}
	}
	return iter.Error()
}

func (r *Records) IterateEntitlementsByResource(ctx context.Context, resourceTypeID, resourceID string, yield func(*v3.EntitlementRecord) bool) error {
	indexPrefix := encodeEntitlementPrimaryResourcePrefix(resourceTypeID, resourceID)
	iter, err := r.e.db.NewIter(&pebble.IterOptions{
		LowerBound: indexPrefix,
		UpperBound: upperBoundOf(indexPrefix),
	})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		rec := &v3.EntitlementRecord{}
		if err := unmarshalRecord(iter.Value(), rec); err != nil {
			return err
		}
		if !yield(rec) {
			return nil
		}
	}
	return iter.Error()
}
