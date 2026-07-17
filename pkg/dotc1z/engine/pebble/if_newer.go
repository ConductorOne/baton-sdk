package pebble

import (
	"context"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"
	"google.golang.org/protobuf/types/known/timestamppb"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
)

// *IfNewer upsert methods. Mirror the SQLite engine's
// PutGrantsIfNewer / PutResourcesIfNewer / PutEntitlementsIfNewer /
// PutResourceTypesIfNewer semantics: only overwrite the existing
// record when the incoming record's discovered_at is strictly newer.
//
// Used by partial-sync workflows (SyncTypePartialUpserts /
// SyncTypePartialDeletions) where a connector replays a recent
// window of changes and must not regress an existing record's
// discovered_at to an older timestamp.
//
// Mechanism: for each candidate record we read the existing record
// (if any), compare discovered_at, and decide. Records that pass the
// freshness check go into a single batch and commit once. The
// fresh-sync write path is disabled here — *IfNewer is by definition
// not a fresh sync (we're filtering against existing data).

// PutGrantRecordsIfNewer writes records that are strictly newer than
// the stored copy. Records without a discovered_at are treated as
// "always write" (caller is asserting freshness explicitly).
func (e *Engine) PutGrantRecordsIfNewer(ctx context.Context, records ...*v3.GrantRecord) error {
	if len(records) == 0 {
		return nil
	}
	return e.withWrite(func() error {
		if err := e.requireCurrentSync(); err != nil {
			return err
		}
		batch := e.db.NewRecordBatch()
		defer batch.Close()
		// No inline hash-index or digest maintenance here: both are
		// derived at seal time (the fused deferred pass). But IfNewer is
		// the partial-sync path — it mutates a CLONED sealed file whose
		// digests are built — so StageGrantPutInline's derivers stage
		// the touched entitlements' digest invalidation whenever
		// digests are present (StageGrantDigestInvalidation deriver,
		// records.go).
		written := 0
		for _, r := range records {
			if r == nil {
				continue
			}
			id, err := grantIdentityFromRecord(r)
			if err != nil {
				return err
			}
			key := encodeGrantIdentityKey(id)
			hadOld := false
			oldVal, closer, getErr := e.db.Get(key)
			switch {
			case getErr == nil:
				write, err := discoveredAtIsNewerThanRaw(r.GetDiscoveredAt(), oldVal, grantDiscoveredAtField)
				if err != nil {
					closer.Close()
					return fmt.Errorf("PutGrantRecordsIfNewer: scan old discovered_at: %w", err)
				}
				if !write {
					closer.Close()
					continue
				}
				hadOld = true
				closer.Close()
			case errors.Is(getErr, pebble.ErrNotFound):
				// no existing record — write unconditionally
			default:
				return fmt.Errorf("PutGrantRecordsIfNewer: get: %w", getErr)
			}
			val, err := marshalRecord(r)
			if err != nil {
				return err
			}
			// Inline regime: the typed op stages the row plus prior-row
			// index cleanup, both index entries, and digest invalidation
			// (this IS the partial-sync path the invalidation exists for).
			if err := batch.StageGrantPutInline(key, val, hadOld, r.GetNeedsExpansion()); err != nil {
				return err
			}
			written++
		}
		if written == 0 {
			return nil
		}
		return batch.Commit(writeOpts(e.opts.durability))
	})
}

// PutResourceRecordsIfNewer writes resources only when the incoming
// discovered_at is strictly newer than the stored copy.
func (e *Engine) PutResourceRecordsIfNewer(ctx context.Context, records ...*v3.ResourceRecord) error {
	if len(records) == 0 {
		return nil
	}
	return e.withWrite(func() error {
		if err := e.requireCurrentSync(); err != nil {
			return err
		}
		batch := e.db.NewRecordBatch()
		defer batch.Close()
		written := 0
		for _, r := range records {
			if r == nil {
				continue
			}
			key := encodeResourceKey(r.GetResourceTypeId(), r.GetResourceId())
			oldVal, closer, getErr := e.db.Get(key)
			switch {
			case getErr == nil:
				write, err := discoveredAtIsNewerThanRaw(r.GetDiscoveredAt(), oldVal, resourceDiscoveredAtField)
				if err != nil {
					closer.Close()
					return fmt.Errorf("PutResourceRecordsIfNewer: scan old discovered_at: %w", err)
				}
				if !write {
					closer.Close()
					continue
				}
				val, err := marshalRecord(r)
				if err != nil {
					closer.Close()
					return err
				}
				// Typed op consumes the prior value for by_parent cleanup.
				err = batch.StageResourcePut(key, val, oldVal, r.GetResourceTypeId(), r.GetResourceId())
				closer.Close()
				if err != nil {
					return err
				}
				written++
				continue
			case errors.Is(getErr, pebble.ErrNotFound):
			default:
				return fmt.Errorf("PutResourceRecordsIfNewer: get: %w", getErr)
			}
			val, err := marshalRecord(r)
			if err != nil {
				return err
			}
			if err := batch.StageResourcePut(key, val, nil, r.GetResourceTypeId(), r.GetResourceId()); err != nil {
				return err
			}
			written++
		}
		if written == 0 {
			return nil
		}
		return batch.Commit(writeOpts(e.opts.durability))
	})
}

// PutEntitlementRecordsIfNewer writes entitlements only when newer.
func (e *Engine) PutEntitlementRecordsIfNewer(ctx context.Context, records ...*v3.EntitlementRecord) error {
	if len(records) == 0 {
		return nil
	}
	return e.withWrite(func() error {
		if err := e.requireCurrentSync(); err != nil {
			return err
		}
		batch := e.db.NewRecordBatch()
		defer batch.Close()
		written := 0
		for _, r := range records {
			if r == nil {
				continue
			}
			id, err := entitlementIdentityFromRecord(r)
			if err != nil {
				return err
			}
			key := encodeEntitlementIdentityKey(id)
			oldVal, closer, getErr := e.db.Get(key)
			switch {
			case getErr == nil:
				write, err := discoveredAtIsNewerThanRaw(r.GetDiscoveredAt(), oldVal, entitlementDiscoveredAtField)
				if err != nil {
					closer.Close()
					return fmt.Errorf("PutEntitlementRecordsIfNewer: scan old discovered_at: %w", err)
				}
				if !write {
					closer.Close()
					continue
				}
				closer.Close()
			case errors.Is(getErr, pebble.ErrNotFound):
			default:
				return fmt.Errorf("PutEntitlementRecordsIfNewer: get: %w", getErr)
			}
			val, err := marshalRecord(r)
			if err != nil {
				return err
			}
			if err := batch.StageEntitlementPut(key, val); err != nil {
				return err
			}
			written++
		}
		if written == 0 {
			return nil
		}
		if err := batch.Commit(writeOpts(e.opts.durability)); err != nil {
			return err
		}
		e.noteEntitlementKeyspaceWrite()
		return nil
	})
}

// PutResourceTypeRecordsIfNewer writes resource_types only when newer.
func (e *Engine) PutResourceTypeRecordsIfNewer(ctx context.Context, records ...*v3.ResourceTypeRecord) error {
	if len(records) == 0 {
		return nil
	}
	return e.withWrite(func() error {
		if err := e.requireCurrentSync(); err != nil {
			return err
		}
		batch := e.db.NewRecordBatch()
		defer batch.Close()
		written := 0
		for _, r := range records {
			if r == nil {
				continue
			}
			key := encodeResourceTypeKey(r.GetExternalId())
			oldVal, closer, getErr := e.db.Get(key)
			switch {
			case getErr == nil:
				write, err := discoveredAtIsNewerThanRaw(r.GetDiscoveredAt(), oldVal, resourceTypeDiscoveredAtField)
				if err != nil {
					closer.Close()
					return fmt.Errorf("PutResourceTypeRecordsIfNewer: scan old discovered_at: %w", err)
				}
				closer.Close()
				if !write {
					continue
				}
			case errors.Is(getErr, pebble.ErrNotFound):
			default:
				return fmt.Errorf("PutResourceTypeRecordsIfNewer: get: %w", getErr)
			}
			val, err := marshalRecord(r)
			if err != nil {
				return err
			}
			if err := batch.StageResourceTypePut(key, val); err != nil {
				return err
			}
			written++
		}
		if written == 0 {
			return nil
		}
		return batch.Commit(writeOpts(e.opts.durability))
	})
}

// discoveredAtIsNewer returns true iff incoming is strictly after
// existing. Matches SQLite's `EXCLUDED.discovered_at > X.discovered_at`
// semantics, including the NULL-propagation rules:
//
//   - nil incoming → false (SQLite `NULL > X` is NULL, i.e. don't
//     write). Adapter-level PutXxxIfNewer methods stamp DiscoveredAt
//     to time.Now() before calling here, so production code never
//     hits this branch; direct engine callers must supply a non-nil
//     DiscoveredAt to mean "write this".
//   - nil existing → true (no prior record at this key, so the
//     incoming row wins by default — SQLite's INSERT-on-conflict
//     reduces to a plain INSERT).
//   - both non-nil → strict After comparison.
//
// Keep this in sync with extractAndStripExpansion / putGrantsInternal
// in pkg/dotc1z/grants.go if the SQLite IfNewer path ever changes.
func discoveredAtIsNewer(incoming, existing *timestamppb.Timestamp) bool {
	if incoming == nil {
		return false
	}
	if existing == nil {
		return true
	}
	return incoming.AsTime().After(existing.AsTime())
}
