package pebble

// Dangling-reference drops: the engine mechanics behind the syncer's
// lenient-mode referential invariants (I7/I8/I9 in
// pkg/sync/ingest_invariants.go). Connectors ship magic-id construction
// bugs — grants and annotations naming entitlements/resources whose rows
// were never synced — and the platform's ingestion provably discards
// those rows, so the syncer drops them at the post-collection seam and
// reports aggregates. Policy (what to drop, exemptions, strict-mode
// failure) lives in the syncer; these methods are pure mechanics.
//
// All deletes route through the same locked helpers as interactive
// deletes, so secondary indexes and (if already built) digest
// invalidation stay consistent. Populations are small in practice
// (dangling refs are bugs, not steady state), so per-row batches are
// acceptable.

import (
	"context"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/codec"
)

// EnsureGrantIndexes runs the deferred grant-index build NOW if one is
// pending. The by_principal index is inline for connector-emitted grants
// but deferred for expansion/synth writes; the dangling-principal scan
// (I9) needs it complete. Calling early is near-free: the same build
// would run at EndSync anyway — this just moves it before the invariant
// seam (rows dropped afterward maintain the indexes inline and stage
// digest invalidation, which EndSync's repair pass heals).
func (e *Engine) EnsureGrantIndexes(ctx context.Context) error {
	if !e.deferredIdxPending.Load() {
		return nil
	}
	return e.BuildDeferredGrantIndexes(ctx)
}

// ForEachDanglingGrantPrincipal visits each distinct principal referenced
// by grants for which NO resource row exists. One seek per distinct
// principal over the by_principal index (which leads with principal
// identity) plus one point probe each — O(distinct principals), never
// O(grants). Callers must EnsureGrantIndexes first or the scan misses
// synth-written grants.
//
// matchAnnotatedOnly reports that EVERY grant of the dangling principal
// carries an ExternalResourceMatch* annotation — the legitimate carrier
// shape when no external resource file was configured (the match op
// deletes carriers when it runs, so annotated survivors always mean the
// op didn't run or didn't match). carrierGrants is the number of grant
// rows under such a principal, so callers can report per-GRANT totals
// (the mixed-principal path reports its own per-grant skip count).
// Value reads happen only for dangling principals, never on the
// healthy path.
func (e *Engine) ForEachDanglingGrantPrincipal(ctx context.Context, visit func(principalRT, principalID string, matchAnnotatedOnly bool, carrierGrants int64) error) error {
	prefix := []byte{versionV3, typeIndex, idxGrantByPrincipal}
	prefix = codec.AppendTupleSeparator(prefix)
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upperBoundOf(prefix),
	})
	if err != nil {
		return err
	}
	defer iter.Close()

	for valid := iter.First(); valid; {
		if err := ctx.Err(); err != nil {
			return err
		}
		key := iter.Key()
		tail := key[len(prefix):]
		rtBytes, next, ok := codec.DecodeTupleStringAlias(tail, 0)
		if !ok || next >= len(tail) {
			return fmt.Errorf("dangling grant-principal scan: malformed index key %x", key)
		}
		ridBytes, _, ok := codec.DecodeTupleStringAlias(tail, next+1)
		if !ok {
			return fmt.Errorf("dangling grant-principal scan: malformed index key %x", key)
		}
		rt, rid := string(rtBytes), string(ridBytes)
		exists, err := e.HasResourceRecord(ctx, rt, rid)
		if err != nil {
			return err
		}
		if !exists {
			matchOnly, carrierGrants, err := e.grantsForPrincipalAllMatchAnnotated(ctx, rt, rid)
			if err != nil {
				return err
			}
			if err := visit(rt, rid, matchOnly, carrierGrants); err != nil {
				return err
			}
		}
		// Skip every remaining grant of this principal.
		valid = iter.SeekGE(upperBoundOf(encodeGrantByPrincipalPrefix(rt, rid)))
	}
	return iter.Error()
}

// grantsForPrincipalAllMatchAnnotated reports whether every grant under
// the principal carries an ExternalResourceMatch* annotation, and how
// many grant rows that is (valid only when the bool is true — the walk
// stops at the first non-annotated grant). Reserved for dangling
// principals (reads row values).
func (e *Engine) grantsForPrincipalAllMatchAnnotated(ctx context.Context, principalRT, principalID string) (bool, int64, error) {
	ids, err := e.grantIdentitiesForPrincipal(ctx, principalRT, principalID)
	if err != nil {
		return false, 0, err
	}
	var carrierGrants int64
	for _, id := range ids {
		rec, err := e.getGrantRecordByIdentity(id)
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				continue // index entry without a row; the scan tolerates it
			}
			return false, 0, err
		}
		if !grantRecordHasExternalMatch(rec.GetAnnotations()) {
			return false, 0, nil
		}
		carrierGrants++
	}
	return true, carrierGrants, nil
}

// grantIdentitiesForPrincipal collects the grant identities under one
// principal from the by_principal index. Collected before any deletes so
// callers never interleave iteration with writes.
func (e *Engine) grantIdentitiesForPrincipal(ctx context.Context, principalRT, principalID string) ([]grantIdentity, error) {
	prefix := encodeGrantByPrincipalPrefix(principalRT, principalID)
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upperBoundOf(prefix),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var ids []grantIdentity
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		tail := iter.Key()[len(prefix):]
		var comps [4]string
		off := 0
		for i := range comps {
			b, next, ok := codec.DecodeTupleStringAlias(tail, off)
			if !ok {
				return nil, fmt.Errorf("by_principal index: malformed key tail %x", iter.Key())
			}
			comps[i] = string(b)
			off = next + 1
		}
		ids = append(ids, grantIdentity{
			entitlement: entitlementIdentity{
				resourceTypeID: comps[0],
				resourceID:     comps[1],
				stripped:       comps[2] == idFlagStripped,
				tail:           comps[3],
			},
			principalTypeID: principalRT,
			principalID:     principalID,
		})
	}
	return ids, iter.Error()
}

func (e *Engine) getGrantRecordByIdentity(id grantIdentity) (*v3.GrantRecord, error) {
	val, closer, err := e.db.Get(encodeGrantIdentityKey(id))
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	rec := &v3.GrantRecord{}
	if err := unmarshalRecord(val, rec); err != nil {
		return nil, fmt.Errorf("grant record by identity: unmarshal: %w", err)
	}
	return rec, nil
}

// dropBatchRows chunks the streaming drop batches: large enough to
// amortize commit overhead, small enough to bound batch memory. Drops
// during a fresh sync ride NoSync (the seal fsyncs), matching the
// tombstone and replay write paths — WITHOUT this, a dangling
// entitlement with a large grant population would pay one fsync per
// row.
const dropBatchRows = 4096

// dropWriteOpts returns the write options for streaming drops.
func (e *Engine) dropWriteOpts() *pebble.WriteOptions {
	if e.IsFreshSync() {
		return pebble.NoSync
	}
	return writeOpts(e.opts.durability)
}

// DeleteGrantsForPrincipal deletes every grant of one principal EXCEPT
// rows carrying ExternalResourceMatch* annotations (unprocessed match
// carriers are evidence of a missing external-resource file, not bad
// data). Streams one chunked batch pass — never one commit per row.
// Returns (deleted, skippedMatchAnnotated).
func (e *Engine) DeleteGrantsForPrincipal(ctx context.Context, principalRT, principalID string) (int64, int64, error) {
	indexPrefix := encodeGrantByPrincipalPrefix(principalRT, principalID)

	var deleted, skipped int64
	err := e.withWrite(func() error {
		iter, err := e.db.NewIter(&pebble.IterOptions{
			LowerBound: indexPrefix,
			UpperBound: upperBoundOf(indexPrefix),
		})
		if err != nil {
			return err
		}
		defer iter.Close()

		opts := e.dropWriteOpts()
		batch := e.db.NewBatch()
		defer func() { _ = batch.Close() }()
		rowsInBatch := 0

		for iter.First(); iter.Valid(); iter.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			// Index tail: ent identity components (the principal is the
			// prefix). Primary key = grant header + identity tail in
			// primary order.
			tail := iter.Key()[len(indexPrefix):]
			var comps [4]string
			off := 0
			for i := range comps {
				b, next, ok := codec.DecodeTupleStringAlias(tail, off)
				if !ok {
					return fmt.Errorf("by_principal index: malformed key tail %x", iter.Key())
				}
				comps[i] = string(b)
				off = next + 1
			}
			id := grantIdentity{
				entitlement: entitlementIdentity{
					resourceTypeID: comps[0],
					resourceID:     comps[1],
					stripped:       comps[2] == idFlagStripped,
					tail:           comps[3],
				},
				principalTypeID: principalRT,
				principalID:     principalID,
			}
			priKey := encodeGrantIdentityKey(id)
			val, closer, getErr := e.db.Get(priKey)
			if getErr != nil {
				if errors.Is(getErr, pebble.ErrNotFound) {
					continue // index entry without a row; tolerated
				}
				return getErr
			}
			rec := &v3.GrantRecord{}
			if err := unmarshalRecord(val, rec); err != nil {
				closer.Close()
				return fmt.Errorf("dangling principal drop: unmarshal: %w", err)
			}
			if grantRecordHasExternalMatch(rec.GetAnnotations()) {
				closer.Close()
				skipped++
				continue
			}
			if err := e.deleteGrantIndexesRaw(batch, "", val); err != nil {
				closer.Close()
				return err
			}
			closer.Close()
			if err := batch.Delete(priKey, nil); err != nil {
				return err
			}
			deleted++
			rowsInBatch++
			if rowsInBatch >= dropBatchRows {
				if err := batch.Commit(opts); err != nil {
					return err
				}
				_ = batch.Close()
				batch = e.db.NewBatch()
				rowsInBatch = 0
			}
		}
		if err := iter.Error(); err != nil {
			return err
		}
		return batch.Commit(opts)
	})
	if err != nil {
		return 0, 0, err
	}
	return deleted, skipped, nil
}

// DeleteGrantsForEntitlement deletes every grant row under one
// entitlement identity EXCEPT rows carrying the InsertResourceGrants
// annotation — the drop arm for grants referencing an entitlement with no
// row. The exemption is per GRANT, matching the ownership rule
// (docs/tasks/dangling-reference-drops.md): a machinery-owned IRG grant
// legitimately has no entitlement row (downstream synthesizes it), while
// a connector-owned grant under the same missing entitlement is a
// dangling reference and drops. Streams the primary prefix with values in
// hand (index cleanup derives from the value), chunked batches, no
// per-row commits. Returns (deleted, skippedInsertFact).
func (e *Engine) DeleteGrantsForEntitlement(ctx context.Context, entitlementID, entResourceTypeID, entResourceID string) (int64, int64, error) {
	entID := entitlementIdentityFromParts(entResourceTypeID, entResourceID, entitlementID)
	prefix := encodeGrantPrimaryEntitlementIdentityPrefix(
		entID.resourceTypeID, entID.resourceID, entID.flagComponent(), entID.tail)

	var deleted, skipped int64
	err := e.withWrite(func() error {
		iter, err := e.db.NewIter(&pebble.IterOptions{
			LowerBound: prefix,
			UpperBound: upperBoundOf(prefix),
		})
		if err != nil {
			return err
		}
		defer iter.Close()

		opts := e.dropWriteOpts()
		batch := e.db.NewBatch()
		defer func() { _ = batch.Close() }()
		rowsInBatch := 0

		for iter.First(); iter.Valid(); iter.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			flags, flagsErr := scanGrantSourceCacheFlagsRaw(iter.Value())
			if flagsErr != nil {
				return fmt.Errorf("dangling entitlement drop: scan flags: %w", flagsErr)
			}
			if flags.insertResourceGrants {
				skipped++
				continue
			}
			if err := e.deleteGrantIndexesRaw(batch, "", iter.Value()); err != nil {
				return err
			}
			key := iter.Key()
			k := make([]byte, len(key))
			copy(k, key)
			if err := batch.Delete(k, nil); err != nil {
				return err
			}
			deleted++
			rowsInBatch++
			if rowsInBatch >= dropBatchRows {
				if err := batch.Commit(opts); err != nil {
					return err
				}
				_ = batch.Close()
				batch = e.db.NewBatch()
				rowsInBatch = 0
			}
		}
		if err := iter.Error(); err != nil {
			return err
		}
		return batch.Commit(opts)
	})
	if err != nil {
		return 0, 0, err
	}
	return deleted, skipped, nil
}

// DeleteEntitlementsForResource deletes every entitlement row under one
// resource — the drop arm for entitlements referencing a resource with no
// row. Returns the count and up to maxIDs deleted external ids for the
// caller's aggregate report; maxIDs <= 0 collects none (callers pass a
// shrinking example budget, so the guard below doubles as the clamp).
func (e *Engine) DeleteEntitlementsForResource(ctx context.Context, resourceTypeID, resourceID string, maxIDs int) (int64, []string, error) {
	prefix := encodeEntitlementPrimaryResourcePrefix(resourceTypeID, resourceID)
	type entRow struct {
		id        entitlementIdentity
		scopeKey  string
		primaryKy []byte
	}
	var rows []entRow

	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upperBoundOf(prefix),
	})
	if err != nil {
		return 0, nil, err
	}
	const headerLen = 3 // versionV3, typeEntitlement, separator
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			iter.Close()
			return 0, nil, err
		}
		key := iter.Key()
		tail := key[headerLen:]
		var comps [4]string
		off := 0
		ok := true
		for i := range comps {
			b, next, decOK := codec.DecodeTupleStringAlias(tail, off)
			if !decOK {
				ok = false
				break
			}
			comps[i] = string(b)
			off = next + 1
		}
		if !ok {
			iter.Close()
			return 0, nil, fmt.Errorf("dangling entitlement drop: malformed entitlement key %x", key)
		}
		scope, scanErr := scanEntitlementSourceScopeRaw(iter.Value())
		if scanErr != nil {
			iter.Close()
			return 0, nil, scanErr
		}
		k := make([]byte, len(key))
		copy(k, key)
		rows = append(rows, entRow{
			id: entitlementIdentity{
				resourceTypeID: comps[0],
				resourceID:     comps[1],
				stripped:       comps[2] == idFlagStripped,
				tail:           comps[3],
			},
			scopeKey:  scope,
			primaryKy: k,
		})
	}
	if err := iter.Error(); err != nil {
		iter.Close()
		return 0, nil, err
	}
	iter.Close()

	var deleted int64
	var ids []string
	err = e.withWrite(func() error {
		batch := e.db.NewBatch()
		defer batch.Close()
		for _, row := range rows {
			if row.scopeKey != "" {
				if err := batch.Delete(encodeEntitlementBySourceScopeIndexKey(row.scopeKey, row.id), nil); err != nil {
					return err
				}
			}
			if err := batch.Delete(row.primaryKy, nil); err != nil {
				return err
			}
			deleted++
			if len(ids) < maxIDs {
				ids = append(ids, row.id.externalID())
			}
		}
		if err := batch.Commit(e.dropWriteOpts()); err != nil {
			return err
		}
		if deleted > 0 {
			e.noteEntitlementKeyspaceWrite()
		}
		return nil
	})
	return deleted, ids, err
}
