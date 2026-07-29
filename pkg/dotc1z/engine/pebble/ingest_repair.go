package pebble

// The engine side of the syncer's dangling-principal invariant (I9 in
// pkg/sync/ingest_invariants.go): the by_principal scan, the
// match-carrier probe, and the orphan index-entry heal. Policy (what
// to warn on, exemptions, fail-fast) lives in the syncer; these
// methods are mechanics. The heal is the one WRITE here, and it rides
// rawdb's typed StageGrantOrphanIndexHeal op — index-only cleanup of
// entries whose primary row is provably absent.

import (
	"context"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/codec"
)

// EnsureGrantIndexes runs the deferred grant-index build NOW if one is
// pending, and retires the pending marker. The by_principal index is
// inline for connector-emitted grants but deferred for expansion/synth
// writes; the dangling-principal scan (I9) needs it complete. This is
// the SAME build EndSync would run, so the marker must clear here or
// EndSync repeats the whole O(grants) scan/build a second time.
//
// CALLER CONTRACT — WRITE QUIESCENCE REQUIRED: unlike EndSync, which
// seals the engine before its rebuild so a straggler writer blocked on
// writeMu cannot commit between the rebuild and the marker clear, this
// method cannot seal (the invariant pass's own heal still needs the
// write path). A deferred grant write racing this call could land
// after the rebuild and before the clear — present in the primary
// keyspace, permanently absent from by_principal. The syncer's
// post-collection seam guarantees quiescence (parallelSync has
// drained); any new caller must guarantee the same or use EndSync.
func (e *Engine) EnsureGrantIndexes(ctx context.Context) error {
	if e.db == nil {
		return ErrEngineClosing
	}
	if !e.db.DeferredIdxPending() {
		return nil
	}
	if err := e.BuildDeferredGrantIndexes(ctx); err != nil {
		return err
	}
	return e.clearDeferredIdxPending()
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
// rows under such a principal, so callers can report per-GRANT totals.
// Value reads happen only for dangling principals, never on the
// healthy path.
//
// A principal whose index entries are ALL orphans (no primary rows) is
// never visited: it has no grants to judge, so it is healed in place —
// the orphan index keys are deleted — instead of being vacuously
// classified as match-annotated-only.
func (e *Engine) ForEachDanglingGrantPrincipal(ctx context.Context, visit func(principalRT, principalID string, matchAnnotatedOnly bool, carrierGrants int64) error) error {
	if e.db == nil {
		return ErrEngineClosing
	}
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

	// Healed-orphan aggregation: one Warn per sweep (the house shape —
	// never per-principal logs; a systematic writer bug could strand
	// entries for thousands of principals). Orphans are always a bug
	// signal, so the aggregate must be visible at default level — and
	// it fires on EVERY exit path: heals commit durably as the scan
	// goes, so an error/cancellation after the first heal must not
	// suppress the only report those heals will ever get (a resumed
	// scan finds them already healed and says nothing).
	var healedEntries int64
	healedPrincipals := 0
	var healedExamples []string
	scanComplete := false
	defer func() {
		if healedEntries > 0 {
			ctxzap.Extract(ctx).Warn("HEALED orphan by_principal index entries (index keys with no grant row — evidence of a writer bug, not connector data)",
				zap.Int64("healed_entries", healedEntries),
				zap.Int("principals", healedPrincipals),
				zap.Strings("principal_examples", healedExamples),
				zap.Bool("scan_complete", scanComplete),
			)
		}
	}()

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
			switch {
			case matchOnly && carrierGrants == 0:
				// Every index entry under this principal is an orphan
				// (no primary row): there are no grants to judge, so
				// neither the match-carrier exemption nor the warn/fail
				// arms apply — the "all match-annotated" verdict was
				// vacuous. Heal the index garbage instead, so a writer
				// bug that strands by_principal entries is scrubbed
				// rather than vacuously exempted on every future sweep.
				healed, err := e.healOrphanPrincipalIndexEntries(ctx, rt, rid)
				if err != nil {
					return err
				}
				if healed > 0 {
					healedEntries += healed
					healedPrincipals++
					if len(healedExamples) < maxHealedOrphanExamples {
						healedExamples = append(healedExamples, rt+"/"+rid)
					}
				}
			default:
				if err := visit(rt, rid, matchOnly, carrierGrants); err != nil {
					return err
				}
			}
		}
		// Skip every remaining grant of this principal.
		valid = iter.SeekGE(upperBoundOf(encodeGrantByPrincipalPrefix(rt, rid)))
	}
	if err := iter.Error(); err != nil {
		return err
	}
	scanComplete = true
	return nil
}

// maxHealedOrphanExamples caps the principal examples on the aggregated
// healed-orphan warning, matching the dangling-reference warnings' shape.
const maxHealedOrphanExamples = 25

// invariantWriteOpts returns the write options for invariant-pass
// writes (today: the orphan heal). During a fresh sync they ride
// NoSync — the seal's durability flush hardens them, matching every
// other during-sync write path.
func (e *Engine) invariantWriteOpts() *pebble.WriteOptions {
	if e.IsFreshSync() {
		return pebble.NoSync
	}
	return writeOpts(e.opts.durability)
}

// healOrphanPrincipalIndexEntries deletes by_principal index entries
// whose primary grant row does not exist, for one principal. Identities
// are re-collected and re-probed under the write lock, so an entry whose
// row appeared since the caller's read scan is kept. Returns the number
// of entries healed.
func (e *Engine) healOrphanPrincipalIndexEntries(ctx context.Context, principalRT, principalID string) (int64, error) {
	var healed int64
	err := e.withWrite(func() error {
		ids, err := e.grantIdentitiesForPrincipal(ctx, principalRT, principalID)
		if err != nil {
			return err
		}
		rb := e.db.NewRecordBatch()
		defer func() { _ = rb.Close() }()
		for _, id := range ids {
			if err := ctx.Err(); err != nil {
				return err
			}
			primaryKey := encodeGrantIdentityKey(id)
			_, closer, getErr := e.db.Get(primaryKey)
			switch {
			case errors.Is(getErr, pebble.ErrNotFound):
				if err := rb.StageGrantOrphanIndexHeal(primaryKey); err != nil {
					return err
				}
				healed++
			case getErr != nil:
				return getErr
			default:
				closer.Close()
			}
		}
		if healed == 0 {
			return nil
		}
		return rb.Commit(e.invariantWriteOpts())
	})
	if err != nil {
		return 0, err
	}
	if healed > 0 {
		ctxzap.Extract(ctx).Debug("healed orphan by_principal index entries",
			zap.String("principal_resource_type_id", principalRT),
			zap.String("principal_resource_id", principalID),
			zap.Int64("healed", healed),
		)
	}
	return healed, nil
}

// grantsForPrincipalAllMatchAnnotated reports whether every grant under
// the principal carries an ExternalResourceMatch* annotation, and how
// many carrier grants there are. The count is valid in BOTH outcomes:
// mixed populations (annotated carriers beside plain grants) report
// their carriers too, so the syncer's per-GRANT carrier totals don't
// silently lose the mixed case — the full walk is acceptable because
// this probe runs only for DANGLING principals (reads row values).
func (e *Engine) grantsForPrincipalAllMatchAnnotated(ctx context.Context, principalRT, principalID string) (bool, int64, error) {
	ids, err := e.grantIdentitiesForPrincipal(ctx, principalRT, principalID)
	if err != nil {
		return false, 0, err
	}
	allCarry := true
	var carrierGrants int64
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return false, 0, err
		}
		rec, err := e.getGrantRecordByIdentity(id)
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				continue // index entry without a row; the scan tolerates it
			}
			return false, 0, err
		}
		if grantRecordHasExternalMatch(rec.GetAnnotations()) {
			carrierGrants++
		} else {
			allCarry = false
		}
	}
	return allCarry, carrierGrants, nil
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
