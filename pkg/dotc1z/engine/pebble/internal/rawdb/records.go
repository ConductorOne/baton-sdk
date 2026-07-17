package rawdb

// Phase 2b: typed record-keyspace staging with obligation derivation
// FOLDED INTO the ops. The forgotten-obligation bug class — a caller
// staging a primary row and forgetting its index entries, digest
// invalidation, prior-row index cleanup, or the deferred-index crash
// marker — dies here: RecordBatch exposes no generic staging, and each
// Stage* op computes and stages everything its mutation owes in the
// same call.
//
// Grant index maintenance runs TWO REGIMES, and the ops mirror them
// (unifying them behind a flag was considered and rejected — the
// regimes differ in WHICH obligations exist, not how they're done):
//
//   - INLINE (StageGrantPutInline / StageGrantDelete): by_principal
//     and by_needs_expansion maintained inline; overwrite/delete
//     cleans BOTH; digest invalidation when digests are present. The
//     PutGrantRecords and IfNewer paths, and post-seal deletes.
//   - DEFERRED (StageGrantPutDeferred): the durable deferred-index
//     marker is armed FIRST (via the engine-injected arm function —
//     CAS-cheap per record, crash-contract-ordered before the batch
//     can commit), only by_needs_expansion is written inline, and
//     overwrite cleanup deliberately skips by_principal (the whole
//     family is excised and rebuilt at seal). The expanded/synth
//     grant writers.
//
// The derivation contract is BYTE-LEVEL and KEY-DERIVED: index keys
// come from the encoded grant primary key by splice (by_principal:
// segment permutation; by_needs_expansion: header swap; digest
// partition: sub-slice) — the primary key IS the identity encoding,
// so key-derived and value-derived obligations are identical by
// construction, and the splices are pinned against decode+re-encode
// by the engine's TestAppendGrantByPrincipalKeyFromPrimary /
// TestNeedsExpansionKeyHeaderSpliceFromPrimary. The engine injects
// the splice/scan functions once at Open (RecordDerivers): rawdb owns
// the COMPOSITION (what must be staged together, unforgettable), the
// engine owns the byte formats (its keyspace ABI).
//
// Every op asserts its key's family prefix — a Stage* call with a key
// from the wrong keyspace fails loudly rather than landing where it
// doesn't belong.

import (
	"fmt"

	"github.com/cockroachdb/pebble/v2"
)

// RecordDerivers are the engine-injected functions the typed record
// ops derive obligations with. Append-shaped functions write into dst
// and return it (scratch reuse; the batch owns the scratch).
type RecordDerivers struct {
	// GrantKeyValid reports whether a grant primary key decodes as a
	// 6-segment identity. Run unconditionally by every grant op: the
	// splices validate implicitly, but obligation paths are
	// conditional (a deferred put with needsExpansion=false and
	// digests unarmed runs no splice at all), and a malformed key must
	// never stage regardless of which obligations happen to fire.
	GrantKeyValid func(primaryKey []byte) bool
	// GrantByPrincipalKey appends the by_principal index key derived
	// from a grant primary key (segment permutation). ok=false means
	// the key did not decode as a 6-segment grant identity.
	GrantByPrincipalKey func(dst, primaryKey []byte) ([]byte, bool)
	// GrantNeedsExpansionKey appends the by_needs_expansion index key
	// derived from a grant primary key (header swap).
	GrantNeedsExpansionKey func(dst, primaryKey []byte) ([]byte, bool)
	// StageGrantDigestInvalidation stages the digest invalidation a
	// grant mutation owes for its entitlement partition when digest
	// state exists (present-means-exact). Must internally no-op when
	// digests are not armed — the armed probe is engine state.
	StageGrantDigestInvalidation func(st Stager, grantPrimaryKey []byte) error
	// ArmDeferredGrantIndex durably arms the deferred by_principal
	// rebuild marker (CAS + fsync'd meta key; rollback on failure).
	// Called by the DEFERRED regime before staging, per record —
	// the engine's CAS makes repeat calls one atomic load.
	ArmDeferredGrantIndex func() error

	// ResourceParent scans a marshaled ResourceRecord value for its
	// parent ref ("", "" = no parent, no index entry owed).
	ResourceParent func(value []byte) (parentRT, parentID string, err error)
	// ResourceByParentKey builds the by_parent index key.
	ResourceByParentKey func(parentRT, parentID, childRT, childID string) []byte

	// Family prefixes for the keyspace assertions.
	GrantPrimaryPrefix        []byte
	ResourcePrimaryPrefix     []byte
	EntitlementPrimaryPrefix  []byte
	ResourceTypePrimaryPrefix []byte
}

func (d *RecordDerivers) complete() error {
	switch {
	case d.GrantKeyValid == nil, d.GrantByPrincipalKey == nil, d.GrantNeedsExpansionKey == nil,
		d.StageGrantDigestInvalidation == nil, d.ArmDeferredGrantIndex == nil,
		d.ResourceParent == nil, d.ResourceByParentKey == nil:
		return fmt.Errorf("rawdb: RecordDerivers incomplete: every derivation function is load-bearing")
	case len(d.GrantPrimaryPrefix) == 0, len(d.ResourcePrimaryPrefix) == 0,
		len(d.EntitlementPrimaryPrefix) == 0, len(d.ResourceTypePrimaryPrefix) == 0:
		return fmt.Errorf("rawdb: RecordDerivers incomplete: family prefixes required for keyspace assertions")
	}
	return nil
}

// SetRecordDerivers wires the engine's derivation functions. Must be
// called exactly once, at engine Open, before any record staging.
func (d *DB) SetRecordDerivers(rd RecordDerivers) error {
	if err := rd.complete(); err != nil {
		return err
	}
	if d.derivers != nil {
		return fmt.Errorf("rawdb: RecordDerivers already set")
	}
	d.derivers = &rd
	return nil
}

func (d *DB) mustDerivers() *RecordDerivers {
	if d.derivers == nil {
		panic("rawdb: record staging before SetRecordDerivers — the engine must wire derivers at Open")
	}
	return d.derivers
}

func assertFamily(op string, key, prefix []byte) error {
	if len(key) < len(prefix) || string(key[:len(prefix)]) != string(prefix) {
		return fmt.Errorf("rawdb.%s: key %x is outside this op's keyspace family (want prefix %x) — use the family op that owns that keyspace", op, key, prefix)
	}
	return nil
}

// === grant staging ===

// StageGrantPutInline stages one grant row in the INLINE index regime
// and everything it owes: prior-row index cleanup (both families, when
// hadOldVal — grant index keys derive from the primary key, which IS
// the identity encoding, so cleanup needs only an existence fact, not
// the prior bytes), the primary row, the by_principal entry, the
// by_needs_expansion entry (when needsExpansion), and the digest
// invalidation. needsExpansion comes from the caller's decoded record
// (the write path has it; deriving it here would force a value scan).
// Contrast StageResourcePut, which takes the prior VALUE bytes —
// resource index keys derive from the value (parent ref), not the key.
func (rb *RecordBatch) StageGrantPutInline(key, val []byte, hadOldVal, needsExpansion bool) error {
	d := rb.db.mustDerivers()
	if err := assertFamily("StageGrantPutInline", key, d.GrantPrimaryPrefix); err != nil {
		return err
	}
	if !d.GrantKeyValid(key) {
		return fmt.Errorf("rawdb.StageGrantPutInline: grant key %x did not decode as a 6-segment identity", key)
	}
	if hadOldVal {
		if err := rb.deleteGrantIndexKey(d.GrantByPrincipalKey, key); err != nil {
			return err
		}
		if err := rb.deleteGrantIndexKey(d.GrantNeedsExpansionKey, key); err != nil {
			return err
		}
	}
	if err := rb.core.b.Set(key, val, nil); err != nil {
		return err
	}
	if err := rb.setGrantIndexKey(d.GrantByPrincipalKey, key); err != nil {
		return err
	}
	if needsExpansion {
		if err := rb.setGrantIndexKey(d.GrantNeedsExpansionKey, key); err != nil {
			return err
		}
	}
	return d.StageGrantDigestInvalidation(&rb.stager, key)
}

// StageGrantDelete stages one grant row's removal (INLINE regime:
// post-seal deletes on files whose by_principal index is live) and
// everything it owes: both index entries' cleanup and the digest
// invalidation.
//
// INTENTIONAL TIGHTENING vs the pre-2b helpers (review-verified): the
// old value-derived cleanup silently SKIPPED index deletes AND the
// digest invalidation when the prior value's identity fields were
// missing (malformed legacy rows) — deleting the primary while leaving
// a stale-but-present digest, a present-means-exact hole. Key-derived
// cleanup cannot be skipped; tombstones on absent index keys are
// harmless.
func (rb *RecordBatch) StageGrantDelete(key []byte) error {
	d := rb.db.mustDerivers()
	if err := assertFamily("StageGrantDelete", key, d.GrantPrimaryPrefix); err != nil {
		return err
	}
	if !d.GrantKeyValid(key) {
		return fmt.Errorf("rawdb.StageGrantDelete: grant key %x did not decode as a 6-segment identity", key)
	}
	if err := rb.deleteGrantIndexKey(d.GrantByPrincipalKey, key); err != nil {
		return err
	}
	if err := rb.deleteGrantIndexKey(d.GrantNeedsExpansionKey, key); err != nil {
		return err
	}
	if err := rb.core.b.Delete(key, nil); err != nil {
		return err
	}
	return d.StageGrantDigestInvalidation(&rb.stager, key)
}

// StageGrantPutDeferred stages one grant row in the DEFERRED index
// regime: the durable rebuild marker is armed first (crash contract —
// an interrupted sync must rebuild at its resumed EndSync), only the
// by_needs_expansion entry is maintained inline, overwrite cleanup
// deliberately skips by_principal (the family is excised and rebuilt
// wholesale at seal, which also clears stale entries), and the digest
// invalidation is staged. hadOldVal selects overwrite cleanup of the
// needs_expansion entry.
func (rb *RecordBatch) StageGrantPutDeferred(key, val []byte, hadOldVal, needsExpansion bool) error {
	d := rb.db.mustDerivers()
	if err := assertFamily("StageGrantPutDeferred", key, d.GrantPrimaryPrefix); err != nil {
		return err
	}
	if !d.GrantKeyValid(key) {
		return fmt.Errorf("rawdb.StageGrantPutDeferred: grant key %x did not decode as a 6-segment identity", key)
	}
	if err := d.ArmDeferredGrantIndex(); err != nil {
		return err
	}
	if hadOldVal {
		if err := rb.deleteGrantIndexKey(d.GrantNeedsExpansionKey, key); err != nil {
			return err
		}
	}
	if err := rb.core.b.Set(key, val, nil); err != nil {
		return err
	}
	if needsExpansion {
		if err := rb.setGrantIndexKey(d.GrantNeedsExpansionKey, key); err != nil {
			return err
		}
	}
	return d.StageGrantDigestInvalidation(&rb.stager, key)
}

func (rb *RecordBatch) setGrantIndexKey(derive func(dst, primaryKey []byte) ([]byte, bool), key []byte) error {
	idx, ok := derive(rb.scratch[:0], key)
	rb.scratch = idx
	if !ok {
		return fmt.Errorf("rawdb: grant key %x did not decode as a 6-segment identity", key)
	}
	return rb.core.b.Set(idx, nil, nil)
}

func (rb *RecordBatch) deleteGrantIndexKey(derive func(dst, primaryKey []byte) ([]byte, bool), key []byte) error {
	idx, ok := derive(rb.scratch[:0], key)
	rb.scratch = idx
	if !ok {
		return fmt.Errorf("rawdb: grant key %x did not decode as a 6-segment identity", key)
	}
	return rb.core.b.Delete(idx, nil)
}

// recordStager adapts RecordBatch's internal staging to the Stager
// interface for the injected digest-invalidation composer, WITHOUT
// exporting generic staging on RecordBatch itself.
type recordStager struct{ rb *RecordBatch }

func (s *recordStager) Set(key, val []byte) error { return s.rb.core.b.Set(key, val, nil) }
func (s *recordStager) Delete(key []byte) error   { return s.rb.core.b.Delete(key, nil) }
func (s *recordStager) DeleteRange(a, b []byte) error {
	return s.rb.core.b.DeleteRange(a, b, nil)
}

// === resource staging ===

// StageResourcePut stages one resource row plus its by_parent index
// obligations: the prior value's entry is deleted when oldVal is
// non-nil, the new value's entry is written when the record has a
// parent. childRT/childID are the record's identity (the caller has
// them decoded; they address, they are not obligations). Contrast
// StageGrantPutInline, which takes only an existence bool — grant
// index keys derive from the primary key, not the value.
func (rb *RecordBatch) StageResourcePut(key, val, oldVal []byte, childRT, childID string) error {
	d := rb.db.mustDerivers()
	if err := assertFamily("StageResourcePut", key, d.ResourcePrimaryPrefix); err != nil {
		return err
	}
	if oldVal != nil {
		if err := rb.stageResourceParentDelete(d, oldVal, childRT, childID); err != nil {
			return err
		}
	}
	if err := rb.core.b.Set(key, val, nil); err != nil {
		return err
	}
	parentRT, parentID, err := d.ResourceParent(val)
	if err != nil {
		return err
	}
	if parentID == "" {
		return nil
	}
	return rb.core.b.Set(d.ResourceByParentKey(parentRT, parentID, childRT, childID), nil, nil)
}

// StageResourceDelete stages one resource row's removal plus its
// by_parent index cleanup (from the prior value).
func (rb *RecordBatch) StageResourceDelete(key, oldVal []byte, childRT, childID string) error {
	d := rb.db.mustDerivers()
	if err := assertFamily("StageResourceDelete", key, d.ResourcePrimaryPrefix); err != nil {
		return err
	}
	if err := rb.stageResourceParentDelete(d, oldVal, childRT, childID); err != nil {
		return err
	}
	return rb.core.b.Delete(key, nil)
}

func (rb *RecordBatch) stageResourceParentDelete(d *RecordDerivers, oldVal []byte, childRT, childID string) error {
	parentRT, parentID, err := d.ResourceParent(oldVal)
	if err != nil {
		return err
	}
	if parentID == "" {
		return nil
	}
	return rb.core.b.Delete(d.ResourceByParentKey(parentRT, parentID, childRT, childID), nil)
}

// === entitlement / resource-type staging ===
//
// Neither family owes inline index entries. The typed ops exist so the
// generic primitives stay unexported and the keyspace assertion runs.
// The engine-side bare-id lookup invalidation
// (noteEntitlementKeyspaceWrite) is an in-memory ENGINE-state
// obligation and stays with the engine's write paths.

// StageEntitlementPut stages one entitlement row.
func (rb *RecordBatch) StageEntitlementPut(key, val []byte) error {
	d := rb.db.mustDerivers()
	if err := assertFamily("StageEntitlementPut", key, d.EntitlementPrimaryPrefix); err != nil {
		return err
	}
	return rb.core.b.Set(key, val, nil)
}

// StageEntitlementDelete stages one entitlement row's removal.
func (rb *RecordBatch) StageEntitlementDelete(key []byte) error {
	d := rb.db.mustDerivers()
	if err := assertFamily("StageEntitlementDelete", key, d.EntitlementPrimaryPrefix); err != nil {
		return err
	}
	return rb.core.b.Delete(key, nil)
}

// StageResourceTypePut stages one resource-type row.
func (rb *RecordBatch) StageResourceTypePut(key, val []byte) error {
	d := rb.db.mustDerivers()
	if err := assertFamily("StageResourceTypePut", key, d.ResourceTypePrimaryPrefix); err != nil {
		return err
	}
	return rb.core.b.Set(key, val, nil)
}

// StageResourceTypeDelete stages one resource-type row's removal.
func (rb *RecordBatch) StageResourceTypeDelete(key []byte) error {
	d := rb.db.mustDerivers()
	if err := assertFamily("StageResourceTypeDelete", key, d.ResourceTypePrimaryPrefix); err != nil {
		return err
	}
	return rb.core.b.Delete(key, nil)
}

// === fold family: the compactor's keep-newer merge ===
//
// The one surface that legitimately stages RAW keys it did not encode:
// the fold copies winner rows and index entries verbatim from source
// files and deletes incumbents' stale entries, with keys derived by
// the synccompactor's own splice helpers (or borrowed byte-exact from
// the sources). Generic by design — and confined by design: FoldBatch
// is reachable only through Engine.DB() (the documented exemption
// surface), and the engine package's meta-test forbids raw-write
// signals in engine production code.
type FoldBatch struct {
	batch
}

// NewFoldBatch mints a generic staged batch for the compactor's
// keep-newer fold and overlay writers. Engine production code must
// not use it; see the choke-point meta-tests.
func (d *DB) NewFoldBatch() *FoldBatch { return &FoldBatch{batch{b: d.newBatch()}} }

var _ Stager = (*recordStager)(nil)
var _ = pebble.Sync
