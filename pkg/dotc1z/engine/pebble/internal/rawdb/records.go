package rawdb

// Typed record-keyspace staging with obligation derivation FOLDED INTO
// the ops. The forgotten-obligation bug class — a caller staging a
// primary row and forgetting its index entries, digest invalidation,
// prior-row index cleanup, or the deferred-index crash marker — dies
// here: RecordBatch exposes no generic staging, and each Stage* op
// computes and stages everything its mutation owes in the same call,
// deriving directly from the keyspace ABI this package owns
// (keyspace.go).
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
//     marker is armed FIRST (ArmDeferredGrantIndex — CAS-cheap per
//     record, crash-contract-ordered before the batch can commit),
//     only by_needs_expansion is written inline, and overwrite cleanup
//     deliberately skips by_principal (the whole family is excised and
//     rebuilt at seal). The expanded/synth grant writers.
//
// The derivation contract is BYTE-LEVEL and KEY-DERIVED: index keys
// come from the encoded grant primary key by splice (by_principal:
// segment permutation; by_needs_expansion: header swap; digest
// partition: sub-slice) — the primary key IS the identity encoding,
// so key-derived and value-derived obligations are identical by
// construction, and the splices are pinned against decode+re-encode
// by the engine's TestAppendGrantByPrincipalKeyFromPrimary /
// TestNeedsExpansionKeyHeaderSpliceFromPrimary.
//
// Every op asserts its key's family prefix — a Stage* call with a key
// from the wrong keyspace fails loudly rather than landing where it
// doesn't belong.

import (
	"fmt"
)

// Family prefixes for the keyspace assertions.
var (
	grantPrimaryPrefix        = []byte{VersionV3, TypeGrant}
	resourcePrimaryPrefix     = []byte{VersionV3, TypeResource}
	entitlementPrimaryPrefix  = []byte{VersionV3, TypeEntitlement}
	resourceTypePrimaryPrefix = []byte{VersionV3, TypeResourceType}
)

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
	if err := assertFamily("StageGrantPutInline", key, grantPrimaryPrefix); err != nil {
		return err
	}
	sep4, ok := SplitGrantPrimaryKey(key)
	if !ok {
		return fmt.Errorf("rawdb.StageGrantPutInline: grant key %x did not decode as a 6-segment identity", key)
	}
	if hadOldVal {
		if err := rb.deleteByPrincipalKey(key); err != nil {
			return err
		}
		if err := rb.deleteNeedsExpansionKey(key); err != nil {
			return err
		}
	}
	if err := rb.core.b.Set(key, val, nil); err != nil {
		return err
	}
	if err := rb.setByPrincipalKey(key); err != nil {
		return err
	}
	if needsExpansion {
		if err := rb.setNeedsExpansionKey(key); err != nil {
			return err
		}
	}
	return rb.stageGrantDigestInvalidation(key, sep4)
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
	if err := assertFamily("StageGrantDelete", key, grantPrimaryPrefix); err != nil {
		return err
	}
	sep4, ok := SplitGrantPrimaryKey(key)
	if !ok {
		return fmt.Errorf("rawdb.StageGrantDelete: grant key %x did not decode as a 6-segment identity", key)
	}
	if err := rb.deleteByPrincipalKey(key); err != nil {
		return err
	}
	if err := rb.deleteNeedsExpansionKey(key); err != nil {
		return err
	}
	if err := rb.core.b.Delete(key, nil); err != nil {
		return err
	}
	return rb.stageGrantDigestInvalidation(key, sep4)
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
	if err := assertFamily("StageGrantPutDeferred", key, grantPrimaryPrefix); err != nil {
		return err
	}
	sep4, ok := SplitGrantPrimaryKey(key)
	if !ok {
		return fmt.Errorf("rawdb.StageGrantPutDeferred: grant key %x did not decode as a 6-segment identity", key)
	}
	if err := rb.db.ArmDeferredGrantIndex(); err != nil {
		return err
	}
	if hadOldVal {
		if err := rb.deleteNeedsExpansionKey(key); err != nil {
			return err
		}
	}
	if err := rb.core.b.Set(key, val, nil); err != nil {
		return err
	}
	if needsExpansion {
		if err := rb.setNeedsExpansionKey(key); err != nil {
			return err
		}
	}
	return rb.stageGrantDigestInvalidation(key, sep4)
}

// StageGrantOrphanIndexHeal stages the removal of ONE orphan
// by_principal index entry — an index key whose grant primary row does
// not exist (a stranded write, never legitimate state). It takes the
// grant PRIMARY key the orphan entry derives from, and derives the
// index key with the same splice every maintained write uses, so the
// heal and the writers can never disagree about the bytes.
//
// Deliberately a distinct op rather than a flavor of StageGrantDelete:
// the primary row is ABSENT, so there is no primary delete to stage
// and no digest invalidation owed (digests fold primary rows; removing
// index garbage changes no partition content). Callers must verify the
// primary row's absence under the write lock before staging — healing
// a live row's index entry would orphan the row from its own index.
func (rb *RecordBatch) StageGrantOrphanIndexHeal(primaryKey []byte) error {
	if err := assertFamily("StageGrantOrphanIndexHeal", primaryKey, grantPrimaryPrefix); err != nil {
		return err
	}
	if _, ok := SplitGrantPrimaryKey(primaryKey); !ok {
		return fmt.Errorf("rawdb.StageGrantOrphanIndexHeal: grant key %x did not decode as a 6-segment identity", primaryKey)
	}
	return rb.deleteByPrincipalKey(primaryKey)
}

func (rb *RecordBatch) setByPrincipalKey(key []byte) error {
	idx, ok := AppendGrantByPrincipalKeyFromPrimary(rb.scratch[:0], key)
	rb.scratch = idx
	if !ok {
		return fmt.Errorf("rawdb: grant key %x did not decode as a 6-segment identity", key)
	}
	return rb.core.b.Set(idx, nil, nil)
}

func (rb *RecordBatch) deleteByPrincipalKey(key []byte) error {
	idx, ok := AppendGrantByPrincipalKeyFromPrimary(rb.scratch[:0], key)
	rb.scratch = idx
	if !ok {
		return fmt.Errorf("rawdb: grant key %x did not decode as a 6-segment identity", key)
	}
	return rb.core.b.Delete(idx, nil)
}

func (rb *RecordBatch) setNeedsExpansionKey(key []byte) error {
	idx, ok := AppendGrantByNeedsExpansionKeyFromPrimary(rb.scratch[:0], key)
	rb.scratch = idx
	if !ok {
		return fmt.Errorf("rawdb: grant key %x did not decode as a 6-segment identity", key)
	}
	return rb.core.b.Set(idx, nil, nil)
}

func (rb *RecordBatch) deleteNeedsExpansionKey(key []byte) error {
	idx, ok := AppendGrantByNeedsExpansionKeyFromPrimary(rb.scratch[:0], key)
	rb.scratch = idx
	if !ok {
		return fmt.Errorf("rawdb: grant key %x did not decode as a 6-segment identity", key)
	}
	return rb.core.b.Delete(idx, nil)
}

// stageGrantDigestInvalidation stages the post-seal digest
// invalidation a grant mutation owes for its entitlement partition,
// when digest state exists (present-means-exact — a mutated
// partition's digest must read as missing, never stale-but-present):
// DeleteRange over the partition's digest nodes, the whole-file global
// root (the fold of every partition root is stale once any partition
// is), and the partition's hash-index range. Gated on the
// digests-present flag so ordinary sync paths pay nothing — during a
// fresh sync both keyspaces are empty by construction, and emitting
// tombstones per record there would bloat the LSM for no reason. The
// partition is the entitlement region of the grant primary key, a
// plain sub-slice (sep4 from SplitGrantPrimaryKey on the same key).
func (rb *RecordBatch) stageGrantDigestInvalidation(primaryKey []byte, sep4 int) error {
	if !rb.db.grantDigestsPresent.Load() {
		return nil
	}
	partition := string(primaryKey[GrantPrimaryKeyPrefixLen:sep4])
	lo := DigestPartitionPrefix(IdxGrantByEntitlementPrincipalHash, partition)
	if err := rb.core.b.DeleteRange(lo, UpperBound(lo), nil); err != nil {
		return err
	}
	if err := rb.core.b.Delete(GlobalGrantDigestNodeKey(), nil); err != nil {
		return err
	}
	hlo := GrantHashIndexEntitlementPrefix(partition)
	return rb.core.b.DeleteRange(hlo, UpperBound(hlo), nil)
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
	if err := assertFamily("StageResourcePut", key, resourcePrimaryPrefix); err != nil {
		return err
	}
	if oldVal != nil {
		if err := rb.stageResourceParentDelete(oldVal, childRT, childID); err != nil {
			return err
		}
	}
	if err := rb.core.b.Set(key, val, nil); err != nil {
		return err
	}
	parentRT, parentID, err := ScanResourceParentRaw(val)
	if err != nil {
		return err
	}
	if parentID == "" {
		return nil
	}
	return rb.core.b.Set(EncodeResourceByParentIndexKey(parentRT, parentID, childRT, childID), nil, nil)
}

// StageResourceDelete stages one resource row's removal plus its
// by_parent index cleanup (from the prior value).
func (rb *RecordBatch) StageResourceDelete(key, oldVal []byte, childRT, childID string) error {
	if err := assertFamily("StageResourceDelete", key, resourcePrimaryPrefix); err != nil {
		return err
	}
	if err := rb.stageResourceParentDelete(oldVal, childRT, childID); err != nil {
		return err
	}
	return rb.core.b.Delete(key, nil)
}

func (rb *RecordBatch) stageResourceParentDelete(oldVal []byte, childRT, childID string) error {
	parentRT, parentID, err := ScanResourceParentRaw(oldVal)
	if err != nil {
		return err
	}
	if parentID == "" {
		return nil
	}
	return rb.core.b.Delete(EncodeResourceByParentIndexKey(parentRT, parentID, childRT, childID), nil)
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
	if err := assertFamily("StageEntitlementPut", key, entitlementPrimaryPrefix); err != nil {
		return err
	}
	return rb.core.b.Set(key, val, nil)
}

// StageEntitlementDelete stages one entitlement row's removal.
func (rb *RecordBatch) StageEntitlementDelete(key []byte) error {
	if err := assertFamily("StageEntitlementDelete", key, entitlementPrimaryPrefix); err != nil {
		return err
	}
	return rb.core.b.Delete(key, nil)
}

// StageResourceTypePut stages one resource-type row.
func (rb *RecordBatch) StageResourceTypePut(key, val []byte) error {
	if err := assertFamily("StageResourceTypePut", key, resourceTypePrimaryPrefix); err != nil {
		return err
	}
	return rb.core.b.Set(key, val, nil)
}

// StageResourceTypeDelete stages one resource-type row's removal.
func (rb *RecordBatch) StageResourceTypeDelete(key []byte) error {
	if err := assertFamily("StageResourceTypeDelete", key, resourceTypePrimaryPrefix); err != nil {
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
// is reachable only through the engine's documented exemption surface,
// and the engine package's meta-test fences its production use to the
// fold compactor.
type FoldBatch struct {
	batch
}

// NewFoldBatch mints a generic staged batch for the compactor's
// keep-newer fold and overlay writers. Engine production code must
// not use it; see the choke-point meta-tests.
func (d *DB) NewFoldBatch() *FoldBatch { return &FoldBatch{batch{b: d.newBatch()}} }
