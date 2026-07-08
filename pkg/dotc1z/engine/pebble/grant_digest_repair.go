package pebble

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"

	"github.com/cockroachdb/pebble/v2"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

// Targeted repair of the grant hash index + digests: heal exactly the
// entitlement partitions missing a digest root, trusting every
// partition that already has one, instead of the seal-time build's
// unconditional full-file rebuild. This is the load-bearing primitive
// behind compaction's fold strategy carrying built digests without
// paying an O(base) rebuild on every fold (see
// InvalidateGrantDigestPartitions and RepairMissingGrantDigests below,
// and compactPebbleFold in pkg/synccompactor).
//
// Contract: present-means-exact (digest.go) still holds throughout —
// nothing here ever leaves a partition looking present with wrong
// data. A partition is either fully correct (untouched, or freshly
// rebuilt from its own primaries) or absent (never repaired, never
// half-built); the whole-file global root is recomputed whenever
// anything changes, from the digest keyspace itself (cheap — bounded
// by entitlement count, not grant count), so it can never drift from
// the sum of what's actually stored.

// GrantPartitionFromPrimaryKey returns the entitlement partition — the
// same string digestPartitionForEntitlement (and this file's repair
// functions) key off — that a raw grant primary key belongs to, by
// splicing the key rather than decoding it. For callers that mutate
// grants through the raw DB handle and need to report which
// partitions they touched without ever constructing an
// entitlementIdentity (the fold compactor's raw merge — see
// InvalidateGrantDigestPartitions). ok is false for a key that does
// not parse as a 6-segment grant identity.
func GrantPartitionFromPrimaryKey(key []byte) (string, bool) {
	sep4, ok := splitGrantPrimaryKey(key)
	if !ok {
		return "", false
	}
	return string(key[grantPrimaryKeyPrefixLen:sep4]), true
}

// grantPrimaryEntitlementBoundsFromPartition returns the primary grant
// key range for one entitlement partition, derived directly from the
// raw partition bytes. Identical to what encodeGrantPrimaryEntitlementPrefix
// produces from a decoded identity, since partition IS that identity's
// encoded tail (digestPartitionForEntitlement) — this just skips
// needing an entitlementIdentity value to get there.
func grantPrimaryEntitlementBoundsFromPartition(partition string) ([]byte, []byte) {
	lower := make([]byte, 0, grantPrimaryKeyPrefixLen+len(partition)+1)
	lower = append(lower, versionV3, typeGrant, 0)
	lower = append(lower, partition...)
	lower = append(lower, 0)
	return lower, upperBoundOf(lower)
}

// isGrantDigestRootKey reports whether key is a per-entitlement grant-
// digest ROOT node (level digestLevelRoot) for the grant digest index
// specifically — the unit recomputeGrantDigestGlobalRootLocked folds
// into the whole-file root. The global root's own key uses
// digestLevelGlobalRoot, a different level value, so it never matches
// here by construction (globalGrantDigestNodeKey). Finding the level
// byte by scanning for the next bare 0x00 after indexID is safe
// because the partition region is TUPLE-ESCAPED (encodeDigestPartitionPrefix),
// unlike the hash-index key's raw splice — a bare 0x00 there can only
// be the separator ending it, never embedded partition data.
func isGrantDigestRootKey(key []byte) bool {
	const headerLen = 3 // versionV3, typeDigest, indexID
	if len(key) < headerLen+1 || key[0] != versionV3 || key[1] != typeDigest || key[2] != grantDigestSpec.indexID {
		return false
	}
	if key[headerLen] != 0 {
		return false
	}
	_, afterSep, found := bytes.Cut(key[headerLen+1:], []byte{0})
	if !found {
		return false
	}
	return len(afterSep) == 1 && afterSep[0] == digestLevelRoot
}

// InvalidateGrantDigestPartitions drops the digest + hash-index state
// for exactly the given entitlement partitions, plus the whole-file
// global root — which folds over every partition and goes stale the
// moment any one of them does. Every other partition's state is left
// completely untouched.
//
// For callers that mutate the primary grant keyspace directly,
// bypassing the engine's own per-record invalidation path
// (stageGrantDigestInvalidation) — the fold compactor's raw merge — to
// report exactly what they changed. RepairMissingGrantDigests
// recomputes exactly what this drops (and only that), so the pair
// gives compaction per-entitlement-scoped digest maintenance without
// an O(file) rebuild. No-op when partitions is empty or no digest has
// ever been built for this file.
func (e *Engine) InvalidateGrantDigestPartitions(ctx context.Context, partitions []string) error {
	if len(partitions) == 0 || !e.grantDigestsPresent.Load() {
		return nil
	}
	return e.withWrite(func() error {
		batch := e.db.NewBatch()
		defer batch.Close()
		for _, partition := range partitions {
			if err := dropPartitionDigest(batch, grantDigestSpec, partition); err != nil {
				return err
			}
			lo := encodeGrantByEntPrincHashEntPrefix(partition)
			if err := batch.DeleteRange(lo, upperBoundOf(lo), nil); err != nil {
				return err
			}
		}
		if err := batch.Delete(globalGrantDigestNodeKey(), nil); err != nil {
			return err
		}
		opts := writeOpts(e.opts.durability)
		if e.IsFreshSync() {
			opts = pebble.NoSync
		}
		return batch.Commit(opts)
	})
}

// RepairMissingGrantDigests rebuilds the by_entitlement_principal_hash
// rows and digest nodes for exactly the entitlements missing a digest
// root — trusting every entitlement that already has one — then
// recomputes the whole-file global root if anything changed. The
// whole operation holds the engine's write lock for its entire
// duration, like BuildGrantDigests/BuildDeferredGrantIndexes, so no
// concurrent grant write can interleave with a partition mid-repair
// and leave it silently wrong.
//
// When digests were never built for this file at all (or were dropped
// wholesale — grantDigestsPresent false), a full BuildGrantDigests is
// strictly cheaper than a point-Get per entitlement that would all
// miss anyway, so this delegates to it instead.
//
// Failure policy: a repair error for one partition is logged and that
// partition is left missing (safe — present-means-exact — and
// self-healing on the next repair or seal) rather than failing the
// whole call; only context cancellation propagates. Unlike a fused
// full rebuild, a partial failure here does not require dropping
// already-repaired partitions — each one commits its own correct,
// complete state independently.
//
// Orphan grants (an entitlement identity with grants but no
// entitlement RECORD) are not discovered by the missing-partition scan
// below, which walks entitlement records — matching the scope of
// writeMissingEntitlementDigestRoots's own backfill. An orphan that
// already has a valid root from an earlier full build is still
// correctly folded into the recomputed global root, since that step
// scans the digest keyspace itself, not entitlement records.
//
// Called from Adapter.endSyncFinalize in place of a bare
// BuildGrantDigests whenever the deferred (by_principal-rebuilding)
// pass didn't run: for a brand-new sync grantDigestsPresent is always
// false (ResetForNewSync excises the digest keyspace at StartNewSync),
// so this reduces to exactly BuildGrantDigests's existing full scan —
// no behavior change for the common single-EndSync-per-sync case. It
// only diverges — into the cheap fast path or the targeted repair —
// when EndSync runs a SECOND time on an already-digested, rebound sync
// (SetCurrentSync, not StartNewSync) without a fresh reset in between:
// the shape grant-expansion's own follow-up sync produces, and exactly
// the case that made a full rebuild here wasteful (see RFC 0003 /
// compactPebbleFold).
//
// Like BuildGrantDigests, a failure that isn't context cancellation is
// logged and downgraded to a full state drop rather than propagated —
// digest problems must never fail EndSync. This only covers failures
// in the outer scan/orchestration (e.g. the entitlement-keyspace scan
// itself erroring): a failure repairing one INDIVIDUAL partition is
// already handled without dropping anything (see
// repairMissingGrantDigestsLocked).
func (e *Engine) RepairMissingGrantDigests(ctx context.Context) error {
	if !e.opts.grantDigestIndex {
		return nil
	}
	if !e.grantDigestsPresent.Load() {
		return e.BuildGrantDigests(ctx)
	}
	err := e.repairMissingGrantDigestsAttempt(ctx)
	if err == nil {
		return nil
	}
	if ctx.Err() != nil {
		return err
	}
	ctxzap.Extract(ctx).Error("grant digest repair failed; dropping digest state — grant-diff callers must re-read grants until the next successful repair or seal",
		zap.Error(err))
	if dropErr := e.withWriteAllowSealed(func() error { return e.dropAllGrantDigestStateLocked() }); dropErr != nil {
		return fmt.Errorf("RepairMissingGrantDigests: drop grant digest state after failed repair: %w", dropErr)
	}
	return nil
}

// repairMissingGrantDigestsAttempt is the un-wrapped repair attempt:
// the fast path (trust a present global root) plus the locked
// scan-and-repair. RepairMissingGrantDigests applies the uniform
// "downgrade to a full drop, log, never fail the caller" policy to
// whatever this returns.
func (e *Engine) repairMissingGrantDigestsAttempt(ctx context.Context) error {
	// Fast path: EVERY code path that can make a single entitlement's
	// digest go missing also drops the whole-file global root in the
	// same commit (stageGrantDigestInvalidation, InvalidateGrantDigestPartitions,
	// the Drop* family — see their doc comments), and the loop below
	// only ever writes the global root back once it has verified NOTHING
	// is missing (repaired everything it found, zero failures). So the
	// root's mere presence certifies every entitlement's digest is
	// present and correct, without walking the entitlement keyspace at
	// all: one point Get instead of a scan, for what should be the
	// overwhelmingly common "nothing to repair" case (e.g. a fold that
	// invalidated nothing, or a periodic health-check call).
	if _, ok, err := e.GetGrantDigestGlobalRoot(ctx); err != nil {
		return err
	} else if ok {
		return nil
	}
	return e.withWriteAllowSealed(func() error {
		return e.repairMissingGrantDigestsLocked(ctx)
	})
}

func (e *Engine) repairMissingGrantDigestsLocked(ctx context.Context) error {
	l := ctxzap.Extract(ctx)
	missing, err := e.findMissingGrantDigestPartitionsLocked(ctx)
	if err != nil {
		return err
	}
	var repaired, failed int
	for _, partition := range missing {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := e.repairOneGrantDigestPartitionLocked(ctx, partition); err != nil {
			if ctx.Err() != nil {
				return err
			}
			l.Error("grant digest repair: rebuilding one entitlement partition failed; it remains missing (recalculated on the next repair or seal)",
				zap.Error(err))
			failed++
			continue
		}
		repaired++
	}
	if repaired > 0 || failed > 0 {
		l.Info("grant digest repair: rebuilt missing entitlement partitions",
			zap.Int("repaired", repaired),
			zap.Int("failed", failed),
			zap.Int("candidates", len(missing)),
		)
	}
	if failed > 0 {
		// At least one entitlement is still missing: the whole-file
		// root must stay absent too. Writing it now — even folded only
		// over what IS present — would make RepairMissingGrantDigests's
		// own fast path trust an incomplete file as fully repaired on
		// its next call.
		return nil
	}
	if err := e.recomputeGrantDigestGlobalRootLocked(ctx); err != nil {
		if ctx.Err() != nil {
			return err
		}
		l.Error("grant digest repair: recomputing the whole-file root failed; it remains missing (recalculated on the next repair or seal)",
			zap.Error(err))
	}
	return nil
}

// findMissingGrantDigestPartitionsLocked scans every entitlement
// record and returns the partitions with no stored digest root: one
// point Get per entitlement, no grant scan unless (in the caller) a
// partition turns out to be missing. Orphan grant-bearing entitlements
// without a record are not covered — see RepairMissingGrantDigests's
// doc comment.
func (e *Engine) findMissingGrantDigestPartitionsLocked(ctx context.Context) ([]string, error) {
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: EntitlementLowerBound(),
		UpperBound: EntitlementUpperBound(),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	var missing []string
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		key := iter.Key()
		if len(key) <= grantPrimaryKeyPrefixLen {
			continue
		}
		// The entitlement primary tail IS the digest partition (see the
		// partition convention in keys.go) — a raw splice, no decode.
		partition := string(key[grantPrimaryKeyPrefixLen:])
		rootKey := encodeDigestNodeKey(grantDigestSpec.indexID, partition, digestLevelRoot, nil)
		if _, closer, err := e.db.Get(rootKey); err == nil {
			closer.Close()
			continue
		} else if !errors.Is(err, pebble.ErrNotFound) {
			return nil, err
		}
		missing = append(missing, partition)
	}
	return missing, iter.Error()
}

// repairOneGrantDigestPartitionLocked rebuilds BOTH the hash-index
// rows and the digest nodes for one entitlement partition, from a scan
// of its primary grants — it does NOT assume the hash index has
// anything usable for this partition (invalidation always drops both
// together, see InvalidateGrantDigestPartitions /
// stageGrantDigestInvalidation). Must be called with the engine's
// write lock already held (RepairMissingGrantDigests): it commits
// through the raw DB handle directly rather than nesting another
// withWrite call.
//
// One entitlement's grants are expected to fit comfortably in memory
// (this is a targeted repair, not the seal-time build sized for the
// whole file), so this sorts them directly instead of spilling to
// disk like the deferred build's spill-sorter.
func (e *Engine) repairOneGrantDigestPartitionLocked(ctx context.Context, partition string) error {
	lower, upper := grantPrimaryEntitlementBoundsFromPartition(partition)

	type hashRow struct {
		key []byte
		val [hashLen]byte
	}
	var rows []hashRow
	iter, err := e.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return err
	}
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			_ = iter.Close()
			return err
		}
		key, value := iter.Key(), iter.Value()
		sep4, ok := splitGrantPrimaryKey(key)
		if !ok {
			continue
		}
		srcs, serr := scanGrantSourceKeysRawBytes(value, nil)
		if serr != nil {
			_ = iter.Close()
			return serr
		}
		if len(srcs) > 1 {
			sortByteSlices(srcs)
		}
		ch64, _ := grantContentHash64(nil, key[grantPrimaryKeyPrefixLen:], srcs)
		bh64 := grantPrincipalBucketHash64(key[sep4+1:])
		idxKey := appendGrantHashIndexKeyFromPrimary(nil, key, sep4, bh64)
		var row hashRow
		row.key = idxKey
		binary.BigEndian.PutUint64(row.val[:], ch64)
		rows = append(rows, row)
	}
	if err := iter.Error(); err != nil {
		_ = iter.Close()
		return err
	}
	if err := iter.Close(); err != nil {
		return err
	}

	sort.Slice(rows, func(i, j int) bool { return bytes.Compare(rows[i].key, rows[j].key) < 0 })

	opts := writeOpts(e.opts.durability)
	if e.IsFreshSync() {
		opts = pebble.NoSync
	}

	// Pass 1: replace the hash-index range with the freshly-derived
	// rows (a clean DeleteRange first: nothing may assume what, if
	// anything, was left there).
	hiBatch := e.db.NewBatch()
	hiLower := encodeGrantByEntPrincHashEntPrefix(partition)
	if err := hiBatch.DeleteRange(hiLower, upperBoundOf(hiLower), nil); err != nil {
		hiBatch.Close()
		return err
	}
	for i := range rows {
		if err := hiBatch.Set(rows[i].key, rows[i].val[:], nil); err != nil {
			hiBatch.Close()
			return err
		}
	}
	if err := hiBatch.Commit(opts); err != nil {
		hiBatch.Close()
		return err
	}
	hiBatch.Close()

	// Pass 2: fold the just-committed hash index into the digest,
	// reusing the exact fold logic the seal-time build runs
	// (foldPartitionNodes) instead of re-deriving the same computation
	// a second way from the in-memory rows.
	width := chooseDigestWidth(int64(len(rows)))
	rootVal, leaves, err := e.foldPartitionNodes(ctx, grantDigestSpec, partition, width)
	if err != nil {
		return err
	}
	digestBatch := e.db.NewBatch()
	defer digestBatch.Close()
	digestLower := encodeDigestPartitionPrefix(grantDigestSpec.indexID, partition)
	if err := digestBatch.DeleteRange(digestLower, upperBoundOf(digestLower), nil); err != nil {
		return err
	}
	rootKey := encodeDigestNodeKey(grantDigestSpec.indexID, partition, digestLevelRoot, nil)
	if err := digestBatch.Set(rootKey, rootVal, nil); err != nil {
		return err
	}
	for i := range leaves {
		leafKey := encodeDigestNodeKey(grantDigestSpec.indexID, partition, digestLevelLeaf, leaves[i].prefix[:])
		if err := digestBatch.Set(leafKey, leaves[i].val, nil); err != nil {
			return err
		}
	}
	if err := digestBatch.Commit(opts); err != nil {
		return err
	}
	e.grantDigestsPresent.Store(true)
	return nil
}

// recomputeGrantDigestGlobalRootLocked folds every CURRENTLY STORED
// per-entitlement grant-digest root into the whole-file global root
// and writes it — a scan of the (small) digest ROOT keyspace, not the
// grants, so it stays cheap even for a whale-scale file (bounded by
// entitlement count, not grant count). Must be called with the
// engine's write lock already held.
func (e *Engine) recomputeGrantDigestGlobalRootLocked(ctx context.Context) error {
	var xor [hashLen]byte
	var total int64
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: DigestLowerBound(),
		UpperBound: DigestUpperBound(),
	})
	if err != nil {
		return err
	}
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			_ = iter.Close()
			return err
		}
		if !isGrantDigestRootKey(iter.Key()) {
			continue
		}
		_, count, digest, ok := unpackDigestRoot(iter.Value())
		if !ok {
			_ = iter.Close()
			return fmt.Errorf("recomputeGrantDigestGlobalRoot: malformed root at %x", iter.Key())
		}
		xorInto(xor[:], digest)
		total += count
	}
	if err := iter.Error(); err != nil {
		_ = iter.Close()
		return err
	}
	if err := iter.Close(); err != nil {
		return err
	}
	opts := writeOpts(e.opts.durability)
	if e.IsFreshSync() {
		opts = pebble.NoSync
	}
	if err := e.db.Set(globalGrantDigestNodeKey(), packDigestLeaf(total, xor[:]), opts); err != nil {
		return err
	}
	e.grantDigestsPresent.Store(true)
	return nil
}
