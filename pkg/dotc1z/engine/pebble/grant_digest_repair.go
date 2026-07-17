package pebble

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"

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
		batch := e.db.NewDigestBatch()
		defer batch.Close()
		for _, partition := range partitions {
			if err := dropPartitionDigest(batch, grantDigestSpec, partition); err != nil {
				return err
			}
			lo := encodeGrantByEntPrincHashEntPrefix(partition)
			if err := batch.DeleteRange(lo, upperBoundOf(lo)); err != nil {
				return err
			}
		}
		if err := batch.Delete(globalGrantDigestNodeKey()); err != nil {
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
// entitlement RECORD) are covered: the missing-partition scan
// discovers grant-bearing partitions from the grant primary keyspace
// itself, not from entitlement records, so an invalidated orphan is
// rebuilt like any other partition and the recomputed global root
// always includes it. That keeps the global root a pure function of
// the stored grant set — identical whether it was produced by a seal
// (whose fold covers orphans too) or by a fold + targeted repair —
// which the manifest's header-only "did the grants change?"
// comparison depends on. Without it, a touched orphan would go
// missing forever: the recomputed root's presence would satisfy the
// repair fast path (repairMissingGrantDigestsAttempt) on every later
// call.
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
	if e.grantDigestBuildPending.Load() {
		// A prior digest build died mid-commit and the drop that should
		// have consumed the durable marker never completed (a writable
		// Open normally does it; in-process, a failed build's own
		// cleanup). Whatever nodes it left look present over a hash
		// index that was never ingested, so nothing stored is
		// trustworthy — restore the safe absent state first and let the
		// full rebuild below recalculate from scratch.
		if err := e.withWriteAllowSealed(func() error { return e.dropAllGrantDigestStateLocked() }); err != nil {
			return fmt.Errorf("RepairMissingGrantDigests: drop digest state left by an interrupted build: %w", err)
		}
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

// findMissingGrantDigestPartitionsLocked returns the partitions with
// no stored digest root, in two passes. Pass 1 discovers every
// grant-BEARING partition from the grant primary keyspace itself —
// one boundary seek plus one root point-Get per partition, so still
// bounded by partition count, not grant count. Deriving these from
// the grants rather than the entitlement records is what makes orphan
// partitions (grants with no entitlement record) repairable: the
// seal-time fold includes orphans in the global root, so a repair
// that couldn't rediscover an invalidated orphan would recompute a
// root that represents the same grant set differently (see
// RepairMissingGrantDigests's doc comment). Pass 2 walks the
// entitlement records (one point Get each) and adds only what pass 1
// can't see: entitlements with zero grants, whose {count: 0} root
// (writeMissingEntitlementDigestRoots) may equally have been
// invalidated — e.g. by deleting a partition's last grant.
func (e *Engine) findMissingGrantDigestPartitionsLocked(ctx context.Context) ([]string, error) {
	var missing []string
	missingSet := map[string]struct{}{}

	giter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: GrantLowerBound(),
		UpperBound: GrantUpperBound(),
	})
	if err != nil {
		return nil, err
	}
	defer giter.Close()
	for valid := giter.First(); valid; {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		partition, ok := GrantPartitionFromPrimaryKey(giter.Key())
		if !ok {
			// Malformed key: no partition to derive seek bounds from, so
			// step one key. (One inside a well-formed partition's range
			// is jumped by the seek below instead, and counted when that
			// partition is repaired — repairOneGrantDigestPartitionLocked.)
			valid = giter.Next()
			continue
		}
		present, err := e.grantDigestRootPresent(partition)
		if err != nil {
			return nil, err
		}
		if !present {
			missing = append(missing, partition)
			missingSet[partition] = struct{}{}
		}
		_, partitionUpper := grantPrimaryEntitlementBoundsFromPartition(partition)
		valid = giter.SeekGE(partitionUpper)
	}
	if err := giter.Error(); err != nil {
		return nil, err
	}

	eiter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: EntitlementLowerBound(),
		UpperBound: EntitlementUpperBound(),
	})
	if err != nil {
		return nil, err
	}
	defer eiter.Close()
	for eiter.First(); eiter.Valid(); eiter.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		key := eiter.Key()
		if len(key) <= grantPrimaryKeyPrefixLen {
			continue
		}
		// The entitlement primary tail IS the digest partition (see the
		// partition convention in keys.go) — a raw splice, no decode.
		partition := string(key[grantPrimaryKeyPrefixLen:])
		if _, dup := missingSet[partition]; dup {
			continue
		}
		present, err := e.grantDigestRootPresent(partition)
		if err != nil {
			return nil, err
		}
		if !present {
			missing = append(missing, partition)
		}
	}
	return missing, eiter.Error()
}

// grantDigestRootPresent reports whether partition has a stored grant-
// digest root node: one point Get, no value read.
func (e *Engine) grantDigestRootPresent(partition string) (bool, error) {
	rootKey := encodeDigestNodeKey(grantDigestSpec.indexID, partition, digestLevelRoot, nil)
	_, closer, err := e.db.Get(rootKey)
	if err == nil {
		closer.Close()
		return true, nil
	}
	if errors.Is(err, pebble.ErrNotFound) {
		return false, nil
	}
	return false, err
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
// The scan STREAMS each derived row straight into the hash-index
// batch, rotated at digestNodeBatchFlushBytes, instead of buffering
// the partition's rows in memory: an "everyone"-style entitlement can
// hold millions of grants, and one allocation per row made a targeted
// repair of such a partition multi-GB. No sort is needed — batch Sets
// accept any order, (entitlement, principal) is the primary identity
// so duplicates are impossible, and the digest fold (pass 2) reads
// the committed index, which pebble returns sorted. A crash between
// rotated commits leaves hash rows without a digest root, which still
// reads as "missing"; the next repair's leading DeleteRange re-cleans
// them — the same window that already exists between the hash-index
// and digest commits.
func (e *Engine) repairOneGrantDigestPartitionLocked(ctx context.Context, partition string) error {
	lower, upper := grantPrimaryEntitlementBoundsFromPartition(partition)

	opts := writeOpts(e.opts.durability)
	if e.IsFreshSync() {
		opts = pebble.NoSync
	}
	flushBytes := digestNodeBatchFlushBytes
	if e.test.digestNodeFlushBytes > 0 {
		flushBytes = e.test.digestNodeFlushBytes
	}

	// Pass 1: replace the hash-index range with the freshly-derived
	// rows (a clean DeleteRange first: nothing may assume what, if
	// anything, was left there). Closure defer, not a bound one: the
	// loop rotates `hiBatch` on flush.
	hiBatch := e.db.NewDigestBatch()
	defer func() { hiBatch.Close() }()
	hiLower := encodeGrantByEntPrincHashEntPrefix(partition)
	if err := hiBatch.DeleteRange(hiLower, upperBoundOf(hiLower)); err != nil {
		return err
	}

	iter, err := e.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return err
	}
	var rows, droppedMalformedKeys int64
	var scratch grantHashRowScratch
	var chb [hashLen]byte
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			_ = iter.Close()
			return err
		}
		key, value := iter.Key(), iter.Value()
		sep4, ok := splitGrantPrimaryKey(key)
		if !ok {
			// Same key-layout-drift/corruption case the build paths
			// count: such rows cannot be represented in the digests.
			droppedMalformedKeys++
			continue
		}
		srcs, serr := scanGrantSourceKeysRawBytes(value, scratch.srcKeys[:0])
		if serr != nil {
			_ = iter.Close()
			return serr
		}
		scratch.srcKeys = srcs
		if len(srcs) > 1 {
			sortByteSlices(srcs)
		}
		ch64, tuple := grantContentHash64(scratch.tupleBuf, key[grantPrimaryKeyPrefixLen:], srcs)
		scratch.tupleBuf = tuple
		bh64 := grantPrincipalBucketHash64(key[sep4+1:])
		scratch.keyBuf = appendGrantHashIndexKeyFromPrimary(scratch.keyBuf[:0], key, sep4, bh64)
		binary.BigEndian.PutUint64(chb[:], ch64)
		if err := hiBatch.Set(scratch.keyBuf, chb[:]); err != nil {
			_ = iter.Close()
			return err
		}
		rows++
		if hiBatch.Len() >= flushBytes {
			if err := hiBatch.Commit(opts); err != nil {
				_ = iter.Close()
				return err
			}
			hiBatch.Close()
			hiBatch = e.db.NewDigestBatch()
		}
	}
	if err := iter.Error(); err != nil {
		_ = iter.Close()
		return err
	}
	if err := iter.Close(); err != nil {
		return err
	}
	if droppedMalformedKeys > 0 {
		// Mirrors the build paths' loud skip (BuildGrantDigests /
		// BuildDeferredGrantIndexes): a principal silently losing grants
		// must be observable.
		ctxzap.Extract(ctx).Error("grant digest repair: grant primary keys did not decode as 6-segment identities; their rows are NOT represented in the repaired digest",
			zap.Int64("dropped", droppedMalformedKeys),
		)
	}
	if err := hiBatch.Commit(opts); err != nil {
		return err
	}

	// Pass 2: fold the just-committed hash index into the digest,
	// reusing the exact fold logic the seal-time build runs
	// (foldPartitionNodes) instead of re-deriving the same computation
	// a second way from the scanned rows.
	width := chooseDigestWidth(rows)
	rootVal, leaves, err := e.foldPartitionNodes(ctx, grantDigestSpec, partition, width)
	if err != nil {
		return err
	}
	digestBatch := e.db.NewDigestBatch()
	defer digestBatch.Close()
	digestLower := encodeDigestPartitionPrefix(grantDigestSpec.indexID, partition)
	if err := digestBatch.DeleteRange(digestLower, upperBoundOf(digestLower)); err != nil {
		return err
	}
	rootKey := encodeDigestNodeKey(grantDigestSpec.indexID, partition, digestLevelRoot, nil)
	if err := digestBatch.Set(rootKey, rootVal); err != nil {
		return err
	}
	for i := range leaves {
		leafKey := encodeDigestNodeKey(grantDigestSpec.indexID, partition, digestLevelLeaf, leaves[i].prefix[:])
		if err := digestBatch.Set(leafKey, leaves[i].val); err != nil {
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
	if err := e.db.DigestSet(globalGrantDigestNodeKey(), packDigestLeaf(total, xor[:]), opts); err != nil {
		return err
	}
	e.grantDigestsPresent.Store(true)
	return nil
}
