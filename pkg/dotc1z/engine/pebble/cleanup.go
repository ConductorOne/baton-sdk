package pebble

import (
	"context"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"
)

// scopedRanges returns the half-open [lo, hi) ranges covering every
// record/index/sync-run/stats keyspace. A v3 Pebble c1z holds one
// sync and keys carry no sync_id, so these cover the entire data
// keyspace. Engine-global metadata (keyspace-version stamp,
// index-migration markers) is deliberately NOT included. Mirrors the
// bucket plan in adapter_clone_sync.go; kept in lockstep so
// ResetForNewSync leaves no orphan rows that CloneSync would
// otherwise have copied.
//
// The returned ranges are NOT ordered for compaction efficiency.
// Callers that want one Compact() per range should iterate the slice
// and Compact each independently — Pebble's per-Compact overhead is
// small relative to the L0/L1 work the range itself triggers.
func scopedRanges() [][2][]byte {
	return [][2][]byte{
		{encodeSyncRunKey(), upperBoundOf(encodeSyncRunKey())},
		{encodeResourceTypePrefix(), upperBoundOf(encodeResourceTypePrefix())},
		{encodeResourcePrefix(), upperBoundOf(encodeResourcePrefix())},
		{ResourceByParentLowerBound(), ResourceByParentUpperBound()},
		{encodeEntitlementPrefix(), upperBoundOf(encodeEntitlementPrefix())},
		{EntitlementByResourceLowerBound(), EntitlementByResourceUpperBound()},
		{encodeGrantPrefix(), upperBoundOf(encodeGrantPrefix())},
		{GrantByEntitlementLowerBound(), GrantByEntitlementUpperBound()},
		{GrantByEntitlementResourceLowerBound(), GrantByEntitlementResourceUpperBound()},
		{GrantByPrincipalLowerBound(), GrantByPrincipalUpperBound()},
		{GrantByPrincipalResourceTypeLowerBound(), GrantByPrincipalResourceTypeUpperBound()},
		{GrantByNeedsExpansionLowerBound(), GrantByNeedsExpansionUpperBound()},
		{GrantByEntPrincHashLowerBound(), GrantByEntPrincHashUpperBound()},
		{DigestLowerBound(), DigestUpperBound()},
		{encodeAssetPrefix(), upperBoundOf(encodeAssetPrefix())},
		// Stats sidecar — single key; the half-open range shape
		// contains exactly that one key.
		{encodeSyncStatsKey(), upperBoundOf(encodeSyncStatsKey())},
	}
}

// ResetForNewSync wipes every sync-scoped keyspace (records, indexes,
// the sync-run record, and the stats sidecar) so a freshly started
// sync begins on an empty keyspace.
//
// Why this exists: a v3 Pebble c1z holds exactly one sync, and the
// keys carry no sync_id. StartNewSync calls this before binding a new
// sync so the replacement sync never inherits orphan records from a
// prior sync — records the new sync doesn't happen to overwrite would
// otherwise linger under identical keys.
//
// The wipe uses pebble.DB.Excise rather than DeleteRange tombstones:
// excise drops fully-covered SSTs from the manifest outright
// (O(metadata), immediate disk reclaim) instead of leaving range
// tombstones whose dead bytes survive until compaction. With
// tombstones, a replacement sync that finishes before background
// compaction catches up would hard-link the prior sync's dead SSTs
// into the CheckpointTo envelope at save — the same bloat the old
// multi-sync Cleanup path ran CompactSyncRanges to avoid. Excise also
// keeps the MarkFreshSync "empty by construction" fast path honest
// physically, not just logically.
//
// Two spans cover everything sync-scoped:
//
//   - [v3|typeResourceType, v3|typeEngineMeta): every record type,
//     the sync-run record, all secondary indexes, assets, and the
//     reserved counter/session bytes — one contiguous span, so
//     almost every SST is fully covered and dropped whole.
//   - the stats sidecar range inside engine-meta.
//
// Engine-global metadata — the keyspace-version stamp and
// index-migration markers — lives elsewhere in engine-meta and is
// intentionally preserved.
//
// Refuses while a fresh sync is in progress (between MarkFreshSync and
// EndFreshSync): wiping mid-sync would corrupt the in-flight sync.
func (e *Engine) ResetForNewSync(ctx context.Context) error {
	if e.IsFreshSync() {
		return errors.New("ResetForNewSync: refusing to reset while a sync is in progress")
	}
	spans := []pebble.KeyRange{
		{Start: []byte{versionV3, typeResourceType}, End: []byte{versionV3, typeEngineMeta}},
		{Start: SyncStatsSidecarLowerBound(), End: SyncStatsSidecarUpperBound()},
	}
	// AllowSealed: StartNewSync legitimately replaces a finished (sealed)
	// sync; the wipe is the first step of leaving the sealed state. The
	// engine stays sealed until MarkFreshSync unseals it right after.
	return e.withWriteAllowSealed(func() error {
		for _, span := range spans {
			if err := e.db.Excise(ctx, span); err != nil {
				return fmt.Errorf("ResetForNewSync: excise [%x, %x): %w", span.Start, span.End, err)
			}
		}
		// The record-type span above covers typeDigest and the hash
		// index too; disarm the mutation-path digest invalidation.
		e.grantDigestsPresent.Store(false)
		// The digest-build crash marker lives in the preserved
		// engine-meta range, but the excise just removed everything it
		// was guarding against trusting — consume it (only reachable
		// when an interrupted build's cleanup drop itself failed;
		// writable Opens consume it before anything else runs).
		if e.grantDigestBuildPending.Load() {
			if err := e.clearGrantDigestBuildPending(); err != nil {
				return err
			}
		}
		e.noteEntitlementKeyspaceWrite()
		return nil
	})
}

// CompactAllRanges runs pebble.Compact over every sync-scoped range to
// reclaim disk from the tombstones ResetForNewSync (or any bulk
// delete) leaves behind. The file holds one sync, so "all ranges" is
// the whole data keyspace. Without this, the next checkpoint would
// still include the deleted bytes.
//
// Errors are best-effort: a Compact failure on one range doesn't block
// the others — pebble retries compaction in the background. ctx is
// honored between ranges and surfaced verbatim on cancellation.
//
// Refuses with ErrEngineSealed after EndSync (via checkWritable): manual
// compactions go through the same CompactionScheduler as automatic ones,
// so on a sealed (paused) engine db.Compact would block forever waiting
// for a grant — and, because we hold writeWG, deadlock Engine.Close too.
// Bind a sync (SetCurrentSync) first.
//
// KNOWN LIMITATION: the gate only refuses calls made after the seal. A
// CompactAllRanges already inside its loop when EndSync pauses the
// scheduler blocks in db.Compact indefinitely (and holds writeWG, so a
// later Close hangs too). Do not run this concurrently with EndSync; no
// in-tree caller does.
func (e *Engine) CompactAllRanges(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := e.checkWritable(); err != nil {
		return err
	}
	// Hold the engine's writeWG for the duration of the compaction
	// so Engine.Close blocks until our in-flight Compact returns.
	// Without this guard, Close → e.db.Close() can race with our
	// e.db.Compact(...) call, and pebble.DB.Compact PANICS on a
	// closed DB (vendor/.../pebble/v2/db.go:1826) rather than
	// returning an error. Compact doesn't need writeMu — pebble's
	// own compaction is concurrency-safe with foreground writes,
	// so we don't go through withWrite (which serializes against
	// other Puts and DeleteRanges).
	e.writeWG.Add(1)
	defer e.writeWG.Done()
	if e.closing.Load() {
		return ErrEngineClosing
	}

	var firstErr error
	for _, r := range scopedRanges() {
		if err := ctx.Err(); err != nil {
			return err
		}
		// Compact requires start < end. Empty ranges (when the sync
		// had no records of a given type) are silently skipped.
		if len(r[0]) == 0 || len(r[1]) == 0 {
			continue
		}
		if err := e.db.Compact(ctx, r[0], r[1], true); err != nil {
			// pebble.ErrCancelled and the like surface here; track
			// the first error but continue so a transient compaction
			// failure in one range doesn't strand the others.
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	if firstErr != nil {
		return fmt.Errorf("CompactAllRanges: %w", firstErr)
	}
	return nil
}

// Flush forces the engine's memtable to disk. Exported so the
// cleanup orchestration can ensure tombstones are durable before
// the next checkpoint reads the LSM.
//
// This is a thin wrapper over pebble.DB.Flush + a WAL fsync; the
// EndFreshSync path uses the same combination at sync end. ctx is
// checked before the blocking calls so a cancelled deadline doesn't
// trigger a full memtable flush.
func (e *Engine) Flush(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := e.checkWritable(); err != nil {
		return err
	}
	// Hold writeWG so Engine.Close blocks until the Flush + WAL
	// fsync finish — same close-race protection as CompactSyncRanges.
	// pebble.DB.Flush and pebble.DB.LogData both panic on a closed
	// DB rather than returning an error.
	e.writeWG.Add(1)
	defer e.writeWG.Done()
	if e.closing.Load() {
		return ErrEngineClosing
	}
	if err := e.db.Flush(); err != nil {
		return fmt.Errorf("engine: flush: %w", err)
	}
	if err := e.db.LogData(nil, pebble.Sync); err != nil {
		return fmt.Errorf("engine: fsync WAL: %w", err)
	}
	return nil
}
