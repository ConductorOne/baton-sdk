package pebble

import (
	"context"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/codec"
)

// syncScopedRanges returns the half-open [lo, hi) ranges covering
// every keyspace scoped by the given sync_id. Mirrors the bucket
// plan in adapter_clone_sync.go; kept in lockstep so a sync deleted
// by Cleanup leaves no orphan rows behind that CloneSync would
// otherwise have copied.
//
// The returned ranges are NOT ordered for compaction efficiency.
// Callers that want one Compact() per sync should still iterate
// the slice and Compact each range independently — Pebble's
// per-Compact overhead is small relative to the L0/L1 work the
// range itself triggers.
func syncScopedRanges(syncIDBytes []byte) [][2][]byte {
	return [][2][]byte{
		{encodeSyncRunKey(syncIDBytes), upperBoundOf(encodeSyncRunKey(syncIDBytes))},
		{encodeResourceTypePrefix(syncIDBytes), upperBoundOf(encodeResourceTypePrefix(syncIDBytes))},
		{encodeResourcePrefix(syncIDBytes), upperBoundOf(encodeResourcePrefix(syncIDBytes))},
		{ResourceByParentSyncLowerBound(syncIDBytes), ResourceByParentSyncUpperBound(syncIDBytes)},
		{encodeEntitlementPrefix(syncIDBytes), upperBoundOf(encodeEntitlementPrefix(syncIDBytes))},
		{EntitlementByResourceSyncLowerBound(syncIDBytes), EntitlementByResourceSyncUpperBound(syncIDBytes)},
		{encodeGrantPrefix(syncIDBytes), upperBoundOf(encodeGrantPrefix(syncIDBytes))},
		{GrantByEntitlementSyncLowerBound(syncIDBytes), GrantByEntitlementSyncUpperBound(syncIDBytes)},
		{GrantByEntitlementResourceSyncLowerBound(syncIDBytes), GrantByEntitlementResourceSyncUpperBound(syncIDBytes)},
		{GrantByPrincipalSyncLowerBound(syncIDBytes), GrantByPrincipalSyncUpperBound(syncIDBytes)},
		{GrantByPrincipalResourceTypeSyncLowerBound(syncIDBytes), GrantByPrincipalResourceTypeSyncUpperBound(syncIDBytes)},
		{GrantByNeedsExpansionSyncLowerBound(syncIDBytes), GrantByNeedsExpansionSyncUpperBound(syncIDBytes)},
		{encodeAssetPrefix(syncIDBytes), upperBoundOf(encodeAssetPrefix(syncIDBytes))},
		// Stats sidecar — single key per sync; the half-open range
		// shape contains exactly the one key for this sync.
		{encodeSyncStatsKey(syncIDBytes), upperBoundOf(encodeSyncStatsKey(syncIDBytes))},
	}
}

// DeleteSyncData removes every key scoped to syncID from the engine.
// This is the Pebble counterpart to the SQLite Cleanup path's
// per-table DELETE WHERE sync_id = ? loop.
//
// Refuses to operate on a sync that is currently being written.
// "Currently being written" means the engine is in the fresh-sync
// fast path (between MarkFreshSync and EndFreshSync) for this
// sync_id. The mere fact that e.currentSync still points at a
// previously-ended sync is not enough to lock it out — EndFreshSync
// intentionally leaves currentSync set so Reader-side empty-syncID
// reads keep working until SetCurrentSync rebinds.
//
// Each DeleteRange call writes a single tombstone to the memtable
// (~µs), so we deliberately do NOT check ctx between ranges. A
// partial DeleteSyncData would leave some keyspaces tombstoned and
// others not — a "half-deleted sync" the next Cleanup pass would
// have to re-tombstone. Callers that need ctx-aware cancellation
// should check ctx around the DeleteSyncData call, not inside it.
//
// After this returns the keys are tombstoned but the physical bytes
// remain in the LSM until compaction. Pair with CompactSyncRanges
// (or one broader Compact call) to actually reclaim disk before
// CheckpointTo.
func (e *Engine) DeleteSyncData(ctx context.Context, syncID string) error {
	if syncID == "" {
		return errors.New("DeleteSyncData: empty sync_id")
	}
	idBytes, err := codec.EncodeSyncID(syncID)
	if err != nil {
		return fmt.Errorf("DeleteSyncData: encode sync_id: %w", err)
	}
	if e.isFreshSyncFor(idBytes) {
		return fmt.Errorf("DeleteSyncData: refusing to delete the active sync %q", syncID)
	}

	ranges := syncScopedRanges(idBytes)
	return e.withWrite(func() error {
		opts := writeOpts(e.opts.durability)
		for _, r := range ranges {
			if err := e.db.DeleteRange(r[0], r[1], opts); err != nil {
				return fmt.Errorf("DeleteSyncData: DeleteRange: %w", err)
			}
		}
		return nil
	})
}

// isFreshSyncFor reports whether the engine is in the fresh-sync
// fast path AND currentSync equals idBytes. Used by DeleteSyncData
// to refuse pruning a sync the engine is actively writing to.
func (e *Engine) isFreshSyncFor(idBytes []byte) bool {
	e.currentSyncMu.RLock()
	defer e.currentSyncMu.RUnlock()
	if !e.freshSync {
		return false
	}
	if len(e.currentSync) != len(idBytes) {
		return false
	}
	return string(e.currentSync) == string(idBytes)
}

// CompactSyncRanges runs pebble.Compact over each sync-scoped range
// for syncID. Called after DeleteSyncData to reclaim disk space
// from the tombstones DeleteRange leaves behind. Without this, the
// next checkpoint would still include the deleted bytes.
//
// Compaction is bounded per-range, not engine-wide, so the cost
// scales with the data being pruned rather than the engine's total
// size. Errors are best-effort: a Compact failure on one range
// doesn't block the others — pebble retries compaction in the
// background and the next Cleanup call will re-attempt.
//
// ctx is honored between ranges; if the caller cancels mid-pass we
// surface ctx.Err() verbatim so the orchestrator (registeredStore
// .Cleanup) can flip the sync to ErrSyncNotComplete.
func (e *Engine) CompactSyncRanges(ctx context.Context, syncID string) error {
	if syncID == "" {
		return errors.New("CompactSyncRanges: empty sync_id")
	}
	idBytes, err := codec.EncodeSyncID(syncID)
	if err != nil {
		return fmt.Errorf("CompactSyncRanges: encode sync_id: %w", err)
	}
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
		return ErrEngineQuiesced
	}

	var firstErr error
	for _, r := range syncScopedRanges(idBytes) {
		if err := ctx.Err(); err != nil {
			return err
		}
		// Compact requires start < end. Empty ranges (when a sync
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
		return fmt.Errorf("CompactSyncRanges: %w", firstErr)
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
		return ErrEngineQuiesced
	}
	if err := e.db.Flush(); err != nil {
		return fmt.Errorf("engine: flush: %w", err)
	}
	if err := e.db.LogData(nil, pebble.Sync); err != nil {
		return fmt.Errorf("engine: fsync WAL: %w", err)
	}
	return nil
}
