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
// bucket plan in adapter_clone_sync.go.
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
		{ResourceBySourceScopeLowerBound(), ResourceBySourceScopeUpperBound()},
		{encodeEntitlementPrefix(), upperBoundOf(encodeEntitlementPrefix())},
		{EntitlementByResourceLowerBound(), EntitlementByResourceUpperBound()},
		{EntitlementBySourceScopeLowerBound(), EntitlementBySourceScopeUpperBound()},
		{encodeGrantPrefix(), upperBoundOf(encodeGrantPrefix())},
		{GrantByEntitlementLowerBound(), GrantByEntitlementUpperBound()},
		{GrantByEntitlementResourceLowerBound(), GrantByEntitlementResourceUpperBound()},
		{GrantByPrincipalLowerBound(), GrantByPrincipalUpperBound()},
		{GrantByPrincipalResourceTypeLowerBound(), GrantByPrincipalResourceTypeUpperBound()},
		{GrantByNeedsExpansionLowerBound(), GrantByNeedsExpansionUpperBound()},
		{GrantBySourceScopeLowerBound(), GrantBySourceScopeUpperBound()},
		{GrantByEntPrincHashLowerBound(), GrantByEntPrincHashUpperBound()},
		{DigestLowerBound(), DigestUpperBound()},
		{SourceCacheEntryLowerBound(), SourceCacheEntryUpperBound()},
		{SourceCachePoisonLowerBound(), SourceCachePoisonUpperBound()},
		{encodeAssetPrefix(), upperBoundOf(encodeAssetPrefix())},
		// Stats sidecar — single key; the half-open range shape
		// contains exactly that one key.
		{encodeSyncStatsKey(), upperBoundOf(encodeSyncStatsKey())},
		// Entitlement-graph sidecar — same single-key shape.
		{EntitlementGraphSidecarLowerBound(), EntitlementGraphSidecarUpperBound()},
		{ledgerLowerBound(), ledgerUpperBound()},
	}
}

// ResetForNewSync empties the keyspace and re-runs Open's fresh-file
// initialization. A v3 Pebble c1z holds one sync and keys carry no
// sync_id, so StartNewSync calls this before binding a replacement.
//
// One Excise over the widest span pebble can express: every SST is fully
// covered and dropped from the manifest, so no bytes survive — including
// the verbatim page tokens of an interrupted ledgered sync, which a
// narrowed SST would carry into the next checkpoint
// (TestLedgerResidueOutlivesTheLedger).
//
// Refuses while a fresh sync is in progress (between MarkFreshSync and
// FinishSync).
func (e *Engine) ResetForNewSync(ctx context.Context) error {
	if e.IsFreshSync() {
		return errors.New("ResetForNewSync: refusing to reset while a sync is in progress")
	}
	// AllowSealed: StartNewSync legitimately replaces a finished (sealed)
	// sync; the wipe is the first step of leaving the sealed state. The
	// engine stays sealed until MarkFreshSync unseals it right after.
	return e.withWriteAllowSealed(func() error {
		// Start must be non-nil: KeyRange.Valid is false on a nil bound and
		// pebble ignores an invalid excise span without error. End is
		// exclusive with no max sentinel; requireKeyspaceEmpty catches a key
		// at or above it.
		span := pebble.KeyRange{Start: []byte{}, End: []byte{0xff}}
		if err := e.db.ExciseRange(ctx, span); err != nil {
			return fmt.Errorf("ResetForNewSync: excise [%x, %x): %w", span.Start, span.End, err)
		}
		if err := e.requireKeyspaceEmpty(); err != nil {
			return fmt.Errorf("ResetForNewSync: %w", err)
		}
		if err := e.initKeyspaceStateLocked(ctx); err != nil {
			return fmt.Errorf("ResetForNewSync: %w", err)
		}
		e.noteEntitlementKeyspaceWrite()
		return nil
	})
}

func (e *Engine) requireKeyspaceEmpty() error {
	iter, err := e.db.NewIter(&pebble.IterOptions{})
	if err != nil {
		return err
	}
	defer iter.Close()
	if iter.First() {
		return fmt.Errorf("key %x survived the excise", iter.Key())
	}
	return iter.Error()
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
// Refuses with ErrEngineSealed after EndSync: manual compactions go
// through the same CompactionScheduler as automatic ones, so on a sealed
// (paused) engine db.Compact would block forever waiting for a grant.
// Bind a sync (SetCurrentSync) first. Holds writeMu for the duration, so
// concurrent writers and Close wait for the compaction; pebble.DB.Compact
// panics on a closed DB rather than returning an error.
func (e *Engine) CompactAllRanges(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	if err := e.checkWritableLocked(); err != nil {
		return err
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
// FinishSync path uses the same combination at sync end. ctx is
// checked before the blocking calls so a cancelled deadline doesn't
// trigger a full memtable flush.
func (e *Engine) Flush(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	if err := e.checkWritableLocked(); err != nil {
		return err
	}
	if err := e.db.FlushMemtables(); err != nil {
		return fmt.Errorf("engine: flush: %w", err)
	}
	if err := e.db.WALSyncPoint(); err != nil {
		return fmt.Errorf("engine: fsync WAL: %w", err)
	}
	return nil
}
