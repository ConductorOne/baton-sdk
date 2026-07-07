package pebble

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"
	"google.golang.org/protobuf/types/known/timestamppb"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/codec"
)

// Sync-stats sidecar. Populated by EndFreshSync (one full pass
// over the per-record-type keyspaces) and read by Stats() /
// GrantStats() as a single LSM Get. Eliminates the O(N) iteration
// the Adapter used to do on every Stats() call.
//
// Key shape (single fixed key — one sync per file):
//
//	v3 | typeEngineMeta | tup_string("stats") | 0x00
//
// Value: marshaled SyncStatsRecord proto.
//
// Older c1z files (or files written by a pre-sidecar SDK) will be
// missing this key. Stats() falls back to the legacy iterate-and-
// count path in that case, and the on-Open migration framework
// backfills the sidecar on writable opens so the next call is fast.

// encodeSyncStatsKey returns the engine-meta key for the single
// sync's SyncStatsRecord. The file holds one sync, so this is a fixed
// key with no sync_id.
func encodeSyncStatsKey() []byte {
	buf := make([]byte, 0, 6+len("stats"))
	buf = append(buf, versionV3, typeEngineMeta)
	buf = codec.AppendTupleString(buf, "stats")
	buf = codec.AppendTupleSeparator(buf)
	return buf
}

// SyncStatsSidecarLowerBound / UpperBound expose the bounds of the
// stats sidecar keyspace for synccompactor and CloneSync. The file
// holds one stats record, so this is a single-key range.
func SyncStatsSidecarLowerBound() []byte {
	return encodeSyncStatsKey()
}

func SyncStatsSidecarUpperBound() []byte {
	return upperBoundOf(SyncStatsSidecarLowerBound())
}

// readSyncStats returns the persisted SyncStatsRecord, or (nil, nil)
// if no sidecar exists yet. syncID guards against returning a
// different sync's stats (the file holds one sync, so a lookup for a
// mismatched id misses); pass "" to read whatever is stored. Errors
// surface for real read failures only.
func (e *Engine) readSyncStats(ctx context.Context, syncID string) (*v3.SyncStatsRecord, error) {
	val, closer, err := e.db.Get(encodeSyncStatsKey())
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	defer closer.Close()
	rec := &v3.SyncStatsRecord{}
	if err := unmarshalRecord(val, rec); err != nil {
		return nil, fmt.Errorf("readSyncStats: unmarshal: %w", err)
	}
	if syncID != "" && rec.GetSyncId() != syncID {
		return nil, nil
	}
	return rec, nil
}

// writeSyncStats persists the SyncStatsRecord at the fixed sidecar
// key. Uses pebble.Sync because Stats() correctness depends on the
// sidecar surviving a crash that occurred mid-sync-end. The sync_id
// is carried in the record value, not the key.
func (e *Engine) writeSyncStats(ctx context.Context, rec *v3.SyncStatsRecord) error {
	val, err := marshalRecord(rec)
	if err != nil {
		return err
	}
	// AllowSealed under the write barrier: callers span EndSync's sealed
	// finalize window and the compactor's bound-sync flow. The barrier
	// (rather than a bare Set) gives the write the closing check, writeWG
	// coverage against Close's teardown, and exclusion from CheckpointTo's
	// Flush→Checkpoint window (a WAL-only record landing mid-window would
	// be truncated out of the saved snapshot).
	return e.withWriteAllowSealed(func() error {
		return e.db.Set(encodeSyncStatsKey(), val, pebble.Sync)
	})
}

// computeSyncStats does one full pass over the per-record-type
// keyspaces for syncID and builds a SyncStatsRecord. This is the
// O(N) work the sidecar is designed to avoid on read — only the
// EndFreshSync write path and the on-Open migration backfill
// invoke it.
//
// This intentionally scans raw Pebble key/value ranges instead of using
// Iterate*BySync. Counting keys and shallow-scanning only the grant grouping
// field removed the final stats-sidecar full unmarshal from compaction; in the
// same-size syncs=50 overlay case allocs dropped from ~3.05M to ~2.41M/op.
func (e *Engine) computeSyncStats(ctx context.Context, syncID string) (*v3.SyncStatsRecord, error) {
	rec := &v3.SyncStatsRecord{}
	rec.SetSyncId(syncID)

	resourceTypes, err := countKeyRange(ctx, e.db, ResourceTypeLowerBound(), ResourceTypeUpperBound(), nil)
	if err != nil {
		return nil, fmt.Errorf("computeSyncStats: resource_types: %w", err)
	}
	rec.SetResourceTypes(resourceTypes)

	// Resource primary keys sort by resource_type_id, so per-RT groups
	// are contiguous: track the current run and materialize one map key
	// per group instead of one string per row. (A plain
	// counts[string(rt)]++ would allocate per row — the compiler only
	// elides the []byte->string conversion for map reads, not writes.)
	resourcesByRT := map[string]int64{}
	resLower := ResourceLowerBound()
	var rtScratch, curRT []byte
	var curCount int64
	resources, err := countKeyRange(ctx, e.db, resLower, ResourceUpperBound(), func(key []byte, _ []byte) error {
		if len(key) <= len(resLower) {
			return fmt.Errorf("resource key shorter than expected lower bound")
		}
		var derr error
		rtScratch, _, derr = codec.DecodeTupleStringTo(rtScratch[:0], key[len(resLower)+1:], 0)
		if derr != nil {
			return derr
		}
		if curCount > 0 && bytes.Equal(rtScratch, curRT) {
			curCount++
			return nil
		}
		if curCount > 0 {
			resourcesByRT[string(curRT)] += curCount
		}
		curRT = append(curRT[:0], rtScratch...)
		curCount = 1
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("computeSyncStats: resources: %w", err)
	}
	if curCount > 0 {
		resourcesByRT[string(curRT)] += curCount
	}
	rec.SetResources(resources)

	// Entitlement primary keys sort by external id, so resource-type
	// groups are NOT contiguous. map[string]*int64 keeps the per-row map
	// read allocation-free and only materializes a key string the first
	// time each resource type appears (same shape as the grant grouping
	// below).
	entitlementsByRTPtr := map[string]*int64{}
	entitlements, err := countKeyRange(ctx, e.db, EntitlementLowerBound(), EntitlementUpperBound(), func(_ []byte, value []byte) error {
		rt, err := scanEntitlementResourceTypeRaw(value)
		if err != nil {
			return err
		}
		if p, ok := entitlementsByRTPtr[string(rt)]; ok {
			*p++
			return nil
		}
		n := int64(1)
		entitlementsByRTPtr[string(rt)] = &n
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("computeSyncStats: entitlements: %w", err)
	}
	entitlementsByRT := make(map[string]int64, len(entitlementsByRTPtr))
	for rt, p := range entitlementsByRTPtr {
		entitlementsByRT[rt] = *p
	}
	rec.SetEntitlements(entitlements)

	// Grant primary keys sort by external id, so entitlement-RT groups
	// are NOT contiguous. map[string]*int64 keeps the per-row map read
	// allocation-free and only materializes a key string the first time
	// each resource type appears.
	//
	// When BuildDeferredGrantIndexes already accumulated these numbers
	// during its own full grant scan (EndSync with a deferred principal
	// index), consume the stash instead of scanning tens of millions of
	// grant rows a second time.
	var grants int64
	var grantsByEntitlementRT map[string]int64
	if ds := e.takeDeferredGrantStats(syncID); ds != nil {
		grants = ds.grants
		grantsByEntitlementRT = ds.grantsByEntitlementRT
	} else {
		grantsByEntRTPtr := map[string]*int64{}
		grants, err = countKeyRange(ctx, e.db, GrantLowerBound(), GrantUpperBound(), func(_ []byte, value []byte) error {
			entRT, err := scanGrantEntitlementResourceTypeRaw(value)
			if err != nil {
				return err
			}
			if p, ok := grantsByEntRTPtr[string(entRT)]; ok {
				*p++
				return nil
			}
			n := int64(1)
			grantsByEntRTPtr[string(entRT)] = &n
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("computeSyncStats: grants: %w", err)
		}
		grantsByEntitlementRT = make(map[string]int64, len(grantsByEntRTPtr))
		for rt, p := range grantsByEntRTPtr {
			grantsByEntitlementRT[rt] = *p
		}
	}
	rec.SetGrants(grants)

	assets, err := countKeyRange(ctx, e.db, AssetLowerBound(), AssetUpperBound(), nil)
	if err != nil {
		return nil, fmt.Errorf("computeSyncStats: assets: %w", err)
	}
	rec.SetAssets(assets)
	rec.SetResourcesByResourceType(resourcesByRT)
	rec.SetEntitlementsByResourceType(entitlementsByRT)
	rec.SetGrantsByEntitlementResourceType(grantsByEntitlementRT)
	rec.SetWrittenAt(timestamppb.Now())
	return rec, nil
}

// stashDeferredGrantStats records the grant counts BuildDeferredGrantIndexes
// accumulated so the next computeSyncStats call for the same sync can skip
// its own grant scan. Overwrites any prior stash.
func (e *Engine) stashDeferredGrantStats(ds *deferredGrantStats) {
	e.deferredGrantStatsMu.Lock()
	e.deferredGrantStats = ds
	e.deferredGrantStatsMu.Unlock()
}

// takeDeferredGrantStats pops the stashed grant stats if they belong to
// syncID, or returns nil. Consume-once: a later RecalculateStats falls back
// to the full scan.
func (e *Engine) takeDeferredGrantStats(syncID string) *deferredGrantStats {
	e.deferredGrantStatsMu.Lock()
	defer e.deferredGrantStatsMu.Unlock()
	ds := e.deferredGrantStats
	if ds == nil || ds.syncID != syncID {
		return nil
	}
	e.deferredGrantStats = nil
	return ds
}

// PersistSyncStats computes and writes the stats sidecar for
// syncID. Exposed for the EndFreshSync caller and the on-Open
// migration backfill. If a caller-computed record was stashed for
// this sync (StashComputedSyncStats), it is persisted instead of
// re-scanning the keyspaces.
func (e *Engine) PersistSyncStats(ctx context.Context, syncID string) error {
	if rec := e.takeStashedSyncStats(syncID); rec != nil {
		return e.PersistComputedSyncStats(ctx, syncID, rec)
	}
	rec, err := e.computeSyncStats(ctx, syncID)
	if err != nil {
		return err
	}
	return e.writeSyncStats(ctx, rec)
}

// StashComputedSyncStats registers a caller-computed stats record to be
// persisted by the next PersistSyncStats call for syncID instead of the
// O(N) keyspace re-scan. Intended for trusted bulk writers (e.g. the
// BulkSyncImport conversion path) that counted every record they wrote;
// the caller owns the record's correctness. The stash is consumed by
// exactly one PersistSyncStats call.
func (e *Engine) StashComputedSyncStats(syncID string, rec *v3.SyncStatsRecord) {
	if rec == nil {
		return
	}
	e.computedStatsMu.Lock()
	if e.computedStats == nil {
		e.computedStats = map[string]*v3.SyncStatsRecord{}
	}
	e.computedStats[syncID] = rec
	e.computedStatsMu.Unlock()
}

// takeStashedSyncStats pops the stashed record for syncID, or nil.
func (e *Engine) takeStashedSyncStats(syncID string) *v3.SyncStatsRecord {
	e.computedStatsMu.Lock()
	defer e.computedStatsMu.Unlock()
	rec, ok := e.computedStats[syncID]
	if !ok {
		return nil
	}
	delete(e.computedStats, syncID)
	return rec
}

// PersistComputedSyncStats writes a caller-computed stats record —
// e.g. one accumulated while the synccompactor wrote merge winners —
// without re-scanning the keyspaces. SyncId and WrittenAt are set
// here so callers only fill counts. Durability matches
// PersistSyncStats (pebble.Sync via writeSyncStats).
func (e *Engine) PersistComputedSyncStats(ctx context.Context, syncID string, rec *v3.SyncStatsRecord) error {
	if rec == nil {
		return fmt.Errorf("PersistComputedSyncStats: nil record")
	}
	rec.SetSyncId(syncID)
	rec.SetWrittenAt(timestamppb.Now())
	return e.writeSyncStats(ctx, rec)
}

func countKeyRange(ctx context.Context, db *pebble.DB, lower []byte, upper []byte, visit func(key []byte, value []byte) error) (int64, error) {
	iter, err := db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return 0, err
	}
	defer iter.Close()
	var count int64
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		count++
		if visit != nil {
			if err := visit(iter.Key(), iter.Value()); err != nil {
				return 0, err
			}
		}
	}
	return count, iter.Error()
}
