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
// Key shape:
//
//	v3 | typeEngineMeta | tup_string("stats") | sync_id_bytes
//
// Value: marshaled SyncStatsRecord proto.
//
// Older c1z files (or files written by a pre-sidecar SDK) will be
// missing this key. Stats() falls back to the legacy iterate-and-
// count path in that case, and the on-Open migration framework
// backfills the sidecar on writable opens so the next call is fast.

// encodeSyncStatsKey returns the engine-meta key for the
// SyncStatsRecord of the named sync.
func encodeSyncStatsKey(syncIDBytes []byte) []byte {
	buf := make([]byte, 0, 6+len("stats")+len(syncIDBytes))
	buf = append(buf, versionV3, typeEngineMeta)
	buf = codec.AppendTupleString(buf, "stats")
	buf = codec.AppendTupleSeparator(buf)
	buf = append(buf, syncIDBytes...)
	return buf
}

// SyncStatsSidecarLowerBound / UpperBound expose the bounds of the
// stats sidecar keyspace for synccompactor and CloneSync. The
// sidecar keys themselves are sync-prefixed (after the "stats"
// tuple prefix), so this is a single contiguous range.
func SyncStatsSidecarLowerBound() []byte {
	buf := make([]byte, 0, 2+len("stats")+2)
	buf = append(buf, versionV3, typeEngineMeta)
	buf = codec.AppendTupleString(buf, "stats")
	buf = codec.AppendTupleSeparator(buf)
	return buf
}

func SyncStatsSidecarUpperBound() []byte {
	return upperBoundOf(SyncStatsSidecarLowerBound())
}

// SyncStatsSidecarSyncLowerBound returns the lowest key in the
// sidecar keyspace for a specific sync. The sidecar stores exactly
// one record per sync_id; this single-key range is used by
// CloneSync and the synccompactor bucket plan.
func SyncStatsSidecarSyncLowerBound(syncIDBytes []byte) []byte {
	return encodeSyncStatsKey(syncIDBytes)
}

// SyncStatsSidecarSyncUpperBound is the exclusive upper bound for
// SyncStatsSidecarSyncLowerBound.
func SyncStatsSidecarSyncUpperBound(syncIDBytes []byte) []byte {
	return upperBoundOf(SyncStatsSidecarSyncLowerBound(syncIDBytes))
}

// readSyncStats returns the persisted SyncStatsRecord for the named
// sync, or (nil, nil) if no sidecar exists yet. Errors surface for
// real read failures only.
func (e *Engine) readSyncStats(ctx context.Context, syncID string) (*v3.SyncStatsRecord, error) {
	idBytes, err := codec.EncodeSyncID(syncID)
	if err != nil {
		return nil, err
	}
	val, closer, err := e.db.Get(encodeSyncStatsKey(idBytes))
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
	return rec, nil
}

// writeSyncStats persists the SyncStatsRecord. Uses pebble.Sync
// because Stats() correctness depends on the sidecar surviving a
// crash that occurred mid-sync-end.
func (e *Engine) writeSyncStats(ctx context.Context, syncID string, rec *v3.SyncStatsRecord) error {
	idBytes, err := codec.EncodeSyncID(syncID)
	if err != nil {
		return err
	}
	val, err := marshalRecord(rec)
	if err != nil {
		return err
	}
	return e.db.Set(encodeSyncStatsKey(idBytes), val, pebble.Sync)
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
	idBytes, err := codec.EncodeSyncID(syncID)
	if err != nil {
		return nil, err
	}

	resourceTypes, err := countKeyRange(ctx, e.db, ResourceTypeSyncLowerBound(idBytes), ResourceTypeSyncUpperBound(idBytes), nil)
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
	resLower := ResourceSyncLowerBound(idBytes)
	var rtScratch, curRT []byte
	var curCount int64
	resources, err := countKeyRange(ctx, e.db, resLower, ResourceSyncUpperBound(idBytes), func(key []byte, _ []byte) error {
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

	entitlements, err := countKeyRange(ctx, e.db, EntitlementSyncLowerBound(idBytes), EntitlementSyncUpperBound(idBytes), nil)
	if err != nil {
		return nil, fmt.Errorf("computeSyncStats: entitlements: %w", err)
	}
	rec.SetEntitlements(entitlements)

	// Grant primary keys sort by external id, so entitlement-RT groups
	// are NOT contiguous. map[string]*int64 keeps the per-row map read
	// allocation-free and only materializes a key string the first time
	// each resource type appears.
	grantsByEntRTPtr := map[string]*int64{}
	grants, err := countKeyRange(ctx, e.db, GrantSyncLowerBound(idBytes), GrantSyncUpperBound(idBytes), func(_ []byte, value []byte) error {
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
	grantsByEntitlementRT := make(map[string]int64, len(grantsByEntRTPtr))
	for rt, p := range grantsByEntRTPtr {
		grantsByEntitlementRT[rt] = *p
	}
	rec.SetGrants(grants)

	assets, err := countKeyRange(ctx, e.db, AssetSyncLowerBound(idBytes), AssetSyncUpperBound(idBytes), nil)
	if err != nil {
		return nil, fmt.Errorf("computeSyncStats: assets: %w", err)
	}
	rec.SetAssets(assets)
	rec.SetResourcesByResourceType(resourcesByRT)
	rec.SetGrantsByEntitlementResourceType(grantsByEntitlementRT)
	rec.SetWrittenAt(timestamppb.Now())
	return rec, nil
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
	return e.writeSyncStats(ctx, syncID, rec)
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
	return e.writeSyncStats(ctx, syncID, rec)
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
