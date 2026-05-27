package pebble

import (
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
func (e *Engine) computeSyncStats(ctx context.Context, syncID string) (*v3.SyncStatsRecord, error) {
	rec := &v3.SyncStatsRecord{}
	rec.SetSyncId(syncID)
	resourcesByRT := map[string]int64{}
	grantsByEntitlementRT := map[string]int64{}

	if err := e.IterateResourceTypesBySync(ctx, syncID, func(*v3.ResourceTypeRecord) bool {
		rec.SetResourceTypes(rec.GetResourceTypes() + 1)
		return true
	}); err != nil {
		return nil, fmt.Errorf("computeSyncStats: resource_types: %w", err)
	}
	if err := e.IterateResourcesBySync(ctx, syncID, func(r *v3.ResourceRecord) bool {
		rec.SetResources(rec.GetResources() + 1)
		resourcesByRT[r.GetResourceTypeId()]++
		return true
	}); err != nil {
		return nil, fmt.Errorf("computeSyncStats: resources: %w", err)
	}
	if err := e.IterateEntitlementsBySync(ctx, syncID, func(*v3.EntitlementRecord) bool {
		rec.SetEntitlements(rec.GetEntitlements() + 1)
		return true
	}); err != nil {
		return nil, fmt.Errorf("computeSyncStats: entitlements: %w", err)
	}
	if err := e.IterateGrantsBySync(ctx, syncID, func(g *v3.GrantRecord) bool {
		rec.SetGrants(rec.GetGrants() + 1)
		// Entitlement's resource_type — matches the SQLite grants
		// table's resource_type_id column semantic, which is what
		// the existing GrantStats() caller expects.
		grantsByEntitlementRT[g.GetEntitlement().GetResourceTypeId()]++
		return true
	}); err != nil {
		return nil, fmt.Errorf("computeSyncStats: grants: %w", err)
	}
	if err := e.IterateAssetsBySync(ctx, syncID, func(*v3.AssetRecord) bool {
		rec.SetAssets(rec.GetAssets() + 1)
		return true
	}); err != nil {
		return nil, fmt.Errorf("computeSyncStats: assets: %w", err)
	}
	rec.SetResourcesByResourceType(resourcesByRT)
	rec.SetGrantsByEntitlementResourceType(grantsByEntitlementRT)
	rec.SetWrittenAt(timestamppb.Now())
	return rec, nil
}

// PersistSyncStats computes and writes the stats sidecar for
// syncID. Exposed for the EndFreshSync caller and the on-Open
// migration backfill.
func (e *Engine) PersistSyncStats(ctx context.Context, syncID string) error {
	rec, err := e.computeSyncStats(ctx, syncID)
	if err != nil {
		return err
	}
	return e.writeSyncStats(ctx, syncID, rec)
}
