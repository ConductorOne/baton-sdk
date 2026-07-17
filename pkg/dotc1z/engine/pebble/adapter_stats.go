package pebble

import (
	"context"

	"github.com/cockroachdb/pebble/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// Stats returns a map of {table_name: row_count} for the given sync.
// Used by pkg/sync/progresslog to show the "syncing X resources, Y
// entitlements, Z grants" progress lines. The SQLite engine answers
// this with a fast COUNT(*) per table; for Pebble we iterate each
// per-record-type bucket and count keys in the [sync_lo, sync_hi)
// range. Counts are exact (not estimates).
//
// Mirrors connectorstore / SQLite semantics: syncType == SyncTypeAny
// accepts any sync; otherwise we filter by the sync_run's type. An
// empty syncID resolves to the latest finished sync of that type
// (NotFound if none).
func (a *Adapter) Stats(ctx context.Context, syncType connectorstore.SyncType, syncID string) (map[string]int64, error) {
	stats, err := a.stats(ctx, syncType, syncID)
	if err != nil {
		return nil, err
	}
	if stats == nil {
		return map[string]int64{}, nil
	}
	return statsRecordToMap(stats), nil
}

// StatsV2 returns structured stats for the given sync.
func (a *Adapter) StatsV2(ctx context.Context, syncType connectorstore.SyncType, syncID string) (*reader_v2.SyncStats, error) {
	stats, err := a.stats(ctx, syncType, syncID)
	if err != nil {
		return nil, err
	}
	return SyncStatsFromRecord(stats), nil
}

func (a *Adapter) stats(ctx context.Context, syncType connectorstore.SyncType, syncID string) (*v3.SyncStatsRecord, error) {
	if syncID == "" {
		// Match SQLite C1File.stats: empty syncID resolves to the
		// latest finished sync of the requested type, never the
		// in-progress current sync.
		var err error
		syncID, err = a.LatestFinishedSyncID(ctx, syncType)
		if err != nil {
			return nil, err
		}
		if syncID == "" {
			return nil, errNoFinishedSync(syncType)
		}
	}

	// Match SQLite C1File.stats: missing sync → NotFound; type
	// mismatch → InvalidArgument.
	sr, err := a.engine.GetSyncRunRecord(ctx, syncID)
	if err != nil {
		return nil, c1zstore.AdaptNotFound(err, pebble.ErrNotFound)
	}
	if sr == nil {
		return nil, status.Errorf(codes.NotFound, "sync '%s' not found", syncID)
	}
	if syncType != connectorstore.SyncTypeAny &&
		syncType != connectorstore.SyncType(v3SyncTypeToString(sr.GetType())) {
		return nil, status.Errorf(codes.InvalidArgument, "sync '%s' is not of type '%s'", syncID, syncType)
	}

	// Fast path: read the sidecar populated at EndFreshSync. Sidecar
	// is one LSM Get vs the legacy fallback's O(N) iteration. Sync
	// types that haven't run through the sidecar writer (Partial /
	// Incremental that bypass EndFreshSync) fall through to the
	// legacy path below. Token timings are written into the sidecar
	// at EndSync; older count-only sidecars are returned as-is.
	if cached, err := a.engine.readSyncStats(ctx, syncID); err == nil && cached != nil {
		return cached, nil
	}
	return a.statsFromIteration(ctx, syncID)
}

// statsFromIteration is the legacy O(N) path retained as the
// sidecar fallback. Used by older c1z files that predate the
// sidecar and by sync types that don't go through EndFreshSync.
func (a *Adapter) statsFromIteration(ctx context.Context, syncID string) (*v3.SyncStatsRecord, error) {
	stats := &v3.SyncStatsRecord{
		SyncId:                          syncID,
		ResourceTypes:                   0,
		Resources:                       0,
		Entitlements:                    0,
		Grants:                          0,
		Assets:                          0,
		ResourcesByResourceType:         make(map[string]int64),
		GrantsByEntitlementResourceType: make(map[string]int64),
		EntitlementsByResourceType:      make(map[string]int64),
	}

	if err := a.engine.IterateResourceTypes(ctx, func(r *v3.ResourceTypeRecord) bool {
		stats.ResourceTypes++
		return true
	}); err != nil {
		return nil, err
	}
	if err := a.engine.IterateResources(ctx, func(r *v3.ResourceRecord) bool {
		stats.Resources++
		stats.ResourcesByResourceType[r.GetResourceTypeId()]++
		return true
	}); err != nil {
		return nil, err
	}
	if err := a.engine.IterateEntitlements(ctx, func(e *v3.EntitlementRecord) bool {
		stats.Entitlements++
		stats.EntitlementsByResourceType[e.GetResource().GetResourceTypeId()]++
		return true
	}); err != nil {
		return nil, err
	}
	if err := a.engine.IterateGrants(ctx, func(g *v3.GrantRecord) bool {
		stats.Grants++
		stats.GrantsByEntitlementResourceType[g.GetEntitlement().GetResourceTypeId()]++
		return true
	}); err != nil {
		return nil, err
	}
	if err := a.engine.IterateAssets(ctx, func(*v3.AssetRecord) bool {
		stats.Assets++
		return true
	}); err != nil {
		return nil, err
	}
	return stats, nil
}

// statsRecordToMap renders a sidecar SyncStatsRecord into the
// map[string]int64 shape Stats() returns. Includes both the
// aggregate keys (resource_types, resources, entitlements, grants,
// assets) and the per-resource-type counts the SQLite Stats() path
// also surfaces.
func statsRecordToMap(s *v3.SyncStatsRecord) map[string]int64 {
	out := map[string]int64{
		"resource_types": s.GetResourceTypes(),
		"resources":      s.GetResources(),
		"entitlements":   s.GetEntitlements(),
		"grants":         s.GetGrants(),
		"assets":         s.GetAssets(),
	}
	for rt, n := range s.GetResourcesByResourceType() {
		out[rt] = n
	}

	return out
}

// errNoFinishedSync is returned when Stats/StatsV2/GrantStats are
// called with an empty syncID and no finished sync of the requested
// type exists. Matches the SQLite C1File.stats contract.
func errNoFinishedSync(syncType connectorstore.SyncType) error {
	if syncType == connectorstore.SyncTypeAny || syncType == "" {
		return status.Error(codes.NotFound, "no finished sync found")
	}
	return status.Errorf(codes.NotFound, "no finished sync of type '%s' found", syncType)
}

// GrantStats returns just the grant count, partitioned by entitlement
// resource_type_id. Used by progresslog to show per-RT progress. Like
// Stats, this is an exact count via iteration.
//
// syncType is validated against the sync_run's actual type to match
// SQLite's C1File.GrantStats: SyncTypeAny accepts any sync; otherwise
// we require the sync_run.Type to match exactly. A type mismatch
// returns (nil, nil) — the syncer treats that as "nothing to show
// for this view," same as the SQLite contract.
//
// Empty syncID resolves to the latest finished sync of syncType
// (matching Stats / SQLite grantStats), and returns NotFound when
// none exists.
func (a *Adapter) GrantStats(ctx context.Context, syncType connectorstore.SyncType, syncID string) (map[string]int64, error) {
	if syncID == "" {
		var err error
		syncID, err = a.LatestFinishedSyncID(ctx, syncType)
		if err != nil {
			return nil, err
		}
		if syncID == "" {
			return nil, errNoFinishedSync(syncType)
		}
	}
	sr, err := a.engine.GetSyncRunRecord(ctx, syncID)
	if err == nil && sr != nil {
		if syncType != connectorstore.SyncTypeAny &&
			syncType != connectorstore.SyncType(v3SyncTypeToString(sr.GetType())) {
			return nil, nil
		}
	}
	// Fast path: sidecar.
	if stats, err := a.engine.readSyncStats(ctx, syncID); err == nil && stats != nil {
		out := make(map[string]int64, len(stats.GetGrantsByEntitlementResourceType()))
		for rt, n := range stats.GetGrantsByEntitlementResourceType() {
			out[rt] = n
		}
		return out, nil
	}
	// Fallback: iterate.
	counts := map[string]int64{}
	if err := a.engine.IterateGrants(ctx, func(rec *v3.GrantRecord) bool {
		// Match SQLite's `resource_type_id` column semantic on
		// the grants table — that's the *entitlement's*
		// resource type, not the principal's.
		rt := rec.GetEntitlement().GetResourceTypeId()
		counts[rt]++
		return true
	}); err != nil {
		return nil, err
	}
	return counts, nil
}

// OutputFilepath returns the on-disk path the adapter writes to. The
// SQLite equivalent returns the .c1z file path; for Pebble we don't
// have a single file, so this returns the engine's directory. Used
// by tooling that wants to copy or inspect the storage.
func (a *Adapter) OutputFilepath() (string, error) {
	return a.engine.DBDir(), nil
}
