package pebble

import (
	"context"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// Stats returns a map of {table_name: row_count} for the given sync.
// Used by pkg/sync/progresslog to show the "syncing X resources, Y
// entitlements, Z grants" progress lines. The SQLite engine answers
// this with a fast COUNT(*) per table; for Pebble we iterate each
// per-record-type bucket and count keys in the [sync_lo, sync_hi)
// range. Counts are exact (not estimates).
//
// Mirrors connectorstore semantics: syncType == SyncTypeAny accepts
// any sync; otherwise we filter by the sync_run's type.
func (a *Adapter) Stats(ctx context.Context, syncType connectorstore.SyncType, syncID string) (map[string]int64, error) {
	if syncID == "" {
		syncID = a.currentSyncID()
	}
	if syncID == "" {
		// No current sync — return empty stats (matches SQLite's
		// behavior when no sync exists yet).
		return map[string]int64{}, nil
	}

	// Verify the sync exists and matches the requested type.
	sr, err := a.engine.GetSyncRunRecord(ctx, syncID)
	if err == nil && sr != nil {
		if syncType != connectorstore.SyncTypeAny &&
			syncType != connectorstore.SyncType(v3SyncTypeToString(sr.GetType())) {
			return nil, nil
		}
	}

	// Fast path: read the sidecar populated at EndFreshSync. Sidecar
	// is one LSM Get vs the legacy fallback's O(N) iteration. Sync
	// types that haven't run through the sidecar writer (Partial /
	// Incremental that bypass EndFreshSync) fall through to the
	// legacy path below.
	if stats, err := a.engine.readSyncStats(ctx, syncID); err == nil && stats != nil {
		return statsRecordToMap(stats), nil
	}

	return a.statsFromIteration(ctx)
}

// statsFromIteration is the legacy O(N) path retained as the
// sidecar fallback. Used by older c1z files that predate the
// sidecar and by sync types that don't go through EndFreshSync.
func (a *Adapter) statsFromIteration(ctx context.Context) (map[string]int64, error) {
	counts := map[string]int64{
		"resource_types": 0,
		"resources":      0,
		"entitlements":   0,
		"grants":         0,
		"assets":         0,
	}

	if err := a.engine.IterateResourceTypes(ctx, func(*v3.ResourceTypeRecord) bool {
		counts["resource_types"]++
		return true
	}); err != nil {
		return nil, err
	}
	if err := a.engine.IterateResources(ctx, func(r *v3.ResourceRecord) bool {
		counts["resources"]++
		counts[r.GetResourceTypeId()]++
		return true
	}); err != nil {
		return nil, err
	}
	if err := a.engine.IterateEntitlements(ctx, func(*v3.EntitlementRecord) bool {
		counts["entitlements"]++
		return true
	}); err != nil {
		return nil, err
	}
	if err := a.engine.IterateGrants(ctx, func(*v3.GrantRecord) bool {
		counts["grants"]++
		return true
	}); err != nil {
		return nil, err
	}
	if err := a.engine.IterateAssets(ctx, func(*v3.AssetRecord) bool {
		counts["assets"]++
		return true
	}); err != nil {
		return nil, err
	}
	return counts, nil
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

// GrantStats returns just the grant count, partitioned by entitlement
// resource_type_id. Used by progresslog to show per-RT progress. Like
// Stats, this is an exact count via iteration.
//
// syncType is validated against the sync_run's actual type to match
// SQLite's C1File.GrantStats: SyncTypeAny accepts any sync; otherwise
// we require the sync_run.Type to match exactly. A type mismatch
// returns (nil, nil) — the syncer treats that as "nothing to show
// for this view," same as the SQLite contract.
func (a *Adapter) GrantStats(ctx context.Context, syncType connectorstore.SyncType, syncID string) (map[string]int64, error) {
	if syncID == "" {
		syncID = a.currentSyncID()
	}
	if syncID == "" {
		return map[string]int64{}, nil
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
