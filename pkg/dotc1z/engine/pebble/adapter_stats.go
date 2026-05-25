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

	counts := map[string]int64{
		"resource_types": 0,
		"resources":      0,
		"entitlements":   0,
		"grants":         0,
		"assets":         0,
	}

	if err := a.engine.IterateResourceTypesBySync(ctx, syncID, func(*v3.ResourceTypeRecord) bool {
		counts["resource_types"]++
		return true
	}); err != nil {
		return nil, err
	}
	if err := a.engine.IterateResourcesBySync(ctx, syncID, func(*v3.ResourceRecord) bool {
		counts["resources"]++
		return true
	}); err != nil {
		return nil, err
	}
	if err := a.engine.IterateEntitlementsBySync(ctx, syncID, func(*v3.EntitlementRecord) bool {
		counts["entitlements"]++
		return true
	}); err != nil {
		return nil, err
	}
	if err := a.engine.IterateGrantsBySync(ctx, syncID, func(*v3.GrantRecord) bool {
		counts["grants"]++
		return true
	}); err != nil {
		return nil, err
	}
	if err := a.engine.IterateAssetsBySync(ctx, syncID, func(*v3.AssetRecord) bool {
		counts["assets"]++
		return true
	}); err != nil {
		return nil, err
	}
	return counts, nil
}

// GrantStats returns just the grant count, partitioned by entitlement
// resource_type_id. Used by progresslog to show per-RT progress. Like
// Stats, this is an exact count via iteration.
func (a *Adapter) GrantStats(ctx context.Context, syncType connectorstore.SyncType, syncID string) (map[string]int64, error) {
	if syncID == "" {
		syncID = a.currentSyncID()
	}
	if syncID == "" {
		return map[string]int64{}, nil
	}
	counts := map[string]int64{}
	if err := a.engine.IterateGrantsBySync(ctx, syncID, func(rec *v3.GrantRecord) bool {
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
