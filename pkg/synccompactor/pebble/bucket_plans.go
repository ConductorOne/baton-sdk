package pebble

import (
	enginepkg "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
)

// bucketPlan describes one contiguous key range to compact. The v3 key
// layout puts the record-type byte (and, for indexes, the index
// discriminator) ahead of the sync_id, so a single sync_id occupies one
// sub-range per record-type bucket. We emit one SST + one excise span
// per bucket.
type bucketPlan struct {
	name  string
	lower []byte
	upper []byte
}

// buildBucketPlans returns the set of (lower, upper) excise spans that
// together cover every key in the engine that belongs to a single
// sync_id. The order is fixed and deterministic so logs and tests are
// stable.
func buildBucketPlans(syncIDBytes []byte) []bucketPlan {
	return []bucketPlan{
		{
			name:  "resource_type",
			lower: enginepkg.ResourceTypeSyncLowerBound(syncIDBytes),
			upper: enginepkg.ResourceTypeSyncUpperBound(syncIDBytes),
		},
		{
			name:  "resource",
			lower: enginepkg.ResourceSyncLowerBound(syncIDBytes),
			upper: enginepkg.ResourceSyncUpperBound(syncIDBytes),
		},
		{
			name:  "resource_by_parent",
			lower: enginepkg.ResourceByParentSyncLowerBound(syncIDBytes),
			upper: enginepkg.ResourceByParentSyncUpperBound(syncIDBytes),
		},
		{
			name:  "entitlement",
			lower: enginepkg.EntitlementSyncLowerBound(syncIDBytes),
			upper: enginepkg.EntitlementSyncUpperBound(syncIDBytes),
		},
		{
			name:  "entitlement_by_resource",
			lower: enginepkg.EntitlementByResourceSyncLowerBound(syncIDBytes),
			upper: enginepkg.EntitlementByResourceSyncUpperBound(syncIDBytes),
		},
		{
			name:  "grant",
			lower: enginepkg.GrantSyncLowerBound(syncIDBytes),
			upper: enginepkg.GrantSyncUpperBound(syncIDBytes),
		},
		{
			name:  "grant_by_entitlement",
			lower: enginepkg.GrantByEntitlementSyncLowerBound(syncIDBytes),
			upper: enginepkg.GrantByEntitlementSyncUpperBound(syncIDBytes),
		},
		{
			name:  "grant_by_entitlement_resource",
			lower: enginepkg.GrantByEntitlementResourceSyncLowerBound(syncIDBytes),
			upper: enginepkg.GrantByEntitlementResourceSyncUpperBound(syncIDBytes),
		},
		{
			name:  "grant_by_principal",
			lower: enginepkg.GrantByPrincipalSyncLowerBound(syncIDBytes),
			upper: enginepkg.GrantByPrincipalSyncUpperBound(syncIDBytes),
		},
		{
			name:  "grant_by_principal_resource_type",
			lower: enginepkg.GrantByPrincipalResourceTypeSyncLowerBound(syncIDBytes),
			upper: enginepkg.GrantByPrincipalResourceTypeSyncUpperBound(syncIDBytes),
		},
		{
			name:  "grant_by_needs_expansion",
			lower: enginepkg.GrantByNeedsExpansionSyncLowerBound(syncIDBytes),
			upper: enginepkg.GrantByNeedsExpansionSyncUpperBound(syncIDBytes),
		},
		{
			name:  "asset",
			lower: enginepkg.AssetSyncLowerBound(syncIDBytes),
			upper: enginepkg.AssetSyncUpperBound(syncIDBytes),
		},
		{
			name:  "sync_run",
			lower: enginepkg.SyncRunLowerBound(syncIDBytes),
			upper: enginepkg.SyncRunUpperBound(syncIDBytes),
		},
		{
			name:  "sync_stats_sidecar",
			lower: enginepkg.SyncStatsSidecarSyncLowerBound(syncIDBytes),
			upper: enginepkg.SyncStatsSidecarSyncUpperBound(syncIDBytes),
		},
	}
}
