package pebble

import (
	enginepkg "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
)

// bucketPlan describes one contiguous key range to compact. The v3 key
// layout is single-sync and carries no sync_id, so each record-type
// (and index) bucket is one contiguous range covering the whole file.
// We emit one SST + one excise span per bucket.
type bucketPlan struct {
	name  string
	lower []byte
	upper []byte
}

// buildBucketPlans returns the set of (lower, upper) excise spans that
// together cover every key in the engine. A v3 Pebble c1z holds one
// sync and keys carry no sync_id, so this is the whole keyspace. The
// order is fixed and deterministic so logs and tests are stable.
func buildBucketPlans() []bucketPlan {
	return []bucketPlan{
		{
			name:  "resource_type",
			lower: enginepkg.ResourceTypeLowerBound(),
			upper: enginepkg.ResourceTypeUpperBound(),
		},
		{
			name:  "resource",
			lower: enginepkg.ResourceLowerBound(),
			upper: enginepkg.ResourceUpperBound(),
		},
		{
			name:  "resource_by_parent",
			lower: enginepkg.ResourceByParentLowerBound(),
			upper: enginepkg.ResourceByParentUpperBound(),
		},
		{
			name:  "entitlement",
			lower: enginepkg.EntitlementLowerBound(),
			upper: enginepkg.EntitlementUpperBound(),
		},
		{
			name:  "entitlement_by_resource",
			lower: enginepkg.EntitlementByResourceLowerBound(),
			upper: enginepkg.EntitlementByResourceUpperBound(),
		},
		{
			name:  "grant",
			lower: enginepkg.GrantLowerBound(),
			upper: enginepkg.GrantUpperBound(),
		},
		{
			name:  "grant_by_entitlement",
			lower: enginepkg.GrantByEntitlementLowerBound(),
			upper: enginepkg.GrantByEntitlementUpperBound(),
		},
		{
			name:  "grant_by_entitlement_resource",
			lower: enginepkg.GrantByEntitlementResourceLowerBound(),
			upper: enginepkg.GrantByEntitlementResourceUpperBound(),
		},
		{
			name:  "grant_by_principal",
			lower: enginepkg.GrantByPrincipalLowerBound(),
			upper: enginepkg.GrantByPrincipalUpperBound(),
		},
		{
			name:  "grant_by_principal_resource_type",
			lower: enginepkg.GrantByPrincipalResourceTypeLowerBound(),
			upper: enginepkg.GrantByPrincipalResourceTypeUpperBound(),
		},
		{
			name:  "grant_by_needs_expansion",
			lower: enginepkg.GrantByNeedsExpansionLowerBound(),
			upper: enginepkg.GrantByNeedsExpansionUpperBound(),
		},
		{
			name:  "asset",
			lower: enginepkg.AssetLowerBound(),
			upper: enginepkg.AssetUpperBound(),
		},
		{
			name:  "sync_run",
			lower: enginepkg.SyncRunLowerBound(),
			upper: enginepkg.SyncRunUpperBound(),
		},
		{
			name:  "sync_stats_sidecar",
			lower: enginepkg.SyncStatsSidecarLowerBound(),
			upper: enginepkg.SyncStatsSidecarUpperBound(),
		},
	}
}
