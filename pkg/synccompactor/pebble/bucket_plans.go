//go:build batonsdkv2

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
//
// Stack 4 MVP covers the GrantRecord buckets implemented by Stack 3:
//   - grant primary records
//   - grant_by_entitlement index
//   - grant_by_principal index
//
// Other record-type buckets are added in the same commit series as
// their Stack 3 implementations.
func buildBucketPlans(syncIDBytes []byte) []bucketPlan {
	return []bucketPlan{
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
			name:  "grant_by_principal",
			lower: enginepkg.GrantByPrincipalSyncLowerBound(syncIDBytes),
			upper: enginepkg.GrantByPrincipalSyncUpperBound(syncIDBytes),
		},
	}
}
