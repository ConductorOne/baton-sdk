package pebble

import (
	"bytes"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
)

// mergeStatsAccumulator builds the dest sync's stats sidecar while the
// compactor writes merge winners, so compactPebble can persist stats
// without re-scanning the freshly written output (the old
// PersistSyncStats post-pass).
//
// Counting contract: exactly one total increment per record that lands
// in the dest sync. The overlay path piggybacks grouping on the index
// key scan (forEachIndexKeyFromRaw already extracts the resource type
// / entitlement resource type for winners); the K-way path uses
// countWinner at its final dedupe sites, because there the index scan
// also ran for losing records and can't be reused for counting.
//
// Group maps are map[string]*int64 so the per-record []byte->string
// map read stays allocation-free (the compiler only elides the
// conversion for reads); a key string is materialized only on the
// first occurrence of each resource type.
type mergeStatsAccumulator struct {
	totals           [runBucketCount]int64
	resourcesByRT    map[string]*int64
	entitlementsByRT map[string]*int64
	grantsByEntRT    map[string]*int64
	scratch          rawIndexScratch
}

func newMergeStatsAccumulator() *mergeStatsAccumulator {
	return &mergeStatsAccumulator{
		resourcesByRT:    map[string]*int64{},
		entitlementsByRT: map[string]*int64{},
		grantsByEntRT:    map[string]*int64{},
	}
}

func incrGroup(m map[string]*int64, key []byte) {
	if p, ok := m[string(key)]; ok {
		*p++
		return
	}
	n := int64(1)
	m[string(key)] = &n
}

// countWinnerTotal records a winning record's existence without
// grouping. Used by the overlay path, where grouping happens inside
// forEachIndexKeyFromRaw via groupResource / groupGrant.
func (a *mergeStatsAccumulator) countWinnerTotal(bucketID int) {
	if a == nil {
		return
	}
	a.totals[bucketID]++
}

func (a *mergeStatsAccumulator) groupResource(rt []byte) {
	if a == nil {
		return
	}
	incrGroup(a.resourcesByRT, rt)
}

func (a *mergeStatsAccumulator) groupEntitlement(rt []byte) {
	if a == nil {
		return
	}
	incrGroup(a.entitlementsByRT, rt)
}

func (a *mergeStatsAccumulator) groupGrant(entRT []byte) {
	if a == nil {
		return
	}
	incrGroup(a.grantsByEntRT, entRT)
}

// regroupGrant moves one grant from oldRT's group to newRT's when an
// overlay replacement swaps in a value with a different entitlement
// resource type. Totals are untouched — the replacement targets the
// same logical key, which was counted at first admission.
func (a *mergeStatsAccumulator) regroupGrant(oldRT, newRT []byte) {
	if a == nil || bytes.Equal(oldRT, newRT) {
		return
	}
	if p, ok := a.grantsByEntRT[string(oldRT)]; ok {
		*p--
	}
	incrGroup(a.grantsByEntRT, newRT)
}

// regroupEntitlement moves one entitlement from oldRT's group to
// newRT's when an overlay replacement swaps in a value whose resource
// has a different resource type. Totals are untouched — the replacement
// targets the same logical key, which was counted at first admission.
func (a *mergeStatsAccumulator) regroupEntitlement(oldRT, newRT []byte) {
	if a == nil || bytes.Equal(oldRT, newRT) {
		return
	}
	if p, ok := a.entitlementsByRT[string(oldRT)]; ok {
		*p--
	}
	incrGroup(a.entitlementsByRT, newRT)
}

// resetBucket zeroes one bucket's accumulated counts. Used by the
// overlay merge's restart path: the bucket's dest keyspace is
// range-deleted and re-materialized through the blind K-way path, so
// every overlay-era count for it is stale. The per-RT group maps are
// per-record-type (resourcesByRT only ever holds resources-bucket
// counts, entitlementsByRT only entitlements, grantsByEntRT only
// grants), so clearing the matching map cannot disturb another bucket's
// grouping.
func (a *mergeStatsAccumulator) resetBucket(bucketID int) {
	if a == nil {
		return
	}
	a.totals[bucketID] = 0
	switch bucketID {
	case runBucketResources:
		clear(a.resourcesByRT)
	case runBucketEntitlements:
		clear(a.entitlementsByRT)
	case runBucketGrants:
		clear(a.grantsByEntRT)
	}
}

// countWinner is the K-way variant: total plus grouping derived from
// the winner's key (resources) or value (entitlements, grants).
func (a *mergeStatsAccumulator) countWinner(bucket bucketSpec, destKey []byte, destLower []byte, value []byte) error {
	if a == nil {
		return nil
	}
	a.totals[bucket.id]++
	switch bucket.id {
	case runBucketResources:
		rt, _, err := decodePrimaryTailBytes2(destKey, destLower, &a.scratch)
		if err != nil {
			return err
		}
		incrGroup(a.resourcesByRT, rt)
	case runBucketEntitlements:
		rt, _, err := scanEntitlementResourceBytes(value)
		if err != nil {
			return err
		}
		incrGroup(a.entitlementsByRT, rt)
	case runBucketGrants:
		entRT, _, _, _, _, _, err := scanGrantIndexFieldsBytes(value)
		if err != nil {
			return err
		}
		incrGroup(a.grantsByEntRT, entRT)
	default:
	}
	return nil
}

// record renders the accumulated counts as the engine sidecar record.
// Assets are always zero: compaction does not copy assets, matching
// what a post-merge recompute would produce. SyncId / WrittenAt are
// filled by Engine.PersistComputedSyncStats.
func (a *mergeStatsAccumulator) record() *v3.SyncStatsRecord {
	rec := &v3.SyncStatsRecord{}
	rec.SetResourceTypes(a.totals[runBucketResourceTypes])
	rec.SetResources(a.totals[runBucketResources])
	rec.SetEntitlements(a.totals[runBucketEntitlements])
	rec.SetGrants(a.totals[runBucketGrants])
	rec.SetAssets(0)
	resourcesByRT := make(map[string]int64, len(a.resourcesByRT))
	for rt, p := range a.resourcesByRT {
		resourcesByRT[rt] = *p
	}
	entitlementsByRT := make(map[string]int64, len(a.entitlementsByRT))
	for rt, p := range a.entitlementsByRT {
		entitlementsByRT[rt] = *p
	}
	grantsByEntRT := make(map[string]int64, len(a.grantsByEntRT))
	for rt, p := range a.grantsByEntRT {
		grantsByEntRT[rt] = *p
	}
	rec.SetResourcesByResourceType(resourcesByRT)
	rec.SetEntitlementsByResourceType(entitlementsByRT)
	rec.SetGrantsByEntitlementResourceType(grantsByEntRT)
	return rec
}

func (a *mergeStatsAccumulator) addRecord(rec *v3.SyncStatsRecord) {
	if a == nil || rec == nil {
		return
	}
	a.totals[runBucketResourceTypes] += rec.GetResourceTypes()
	a.totals[runBucketResources] += rec.GetResources()
	a.totals[runBucketEntitlements] += rec.GetEntitlements()
	a.totals[runBucketGrants] += rec.GetGrants()
	for rt, count := range rec.GetResourcesByResourceType() {
		if p, ok := a.resourcesByRT[rt]; ok {
			*p += count
			continue
		}
		n := count
		a.resourcesByRT[rt] = &n
	}
	for rt, count := range rec.GetEntitlementsByResourceType() {
		if p, ok := a.entitlementsByRT[rt]; ok {
			*p += count
			continue
		}
		n := count
		a.entitlementsByRT[rt] = &n
	}
	for rt, count := range rec.GetGrantsByEntitlementResourceType() {
		if p, ok := a.grantsByEntRT[rt]; ok {
			*p += count
			continue
		}
		n := count
		a.grantsByEntRT[rt] = &n
	}
}
