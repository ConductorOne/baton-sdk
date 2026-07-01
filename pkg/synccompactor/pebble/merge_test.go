package pebble

import (
	"context"
	"testing"
	"time"

	cpebble "github.com/cockroachdb/pebble/v2"
	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	enginepkg "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
)

func grantAt(syncID, externalID string, at time.Time) *v3.GrantRecord {
	return v3.GrantRecord_builder{
		ExternalId: externalID,
		Entitlement: v3.EntitlementRef_builder{
			ResourceTypeId: "app", ResourceId: "github", EntitlementId: "ent-A",
		}.Build(),
		Principal: v3.PrincipalRef_builder{
			ResourceTypeId: "user", ResourceId: externalID,
		}.Build(),
		DiscoveredAt: timestamppb.New(at),
	}.Build()
}

// TestMergeIntoUnionNewerWins is the core parity pin: a k-way merge of
// two sources into an empty dest sync yields the UNION of records, and
// for a key present in both, the record with the strictly-newer
// discovered_at survives (ties keep the earlier-applied incumbent). All
// surviving records are re-keyed to the destination sync id.
func TestMergeIntoUnionNewerWins(t *testing.T) {
	ctx := context.Background()
	src1, _ := newEngine(t, "src1")
	src2, _ := newEngine(t, "src2")
	dst, _ := newEngine(t, "dst")

	syncA := ksuid.New().String()
	syncB := ksuid.New().String()
	destSync := ksuid.New().String()

	older := time.Unix(1000, 0).UTC()
	newer := time.Unix(2000, 0).UTC()

	// src1 (applied first): shared key @older + a unique key.
	require.NoError(t, src1.SetCurrentSync(syncA))
	require.NoError(t, src1.PutGrantRecords(ctx, grantAt(syncA, "g-shared", older), grantAt(syncA, "g-only1", older)))
	// src2 (applied second): shared key @newer (must win) + a unique key.
	require.NoError(t, src2.SetCurrentSync(syncB))
	require.NoError(t, src2.PutGrantRecords(ctx, grantAt(syncB, "g-shared", newer), grantAt(syncB, "g-only2", newer)))

	// No SetCurrentSync here: MergeInto must bind the dest engine to
	// destSyncID itself (record values carry no sync_id, so a stale
	// binding would silently write into the wrong sync's keyspace).
	stats, err := MergeInto(ctx, dst, []SourceSync{{Engine: src1, SyncID: syncA}, {Engine: src2, SyncID: syncB}}, destSync)
	require.NoError(t, err, "MergeInto")
	// src2's g-shared@newer overrode src1's incumbent — exactly one
	// override, and its dead bytes must cover at least the incumbent's
	// key+value.
	require.Equal(t, int64(1), stats.OverriddenRecords, "FoldStats.OverriddenRecords")
	require.Positive(t, stats.DeadBytes, "FoldStats.DeadBytes (overridden incumbent's key+value+index keys)")

	// Union of distinct external_ids: g-shared, g-only1, g-only2 = 3.
	require.Equal(t, 3, countGrants(t, dst, destSync), "merged grant count (union, deduped)")

	// Iterating under destSync proves every survivor is re-keyed there,
	// and g-shared kept the newer discovered_at.
	seen := map[string]*v3.GrantRecord{}
	require.NoError(t, dst.IterateGrants(ctx, func(r *v3.GrantRecord) bool {
		seen[r.GetExternalId()] = r
		return true
	}), "IterateGrants")
	shared, ok := seen["g-shared"]
	require.True(t, ok, "g-shared missing from merged output")
	require.True(t, shared.GetDiscoveredAt().AsTime().Equal(newer),
		"g-shared discovered_at = %s, want newer %s (newer-wins)", shared.GetDiscoveredAt().AsTime(), newer)
	for _, id := range []string{"g-only1", "g-only2"} {
		_, ok := seen[id]
		require.True(t, ok, "%q missing from merged union", id)
	}

	// The raw merge derives index keys itself (no engine put path):
	// every index keyspace must match a recompute from the surviving
	// primary records, including the replaced incumbent's swapped keys.
	assertIndexesMatchDerived(t, ctx, dst)
}

// TestMergeIntoTieKeepsIncumbent pins the equal-discovered_at tie rule
// to SQLite's strict `>`: on a tie, the earlier-applied source's record
// is kept (it is the incumbent the newer write does not replace).
func TestMergeIntoTieKeepsIncumbent(t *testing.T) {
	ctx := context.Background()
	src1, _ := newEngine(t, "tsrc1")
	src2, _ := newEngine(t, "tsrc2")
	dst, _ := newEngine(t, "tdst")

	syncA := ksuid.New().String()
	syncB := ksuid.New().String()
	destSync := ksuid.New().String()
	tie := time.Unix(1500, 0).UTC()

	// Same structured key + same discovered_at in both, distinguished by retained external_id.
	g1 := grantAt(syncA, "g-tie", tie)
	g1.SetExternalId("from-src1")
	g2 := grantAt(syncB, "g-tie", tie)
	g2.SetExternalId("from-src2")

	_ = src1.SetCurrentSync(syncA)
	require.NoError(t, src1.PutGrantRecords(ctx, g1))
	_ = src2.SetCurrentSync(syncB)
	require.NoError(t, src2.PutGrantRecords(ctx, g2))
	// src1 applied first → incumbent wins the tie. MergeInto binds the
	// dest sync itself.
	stats, err := MergeInto(ctx, dst, []SourceSync{{Engine: src1, SyncID: syncA}, {Engine: src2, SyncID: syncB}}, destSync)
	require.NoError(t, err)
	// A tie keeps the incumbent — nothing is overridden, no dead bytes.
	require.Zero(t, stats.OverriddenRecords, "FoldStats.OverriddenRecords (tie keeps incumbent)")
	require.Zero(t, stats.DeadBytes, "FoldStats.DeadBytes (tie keeps incumbent)")

	var winner string
	require.NoError(t, dst.IterateGrants(ctx, func(r *v3.GrantRecord) bool {
		winner = r.GetExternalId()
		return true
	}))
	require.Equal(t, "from-src1", winner, "tie winner (earliest-applied incumbent, strict-> parity)")
	assertIndexesMatchDerived(t, ctx, dst)
}

// baseNewerSets returns a base record set (one record per primary type)
// stamped at `base` and an overriding source set with the SAME keys but
// distinct display/principal tags stamped at `other`. The two stamps
// let a test fold an older source into a newer base, or vice versa, and
// assert per-type winner + FoldStats without rebuilding fixtures.
func baseNewerSets(base, other time.Time) (recordSet, recordSet) {
	baseSet := recordSet{
		rts: []*v3.ResourceTypeRecord{winRT("rt-1", "rt-base", base)},
		rs:  []*v3.ResourceRecord{winRes("user", "u1", "res-base", base)},
		es:  []*v3.EntitlementRecord{winEnt("e-1", "ent-base", base)},
		gs:  []*v3.GrantRecord{winGrant("g-1", "base", base)},
	}
	otherSet := recordSet{
		rts: []*v3.ResourceTypeRecord{winRT("rt-1", "rt-other", other)},
		rs:  []*v3.ResourceRecord{winRes("user", "u1", "res-other", other)},
		es:  []*v3.EntitlementRecord{winEnt("e-1", "ent-other", other)},
		gs:  []*v3.GrantRecord{winGrant("g-1", "other", other)},
	}
	return baseSet, otherSet
}

func seedBase(t *testing.T, ctx context.Context, name string, rs recordSet) (*enginepkg.Engine, string) {
	t.Helper()
	dest, _ := newEngine(t, name)
	destSync := ksuid.New().String()
	require.NoError(t, dest.SetCurrentSync(destSync))
	populateEngine(t, ctx, dest, rs)
	return dest, destSync
}

func assertFoldWinners(t *testing.T, ctx context.Context, eng *enginepkg.Engine, rtDN, resDN, entDN, grantPrincipal string) {
	t.Helper()
	rt, err := eng.GetResourceTypeRecord(ctx, "rt-1")
	require.NoError(t, err)
	require.Equal(t, rtDN, rt.GetDisplayName(), "resource_type winner")
	res, err := eng.GetResourceRecord(ctx, "user", "u1")
	require.NoError(t, err)
	require.Equal(t, resDN, res.GetDisplayName(), "resource winner")
	ent, err := eng.GetEntitlementRecord(ctx, "user:u1:custom:e-1")
	require.NoError(t, err)
	require.Equal(t, entDN, ent.GetDisplayName(), "entitlement winner")
	g, err := eng.GetGrantRecord(ctx, "app:github:custom:g-1:user:"+grantPrincipal)
	require.NoError(t, err)
	require.Equal(t, grantPrincipal, g.GetPrincipal().GetResourceId(), "grant winner")
}

// TestMergeIntoBaseNewerKeepsBaseAllTypes is the symmetric counterpart
// to TestMergeIntoUnionNewerWins for the real fold shape: an OLDER
// partial folded into a newer pre-seeded base must NOT regress any of
// the four primary record types. This is the "newer data already lives
// in the base sync" case — the fold must keep every incumbent, record
// nothing as overridden, and leave the derived indexes untouched. It
// guards the strict-newer comparison (`newTs <= oldTs` keeps the
// incumbent) for every per-type discovered_at field, not just grants.
func TestMergeIntoBaseNewerKeepsBaseAllTypes(t *testing.T) {
	ctx := context.Background()
	older := time.Unix(1000, 0).UTC()
	newer := time.Unix(2000, 0).UTC()

	baseSet, olderSet := baseNewerSets(newer, older)
	dest, destSync := seedBase(t, ctx, "base-newer-dest", baseSet)
	src := buildEngineSource(t, ctx, "older-src", olderSet)

	stats, err := MergeInto(ctx, dest, []SourceSync{src}, destSync)
	require.NoError(t, err)

	// Nothing regressed: the base (newer) version survives for every type.
	assertFoldWinners(t, ctx, dest, "rt-base", "res-base", "ent-base", "base")
	// An older source overrides nothing, so no dead weight accrues.
	require.Zero(t, stats.OverriddenRecords, "older partial must override nothing")
	require.Zero(t, stats.DeadBytes, "no overrides means no dead bytes")
	assertIndexesMatchDerived(t, ctx, dest)
}

// TestMergeIntoNewerPartialOverridesBaseAllTypes is the override
// direction into a pre-seeded base: a NEWER partial replaces the base's
// record for every primary type, FoldStats counts one override per type
// with positive dead bytes, and the stale index keys are swapped for
// the winners'. Together with the base-newer test this pins newest-wins
// in BOTH directions across all four discovered_at fields.
func TestMergeIntoNewerPartialOverridesBaseAllTypes(t *testing.T) {
	ctx := context.Background()
	older := time.Unix(1000, 0).UTC()
	newer := time.Unix(2000, 0).UTC()

	baseSet, newerSet := baseNewerSets(older, newer)
	dest, destSync := seedBase(t, ctx, "base-older-dest", baseSet)
	src := buildEngineSource(t, ctx, "newer-src", newerSet)

	stats, err := MergeInto(ctx, dest, []SourceSync{src}, destSync)
	require.NoError(t, err)

	// The newer partial wins for every type.
	assertFoldWinners(t, ctx, dest, "rt-other", "res-other", "ent-other", "other")
	// The resource_type, resource, and entitlement override. The grant fixture
	// changes principal, which is now part of grant identity, so it coexists.
	require.Equal(t, int64(3), stats.OverriddenRecords, "one override per non-grant primary type")
	require.Positive(t, stats.DeadBytes, "overrides must record the shadowed incumbents' bytes")
	assertIndexesMatchDerived(t, ctx, dest)
}

// sumBucketRawBytes sums len(key)+len(value) over a bucket's primary
// range and every one of its derived index ranges — the exact byte
// total the fold merge must charge as dead when every record in the
// bucket is overridden (index values are empty, so an index entry
// contributes only its key length, matching the merge's accounting).
func sumBucketRawBytes(t *testing.T, eng *enginepkg.Engine, bucket bucketSpec) int64 {
	t.Helper()
	lo, hi := bucket.syncRange()
	ranges := append([][2][]byte{{lo, hi}}, bucketIndexRanges(bucket)...)
	var total int64
	for _, r := range ranges {
		iter, err := eng.DB().NewIter(&cpebble.IterOptions{LowerBound: r[0], UpperBound: r[1]})
		require.NoError(t, err)
		for iter.First(); iter.Valid(); iter.Next() {
			total += int64(len(iter.Key())) + int64(len(iter.Value()))
		}
		require.NoError(t, iter.Error())
		require.NoError(t, iter.Close())
	}
	return total
}

// TestMergeIntoDeadBytesExactCount pins FoldStats.DeadBytes to the exact
// footprint of the shadowed incumbent — primary key+value plus every
// derived index key — instead of just "> 0". It seeds one grant,
// measures its on-disk byte total straight from the dest LSM, then folds
// a strictly-newer override and asserts the reported dead bytes equal
// that measured footprint. The override is a deliberately different size
// (longer principal) and carries an extra index key (needs_expansion),
// so a wrong-record miscount — charging the winner's bytes, or dropping
// the index-key bytes — can't coincidentally match. This is the guard
// the qualitative >0 / grows / resets assertions can't provide.
func TestMergeIntoDeadBytesExactCount(t *testing.T) {
	ctx := context.Background()
	older := time.Unix(1000, 0).UTC()
	newer := time.Unix(2000, 0).UTC()

	dest, destSync := seedBase(t, ctx, "deadbytes-dest", recordSet{
		gs: []*v3.GrantRecord{winGrant("g-1", "p", older)},
	})
	// Exact footprint of the incumbent the override will shadow,
	// measured before the merge touches it.
	wantDead := sumBucketRawBytes(t, dest, grantBucket())

	override := v3.GrantRecord_builder{
		ExternalId: "g-1-newer",
		Entitlement: v3.EntitlementRef_builder{
			ResourceTypeId: "app", ResourceId: "github", EntitlementId: "g-1",
		}.Build(),
		Principal:      v3.PrincipalRef_builder{ResourceTypeId: "user", ResourceId: "p"}.Build(),
		NeedsExpansion: true,
		DiscoveredAt:   timestamppb.New(newer),
	}.Build()
	src := buildEngineSource(t, ctx, "deadbytes-src", recordSet{gs: []*v3.GrantRecord{override}})

	stats, err := MergeInto(ctx, dest, []SourceSync{src}, destSync)
	require.NoError(t, err)
	require.Equal(t, int64(1), stats.OverriddenRecords)
	require.Equal(t, wantDead, stats.DeadBytes,
		"dead bytes must equal the incumbent's primary key+value plus its index keys")
	assertIndexesMatchDerived(t, ctx, dest)
}
