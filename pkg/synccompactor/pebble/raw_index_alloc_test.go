package pebble

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	enginepkg "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
)

// rawIndexAllocFixture builds the inputs forEachIndexKeyFromRaw sees
// for one grant winner: the raw record value, its dest primary key,
// and the bucket's dest lower bound.
type rawIndexAllocFixture struct {
	bucket    bucketSpec
	destKey   []byte
	destLower []byte
	value     []byte
}

func newGrantRawIndexAllocFixture(tb testing.TB) rawIndexAllocFixture {
	tb.Helper()
	const externalID = "grant-ext-1"
	rec := v3.GrantRecord_builder{
		ExternalId: externalID,
		Entitlement: v3.EntitlementRef_builder{
			ResourceTypeId: "group",
			ResourceId:     "g1",
			EntitlementId:  "group:g1:member",
		}.Build(),
		Principal: v3.PrincipalRef_builder{
			ResourceTypeId: "user",
			ResourceId:     "u1",
		}.Build(),
		NeedsExpansion: true,
	}.Build()
	value, err := proto.Marshal(rec)
	require.NoError(tb, err)
	return rawIndexAllocFixture{
		bucket: grantBucket(),
		// The structural-identity primary key — what the overlay actually
		// passes for a grant winner. (forEachIndexKeyFromRaw derives grant
		// index keys from the VALUE, but the fixture should still mirror
		// production inputs, not the retired external-id key layout.)
		destKey:   []byte(enginepkg.GrantRecordIdentityKey(rec)),
		destLower: enginepkg.GrantLowerBound(),
		value:     value,
	}
}

// TestForEachIndexKeyFromRawAllocs locks in the zero-allocation
// contract of the raw index-key scan: with warm scratch buffers and
// warm stats maps, generating the written grant index keys for a winner
// must not allocate. The raw field scanners (scanGrantIndexFieldsBytes
// et al.) return borrowed sub-slices of the value; the tuple decode
// and key construction write into caller-owned scratch. A regression
// here silently multiplies compactor allocations by the record count
// (tens of millions on production-scale merges).
func TestForEachIndexKeyFromRawAllocs(t *testing.T) {
	fx := newGrantRawIndexAllocFixture(t)
	stats := newMergeStatsAccumulator()
	var scratch rawIndexScratch
	var emitted int
	emitCheck := func(key []byte) error {
		emitted++
		require.NotEmpty(t, key, "emitted empty index key")
		return nil
	}
	require.NoError(t, forEachIndexKeyFromRaw(fx.bucket, fx.destKey, fx.destLower, fx.value, &scratch, stats, emitCheck))
	require.Equal(t, 2, emitted, "emitted grant index keys")
	run := func() {
		require.NoError(t, forEachIndexKeyFromRaw(fx.bucket, fx.destKey, fx.destLower, fx.value, &scratch, stats, func([]byte) error { return nil }))
	}
	allocs := testing.AllocsPerRun(100, run)
	require.Equal(t, float64(2), allocs, "forEachIndexKeyFromRaw allocs/op")
}

// TestSeenSuffixSetLookupAllocs locks in the allocation-free lookup
// contract of the overlay dedupe set: hashing a suffix and reading the
// recorded discovered_at must not allocate (the old map[string] set
// materialized a string per scanned record). Fresh inserts only pay
// amortized map growth.
func TestSeenSuffixSetLookupAllocs(t *testing.T) {
	seen := newSeenSuffixSet()
	suffix := []byte("group\x00g1\x00grant-ext-1")
	k := seen.keyOf(suffix)
	_, ok := seen.get(k)
	require.False(t, ok, "fresh key reported as seen")
	seen.put(k, 42)
	allocs := testing.AllocsPerRun(100, func() {
		k := seen.keyOf(suffix)
		ts, ok := seen.get(k)
		if !ok || ts != 42 {
			require.FailNowf(t, "seenSuffixSet lookup mismatch", "lookup = (%d, %v), want (42, true)", ts, ok)
		}
		seen.put(k, 42)
	})
	require.Equal(t, float64(0), allocs, "seenSuffixSet lookup allocs/op")
	require.Equal(t, 1, seen.size(), "seen.size()")
}

func BenchmarkForEachIndexKeyFromRaw(b *testing.B) {
	fx := newGrantRawIndexAllocFixture(b)
	stats := newMergeStatsAccumulator()
	var scratch rawIndexScratch
	var sink int
	emit := func(key []byte) error {
		sink += len(key)
		return nil
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := forEachIndexKeyFromRaw(fx.bucket, fx.destKey, fx.destLower, fx.value, &scratch, stats, emit); err != nil {
			b.Fatal(err)
		}
	}
	_ = sink
}
