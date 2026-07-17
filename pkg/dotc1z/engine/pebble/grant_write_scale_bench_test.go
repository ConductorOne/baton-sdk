package pebble

// Scale benchmarks comparing the two grant bulk-write paths at configurable
// grant counts:
//
//   - Baseline: Engine.PutGrantRecords in 10k batches. Every batch after the
//     first of a fresh sync does a per-record db.Get (read-before-write index
//     cleanup). That Get's cost grows with the LSM size, so the baseline is
//     expected to degrade super-linearly as the grant count climbs.
//   - PutUnique: Engine.UnsafePutUniqueGrantRecords — parallel encode,
//     unconditional writes, no read-before-write. Used by the sqlite->pebble
//     converter.
//
// The grant count defaults to 1,000,000. Set BATONSDK_GRANT_BENCH_N to push it
// toward production scale (e.g. 50000000) on a machine with enough disk.
// Records are generated per-batch so memory stays bounded regardless of N.

import (
	"context"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
)

// benchGrantSyncID is a valid KSUID used as the fresh sync id for the scale
// benchmarks (MarkFreshSync requires a parseable KSUID).
const benchGrantSyncID = "36zM46KKuaBq0wjSSvKh5o0350y"

func benchGrantCount(b *testing.B) int {
	b.Helper()
	const def = 1_000_000
	env := os.Getenv("BATONSDK_GRANT_BENCH_N")
	if env == "" {
		return def
	}
	n, err := strconv.Atoi(env)
	require.NoError(b, err)
	require.Positive(b, n)
	return n
}

// makeGrantRecordBatch builds count grant records with unique external_ids
// (and principals) starting at offset, so the (sync_id, external_id) key is
// unique across the whole run — the invariant UnsafePutUniqueGrantRecords relies on.
func makeGrantRecordBatch(syncID string, offset, count int) []*v3.GrantRecord {
	now := timestamppb.Now()
	out := make([]*v3.GrantRecord, count)
	for i := 0; i < count; i++ {
		id := offset + i
		ent := &v3.EntitlementRef{}
		ent.SetResourceTypeId("group")
		ent.SetResourceId("g1")
		ent.SetEntitlementId("ent1")

		pr := &v3.PrincipalRef{}
		pr.SetResourceTypeId("user")
		pr.SetResourceId("u" + strconv.Itoa(id))

		r := &v3.GrantRecord{}
		r.SetExternalId("grant-" + strconv.Itoa(id))
		r.SetEntitlement(ent)
		r.SetPrincipal(pr)
		r.SetDiscoveredAt(now)
		out[i] = r
	}
	return out
}

func benchmarkGrantWriteScale(b *testing.B, putUnique, grantIndex bool) {
	n := benchGrantCount(b)
	ctx := context.Background()
	const batchSize = 10000

	b.ReportMetric(float64(n), "grants")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e, err := Open(ctx, b.TempDir(), WithGrantDigestIndex(grantIndex))
		require.NoError(b, err)
		require.NoError(b, e.MarkFreshSync(benchGrantSyncID))

		for written := 0; written < n; written += batchSize {
			m := batchSize
			if rem := n - written; rem < m {
				m = rem
			}
			recs := makeGrantRecordBatch(benchGrantSyncID, written, m)
			if putUnique {
				require.NoError(b, e.UnsafePutUniqueGrantRecords(ctx, recs...))
			} else {
				require.NoError(b, e.PutGrantRecords(ctx, recs...))
			}
		}
		require.NoError(b, e.db.FlushMemtables())

		b.StopTimer()
		require.NoError(b, e.Close())
		b.StartTimer()
	}
}

// The digest index is built at seal time, never inline, so the write
// path has no per-grant digest cost to isolate; the flag is kept in the
// matrix only to confirm that.
func BenchmarkGrantWriteScale_NoDigestIndex(b *testing.B)  { benchmarkGrantWriteScale(b, false, false) }
func BenchmarkGrantWriteScale(b *testing.B)                { benchmarkGrantWriteScale(b, false, true) }
func BenchmarkGrantWriteScaleUnsafePutUnique(b *testing.B) { benchmarkGrantWriteScale(b, true, true) }
