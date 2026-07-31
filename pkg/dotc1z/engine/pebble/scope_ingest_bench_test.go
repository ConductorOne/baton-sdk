package pebble

// A/B ingest benchmarks for the source-scope index work: measure ordinary
// UNSCOPED ingest cost per row kind so the branch can be compared against
// origin/main with the identical bench file (it deliberately uses only
// public Engine methods that exist on both).
//
//   - Entitlements are the material case: main stages entitlement puts with
//     no read at all; the branch adds a per-row db.Get (after the first
//     fresh-sync batch) to recover a prior source-scope stamp.
//   - Grants/resources already paid a per-row Get on main; the branch adds
//     raw-value scans for the scope field.
//
// Row count defaults to 200k in 10k batches; override with
// BATONSDK_SCOPE_BENCH_N. Multiple batches matter: the first batch of a
// fresh sync takes the provably-empty fast path, so per-row reads only
// start at batch two.

import (
	"context"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
)

func scopeBenchCount(b *testing.B) int {
	b.Helper()
	const def = 200_000
	env := os.Getenv("BATONSDK_SCOPE_BENCH_N")
	if env == "" {
		return def
	}
	n, err := strconv.Atoi(env)
	require.NoError(b, err)
	require.Positive(b, n)
	return n
}

func makeEntitlementRecordBatch(offset, count int) []*v3.EntitlementRecord {
	now := timestamppb.Now()
	out := make([]*v3.EntitlementRecord, count)
	for i := 0; i < count; i++ {
		id := offset + i
		res := &v3.ResourceRef{}
		res.SetResourceTypeId("group")
		res.SetResourceId("g" + strconv.Itoa(id%1000))
		r := &v3.EntitlementRecord{}
		r.SetExternalId("ent-" + strconv.Itoa(id))
		r.SetResource(res)
		r.SetDisplayName("Entitlement " + strconv.Itoa(id))
		r.SetDiscoveredAt(now)
		out[i] = r
	}
	return out
}

func makeResourceRecordBatch(offset, count int) []*v3.ResourceRecord {
	now := timestamppb.Now()
	out := make([]*v3.ResourceRecord, count)
	for i := 0; i < count; i++ {
		id := offset + i
		r := &v3.ResourceRecord{}
		r.SetResourceTypeId("user")
		r.SetResourceId("u" + strconv.Itoa(id))
		r.SetDisplayName("User " + strconv.Itoa(id))
		r.SetDiscoveredAt(now)
		out[i] = r
	}
	return out
}

func benchmarkScopeIngest(b *testing.B, put func(ctx context.Context, e *Engine, offset, count int) error) {
	n := scopeBenchCount(b)
	ctx := context.Background()
	const batchSize = 10_000

	b.ReportMetric(float64(n), "rows")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e, err := Open(ctx, b.TempDir())
		require.NoError(b, err)
		require.NoError(b, e.MarkFreshSync(benchGrantSyncID))

		for written := 0; written < n; written += batchSize {
			m := batchSize
			if rem := n - written; rem < m {
				m = rem
			}
			require.NoError(b, put(ctx, e, written, m))
		}
		require.NoError(b, e.db.FlushMemtables())

		b.StopTimer()
		require.NoError(b, e.Close())
		b.StartTimer()
	}
}

func BenchmarkScopeIngestEntitlements(b *testing.B) {
	benchmarkScopeIngest(b, func(ctx context.Context, e *Engine, offset, count int) error {
		return e.PutEntitlementRecords(ctx, makeEntitlementRecordBatch(offset, count)...)
	})
}

func BenchmarkScopeIngestGrants(b *testing.B) {
	benchmarkScopeIngest(b, func(ctx context.Context, e *Engine, offset, count int) error {
		return e.PutGrantRecords(ctx, makeGrantRecordBatch(benchGrantSyncID, offset, count)...)
	})
}

func BenchmarkScopeIngestResources(b *testing.B) {
	benchmarkScopeIngest(b, func(ctx context.Context, e *Engine, offset, count int) error {
		return e.PutResourceRecords(ctx, makeResourceRecordBatch(offset, count)...)
	})
}
