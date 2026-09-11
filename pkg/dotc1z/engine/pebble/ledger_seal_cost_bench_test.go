package pebble

// What the seal's token scrub costs, and whether making it the default
// (SetRetainLedgerTokens opts out) is affordable.
//
// EndSync does two things when it scrubs. ScrubLedgerTokens rewrites
// every ledger row to hash-only tokens, which is O(rows) and confined to
// the ledger keyspace. PurgeLedgerResidue then runs db.Compact over
// LedgerBounds, and that is the one with an open-ended cost: a manual
// compaction rewrites every SST that OVERLAPS the range, so an SST
// holding the tail of the record keyspace next to the first ledger key
// gets rewritten whole. Whether that happens decides whether the cost
// tracks the ledger's size or the file's.
//
// The sweep is pages × grants for that reason: pages drive the scrub,
// grants drive how much unrelated data sits near the ledger bounds.
// Reported alongside ns/op:
//
//   - ledger_bytes / db_bytes: EstimateDiskUsage over LedgerBounds and
//     over everything. Their ratio is what the purge SHOULD cost.
//   - compactions / compact_ms: pebble's cumulative counters, delta'd
//     across the call, so the work is attributed rather than inferred
//     from wall time.

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/internal/rawdb"
)

// sealBenchToken stands in for a credential-bearing page token; length
// matters because the scrub's rewrite is proportional to row size.
func sealBenchToken(i int) string {
	return fmt.Sprintf("eyJwYWdlIjoxMjN9-CREDENTIAL-%08d-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", i)
}

// buildSealBenchEngine returns an engine mid-sync with `grants` grant
// records and `pages` committed ledger rows, everything flushed to SSTs
// so the purge has real files to consider. Not timed.
func buildSealBenchEngine(b *testing.B, pages, grants int, grantIndex bool) *Engine {
	b.Helper()
	ctx := context.Background()
	e, err := Open(ctx, b.TempDir(), WithGrantDigestIndex(grantIndex))
	require.NoError(b, err)
	syncID, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(b, err)

	const batchSize = 10_000
	for written := 0; written < grants; written += batchSize {
		m := batchSize
		if rem := grants - written; rem < m {
			m = rem
		}
		require.NoError(b, e.PutGrantRecords(ctx, makeGrantRecordBatch(syncID, written, m)...))
	}

	for i := 0; i < pages; i++ {
		u := e.NewPageUnit()
		row := v3.LedgerRow_builder{NextPageToken: sealBenchToken(i + 1)}.Build()
		require.NoError(b, u.Commit(ctx, grantsPageIdentity("github", sealBenchToken(i)), row))
	}

	// Pages flush to L0 throughout a real sync, so the verbatim tokens are
	// already in SSTs by the time the seal runs. Without this the scrub
	// would rewrite memtable entries and the purge would find nothing,
	// which is not the shape being measured.
	require.NoError(b, e.db.FlushMemtables())
	return e
}

// sealBenchSizes returns the ledger's on-disk estimate and the whole
// DB's, in that order. Their ratio is the cost the purge should track.
func sealBenchSizes(b *testing.B, e *Engine) (uint64, uint64) {
	b.Helper()
	db := e.db.UnsafeForTesting()
	lo, hi := rawdb.LedgerBounds()
	ledgerBytes, err := db.EstimateDiskUsage(lo, hi)
	require.NoError(b, err)
	dbBytes, err := db.EstimateDiskUsage([]byte{0x00}, []byte{0xFF, 0xFF})
	require.NoError(b, err)
	return ledgerBytes, dbBytes
}

func benchmarkSealCost(b *testing.B, pages, grants int, op string, grantIndex bool) {
	ctx := context.Background()
	// Summed across iterations and divided by b.N below. Reporting the
	// last iteration's values instead would describe one run out of b.N,
	// and silently so at any -benchtime above the default.
	var ledgerBytes, dbBytes uint64
	var compactions int64
	var compactMS float64

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		e := buildSealBenchEngine(b, pages, grants, grantIndex)
		iterLedgerBytes, iterDBBytes := sealBenchSizes(b, e)
		ledgerBytes += iterLedgerBytes
		dbBytes += iterDBBytes
		before := e.db.UnsafeForTesting().Metrics()
		beforeCount, beforeDur := before.Compact.Count, before.Compact.Duration
		switch op {
		case "seal-retain":
			e.SetRetainLedgerTokens(true)
		case "seal-scrub-only":
			// Isolates the scrub's contribution INSIDE the seal, which is
			// not the same as calling ScrubLedgerTokens standalone: the
			// scrub's rewritten rows are still in the memtable when the
			// later seal steps run.
			e.test.skipLedgerResiduePurge = true
		}
		b.StartTimer()

		switch op {
		case "scrub":
			require.NoError(b, e.ScrubLedgerTokens(ctx))
		case "purge":
			require.NoError(b, e.PurgeLedgerResidue(ctx))
		case "seal", "seal-retain", "seal-scrub-only":
			require.NoError(b, e.EndSyncWithStats(ctx, c1zstore.SyncStats{}))
		default:
			b.Fatalf("unknown op %q", op)
		}

		b.StopTimer()
		after := e.db.UnsafeForTesting().Metrics()
		compactions += after.Compact.Count - beforeCount
		compactMS += float64((after.Compact.Duration - beforeDur).Microseconds()) / 1000
		require.NoError(b, e.Close())
		b.StartTimer()
	}
	b.StopTimer()
	n := float64(b.N)
	b.ReportMetric(float64(ledgerBytes)/n, "ledger_bytes")
	b.ReportMetric(float64(dbBytes)/n, "db_bytes")
	b.ReportMetric(float64(compactions)/n, "compactions")
	b.ReportMetric(compactMS/n, "compact_ms")
}

// BenchmarkLedgerSealCost sweeps the scrub and the purge separately, then
// the whole seal both ways. The seal pair is the number that decides the
// default: seal minus seal-retain is what every sync now pays.
func BenchmarkLedgerSealCost(b *testing.B) {
	type shape struct{ pages, grants int }
	// Page counts span a small sync to one checkpointing every page of a
	// large one; grant volume is what puts unrelated SSTs near the bounds.
	shapes := []shape{
		// pages=0 is every sync in the fleet until the syncer moves onto
		// the ledger, and the shape the scrub default would otherwise have
		// taxed for nothing: endSyncFinalize gates the scrub and the purge
		// on ledgerActive, so seal here must not differ from seal-retain
		// and compactions must stay at whatever the seal already did.
		{pages: 0, grants: 1_000_000},
		{pages: 1_000, grants: 0},
		{pages: 1_000, grants: 200_000},
		{pages: 10_000, grants: 200_000},
		{pages: 10_000, grants: 1_000_000},
	}
	for _, s := range shapes {
		for _, op := range []string{"scrub", "purge", "seal", "seal-scrub-only", "seal-retain"} {
			b.Run(fmt.Sprintf("pages=%d/grants=%d/%s", s.pages, s.grants, op), func(b *testing.B) {
				benchmarkSealCost(b, s.pages, s.grants, op, true)
			})
		}
	}
}

// BenchmarkLedgerSealCostNoGrantIndex answers what BenchmarkLedgerSealCost
// cannot. There, the whole-seal A/B is dominated by
// BuildDeferredGrantIndexes and the stats pass, whose run-to-run spread
// is larger than the scrub's entire contribution — enough that seal
// sometimes measures FASTER than seal-retain, which is arithmetically
// impossible and marks the number as noise.
//
// WithGrantDigestIndex(false) removes that term, leaving a seal whose
// cost the scrub can actually be seen against.
func BenchmarkLedgerSealCostNoGrantIndex(b *testing.B) {
	for _, pages := range []int{1_000, 10_000} {
		for _, op := range []string{"seal", "seal-scrub-only", "seal-retain"} {
			b.Run(fmt.Sprintf("pages=%d/grants=200000/%s", pages, op), func(b *testing.B) {
				benchmarkSealCost(b, pages, 200_000, op, false)
			})
		}
	}
}
