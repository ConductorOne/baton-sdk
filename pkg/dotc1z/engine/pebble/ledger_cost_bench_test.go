package pebble

// Cost benchmarks for the S3 ledger design
// (docs/tasks/sound-syncs-solutions-brief.md §3.10): the two items the
// brief says to measure before the handler restructuring starts, plus
// the fsync cost the brief argues does not apply.
//
//   - BenchmarkLedgerPageCommit: one page = N record-sized rows plus one
//     ledger row in ONE batch, committed NoSync (the fresh-sync write
//     path). Plain vs indexed batch across sizes, to set the chunk cap
//     (§3.5) and confirm the indexed-batch cost is page-size-negligible.
//     B/op is the batch's memory (the indexed batch's skiplist shows up
//     as the delta); "batch_bytes" is the batch representation size.
//   - BenchmarkLedgerPageCommitSync: the same page committed Sync, for
//     the record — what a per-page fsync WOULD cost if S3 needed it.
//   - BenchmarkLedgerResumeWalk: N ledger rows forming a chain, flushed
//     to SSTs, the DB reopened (cold block cache — the resume case), then
//     the walk: one point read per row following next-cursor pointers.
//     10^4 and 10^5 rows bound how far the checkpoint cadence (§5.4)
//     can be pushed.
//
// These use the engine's production pebble options (via Open) and the
// raw DB through the test escape hatch: the ledger keyspace does not
// exist yet, and the question is what pebble charges for the shape.

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
)

// benchLedgerPrefix is a keyspace byte no production family uses
// (families are VersionV3 0x03 ... TypeEngineMeta 0xFF under the v3
// version byte; this bench owns its DB and uses a distinct version).
const benchLedgerPrefix byte = 0x7E

// ledgerRowKey mirrors the proposed key: prefix, sync id (20 bytes),
// then the flat action tuple (op, rtid, rid, parent rtid, parent rid,
// page token). ~60 bytes for typical ids.
func ledgerRowKey(syncID []byte, aid uint64) []byte {
	k := make([]byte, 0, 64)
	k = append(k, benchLedgerPrefix, 'L')
	k = append(k, syncID...)
	k = append(k, 0x02) // op
	k = append(k, []byte("group")...)
	k = append(k, 0)
	var a [8]byte
	binary.BigEndian.PutUint64(a[:], aid)
	k = append(k, a[:]...) // rid stand-in
	k = append(k, 0)
	k = append(k, []byte("tenant/acme")...) // parent
	k = append(k, 0)
	k = append(k, []byte("eyJwYWdlIjoxMjN9")...) // page token
	return k
}

// ledgerRowValue is a synthetic completion row: the full identity
// (~150 bytes), next token (~64), and children (~150 bytes each). The
// brief's estimate is "a few hundred bytes"; children lists are bounded
// by the page's spawn count.
func ledgerRowValue(nextAID uint64, children int) []byte {
	v := make([]byte, 0, 256+children*150)
	v = append(v, 0x01)                 // version
	v = append(v, make([]byte, 150)...) // identity echo
	var a [8]byte
	binary.BigEndian.PutUint64(a[:], nextAID)
	v = append(v, a[:]...)                                                 // next cursor (aid stand-in), read back by the walk at [151:159]
	v = append(v, []byte("eyJwYWdlIjoxMjR9AAAAAAAAAAAAAAAAAAAAAAAAAA")...) // next page token
	for i := 0; i < children; i++ {
		v = append(v, make([]byte, 150)...)
	}
	return v
}

// recordKey / recordValue are record-sized stand-ins: ~60-byte key,
// ~300-byte value (a grant record's encoded size class).
func recordKey(syncID []byte, page, i int) []byte {
	k := make([]byte, 0, 64)
	k = append(k, benchLedgerPrefix, 'R')
	k = append(k, syncID...)
	k = append(k, []byte(fmt.Sprintf("grant:%08d:%08d:group/g1:member", page, i))...)
	return k
}

// recordValue is a 300-byte value with a per-record varying fill
// (so consecutive values are not byte-identical); the cost under test
// is batch/commit mechanics, not compressibility.
func recordValue(seed int) []byte {
	v := make([]byte, 300)
	x := uint32(seed&0xFFFFFFFF)*2654435761 + 1
	for i := range v {
		x = x*1664525 + 1013904223
		v[i] = byte(x >> 24)
	}
	return v
}

func openBenchRawDB(b *testing.B) (*Engine, *pebble.DB) {
	b.Helper()
	e, err := Open(context.Background(), b.TempDir())
	require.NoError(b, err)
	require.NoError(b, e.MarkFreshSync(benchGrantSyncID))
	return e, e.db.UnsafeForTesting()
}

func benchSyncIDBytes() []byte {
	// 20 raw bytes; the value does not matter for cost.
	return []byte("01234567890123456789")
}

func benchmarkLedgerPageCommit(b *testing.B, rows int, indexed bool, wo *pebble.WriteOptions) {
	e, db := openBenchRawDB(b)
	defer func() { require.NoError(b, e.Close()) }()
	syncID := benchSyncIDBytes()
	// Pre-generate values so the timer measures the batch, not the fill.
	vals := make([][]byte, rows)
	for i := range vals {
		vals[i] = recordValue(i)
	}
	ledgerVal := ledgerRowValue(1, 2)

	b.ReportAllocs()
	b.ResetTimer()
	var batchBytes int
	for n := 0; n < b.N; n++ {
		var batch *pebble.Batch
		if indexed {
			batch = db.NewIndexedBatch()
		} else {
			batch = db.NewBatch()
		}
		for i := 0; i < rows; i++ {
			require.NoError(b, batch.Set(recordKey(syncID, n, i), vals[i], nil))
		}
		require.NoError(b, batch.Set(ledgerRowKey(syncID, uint64(n)), ledgerVal, nil))
		batchBytes = batch.Len()
		require.NoError(b, batch.Commit(wo))
		require.NoError(b, batch.Close())
	}
	b.StopTimer()
	b.ReportMetric(float64(batchBytes), "batch_bytes")
	b.ReportMetric(float64(len(ledgerVal)), "ledger_row_bytes")
}

// BenchmarkLedgerPageCommit: plain vs indexed batch, NoSync, across
// page/chunk sizes. Sub-benchmark names: rows=N/{plain,indexed}.
func BenchmarkLedgerPageCommit(b *testing.B) {
	for _, rows := range []int{100, 1_000, 10_000, 100_000} {
		for _, indexed := range []bool{false, true} {
			kind := "plain"
			if indexed {
				kind = "indexed"
			}
			b.Run(fmt.Sprintf("rows=%d/%s", rows, kind), func(b *testing.B) {
				benchmarkLedgerPageCommit(b, rows, indexed, pebble.NoSync)
			})
		}
	}
}

// BenchmarkLedgerPageCommitSync: the fsync-per-page cost S3 does NOT
// pay (pages commit NoSync as today; §3.10 first bullet). Recorded so
// the "does not apply" claim has a number behind it.
func BenchmarkLedgerPageCommitSync(b *testing.B) {
	for _, rows := range []int{100, 1_000} {
		b.Run(fmt.Sprintf("rows=%d/nosync", rows), func(b *testing.B) {
			benchmarkLedgerPageCommit(b, rows, false, pebble.NoSync)
		})
		b.Run(fmt.Sprintf("rows=%d/sync", rows), func(b *testing.B) {
			benchmarkLedgerPageCommit(b, rows, false, pebble.Sync)
		})
	}
}

// BenchmarkLedgerResumeWalk: the read-only walk over an N-row ledger
// chain after reopen (cold cache). Each iteration reopens the DB
// outside the timer and walks inside it. Reported: ns/op for the whole
// walk, plus reads/op (== N; sanity) and ledger_bytes (the ledger's
// on-disk estimate, the §4 risk-5 "bytes until EndSync").
func BenchmarkLedgerResumeWalk(b *testing.B) {
	for _, rows := range []int{10_000, 100_000} {
		b.Run(fmt.Sprintf("rows=%d", rows), func(b *testing.B) {
			ctx := context.Background()
			dir := b.TempDir()
			syncID := benchSyncIDBytes()

			// Populate: a single chain aid 0 -> 1 -> ... -> rows-1, each
			// row committed in its own NoSync batch (as pages would
			// be), interleaved with record rows so the ledger keys are
			// not the only thing in the LSM.
			e, err := Open(ctx, dir)
			require.NoError(b, err)
			require.NoError(b, e.MarkFreshSync(benchGrantSyncID))
			db := e.db.UnsafeForTesting()
			for i := 0; i < rows; i++ {
				batch := db.NewBatch()
				for j := 0; j < 5; j++ {
					require.NoError(b, batch.Set(recordKey(syncID, i, j), recordValue(i*5+j), nil))
				}
				children := 0
				if i%50 == 0 {
					children = 2
				}
				require.NoError(b, batch.Set(ledgerRowKey(syncID, uint64(i)), ledgerRowValue(uint64(i+1), children), nil))
				require.NoError(b, batch.Commit(pebble.NoSync))
				require.NoError(b, batch.Close())
			}
			require.NoError(b, e.db.FlushMemtables())
			lo := ledgerRowKey(syncID, 0)
			hi := ledgerRowKey(syncID, ^uint64(0))
			ledgerBytes, err := db.EstimateDiskUsage(lo, hi)
			require.NoError(b, err)
			require.NoError(b, e.Close())

			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				b.StopTimer()
				e, err := Open(ctx, dir)
				require.NoError(b, err)
				db := e.db.UnsafeForTesting()
				b.StartTimer()

				// The walk: follow next pointers with point reads until
				// the frontier (absent row).
				reads := 0
				aid := uint64(0)
				for {
					v, closer, err := db.Get(ledgerRowKey(syncID, aid))
					if errors.Is(err, pebble.ErrNotFound) {
						break
					}
					require.NoError(b, err)
					reads++
					aid = binary.BigEndian.Uint64(v[151:159])
					require.NoError(b, closer.Close())
				}

				b.StopTimer()
				require.Equal(b, rows, reads)
				require.NoError(b, e.Close())
				b.StartTimer()
			}
			b.ReportMetric(float64(rows), "reads/op")
			b.ReportMetric(float64(ledgerBytes), "ledger_bytes")
		})
	}
}
