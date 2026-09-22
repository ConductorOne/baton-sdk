package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/stretchr/testify/require"
)

type ledgerCostObservations struct {
	commitNs  atomic.Int64
	commits   atomic.Int64
	handlerNs atomic.Int64
	foldNs    atomic.Int64
}

type ledgerCostPage struct {
	c1zstore.PageWriter
	observations *ledgerCostObservations
}

func (w ledgerCostPage) Commit(ctx context.Context, id c1zstore.LedgerActionIdentity, row *c1zstore.LedgerRow) error {
	start := time.Now()
	err := w.PageWriter.Commit(ctx, id, row)
	w.observations.commitNs.Add(time.Since(start).Nanoseconds())
	if err == nil {
		w.observations.commits.Add(1)
	}
	return err
}

type ledgerPublicCostStore struct {
	c1zstore.PageLedgerStore
	observations     *ledgerCostObservations
	sealNs, reportNs atomic.Int64
}

func (s *ledgerPublicCostStore) BeginPage() c1zstore.PageWriter {
	return ledgerCostPage{PageWriter: s.PageLedgerStore.BeginPage(), observations: s.observations}
}

func (s *ledgerPublicCostStore) LedgerCounters(ctx context.Context) (c1zstore.LedgerCounters, error) {
	start := time.Now()
	counters, err := s.PageLedgerStore.LedgerCounters(ctx)
	s.observations.foldNs.Store(time.Since(start).Nanoseconds())
	return counters, err
}
func (s *ledgerPublicCostStore) EndSyncWithStats(ctx context.Context, stats c1zstore.SyncStats) error {
	start := time.Now()
	err := s.PageLedgerStore.EndSyncWithStats(ctx, stats)
	s.sealNs.Add(time.Since(start).Nanoseconds())
	return err
}
func (s *ledgerPublicCostStore) ArchiveLedgerReport(ctx context.Context) ([]byte, error) {
	start := time.Now()
	report, err := s.PageLedgerStore.ArchiveLedgerReport(ctx)
	s.reportNs.Add(time.Since(start).Nanoseconds())
	return report, err
}
func TestLedgerCostPublic(t *testing.T) {
	if os.Getenv("BATON_LEDGER_COST") != "1" {
		t.Skip("opt-in public sync cost driver")
	}
	arm := os.Getenv("BATON_LEDGER_COST_ARM")
	require.Contains(t, []string{"fresh", "resume"}, arm)
	pages := ledgerCostInput(t, "BATON_LEDGER_COST_PAGES", 10)
	records := ledgerCostInput(t, "BATON_LEDGER_COST_RECORDS", 100)
	workers := ledgerCostInput(t, "BATON_LEDGER_COST_WORKERS", 1)
	require.Greater(t, pages, workers)
	connector := &ledgerCostConnector{mockConnector: newMockConnector(), pages: pages, records: records, streams: workers}
	for stream := range workers {
		connector.rtDB = append(connector.rtDB, v2.ResourceType_builder{
			Id: fmt.Sprintf("cost-%d", stream), DisplayName: "Cost resources", Annotations: annotations.New(&v2.SkipEntitlementsAndGrants{}),
		}.Build())
	}
	path := filepath.Join(t.TempDir(), "public-cost.c1z")
	f := openLedgerFixtureAt(t, path, false)
	observations := &ledgerCostObservations{}
	var walkNs atomic.Int64
	var walkStart time.Time
	source := &ledgerPublicCostStore{PageLedgerStore: f.ledger, observations: observations}
	makeRunner := func(workerCount int) *syncer {
		created, err := NewSyncer(t.Context(), connector, WithConnectorStore(f.store), WithWorkerCount(workerCount), WithDontExpandGrants())
		require.NoError(t, err)
		s := created.(*syncer)
		s.testHooks.ingestHaltHook = ledgerCostMetricsHook(func() string { return f.engine.Metrics().String() })
		s.caps.pageLedger = source
		s.testHooks.checkpointHook = func(string) { t.Error("Pebble public path wrote a checkpoint") }
		s.testHooks.ledgerWalk = func(entering bool) {
			if entering {
				walkStart = time.Now()
			} else {
				walkNs.Add(time.Since(walkStart).Nanoseconds())
			}
		}
		s.testHooks.ledgerCommitted = func(row c1zstore.LedgerRow) { observations.handlerNs.Add(row.PageDuration.Nanoseconds()) }
		return s
	}
	var priorWAL, priorFlushed, priorCompacted uint64
	var reopenNs int64
	start := time.Now()
	if arm == "resume" {
		first := makeRunner(1)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		first.testHooks.ledgerCommitted = func(row c1zstore.LedgerRow) {
			observations.handlerNs.Add(row.PageDuration.Nanoseconds())
			if row.Identity.Op == SyncResourcesOp.String() && connector.calls.Load() == 1 {
				cancel()
			}
		}
		require.ErrorIs(t, first.Sync(ctx), context.Canceled)
		require.EqualValues(t, 1, connector.calls.Load())
		reopen := time.Now()
		require.NoError(t, f.engine.Flush(t.Context()))
		metrics := f.engine.Metrics()
		priorWAL = metrics.WAL.BytesWritten
		for _, level := range metrics.Levels {
			priorFlushed += level.TableBytesFlushed + level.BlobBytesFlushed
			priorCompacted += level.TableBytesCompacted + level.BlobBytesCompacted
		}
		require.NoError(t, f.store.Close(t.Context()))
		f = openLedgerFixtureAt(t, path, false)
		source.PageLedgerStore = f.ledger
		reopenNs = time.Since(reopen).Nanoseconds()
	}
	require.NoError(t, makeRunner(workers).Sync(t.Context()))
	elapsed := time.Since(start)
	require.EqualValues(t, pages, connector.calls.Load())
	report, err := f.ledger.GetArchivedLedgerReport(t.Context())
	require.NoError(t, err)
	require.NotEmpty(t, report)
	if output := os.Getenv("BATON_LEDGER_COST_REPORT"); output != "" {
		require.NoError(t, writeLedgerTestFile(output, append(report, '\n'), 0600))
	}
	facts, err := f.ledger.LedgerFacts(t.Context())
	require.NoError(t, err)
	require.Empty(t, facts, "default public completion must drop the ledger")
	count, token := 0, ""
	for {
		response, err := f.store.ListResources(t.Context(), v2.ResourcesServiceListResourcesRequest_builder{PageToken: token}.Build())
		require.NoError(t, err)
		count += len(response.GetList())
		token = response.GetNextPageToken()
		if token == "" {
			break
		}
	}
	require.Equal(t, pages*records, count)
	metrics := f.engine.Metrics()
	flushed, compacted := priorFlushed, priorCompacted
	for _, level := range metrics.Levels {
		flushed += level.TableBytesFlushed + level.BlobBytesFlushed
		compacted += level.TableBytesCompacted + level.BlobBytesCompacted
	}
	sealCost := f.engine.LastSealCost()
	require.NoError(t, f.store.Close(t.Context()))
	info, err := os.Stat(path)
	require.NoError(t, err)
	result := map[string]any{
		"arm": "ledger-public-" + arm + "-no-sync", "pages": pages, "records_per_page": records, "workers": workers,
		"sync_wall_ns": elapsed.Nanoseconds(), "page_commit_ns": observations.commitNs.Load(), "handler_ns": observations.handlerNs.Load(),
		"seal_ns": source.sealNs.Load(), "seal_fold_ns": observations.foldNs.Load(), "seal_scrub_ns": sealCost.LedgerScrub.Nanoseconds(), "seal_purge_ns": sealCost.LedgerPurge.Nanoseconds(),
		"report_ns": source.reportNs.Load() + sealCost.LedgerArchive.Nanoseconds(), "disposal_ns": sealCost.LedgerDiscard.Nanoseconds(), "resume_walk_ns": walkNs.Load(), "reopen_ns": reopenNs,
		"wal_bytes_before_close": priorWAL + metrics.WAL.BytesWritten, "flush_bytes_before_close": flushed, "compaction_bytes_before_close": compacted,
		"c1z_bytes": info.Size(), "resources_verified": count, "ledger_commits": observations.commits.Load(),
		"scope": "public Sync with default disposal; machine qualification and full matrix required for C49",
	}
	encoded, err := json.Marshal(result)
	require.NoError(t, err)
	if output := os.Getenv("BATON_LEDGER_COST_OUTPUT"); output != "" {
		require.NoError(t, writeLedgerTestFile(output, append(encoded, '\n'), 0600))
	}
	t.Log(string(encoded))
}
