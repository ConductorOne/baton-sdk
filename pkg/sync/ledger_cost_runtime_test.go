package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/stretchr/testify/require"
)

type ledgerCostObservations struct {
	commitNs  atomic.Int64
	commits   atomic.Int64
	handlerNs atomic.Int64
	foldNs    atomic.Int64
	sealing   atomic.Bool
}

type ledgerCostStore struct {
	c1zstore.PageLedgerStore
	observations *ledgerCostObservations
}

func (s ledgerCostStore) BeginPage() c1zstore.PageWriter {
	return ledgerCostPage{PageWriter: s.PageLedgerStore.BeginPage(), observations: s.observations}
}

func (s ledgerCostStore) LedgerCounters(ctx context.Context) (c1zstore.LedgerCounters, error) {
	start := time.Now()
	counters, err := s.PageLedgerStore.LedgerCounters(ctx)
	if s.observations.sealing.Load() {
		s.observations.foldNs.Add(time.Since(start).Nanoseconds())
	}
	return counters, err
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

func TestLedgerCostRuntime(t *testing.T) {
	if os.Getenv("BATON_LEDGER_COST") != "1" {
		t.Skip("opt-in scheduler-adapter cost smoke; not C49 evidence")
	}
	arm := os.Getenv("BATON_LEDGER_COST_ARM")
	if arm == "" {
		arm = "fresh"
	}
	require.Contains(t, []string{"fresh", "resume"}, arm)
	pages := ledgerCostInput(t, "BATON_LEDGER_COST_PAGES", 10)
	records := ledgerCostInput(t, "BATON_LEDGER_COST_RECORDS", 100)
	workers := ledgerCostInput(t, "BATON_LEDGER_COST_WORKERS", 1)
	require.GreaterOrEqual(t, pages, workers)
	workerCount := uint32(1)
	if workers == 4 {
		workerCount = 4
	} else {
		require.Equal(t, 1, workers)
	}
	connector := &ledgerCostConnector{mockConnector: newMockConnector(), pages: pages, records: records, streams: workers}
	for stream := range workers {
		connector.rtDB = append(connector.rtDB, v2.ResourceType_builder{
			Id: fmt.Sprintf("cost-%d", stream), DisplayName: "Cost resources",
			Annotations: annotations.New(&v2.SkipEntitlementsAndGrants{}),
		}.Build())
	}
	path := filepath.Join(t.TempDir(), "runtime-cost.c1z")
	f := openLedgerFixtureAt(t, path, false)
	measurements := &ledgerCostObservations{}
	source := ledgerCostStore{PageLedgerStore: f.ledger, observations: measurements}
	start := time.Now()
	_, err := f.store.StartNewSync(t.Context(), connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	runtime, err := newLedgerRuntime(t.Context(), source, "cost-attempt")
	require.NoError(t, err)
	handler := func(ctx context.Context, s *syncer, action *Action, page *ledgerPage) error {
		start := time.Now()
		defer func() {
			duration := time.Since(start)
			measurements.handlerNs.Add(duration.Nanoseconds())
			page.row.PageDuration = duration
		}()
		if action.Op == SyncResourceTypesOp {
			response, err := connector.ListResourceTypes(ctx, &v2.ResourceTypesServiceListResourceTypesRequest{})
			if err != nil {
				return err
			}
			if err := page.writer.PutResourceTypes(ctx, response.GetList()...); err != nil {
				return err
			}
			children := make([]c1zstore.LedgerChild, 0, len(response.GetList()))
			for _, resourceType := range response.GetList() {
				children = append(children, c1zstore.LedgerChild{Identity: c1zstore.LedgerActionIdentity{Op: "list-resources", ResourceTypeID: resourceType.GetId()}})
			}
			return ledgerFixtureTransition(ctx, s, action, "", children...)
		}
		called := time.Now()
		response, err := connector.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{ResourceTypeId: action.ResourceTypeID, PageToken: action.PageToken}.Build())
		page.row.ConnectorDuration = time.Since(called)
		if err != nil {
			return err
		}
		page.observations.ConnectorCalls = map[string]c1zstore.CallStat{
			"ListResources": {Count: 1, TotalMs: page.row.ConnectorDuration.Milliseconds(), MaxMs: page.row.ConnectorDuration.Milliseconds()},
		}
		if err := page.writer.PutResources(ctx, response.GetList()...); err != nil {
			return err
		}
		return ledgerFixtureTransition(ctx, s, action, response.GetNextPageToken())
	}
	var reopenNs, walkNs int64
	var priorWAL, priorFlushed, priorCompacted uint64
	if arm == "resume" {
		stopped := errors.New("cost fixture: one data page committed")
		f.audit.enter(ledgerHandler)
		err := runLedgerSchedulerFixture(t, runtime, ledgerListingFixtureRoots(), 1, func(ctx context.Context, s *syncer, action *Action, page *ledgerPage) error {
			if connector.calls.Load() == 1 {
				return stopped
			}
			return handler(ctx, s, action, page)
		})
		f.audit.enter(ledgerLifecycle)
		require.ErrorIs(t, err, stopped)
		require.EqualValues(t, 1, connector.calls.Load())
		require.EqualValues(t, 2, measurements.commits.Load())
		reopenStart := time.Now()
		require.NoError(t, f.audit.record(t.Context(), "CostResumeFlush"))
		require.NoError(t, f.engine.Flush(t.Context()))
		priorMetrics := f.engine.Metrics()
		priorWAL = priorMetrics.WAL.BytesWritten
		for _, level := range priorMetrics.Levels {
			priorFlushed += level.TableBytesFlushed + level.BlobBytesFlushed
			priorCompacted += level.TableBytesCompacted + level.BlobBytesCompacted
		}
		require.NoError(t, f.store.Close(t.Context()))
		f = openLedgerFixtureAt(t, path, false)
		source = ledgerCostStore{PageLedgerStore: f.ledger, observations: measurements}
		runtime, err = newLedgerRuntime(t.Context(), source, "cost-resumed-attempt")
		require.NoError(t, err)
		reopenNs = time.Since(reopenStart).Nanoseconds()
	}
	f.audit.enter(ledgerWalk)
	walkStart := time.Now()
	roots, err := runtime.walk(t.Context(), ledgerListingFixtureRoots())
	require.NoError(t, err)
	walkNs = time.Since(walkStart).Nanoseconds()
	f.audit.enter(ledgerHandler)
	require.NoError(t, runLedgerSchedulerFixture(t, runtime, roots, workerCount, handler))
	f.audit.enter(ledgerLifecycle)
	require.NoError(t, runtime.prepareSeal(t.Context(), c1zstore.LedgerCounters{}))
	sealStart := time.Now()
	measurements.sealing.Store(true)
	require.NoError(t, runtime.seal(t.Context()))
	sealElapsed := time.Since(sealStart)
	sealCost := f.engine.LastSealCost()
	elapsed := time.Since(start)
	require.EqualValues(t, pages, connector.calls.Load())
	require.EqualValues(t, pages+2, measurements.commits.Load())
	count := 0
	token := ""
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
	require.NoError(t, f.store.Close(t.Context()))
	info, err := os.Stat(path)
	require.NoError(t, err)
	result := map[string]any{
		"arm":       "ledger-scheduler-" + arm + "-no-sync",
		"reopen_ns": reopenNs, "resume_walk_ns": walkNs, "pages": pages, "records_per_page": records, "workers": workers,
		"sync_wall_ns": elapsed.Nanoseconds(), "page_commit_ns": measurements.commitNs.Load(), "handler_ns": measurements.handlerNs.Load(),
		"seal_ns": sealElapsed.Nanoseconds(), "seal_fold_ns": measurements.foldNs.Load(), "seal_scrub_ns": sealCost.LedgerScrub.Nanoseconds(), "seal_purge_ns": sealCost.LedgerPurge.Nanoseconds(),
		"wal_bytes_before_close": priorWAL + metrics.WAL.BytesWritten, "flush_bytes_before_close": flushed, "compaction_bytes_before_close": compacted,
		"c1z_bytes": info.Size(), "resources_verified": count, "ledger_commits": measurements.commits.Load(),
		"scope": "scheduler adapter with synthetic handlers; not C49 measurement evidence",
	}
	encoded, err := json.Marshal(result)
	require.NoError(t, err)
	if output := os.Getenv("BATON_LEDGER_COST_OUTPUT"); output != "" {
		require.NoError(t, os.WriteFile(output, append(encoded, '\n'), 0600))
	}
	t.Log(string(encoded))
}
