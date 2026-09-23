package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/stretchr/testify/require"
)

func TestLedgerSealRequiresTerminalPage(t *testing.T) {
	f := newLedgerFixture(t)
	require.NoError(t, f.ledger.InitializePendingWork(t.Context(), nil))
	runtime, err := newLedgerRuntime(t.Context(), f.ledger, "attempt")
	require.NoError(t, err)
	before := ledgerRawSnapshot(t, f.engine)
	require.ErrorContains(t, runtime.seal(t.Context()), "requires terminal page")
	require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
}

func TestLedgerTerminalPageAndSealStats(t *testing.T) {
	for _, retain := range []bool{false, true} {
		f := newLedgerFixture(t)
		syncID := f.engine.CurrentSyncID()
		f.ledger.SetRetainLedgerTokens(retain)
		require.NoError(t, f.ledger.InitializePendingWork(t.Context(), nil))
		runtime, err := newLedgerRuntime(t.Context(), f.ledger, "attempt")
		require.NoError(t, err)
		id := c1zstore.LedgerActionIdentity{Op: "list-resource-types", PageToken: "first-page"}
		f.audit.enter(ledgerHandler)
		_, err = runtime.runPage(t.Context(), 0, id, func(ctx context.Context, page *ledgerPage) error {
			require.NoError(t, page.writer.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "type"}.Build()))
			require.NoError(t, page.setFact(ledgerFactIngestKnown))
			require.NoError(t, page.setFact(ledgerFactIngestBlocked))
			page.observations = c1zstore.LedgerCounters{
				Counters: map[string]uint64{ledgerCompletedActions: 3, "ingest.grants_dropped": 4}, Flags: 8,
				ConnectorCalls: map[string]c1zstore.CallStat{"list": {Count: 1, TotalMs: 9, MaxMs: 9}},
			}
			return page.transition("")
		})
		f.audit.enter(ledgerLifecycle)
		require.NoError(t, err)
		runCounters := c1zstore.LedgerCounters{StepDurationsMs: map[string]int64{"collection": 27}, SessionCalls: map[string]c1zstore.CallStat{"get": {Count: 2}}}
		require.NoError(t, runtime.prepareSeal(t.Context(), runCounters))
		facts, err := f.ledger.LedgerFacts(t.Context())
		require.NoError(t, err)
		require.Contains(t, facts, ledgerFactSealReady)
		row, found, err := f.ledger.GetLedgerRow(t.Context(), c1zstore.LedgerActionIdentity{Op: ledgerTerminalOp})
		require.NoError(t, err)
		require.True(t, found)
		require.NotNil(t, row)
		counters, err := f.ledger.LedgerCounters(t.Context())
		require.NoError(t, err)
		require.Equal(t, int64(27), counters.StepDurationsMs["collection"])
		require.Equal(t, uint64(3), counters.Counters[ledgerCompletedActions])
		before := ledgerRawSnapshot(t, f.engine)
		require.NoError(t, runtime.prepareSeal(t.Context(), c1zstore.LedgerCounters{}))
		require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
		require.NoError(t, runtime.seal(t.Context()))
		row, found, err = f.ledger.GetLedgerRow(t.Context(), id)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, !retain, row.Scrubbed)
		stats, err := f.store.SyncMeta().StatsV2(t.Context(), connectorstore.SyncTypeAny, syncID)
		require.NoError(t, err)
		require.NotNil(t, stats)
		require.EqualValues(t, 1, stats.GetResourceTypes())
		require.Equal(t, int64(27), stats.GetStepDurationsMs()["collection"])
		require.Equal(t, int64(1), stats.GetConnectorCallStats()["list"].GetCount())
		require.Equal(t, int64(9), stats.GetConnectorCallStats()["list"].GetMaxMs())
		require.Equal(t, int64(2), stats.GetSessionStoreStats()["get"].GetCount())
		require.NoError(t, f.store.SetCurrentSync(t.Context(), syncID))
		finished, err := f.ledger.BoundSyncFinished(t.Context())
		require.NoError(t, err)
		require.True(t, finished)
	}
}

func TestLedgerTerminalRejectsActivePage(t *testing.T) {
	f := newLedgerFixture(t)
	require.NoError(t, f.ledger.InitializePendingWork(t.Context(), nil))
	runtime, err := newLedgerRuntime(t.Context(), f.ledger, "attempt")
	require.NoError(t, err)
	_, err = runtime.runPage(t.Context(), 0, c1zstore.LedgerActionIdentity{Op: "init"}, func(ctx context.Context, page *ledgerPage) error {
		require.ErrorContains(t, runtime.prepareSeal(ctx, c1zstore.LedgerCounters{}), "active pages")
		return page.transition("")
	})
	require.NoError(t, err)
	require.NoError(t, runtime.prepareSeal(t.Context(), c1zstore.LedgerCounters{}))
	_, err = runtime.runPage(t.Context(), 0, c1zstore.LedgerActionIdentity{Op: "late"}, func(_ context.Context, page *ledgerPage) error {
		t.Fatal("page ran after terminal marker")
		return nil
	})
	require.ErrorIs(t, err, errLedgerWorkerBusy)
}

func TestLedgerSealReadyBypassesScrubbedFrontier(t *testing.T) {
	f := newLedgerFixture(t)
	require.NoError(t, f.ledger.InitializePendingWork(t.Context(), nil))
	require.NoError(t, f.ledger.InitializePendingWork(t.Context(), nil))
	runtime, err := newLedgerRuntime(t.Context(), f.ledger, "attempt")
	require.NoError(t, err)
	require.NoError(t, runtime.prepareSeal(t.Context(), c1zstore.LedgerCounters{}))
	before := ledgerRawSnapshot(t, f.engine)
	f.audit.enter(ledgerWalk)
	source := ledgerFrontierReadOverride{PageLedgerStore: f.ledger, frontier: &c1zstore.LedgerFrontier{}, found: true}
	resume, err := loadLedgerResume(t.Context(), f.store, source, "resumed")
	require.NoError(t, err)
	require.True(t, resume.sealReady)
	require.Empty(t, resume.actions)
	pending, _, err := f.ledger.PendingWork(t.Context(), 0, 64)
	f.audit.enter(ledgerLifecycle)
	require.NoError(t, err)
	require.Empty(t, pending)
	require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
}

func TestLedgerSealCostConsumer(t *testing.T) {
	for _, retain := range []bool{false, true} {
		f := newLedgerFixture(t)
		f.ledger.SetRetainLedgerTokens(retain)
		require.NoError(t, f.ledger.InitializePendingWork(t.Context(), nil))
		runtime, err := newLedgerRuntime(t.Context(), f.ledger, "attempt")
		require.NoError(t, err)
		require.NoError(t, runtime.prepareSeal(t.Context(), c1zstore.LedgerCounters{}))
		require.NoError(t, runtime.seal(t.Context()))
		cost := f.engine.LastSealCost()
		if retain {
			require.Zero(t, cost.LedgerScrub)
			require.Zero(t, cost.LedgerPurge)
		} else {
			require.Positive(t, cost.LedgerScrub)
			require.Positive(t, cost.LedgerPurge)
		}
	}
}

func TestLedgerSealPreservesUnknownIngestQuality(t *testing.T) {
	var baseline ingestFilterStats
	require.Nil(t, baseline.snapshot())
	require.Nil(t, ledgerSyncStats(nil, c1zstore.LedgerCounters{}).IngestQuality)
	known := ledgerSyncStats(map[string]string{ledgerFactIngestKnown: ""}, c1zstore.LedgerCounters{}).IngestQuality
	require.NotNil(t, known)
	require.False(t, known.SourceCacheReplayBlocked)
	blocked := ledgerSyncStats(map[string]string{ledgerFactIngestBlocked: ""}, c1zstore.LedgerCounters{Flags: ingestQualityReasonUnknownPriorCheckpoint}).IngestQuality
	require.NotNil(t, blocked)
	require.True(t, blocked.SourceCacheReplayBlocked)
	require.Equal(t, ingestQualityReasonUnknownPriorCheckpoint, blocked.ReasonFlags)
}
