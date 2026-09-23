package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func assertRestoredCheckpoint(t *testing.T, expected tokenParts, s *syncer) {
	t.Helper()
	if len(expected.run.actionOrder) == 0 {
		expected.run.pushAction(t.Context(), Action{Op: InitOp})
	}
	require.Len(t, s.run.actionOrder, len(expected.run.actionOrder))
	for i, key := range expected.run.actionOrder {
		before := expected.run.actions[key]
		if before.Op == SyncGrantExpansionOp {
			before.PageToken = ""
		}
		after := s.run.actions[s.run.actionOrder[i]]
		require.Equal(t, ledgerIdentity(&before), ledgerIdentity(&after))
		require.Equal(t, before.Spawned, after.Spawned)
		require.Equal(t, before.TypeScopedPlanned, after.TypeScopedPlanned)
	}
	require.Equal(t, expected.run.completedActionsCount(), s.run.completedActionsCount())
	require.Equal(t, expected.run.actionCounts, s.run.actionCounts)
	for fact, established := range expected.run.facts.established {
		require.Equal(t, established, s.run.hasFact(fact))
	}
	require.Equal(t, expected.stats.stepDurations(), s.stats.stepDurations())
	require.Equal(t, expected.stats.connectorCallStats(), s.stats.connectorCallStats())
	require.Equal(t, expected.stats.sessionStoreStats(), s.stats.sessionStoreStats())
	quality := expected.stats.ingestQuality()
	if quality == nil {
		quality = &IngestQualityCheckpoint{SourceCacheReplayBlocked: true, ReasonFlags: ingestQualityReasonUnknownPriorCheckpoint}
	}
	require.Equal(t, quality, s.ingestFilterStats.snapshot())
	require.Nil(t, s.stats.compactionStats())
	require.Nil(t, s.graph.peek())
	require.Zero(t, s.listResourceActionsCompletedThisRun.Load())
}

func TestLedgerRestoreCheckpointFixtures(t *testing.T) {
	for _, name := range []string{
		"v0_current_action.json", "v0_no_current_action.json", "v1_actions_multi.json",
		"v1_facts_all.json", "v1_run_stats.json", "v1_action_counts.json",
		"v2_type_scoped.json", "v1_inline_graph.json", "v1_compaction.json",
	} {
		t.Run(name, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join("testdata", "tokens", name))
			require.NoError(t, err)
			expected, err := unmarshalToken(string(data))
			require.NoError(t, err)
			s, f := newLedgerSchedulerFixture(t, 1)
			require.NoError(t, f.store.CheckpointSync(t.Context(), string(data)))
			resume, err := loadLedgerResume(t.Context(), f.store, f.ledger, "takeover")
			require.NoError(t, err)
			s.ledger, err = newTestLedgerRuntime(t.Context(), f.ledger, "resumed")
			require.NoError(t, err)
			before := ledgerRawSnapshot(t, f.engine)
			s.listResourceActionsCompletedThisRun.Store(23)
			f.audit.enter(ledgerWalk)
			err = restoreLedgerTestState(t, s, t.Context(), resume, false)
			f.audit.enter(ledgerLifecycle)
			require.NoError(t, err)
			assertRestoredCheckpoint(t, expected, s)
			require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
		})
	}
}

func TestLedgerRestoreFinishedCheckpointPreservesLifecycle(t *testing.T) {
	for _, pending := range []bool{false, true} {
		t.Run(map[bool]string{false: "empty", true: "pending"}[pending], func(t *testing.T) {
			s, f := newLedgerSchedulerFixture(t, 1)
			prior := newRunState()
			prior.setFact(factNeedsExpansion)
			prior.completedActions = 17
			if pending {
				prior.pushAction(t.Context(), Action{Op: SyncGrantsOp, ResourceTypeID: "type", PageToken: "remaining"})
			}
			stats := newRunStats()
			stats.connectorCalls["ListResources"] = &ConnectorCallStat{Count: 8, TotalMs: 40, MaxMs: 9}
			state, err := marshalToken(prior, stats)
			require.NoError(t, err)
			require.NoError(t, f.store.CheckpointSync(t.Context(), state))
			syncID := f.engine.CurrentSyncID()
			require.NoError(t, f.store.EndSync(t.Context()))
			require.NoError(t, f.store.SetCurrentSync(t.Context(), syncID))
			before, err := f.engine.GetSyncRunRecord(t.Context(), syncID)
			require.NoError(t, err)
			require.NotNil(t, before.GetEndedAt())
			expected, err := unmarshalToken(state)
			require.NoError(t, err)
			for attempt := range 2 {
				resume, err := loadLedgerResume(t.Context(), f.store, f.ledger, "resume")
				require.NoError(t, err)
				s.ledger, err = newTestLedgerRuntime(t.Context(), f.ledger, "resumed")
				require.NoError(t, err)
				snapshot := ledgerRawSnapshot(t, f.engine)
				f.audit.enter(ledgerWalk)
				err = restoreLedgerTestState(t, s, t.Context(), resume, false)
				f.audit.enter(ledgerLifecycle)
				require.NoError(t, err)
				assertRestoredCheckpoint(t, expected, s)
				require.True(t, equalLedgerSnapshot(snapshot, ledgerRawSnapshot(t, f.engine)))
				after, err := f.engine.GetSyncRunRecord(t.Context(), syncID)
				require.NoError(t, err)
				normalized := proto.Clone(before).(*v3.SyncRunRecord)
				normalized.SetSyncToken("")
				require.True(t, proto.Equal(normalized, after), "attempt %d changed sync metadata", attempt)
			}
		})
	}
}

func TestLedgerRestoreRunsOnlyPendingContinuation(t *testing.T) {
	s, f := newLedgerSchedulerFixture(t, 4)
	require.NoError(t, f.ledger.PutCounterBucket(t.Context(), "prior", c1zstore.TakeoverBucketWorker, c1zstore.LedgerCounters{
		Counters:       map[string]uint64{ledgerCompletedActions: 12, ledgerCompletedPrefix + SyncResourcesOp.String(): 9, ledgerWarningsPrefix + SyncResourcesOp.String(): 1},
		ConnectorCalls: map[string]c1zstore.CallStat{"ListResources": {Count: 2, TotalMs: 17, MaxMs: 11}},
	}))
	root := c1zstore.LedgerActionIdentity{Op: SyncResourcesOp.String(), ResourceTypeID: "type"}
	require.NoError(t, f.ledger.InitializePendingWork(t.Context(), []c1zstore.LedgerWork{{Action: c1zstore.LedgerChild{Identity: root}}}))
	work, _, err := f.ledger.PendingWork(t.Context(), 0, 1)
	require.NoError(t, err)
	_, err = s.ledger.runPage(t.Context(), 0, root, func(_ context.Context, page *ledgerPage) error {
		if err := page.writer.SetPendingWork(work[0]); err != nil {
			return err
		}
		if err := page.setFact(factNeedsExpansion); err != nil {
			return err
		}
		page.row.TypeScopedPlanned = true
		return page.transition("remaining")
	})
	require.NoError(t, err)
	s.ledger, err = newTestLedgerRuntime(t.Context(), f.ledger, "resumed")
	require.NoError(t, err)
	before := ledgerRawSnapshot(t, f.engine)
	f.audit.enter(ledgerWalk)
	err = restoreLedgerTestState(t, s, t.Context(), ledgerResume{actions: []ledgerAction{{identity: root}}}, false)
	f.audit.enter(ledgerLifecycle)
	require.NoError(t, err)
	require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
	calls := 0
	s.testHooks.ledgerHandler = func(ctx context.Context, action *Action, _ *ledgerPage) error {
		calls++
		require.Equal(t, "remaining", action.PageToken)
		require.True(t, action.TypeScopedPlanned)
		require.True(t, s.run.hasFact(factNeedsExpansion))
		require.EqualValues(t, 12, s.run.completedActionsCount())
		require.EqualValues(t, 2, s.stats.connectorCallStats()["ListResources"].Count)
		require.Zero(t, s.listResourceActionsCompletedThisRun.Load())
		return s.nextPageOrFinishAction(ctx, action, "")
	}
	f.audit.enter(ledgerHandler)
	_, err = runLedgerTestSync(t, s, t.Context(), t.Context(), nil)
	f.audit.enter(ledgerLifecycle)
	require.NoError(t, err)
	require.Equal(t, 1, calls)
	require.Nil(t, s.run.current())
	require.EqualValues(t, 13, s.run.completedActionsCount())
	require.EqualValues(t, 1, s.listResourceActionsCompletedThisRun.Load())
	counters, err := f.ledger.LedgerCounters(t.Context())
	require.NoError(t, err)
	require.Equal(t, s.run.completedActionsCount(), counters.Counters[ledgerCompletedActions])
}

type ledgerRestoreReadFault struct {
	c1zstore.PageLedgerStore
	err error
}

func (s ledgerRestoreReadFault) LedgerCounters(context.Context) (c1zstore.LedgerCounters, error) {
	return c1zstore.LedgerCounters{}, s.err
}

type ledgerRestorePendingFault struct {
	c1zstore.PageLedgerStore
	err error
}

func (s ledgerRestorePendingFault) PendingWork(context.Context, uint64, int) ([]c1zstore.LedgerWork, bool, error) {
	return nil, false, s.err
}

func TestLedgerRestoreFailureDoesNotPublishState(t *testing.T) {
	for name, fault := range map[string]func(c1zstore.PageLedgerStore, error) c1zstore.PageLedgerStore{
		"counters": func(store c1zstore.PageLedgerStore, err error) c1zstore.PageLedgerStore {
			return ledgerRestoreReadFault{PageLedgerStore: store, err: err}
		},
		"pending": func(store c1zstore.PageLedgerStore, err error) c1zstore.PageLedgerStore {
			return ledgerRestorePendingFault{PageLedgerStore: store, err: err}
		},
	} {
		t.Run(name, func(t *testing.T) {
			s, f := newLedgerSchedulerFixture(t, 1)
			originalRun, originalStats, originalRuntime := s.run, s.stats, s.ledger
			s.run.setFact("old-state")
			s.childSchedule.recordIfNew("old-child", "old-parent", "one")
			injected := errors.New("restore read failed")
			s.ledger.store = fault(f.ledger, injected)
			require.NoError(t, f.ledger.InitializePendingWork(t.Context(), pendingSeeds(ledgerListingFixtureRoots())))
			before := ledgerRawSnapshot(t, f.engine)
			f.audit.enter(ledgerWalk)
			err := restoreLedgerTestState(t, s, t.Context(), ledgerResume{actions: ledgerListingFixtureRoots()}, false)
			f.audit.enter(ledgerLifecycle)
			require.ErrorIs(t, err, injected)
			require.Same(t, originalRun, s.run)
			require.Same(t, originalStats, s.stats)
			require.Same(t, originalRuntime, s.ledger)
			require.True(t, s.run.hasFact("old-state"))
			require.True(t, s.childSchedule.has("old-child", "old-parent", "one"))
			require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
		})
	}
}

func TestLedgerRestoreRepeatedChildIsNewWork(t *testing.T) {
	s, f := newLedgerSchedulerFixture(t, 1)
	child := c1zstore.LedgerActionIdentity{Op: SyncResourcesOp.String(), ResourceTypeID: "type"}
	seeds := []c1zstore.LedgerWork{{Action: c1zstore.LedgerChild{Identity: ledgerListingFixtureRoots()[0].identity}}, {Action: c1zstore.LedgerChild{Identity: child}}}
	require.NoError(t, f.ledger.InitializePendingWork(t.Context(), seeds))
	work, _, err := f.ledger.PendingWork(t.Context(), 0, 1)
	require.NoError(t, err)
	_, err = s.ledger.runPage(t.Context(), 0, child, func(_ context.Context, page *ledgerPage) error {
		if err := page.writer.SetPendingWork(work[0]); err != nil {
			return err
		}
		page.observations.Counters = map[string]uint64{ledgerCompletedActions: 1, ledgerCompletedPrefix + SyncResourcesOp.String(): 1}
		return page.transition("")
	})
	require.NoError(t, err)
	s.ledger, err = newTestLedgerRuntime(t.Context(), f.ledger, "resumed")
	require.NoError(t, err)
	f.audit.enter(ledgerWalk)
	err = s.restoreLedgerState(t.Context(), s.ledger.store, s.ledger.runID, false)
	f.audit.enter(ledgerLifecycle)
	require.NoError(t, err)
	calls := 0
	s.testHooks.ledgerHandler = func(ctx context.Context, action *Action, _ *ledgerPage) error {
		if action.Op == SyncResourceTypesOp {
			return s.nextPageOrFinishAction(ctx, action, "", ledgerActionFromIdentity(child))
		}
		calls++
		return s.nextPageOrFinishAction(ctx, action, "")
	}
	_, err = s.parallelSync(t.Context(), t.Context(), nil)
	require.NoError(t, err)
	require.Equal(t, 1, calls)
	counts, err := f.ledger.LedgerCounters(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 3, counts.Counters[ledgerCompletedActions])
	require.Equal(t, counts.Counters[ledgerCompletedActions], s.run.completedActionsCount())
	require.EqualValues(t, 2, s.run.getActionCount(SyncResourcesOp).CompletedCount)
	require.EqualValues(t, 1, s.listResourceActionsCompletedThisRun.Load())
}

type ledgerCountedCounterReads struct {
	c1zstore.PageLedgerStore
	reads int
}

func (s *ledgerCountedCounterReads) LedgerCounters(ctx context.Context) (c1zstore.LedgerCounters, error) {
	s.reads++
	return s.PageLedgerStore.LedgerCounters(ctx)
}

func TestLedgerStartupLoadsCountersOnce(t *testing.T) {
	s, f := newLedgerSchedulerFixture(t, 1)
	require.NoError(t, f.ledger.InitializePendingWork(t.Context(), pendingSeeds(ledgerListingFixtureRoots()), ledgerFactIngestKnown))
	counted := &ledgerCountedCounterReads{PageLedgerStore: f.ledger}
	s.caps.pageLedger = counted
	f.audit.enter(ledgerWalk)
	_, err := s.prepareLedgerState(t.Context(), "resume-once", false)
	f.audit.enter(ledgerLifecycle)
	require.NoError(t, err)
	require.Equal(t, 1, counted.reads)
	require.Contains(t, s.ledger.facts, ledgerFactIngestKnown)
	require.True(t, s.run.hasFact(ledgerFactIngestKnown))
}
