package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"errors"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func ledgerContinuationSyncer(f *ledgerFixture) *syncer {
	return &syncer{ledgered: true, syncID: f.engine.CurrentSyncID(), store: f.store, caps: resolveStoreCaps(f.store), cfg: syncConfig{workerCount: 1, onlyExpandGrants: true}}
}

func TestLedgerFinishedProcessingResumesWithoutReset(t *testing.T) {
	for _, cut := range []string{"after-clear", "after-page"} {
		t.Run(cut, func(t *testing.T) {
			f := newLedgerFixture(t)
			syncID := f.engine.CurrentSyncID()
			require.NoError(t, f.ledger.InitializePendingWork(t.Context(), nil))
			runtime, err := newTestLedgerRuntime(t.Context(), f.ledger, "collection")
			require.NoError(t, err)
			_, err = runtime.runPage(t.Context(), 0, c1zstore.LedgerActionIdentity{Op: InitOp.String()}, func(_ context.Context, page *ledgerPage) error {
				if err := page.setFact(factNeedsExpansion); err != nil {
					return err
				}
				if err := page.setFact(ledgerFactIngestKnown); err != nil {
					return err
				}
				page.observations.Counters = map[string]uint64{ledgerCompletedActions: 17}
				return page.transition("")
			})
			require.NoError(t, err)
			require.NoError(t, runtime.prepareSeal(t.Context(), c1zstore.LedgerCounters{}))
			require.NoError(t, runtime.seal(t.Context()))
			require.NoError(t, f.store.SetCurrentSync(t.Context(), syncID))
			before, err := f.engine.GetSyncRunRecord(t.Context(), syncID)
			require.NoError(t, err)
			require.NotNil(t, before.GetEndedAt())
			s := ledgerContinuationSyncer(f)
			err = s.prepareLedgerState(t.Context(), "processing", false)
			require.NoError(t, err)
			require.False(t, s.run.hasFact(ledgerFactSealReady))
			require.Equal(t, InitOp, s.run.current().Op)
			require.EqualValues(t, 17, s.run.completedActionsCount())
			require.True(t, s.run.hasFact(factNeedsExpansion))
			if cut == "after-page" {
				stopped := errors.New("stop after processing page")
				s.testHooks.ledgerHandler = func(ctx context.Context, action *Action, _ *ledgerPage) error {
					if action.Op == InitOp {
						return s.nextPageOrFinishAction(ctx, action, "", Action{Op: SyncResourcesOp, ResourceTypeID: "type"})
					}
					require.Equal(t, SyncResourcesOp, action.Op)
					if action.PageToken != "" {
						return stopped
					}
					return s.nextPageOrFinishAction(ctx, action, "remaining")
				}
				_, err = runLedgerTestSync(t, s, t.Context(), t.Context(), nil)
				require.ErrorIs(t, err, stopped)
			}
			middle, err := f.engine.GetSyncRunRecord(t.Context(), syncID)
			require.NoError(t, err)
			require.True(t, proto.Equal(before.GetEndedAt(), middle.GetEndedAt()))
			require.Equal(t, before.GetStartedAt(), middle.GetStartedAt())
			require.NoError(t, f.store.Close(t.Context()))
			f = openLedgerFixtureAt(t, f.path, false)
			require.NoError(t, f.store.SetCurrentSync(t.Context(), syncID))
			snapshot := ledgerSnapshotWithFoldedCounters(t, f.engine)
			s = ledgerContinuationSyncer(f)
			observeLedgerRestore(t, s, f)
			err = s.prepareLedgerState(t.Context(), "resumed", false)
			f.audit.enter(ledgerLifecycle)
			require.NoError(t, err)
			require.False(t, s.run.hasFact(ledgerFactSealReady))
			require.True(t, equalLedgerSnapshot(snapshot, ledgerSnapshotWithFoldedCounters(t, f.engine)))
			if cut == "after-page" {
				require.Equal(t, SyncResourcesOp, s.run.current().Op)
				require.Equal(t, "remaining", s.run.current().PageToken)
				require.EqualValues(t, 18, s.run.completedActionsCount())
			} else {
				require.Equal(t, InitOp, s.run.current().Op)
			}
			calls := 0
			s.testHooks.ledgerHandler = func(ctx context.Context, action *Action, _ *ledgerPage) error {
				if action.Op == InitOp {
					return s.nextPageOrFinishAction(ctx, action, "", Action{Op: SyncResourcesOp, ResourceTypeID: "type"})
				}
				require.Equal(t, SyncResourcesOp, action.Op)
				if cut == "after-page" {
					require.Equal(t, "remaining", action.PageToken)
				}
				calls++
				return s.nextPageOrFinishAction(ctx, action, "")
			}
			_, err = runLedgerTestSync(t, s, t.Context(), t.Context(), nil)
			require.NoError(t, err)
			require.Equal(t, 1, calls)
			require.EqualValues(t, 19, s.run.completedActionsCount())
			require.NoError(t, s.ledger.prepareSeal(t.Context(), c1zstore.LedgerCounters{}))
			require.NoError(t, s.ledger.seal(t.Context()))
			counters, err := f.ledger.LedgerCounters(t.Context())
			require.NoError(t, err)
			require.EqualValues(t, 19, counters.Counters[ledgerCompletedActions])
		})
	}
}

func TestLedgerFinishedLegacyFrontierKeepsPendingWork(t *testing.T) {
	for _, pending := range []bool{false, true} {
		t.Run(map[bool]string{false: "empty", true: "pending"}[pending], func(t *testing.T) {
			f := newLedgerFixture(t)
			syncID := f.engine.CurrentSyncID()
			prior := newRunState()
			prior.setFact(factNeedsExpansion)
			prior.completedActions = 17
			if pending {
				prior.pushAction(t.Context(), Action{Op: SyncGrantsOp, ResourceTypeID: "type", PageToken: "remaining"})
			}
			token, err := marshalToken(prior, newRunStats())
			require.NoError(t, err)
			require.NoError(t, f.store.CheckpointSync(t.Context(), token))
			require.NoError(t, f.store.EndSync(t.Context()))
			require.NoError(t, f.store.SetCurrentSync(t.Context(), syncID))
			before, err := f.engine.GetSyncRunRecord(t.Context(), syncID)
			require.NoError(t, err)
			before.SetSyncToken("")
			s := ledgerContinuationSyncer(f)
			err = s.prepareLedgerState(t.Context(), "first", false)
			require.NoError(t, err)
			require.EqualValues(t, 17, s.run.completedActionsCount())
			require.True(t, s.run.hasFact(factNeedsExpansion))
			if pending {
				require.Equal(t, "remaining", s.run.current().PageToken)
			} else {
				require.Equal(t, InitOp, s.run.current().Op)
			}
			after, err := f.engine.GetSyncRunRecord(t.Context(), syncID)
			require.NoError(t, err)
			require.True(t, proto.Equal(before, after))
			snapshot := ledgerSnapshotWithFoldedCounters(t, f.engine)
			observeLedgerRestore(t, s, f)
			err = s.prepareLedgerState(t.Context(), "second", false)
			f.audit.enter(ledgerLifecycle)
			require.NoError(t, err)
			require.True(t, equalLedgerSnapshot(snapshot, ledgerSnapshotWithFoldedCounters(t, f.engine)))
			if pending {
				require.Equal(t, "remaining", s.run.current().PageToken)
			} else {
				require.Equal(t, InitOp, s.run.current().Op)
			}
		})
	}
}

func TestLedgerSealReadyUnfinishedDoesNotStartAnotherPass(t *testing.T) {
	f := newLedgerFixture(t)
	require.NoError(t, f.ledger.InitializePendingWork(t.Context(), nil))
	runtime, err := newTestLedgerRuntime(t.Context(), f.ledger, "old")
	require.NoError(t, err)
	require.NoError(t, runtime.prepareSeal(t.Context(), c1zstore.LedgerCounters{Counters: map[string]uint64{ledgerCompletedActions: 17}}))
	s := ledgerContinuationSyncer(f)
	before := ledgerSnapshotWithFoldedCounters(t, f.engine)
	observeLedgerRestore(t, s, f)
	err = s.prepareLedgerState(t.Context(), "resume", false)
	f.audit.enter(ledgerLifecycle)
	require.NoError(t, err)
	require.True(t, s.run.hasFact(ledgerFactSealReady))
	require.Nil(t, s.run.current())
	require.EqualValues(t, 17, s.run.completedActionsCount())
	require.True(t, equalLedgerSnapshot(before, ledgerSnapshotWithFoldedCounters(t, f.engine)))
}

func TestLedgerPreparedSealSurvivesEarlyEnd(t *testing.T) {
	f := newLedgerFixture(t)
	id := f.engine.CurrentSyncID()
	require.NoError(t, f.ledger.InitializePendingWork(t.Context(), nil))
	runtime, err := newTestLedgerRuntime(t.Context(), f.ledger, "prior")
	require.NoError(t, err)
	require.NoError(t, runtime.prepareSeal(t.Context(), c1zstore.LedgerCounters{
		Counters: map[string]uint64{ledgerCompletedActions: 17},
	}, c1zstore.LedgerFactRetainTokens))
	require.NoError(t, f.store.EndSync(t.Context()))
	require.NoError(t, f.store.Close(t.Context()))
	f = openLedgerFixtureAt(t, f.path, false)
	require.NoError(t, f.store.SetCurrentSync(t.Context(), id))
	before := ledgerSnapshotWithFoldedCounters(t, f.engine)
	s := ledgerContinuationSyncer(f)
	observeLedgerRestore(t, s, f)
	require.NoError(t, s.prepareLedgerState(t.Context(), "retry", false))
	f.audit.enter(ledgerLifecycle)
	require.True(t, s.run.hasFact(ledgerFactSealReady))
	require.True(t, s.run.hasFact(c1zstore.LedgerFactRetainTokens))
	require.Nil(t, s.run.current())
	require.EqualValues(t, 17, s.run.completedActionsCount())
	require.True(t, equalLedgerSnapshot(before, ledgerSnapshotWithFoldedCounters(t, f.engine)))
	require.NoError(t, s.ledger.seal(t.Context()))
	facts, err := f.ledger.LedgerFacts(t.Context())
	require.NoError(t, err)
	require.Contains(t, facts, c1zstore.LedgerFactRetainTokens)
}
