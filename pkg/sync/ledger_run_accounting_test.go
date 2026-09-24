package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"fmt"
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	engine "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	"github.com/conductorone/baton-sdk/pkg/ratelimit"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestLedgerRunAccountingAcrossAttempts(t *testing.T) {
	s, f := newLedgerSchedulerFixture(t, 1)
	s.recordStats = true
	s.run.pushAction(t.Context(), Action{Op: SyncResourceTypesOp})
	usage, err := anypb.New(v2.SessionStoreUsage_builder{Ops: []*v2.SessionStoreUsage_OpStats{v2.SessionStoreUsage_OpStats_builder{Op: "get", Count: 2, TotalMs: 6, MaxMs: 4}.Build()}}.Build())
	require.NoError(t, err)
	s.testHooks.ledgerHandler = func(ctx context.Context, action *Action, page *ledgerPage) error {
		invocation := ctx.Value(ledgerInvocationKey{}).(*ledgerInvocation)
		s.recordLedgerConnectorResponse(ctx, invocation, "list-resource-types", 4*time.Millisecond, nil)
		s.recordLedgerSessionUsage(invocation, []*anypb.Any{usage})
		return s.nextPageOrFinishAction(ctx, action, "")
	}
	f.audit.enter(ledgerHandler)
	require.NoError(t, invokeLedgerTestPage(t, s, t.Context(), s.run.current(), nil, false))
	f.audit.enter(ledgerLifecycle)
	s.recordSessionOp("get", 9*time.Millisecond, nil)
	s.recordRetryWait(ratelimit.WithWaitLabel(t.Context(), "user"), 3*time.Millisecond, false)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	s.checkpointOnStop(ctx)
	s.checkpointOnStop(ctx)
	counts, err := f.ledger.LedgerCounters(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 1, counts.ConnectorCalls["list-resource-types"].Count)
	require.EqualValues(t, 1, counts.SessionCalls["store.get"].Count)
	require.EqualValues(t, 2, counts.SessionCalls["connector.get"].Count)
	require.EqualValues(t, 9, counts.SessionCalls["store.get"].MaxMs)
	require.EqualValues(t, 3, counts.StepDurationsMs["retry_wait"])
	require.EqualValues(t, 3, counts.StepDurationsMs["retry_wait:user"])
	require.NoError(t, f.store.Close(t.Context()))
	f = openLedgerFixtureAt(t, f.path, false)
	s.store, s.caps = f.store, resolveStoreCaps(f.store)
	s.ledger, err = newTestLedgerRuntime(t.Context(), f.ledger, "next-attempt")
	require.NoError(t, err)
	f.audit.enter(ledgerWalk)
	require.NoError(t, s.restoreLedgerState(t.Context(), s.ledger.store, s.ledger.runID, false))
	f.audit.enter(ledgerLifecycle)
	s.recordSessionOp("get", 2*time.Millisecond, context.DeadlineExceeded)
	require.EqualValues(t, 2, s.ledger.accounting.snapshot().SessionCalls["store.get"].MaxMs)
	s.recordRetryWait(t.Context(), time.Millisecond, false)
	s.checkpointOnStop(ctx)
	counts, err = f.ledger.LedgerCounters(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 1, counts.ConnectorCalls["list-resource-types"].Count)
	require.Equal(t, c1zstore.CallStat{Count: 2, TotalMs: 11, MaxMs: 9, Errors: 1, Timeouts: 1}, counts.SessionCalls["store.get"])
	require.EqualValues(t, 4, counts.StepDurationsMs["retry_wait"])
	require.EqualValues(t, 2, counts.SessionCalls["connector.get"].Count)
}

func TestLedgerRunAccountingDurationStop(t *testing.T) {
	for _, inOperation := range []bool{false, true} {
		t.Run(fmt.Sprint(inOperation), func(t *testing.T) {
			s, f := newLedgerSchedulerFixture(t, 1)
			s.recordStats = true
			s.recordSessionOp("get", time.Millisecond, nil)
			s.recordStats = false
			s.run.pushAction(t.Context(), Action{Op: SyncResourceTypesOp})
			runCtx, cancel := context.WithTimeout(t.Context(), -time.Second)
			defer cancel()
			var err error
			if inOperation {
				_, err = s.handleOperationError(t.Context(), runCtx, nil, context.DeadlineExceeded)
			} else {
				_, err = runLedgerTestSync(t, s, t.Context(), runCtx, nil)
			}
			require.ErrorIs(t, err, ErrSyncNotComplete)
			counters, err := f.ledger.LedgerCounters(t.Context())
			require.NoError(t, err)
			require.EqualValues(t, 1, counters.SessionCalls["store.get"].Count)
		})
	}
}

func TestLedgerResponseObservationsAreCapturedBeforeCommit(t *testing.T) {
	s, f := newLedgerSchedulerFixture(t, 1)
	s.recordStats = true
	s.run.pushAction(t.Context(), Action{Op: SyncResourceTypesOp})
	wait, err := anypb.New(v2.RateLimitWaitReport_builder{WaitMs: 7}.Build())
	require.NoError(t, err)
	usage, err := anypb.New(v2.SessionStoreUsage_builder{Ops: []*v2.SessionStoreUsage_OpStats{
		v2.SessionStoreUsage_OpStats_builder{Op: "get", Count: 2, TotalMs: 5, MaxMs: 4}.Build(),
	}}.Build())
	require.NoError(t, err)
	annos := []*anypb.Any{wait, usage}
	s.testHooks.ledgerHandler = func(ctx context.Context, action *Action, _ *ledgerPage) error {
		invocation := ctx.Value(ledgerInvocationKey{}).(*ledgerInvocation)
		s.recordLedgerConnectorResponse(ctx, invocation, "list-resource-types", time.Millisecond, annos)
		s.recordLedgerSessionUsage(invocation, annos)
		wait.Value = nil
		usage.Value = nil
		return s.nextPageOrFinishAction(ctx, action, "")
	}
	require.NoError(t, invokeLedgerTestPage(t, s, t.Context(), s.run.current(), nil, false))
	stored, err := f.ledger.LedgerCounters(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 7, stored.StepDurationsMs["rate_limit_wait"])
	require.Equal(t, stored.StepDurationsMs["rate_limit_wait"], s.stats.stepDurations()["rate_limit_wait"])
	require.EqualValues(t, 2, stored.SessionCalls["connector.get"].Count)
	require.Equal(t, stored.SessionCalls["connector.get"].Count, s.stats.sessionStoreStats()["connector.get"].Count)
}

func TestLedgerMigratedAccountingAcrossColdResumes(t *testing.T) {
	for _, version := range []int{0, 1, 2} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			f := newLedgerFixture(t)
			ctx := t.Context()
			syncID := f.engine.CurrentSyncID()
			state := `{"actions":[{"operation":"list-resource-types"}],"completed_actions_count":7}`
			var importedCalls, importedWait, importedErrors, importedMs int64
			wantFlags := ingestQualityReasonUnknownPriorCheckpoint
			if version != 0 {
				state = fmt.Sprintf(`{"version":%d,"actions_map":{"a":{"id":"a","operation":"list-resource-types"}},`+
					`"action_order":["a"],"current_action_id":1,"completed_actions_count":7,"step_durations_ms":{"retry_wait":11},`+
					`"connector_call_stats":{"list-resource-types":{"count":2,"total_ms":9,"max_ms":6}},`+
					`"session_store_stats":{"store.get":{"count":2,"total_ms":9,"max_ms":6,"errors":1,"timeouts":1}},`+
					`"ingest_quality":{}}`, version)
				importedCalls, importedWait, importedErrors, importedMs = 2, 11, 1, 9
				wantFlags = 0
			}
			require.NoError(t, f.store.CheckpointSync(ctx, state))
			for attempt := range 3 {
				require.NoError(t, f.store.Close(ctx))
				f = openLedgerFixtureAt(t, f.path, false)
				require.NoError(t, f.store.SetCurrentSync(ctx, syncID))
				s := ledgerContinuationSyncer(f)
				s.recordStats = true
				require.NoError(t, s.prepareLedgerState(ctx, fmt.Sprintf("attempt-%d", attempt), false))
				require.EqualValues(t, 7, s.run.completedActionsCount())
				wantToken := ""
				if attempt != 0 {
					wantToken = fmt.Sprint(attempt)
				}
				require.Equal(t, wantToken, s.run.current().PageToken)
				s.testHooks.ledgerHandler = func(ctx context.Context, action *Action, _ *ledgerPage) error {
					invocation := ctx.Value(ledgerInvocationKey{}).(*ledgerInvocation)
					s.recordLedgerConnectorResponse(ctx, invocation, "list-resource-types", 4*time.Millisecond, nil)
					next := ""
					if attempt != 2 {
						next = fmt.Sprint(attempt + 1)
					}
					return s.nextPageOrFinishAction(ctx, action, next)
				}
				require.NoError(t, invokeLedgerTestPage(t, s, ctx, s.run.current(), nil, false))
				s.recordSessionOp("get", 8*time.Millisecond, context.DeadlineExceeded)
				s.recordRetryWait(ctx, time.Duration(attempt+1)*time.Millisecond, false)
				s.checkpointLedgerOnStop(ctx)
				s.checkpointLedgerOnStop(ctx)
				counters, err := f.ledger.LedgerCounters(ctx)
				require.NoError(t, err)
				n := int64(attempt + 1)
				require.Equal(t, importedCalls+n, counters.ConnectorCalls["list-resource-types"].Count)
				require.Equal(t, importedCalls+n, counters.SessionCalls["store.get"].Count)
				require.Equal(t, importedMs+4*n, counters.ConnectorCalls["list-resource-types"].TotalMs)
				require.Equal(t, importedMs+8*n, counters.SessionCalls["store.get"].TotalMs)
				require.Equal(t, importedErrors+n, counters.SessionCalls["store.get"].Errors)
				require.Equal(t, importedErrors+n, counters.SessionCalls["store.get"].Timeouts)
				require.EqualValues(t, 8, counters.SessionCalls["store.get"].MaxMs)
				require.Equal(t, importedWait+n*(n+1)/2, counters.StepDurationsMs["retry_wait"])
				require.Equal(t, wantFlags, counters.Flags)
			}
			require.NoError(t, f.store.Close(ctx))
			f = openLedgerFixtureAt(t, f.path, false)
			require.NoError(t, f.store.SetCurrentSync(ctx, syncID))
			s := ledgerContinuationSyncer(f)
			require.NoError(t, s.prepareLedgerState(ctx, "seal-attempt", false))
			require.Nil(t, s.run.current())
			require.EqualValues(t, 8, s.run.completedActionsCount())
			require.NoError(t, s.prepareLedgerSeal(ctx, s.ledger.accounting.snapshot()))
			require.NoError(t, s.ledger.seal(ctx))
			require.NoError(t, f.store.Close(ctx))
			f = openLedgerFixtureAt(t, f.path, false)
			stats, err := engine.ReadSyncStatsRecord(ctx, f.engine, syncID)
			require.NoError(t, err)
			require.Equal(t, importedCalls+3, stats.GetConnectorCallStats()["list-resource-types"].GetCount())
			require.Equal(t, importedCalls+3, stats.GetSessionStoreStats()["store.get"].GetCount())
			require.Equal(t, importedWait+6, stats.GetStepDurationsMs()["retry_wait"])
		})
	}
}
