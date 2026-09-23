package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"fmt"
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
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
	require.EqualValues(t, 2, s.ledger.runCounterSnapshot().SessionCalls["store.get"].MaxMs)
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
