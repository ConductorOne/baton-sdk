package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	native_sync "sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/ratelimit"
	"github.com/conductorone/baton-sdk/pkg/retry"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestLedgerRetryObservationsAcrossPages(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, f := newLedgerSchedulerFixture(t, 1)
		s.recordStats = true
		action := s.run.pushAction(t.Context(), Action{Op: SyncResourcesOp, ResourceTypeID: "type"})
		firstID := ledgerIdentity(action)
		var calls int
		s.testHooks.ledgerHandler = func(ctx context.Context, action *Action, page *ledgerPage) error {
			calls++
			invocation := ctx.Value(ledgerInvocationKey{}).(*ledgerInvocation)
			s.recordLedgerConnectorResponse(ctx, invocation, "list-resources", time.Millisecond, nil)
			if calls <= 2 {
				err := status.Error(codes.Unavailable, "retry")
				recordLedgerConnectorError(invocation, err)
				return err
			}
			ratelimit.ObserveWait(ctx, ratelimit.WaitEvent{Duration: 3 * time.Millisecond})
			next := ""
			if action.PageToken == "" {
				next = "next"
			}
			return s.nextPageOrFinishAction(ctx, action, next)
		}
		ctx := s.withRateLimitWaitObserver(t.Context())
		r := retry.NewRetryer(ctx, retry.RetryConfig{MaxAttempts: 4, InitialDelay: time.Millisecond, MaxDelay: time.Millisecond})
		warnings, err := s.syncParallel(ctx, r, s.run.peekMatchingActions(ctx, SyncResourcesOp), s.SyncResources)
		require.NoError(t, err)
		require.Empty(t, warnings)
		require.Equal(t, 4, calls)
		first, found, err := f.ledger.GetLedgerRow(ctx, firstID)
		require.NoError(t, err)
		require.True(t, found)
		require.EqualValues(t, 3, first.ConnectorAttempts)
		require.EqualValues(t, 2, first.ConnectorErrors)
		require.Equal(t, 3*time.Millisecond, first.ConnectorDuration)
		require.Equal(t, 2*time.Millisecond, first.SDKRetryWaitDuration)
		require.Equal(t, 3*time.Millisecond, first.SDKRateLimitWaitDuration)
		firstID.PageToken = "next"
		second, found, err := f.ledger.GetLedgerRow(ctx, firstID)
		require.NoError(t, err)
		require.True(t, found)
		require.EqualValues(t, 1, second.ConnectorAttempts)
		require.Zero(t, second.ConnectorErrors)
		require.Equal(t, time.Millisecond, second.ConnectorDuration)
		require.Zero(t, second.SDKRetryWaitDuration)
		require.Equal(t, 3*time.Millisecond, second.SDKRateLimitWaitDuration)
	})
}

func TestLedgerExhaustedRetryHasNoRow(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, f := newLedgerSchedulerFixture(t, 1)
		action := s.run.pushAction(t.Context(), Action{Op: SyncResourcesOp, ResourceTypeID: "type"})
		id := ledgerIdentity(action)
		s.testHooks.ledgerHandler = func(ctx context.Context, _ *Action, _ *ledgerPage) error {
			invocation := ctx.Value(ledgerInvocationKey{}).(*ledgerInvocation)
			s.recordLedgerConnectorResponse(ctx, invocation, "list-resources", time.Millisecond, nil)
			err := status.Error(codes.Unavailable, "unavailable")
			recordLedgerConnectorError(invocation, err)
			return err
		}
		_, err := runLedgerSchedulerBatch(t, s, SyncResourcesOp)
		require.Error(t, err)
		_, found, err := f.ledger.GetLedgerRow(t.Context(), id)
		require.NoError(t, err)
		require.False(t, found)
	})
}

func TestLedgerCoordinatorRetryObservations(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, f := newLedgerSchedulerFixture(t, 1)
		s.recordStats = true
		action := s.run.pushAction(t.Context(), Action{Op: SyncResourceTypesOp})
		id := ledgerIdentity(action)
		calls := 0
		s.testHooks.ledgerHandler = func(ctx context.Context, action *Action, _ *ledgerPage) error {
			calls++
			invocation := ctx.Value(ledgerInvocationKey{}).(*ledgerInvocation)
			s.recordLedgerConnectorResponse(ctx, invocation, "list-resource-types", time.Millisecond, nil)
			if calls == 1 {
				err := status.Error(codes.Unavailable, "retry")
				recordLedgerConnectorError(invocation, err)
				return err
			}
			return s.nextPageOrFinishAction(ctx, action, "")
		}
		ctx := s.withRateLimitWaitObserver(t.Context())
		warnings, err := s.parallelSync(ctx, ctx, nil)
		require.NoError(t, err)
		require.Empty(t, warnings)
		row, found, err := f.ledger.GetLedgerRow(ctx, id)
		require.NoError(t, err)
		require.True(t, found)
		require.EqualValues(t, 2, row.ConnectorAttempts)
		require.EqualValues(t, 1, row.ConnectorErrors)
		require.Equal(t, time.Second, row.SDKRetryWaitDuration)
	})
}

func TestLedgerCancelledRetryDoesNotSurviveNewWorker(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, f := newLedgerSchedulerFixture(t, 1)
		s.recordStats = true
		action := s.run.pushAction(t.Context(), Action{Op: SyncResourcesOp, ResourceTypeID: "type"})
		id := ledgerIdentity(action)
		ctx, cancel := context.WithCancel(s.withRateLimitWaitObserver(t.Context()))
		defer cancel()
		s.testHooks.ledgerHandler = func(ctx context.Context, _ *Action, _ *ledgerPage) error {
			invocation := ctx.Value(ledgerInvocationKey{}).(*ledgerInvocation)
			s.recordLedgerConnectorResponse(ctx, invocation, "list-resources", time.Millisecond, nil)
			err := status.Error(codes.Unavailable, "retry")
			recordLedgerConnectorError(invocation, err)
			time.AfterFunc(5*time.Millisecond, cancel)
			return err
		}
		r := retry.NewRetryer(ctx, retry.RetryConfig{MaxAttempts: 4, InitialDelay: time.Second, MaxDelay: time.Second})
		_, err := s.syncParallel(ctx, r, s.run.peekMatchingActions(ctx, SyncResourcesOp), s.SyncResources)
		require.Error(t, err)
		_, found, err := f.ledger.GetLedgerRow(t.Context(), id)
		require.NoError(t, err)
		require.False(t, found)
		s.testHooks.ledgerHandler = func(ctx context.Context, action *Action, _ *ledgerPage) error {
			invocation := ctx.Value(ledgerInvocationKey{}).(*ledgerInvocation)
			s.recordLedgerConnectorResponse(ctx, invocation, "list-resources", time.Millisecond, nil)
			return s.nextPageOrFinishAction(ctx, action, "")
		}
		_, err = runLedgerSchedulerBatch(t, s, SyncResourcesOp)
		require.NoError(t, err)
		row, found, err := f.ledger.GetLedgerRow(t.Context(), id)
		require.NoError(t, err)
		require.True(t, found)
		require.EqualValues(t, 1, row.ConnectorAttempts)
		require.Zero(t, row.ConnectorErrors)
		require.Zero(t, row.SDKRetryWaitDuration)
	})
}

func TestLedgerConcurrentWaitObservationsStayWithWorker(t *testing.T) {
	s, f := newLedgerSchedulerFixture(t, 2)
	s.recordStats = true
	ids := make([]c1zstore.LedgerActionIdentity, 0, 2)
	for _, resource := range []string{"one", "two"} {
		action := s.run.pushAction(t.Context(), Action{Op: SyncResourcesOp, ResourceTypeID: "type", ResourceID: resource})
		ids = append(ids, ledgerIdentity(action))
	}
	var barrier native_sync.WaitGroup
	barrier.Add(2)
	s.testHooks.ledgerHandler = func(ctx context.Context, action *Action, _ *ledgerPage) error {
		invocation := ctx.Value(ledgerInvocationKey{}).(*ledgerInvocation)
		s.recordLedgerConnectorResponse(ctx, invocation, "list-resources", time.Millisecond, nil)
		barrier.Done()
		barrier.Wait()
		var callbacks native_sync.WaitGroup
		for range 8 {
			callbacks.Go(func() { ratelimit.ObserveWait(ctx, ratelimit.WaitEvent{Duration: time.Millisecond}) })
		}
		callbacks.Wait()
		return s.nextPageOrFinishAction(ctx, action, "")
	}
	ctx := s.withRateLimitWaitObserver(t.Context())
	r := retry.NewRetryer(ctx, retry.RetryConfig{MaxAttempts: 1})
	_, err := s.syncParallel(ctx, r, s.run.peekMatchingActions(ctx, SyncResourcesOp), s.SyncResources)
	require.NoError(t, err)
	for _, id := range ids {
		row, found, err := f.ledger.GetLedgerRow(ctx, id)
		require.NoError(t, err)
		require.True(t, found)
		require.EqualValues(t, 1, row.ConnectorAttempts)
		require.Equal(t, 8*time.Millisecond, row.SDKRateLimitWaitDuration)
	}
}
