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
		s.testHooks.ledgerHandler = func(ctx context.Context, page *ledgerPage, action *Action) error {
			calls++
			s.recordLedgerConnectorResponse(ctx, page, "list-resources", time.Millisecond, nil)
			if calls <= 2 {
				err := status.Error(codes.Unavailable, "retry")
				recordLedgerConnectorError(page, err)
				return err
			}
			ratelimit.ObserveWait(ctx, ratelimit.WaitEvent{Duration: 3 * time.Millisecond})
			next := ""
			if action.PageToken == "" {
				next = "next"
			}
			return page.stage(action, next, nil)
		}
		ctx := s.withRateLimitWaitObserver(t.Context())
		r := retry.NewRetryer(ctx, retry.RetryConfig{MaxAttempts: 4, InitialDelay: time.Millisecond, MaxDelay: time.Millisecond})
		seedLedgerTestRun(t, s, nil)
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
		s.testHooks.ledgerHandler = func(ctx context.Context, page *ledgerPage, _ *Action) error {
			s.recordLedgerConnectorResponse(ctx, page, "list-resources", time.Millisecond, nil)
			err := status.Error(codes.Unavailable, "unavailable")
			recordLedgerConnectorError(page, err)
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
		s.testHooks.ledgerHandler = func(ctx context.Context, page *ledgerPage, action *Action) error {
			calls++
			s.recordLedgerConnectorResponse(ctx, page, "list-resource-types", time.Millisecond, nil)
			if calls == 1 {
				err := status.Error(codes.Unavailable, "retry")
				recordLedgerConnectorError(page, err)
				return err
			}
			return page.stage(action, "", nil)
		}
		ctx := s.withRateLimitWaitObserver(t.Context())
		warnings, err := runLedgerTestSync(t, s, ctx, ctx, nil)
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
		s.testHooks.ledgerHandler = func(ctx context.Context, page *ledgerPage, _ *Action) error {
			s.recordLedgerConnectorResponse(ctx, page, "list-resources", time.Millisecond, nil)
			err := status.Error(codes.Unavailable, "retry")
			recordLedgerConnectorError(page, err)
			time.AfterFunc(5*time.Millisecond, cancel)
			return err
		}
		r := retry.NewRetryer(ctx, retry.RetryConfig{MaxAttempts: 4, InitialDelay: time.Second, MaxDelay: time.Second})
		seedLedgerTestRun(t, s, nil)
		_, err := s.syncParallel(ctx, r, s.run.peekMatchingActions(ctx, SyncResourcesOp), s.SyncResources)
		require.Error(t, err)
		_, found, err := f.ledger.GetLedgerRow(t.Context(), id)
		require.NoError(t, err)
		require.False(t, found)
		s.testHooks.ledgerHandler = func(ctx context.Context, page *ledgerPage, action *Action) error {
			s.recordLedgerConnectorResponse(ctx, page, "list-resources", time.Millisecond, nil)
			return page.stage(action, "", nil)
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

func TestLedgerWaitObservationsStayWithWorker(t *testing.T) {
	s, f := newLedgerSchedulerFixture(t, 2)
	s.recordStats = true
	ids := make([]c1zstore.LedgerActionIdentity, 0, 2)
	for _, resource := range []string{"one", "two"} {
		action := s.run.pushAction(t.Context(), Action{Op: SyncResourcesOp, ResourceTypeID: "type", ResourceID: resource})
		ids = append(ids, ledgerIdentity(action))
	}
	var barrier native_sync.WaitGroup
	barrier.Add(2)
	var seenMu native_sync.Mutex
	seen := map[*ledgerAttempts]int{}
	s.testHooks.ledgerHandler = func(ctx context.Context, page *ledgerPage, action *Action) error {
		seenMu.Lock()
		seen[page.attempts] = page.worker
		seenMu.Unlock()
		s.recordLedgerConnectorResponse(ctx, page, "list-resources", time.Millisecond, nil)
		barrier.Done()
		barrier.Wait()
		for range 8 {
			ratelimit.ObserveWait(ctx, ratelimit.WaitEvent{Duration: time.Millisecond})
		}
		return page.stage(action, "", nil)
	}
	coordinator := &ledgerAttempts{}
	ctx := s.withLedgerAttempts(s.withRateLimitWaitObserver(t.Context()), coordinator)
	r := retry.NewRetryer(ctx, retry.RetryConfig{MaxAttempts: 1})
	seedLedgerTestRun(t, s, nil)
	_, err := s.syncParallel(ctx, r, s.run.peekMatchingActions(ctx, SyncResourcesOp), s.SyncResources)
	require.NoError(t, err)
	require.Len(t, seen, 2, "each worker minted its own accumulator")
	require.NotContains(t, seen, coordinator, "workers do not share the coordinator's accumulator")
	require.Zero(t, coordinator.rateLimitWait, "worker waits did not reach the coordinator")
	for _, id := range ids {
		row, found, err := f.ledger.GetLedgerRow(ctx, id)
		require.NoError(t, err)
		require.True(t, found)
		require.EqualValues(t, 1, row.ConnectorAttempts)
		require.Equal(t, 8*time.Millisecond, row.SDKRateLimitWaitDuration)
	}
}
