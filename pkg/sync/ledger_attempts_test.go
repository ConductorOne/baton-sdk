package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

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
		require.Equal(t, 2*time.Millisecond, first.SDKRetryWaitDuration)
		require.Equal(t, 3*time.Millisecond, first.SDKRateLimitWaitDuration)
		firstID.PageToken = "next"
		second, found, err := f.ledger.GetLedgerRow(ctx, firstID)
		require.NoError(t, err)
		require.True(t, found)
		require.EqualValues(t, 1, second.ConnectorAttempts)
		require.Zero(t, second.ConnectorErrors)
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
