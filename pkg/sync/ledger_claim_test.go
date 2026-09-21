package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLedgerDuplicatePageWaitsForCommittedRow(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, f := newLedgerSchedulerFixture(t, 2)
		first := s.run.pushAction(t.Context(), Action{Op: SyncResourcesOp, ResourceTypeID: "type"})
		second := s.run.pushAction(t.Context(), Action{Op: SyncResourcesOp, ResourceTypeID: "type"})
		entered, release := make(chan struct{}), make(chan struct{})
		var calls atomic.Int32
		s.testHooks.ledgerHandler = func(ctx context.Context, action *Action, _ *ledgerPage) error {
			if calls.Add(1) == 1 {
				close(entered)
				<-release
			}
			return s.nextPageOrFinishAction(ctx, action, "")
		}
		done := make(chan error, 2)
		go func() {
			done <- s.invokeActionPage(context.WithValue(t.Context(), ledgerWorkerKey{}, 0), first, s.SyncResources, false)
		}()
		<-entered
		go func() {
			done <- s.invokeActionPage(context.WithValue(t.Context(), ledgerWorkerKey{}, 1), second, s.SyncResources, false)
		}()
		synctest.Wait()
		assert.Empty(t, done)
		close(release)
		require.NoError(t, <-done)
		require.NoError(t, <-done)
		require.EqualValues(t, 1, calls.Load())
		require.Empty(t, s.ledger.claims)
		row, found, err := f.ledger.GetLedgerRow(t.Context(), ledgerIdentity(first))
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, ledgerIdentity(first), row.Identity)
	})
}

func TestLedgerPageClaimWaitHonorsCancellation(t *testing.T) {
	s, _ := newLedgerSchedulerFixture(t, 1)
	id := c1zstore.LedgerActionIdentity{Op: "resources"}
	release, err := s.ledger.claimPage(t.Context(), id)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { _, err := s.ledger.claimPage(ctx, id); done <- err }()
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	release()
	require.Empty(t, s.ledger.claims)
}
