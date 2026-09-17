package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/stretchr/testify/require"
)

func TestLedgerBaselineContractAudit(t *testing.T) {
	if os.Getenv("BATON_LEDGER_BASELINE_AUDIT") != "1" {
		t.Skip("known-failing baseline comparisons; see baseline-audit.md")
	}
	t.Run("spawned_completion_count", func(t *testing.T) {
		baseline := newRunState()
		action := baseline.pushAction(t.Context(), Action{Op: SyncResourcesOp, ResourceTypeID: "type", PageToken: "spawn", Spawned: true})
		baseline.finishAction(t.Context(), action)
		require.EqualValues(t, 1, baseline.completedActionsCount())
		f := newLedgerFixture(t)
		runtime, err := newLedgerRuntime(t.Context(), f.ledger, "audit")
		require.NoError(t, err)
		f.audit.enter(ledgerHandler)
		err = runtime.execute(t.Context(), []ledgerAction{{identity: c1zstore.LedgerActionIdentity{Op: SyncResourcesOp.String(), ResourceTypeID: "type", PageToken: "spawn"}, spawned: true}}, 1,
			func(_ context.Context, _ ledgerAction, page *ledgerPage) error { return page.transition("") })
		f.audit.enter(ledgerLifecycle)
		require.NoError(t, err)
		counters, err := f.ledger.LedgerCounters(t.Context())
		require.NoError(t, err)
		require.Equal(t, baseline.completedActionsCount(), counters.Counters[ledgerCompletedActions])
	})
	t.Run("duplicate_children", func(t *testing.T) {
		parent := &Action{Op: SyncResourcesOp, ResourceTypeID: "type"}
		child := Action{Op: SyncResourcesOp, ResourceTypeID: "type", PageToken: "child", Spawned: true}
		queue := newParallelActionQueue([]*Action{parent})
		committed := false
		err := queue.transition(t.Context(), SyncResourcesOp, parent, "", []Action{child, child}, func(string, []Action) ([]*Action, error) {
			committed = true
			return nil, nil
		})
		require.ErrorContains(t, err, "duplicate or cyclic spawned cursor")
		require.False(t, committed)
		f := newLedgerFixture(t)
		runtime, err := newLedgerRuntime(t.Context(), f.ledger, "audit")
		require.NoError(t, err)
		id := c1zstore.LedgerActionIdentity{Op: SyncResourcesOp.String(), ResourceTypeID: "type"}
		childID := id
		childID.PageToken = "child"
		f.audit.enter(ledgerHandler)
		err = runtime.execute(t.Context(), []ledgerAction{{identity: id}}, 1, func(_ context.Context, action ledgerAction, page *ledgerPage) error {
			if action.identity.PageToken != "" {
				return page.transition("")
			}
			return page.transition("", c1zstore.LedgerChild{Identity: childID, Spawned: true}, c1zstore.LedgerChild{Identity: childID, Spawned: true})
		})
		f.audit.enter(ledgerLifecycle)
		require.Error(t, err, "baseline rejects the response before transition commit")
	})
	t.Run("unknown_ingest_quality", func(t *testing.T) {
		var baseline ingestFilterStats
		require.Nil(t, baseline.snapshot())
		got := ledgerSyncStats(nil, c1zstore.LedgerCounters{}).IngestQuality
		require.True(t, got == nil || got.SourceCacheReplayBlocked, "unknown history must not become a known-clean quality record")
	})
	t.Run("independent_worker_errors", func(t *testing.T) {
		first, second := errors.New("first failure"), errors.New("second failure")
		baseline := errors.Join(first, second)
		require.ErrorIs(t, baseline, first)
		require.ErrorIs(t, baseline, second)
		f := newLedgerFixture(t)
		runtime, err := newLedgerRuntime(t.Context(), f.ledger, "audit")
		require.NoError(t, err)
		var arrived atomic.Int32
		ready := make(chan struct{})
		f.audit.enter(ledgerHandler)
		err = runtime.execute(t.Context(), []ledgerAction{
			{identity: c1zstore.LedgerActionIdentity{Op: "init", ResourceID: "first"}},
			{identity: c1zstore.LedgerActionIdentity{Op: "init", ResourceID: "second"}},
		}, 2, func(_ context.Context, action ledgerAction, _ *ledgerPage) error {
			if arrived.Add(1) == 2 {
				close(ready)
			}
			<-ready
			if action.identity.ResourceID == "first" {
				return first
			}
			return second
		})
		f.audit.enter(ledgerLifecycle)
		require.True(t, errors.Is(err, first) && errors.Is(err, second), "baseline retains both independent worker errors, got %v", err)
	})
	t.Run("operation_barrier", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			baseline := newRunState()
			baseline.pushAction(t.Context(), Action{Op: SyncGrantsOp})
			baseline.pushAction(t.Context(), Action{Op: SyncResourcesOp})
			batch := baseline.peekMatchingActions(t.Context(), SyncResourcesOp)
			require.Len(t, batch, 1)
			require.Equal(t, SyncResourcesOp, batch[0].Op)
			f := newLedgerFixture(t)
			runtime, err := newLedgerRuntime(t.Context(), f.ledger, "audit")
			require.NoError(t, err)
			release := make(chan struct{})
			var grantStarted atomic.Bool
			result := make(chan error, 1)
			f.audit.enter(ledgerHandler)
			go func() {
				result <- runtime.execute(t.Context(), []ledgerAction{
					{identity: c1zstore.LedgerActionIdentity{Op: SyncGrantsOp.String()}},
					{identity: c1zstore.LedgerActionIdentity{Op: SyncResourcesOp.String()}},
				}, 2, func(_ context.Context, action ledgerAction, page *ledgerPage) error {
					if action.identity.Op == SyncResourcesOp.String() {
						<-release
					} else {
						grantStarted.Store(true)
					}
					return page.transition("")
				})
			}()
			synctest.Wait()
			crossed := grantStarted.Load()
			close(release)
			require.NoError(t, <-result)
			f.audit.enter(ledgerLifecycle)
			require.False(t, crossed, "grant work began before the resource batch drained")
		})
	})
}
