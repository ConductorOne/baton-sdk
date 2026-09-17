package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/retry"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func newLedgerSchedulerFixture(t *testing.T, workers int) (*syncer, *ledgerFixture) {
	t.Helper()
	f := newLedgerFixture(t)
	runtime, err := newLedgerRuntime(t.Context(), f.ledger, "scheduler-attempt")
	require.NoError(t, err)
	s := &syncer{ledgered: true, ledger: runtime, store: f.store, caps: resolveStoreCaps(f.store), run: newRunState(), stats: newRunStats(), cfg: syncConfig{workerCount: workers}}
	return s, f
}

func runLedgerSchedulerBatch(t *testing.T, s *syncer, op ActionOp) ([]error, error) {
	t.Helper()
	retryer := retry.NewRetryer(t.Context(), retry.RetryConfig{MaxAttempts: 1})
	return s.syncParallel(t.Context(), retryer, s.run.peekMatchingActions(t.Context(), op), func(context.Context, *Action) error {
		return errors.New("ledger invocation reached the token handler")
	})
}

func TestLedgerExistingSchedulerOperationBarrier(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, f := newLedgerSchedulerFixture(t, 4)
		s.run.pushAction(t.Context(), Action{Op: SyncGrantsOp, ResourceTypeID: "type", ResourceID: "grant"})
		s.run.pushAction(t.Context(), Action{Op: SyncResourcesOp, ResourceTypeID: "type", ResourceID: "one"})
		s.run.pushAction(t.Context(), Action{Op: SyncResourcesOp, ResourceTypeID: "type", ResourceID: "two"})
		release := make(chan struct{})
		var resourcesStarted atomic.Int32
		var grantStarted atomic.Bool
		s.testHooks.ledgerHandler = func(ctx context.Context, action *Action, page *ledgerPage) error {
			if action.Op == SyncResourcesOp {
				resourcesStarted.Add(1)
				<-release
			} else {
				grantStarted.Store(true)
			}
			if err := page.writer.PutResourceTypes(ctx, v2.ResourceType_builder{Id: action.ResourceID}.Build()); err != nil {
				return err
			}
			return s.nextPageOrFinishAction(ctx, action, "")
		}
		result := make(chan error, 1)
		f.audit.enter(ledgerHandler)
		go func() { _, err := s.parallelSync(t.Context(), t.Context(), nil); result <- err }()
		synctest.Wait()
		count := resourcesStarted.Load()
		crossed := grantStarted.Load()
		close(release)
		require.NoError(t, <-result)
		f.audit.enter(ledgerLifecycle)
		require.EqualValues(t, 2, count)
		require.False(t, crossed)
		require.True(t, grantStarted.Load())
		require.Nil(t, s.run.current())
		require.EqualValues(t, 3, s.run.completedActionsCount())
		counters, err := f.ledger.LedgerCounters(t.Context())
		require.NoError(t, err)
		require.Equal(t, s.run.completedActionsCount(), counters.Counters[ledgerCompletedActions])
	})
}

func TestLedgerExistingSchedulerSpawnedCompletion(t *testing.T) {
	s, f := newLedgerSchedulerFixture(t, 2)
	s.run.pushAction(t.Context(), Action{Op: SyncResourcesOp, ResourceTypeID: "type", PageToken: "spawn", Spawned: true})
	s.testHooks.ledgerHandler = func(ctx context.Context, action *Action, _ *ledgerPage) error {
		return s.nextPageOrFinishAction(ctx, action, "")
	}
	f.audit.enter(ledgerHandler)
	warnings, err := runLedgerSchedulerBatch(t, s, SyncResourcesOp)
	f.audit.enter(ledgerLifecycle)
	require.NoError(t, err)
	require.Empty(t, warnings)
	require.EqualValues(t, 1, s.run.completedActionsCount())
	counters, err := f.ledger.LedgerCounters(t.Context())
	require.NoError(t, err)
	require.Equal(t, s.run.completedActionsCount(), counters.Counters[ledgerCompletedActions])
	require.Equal(t, s.run.getActionCount(SyncResourcesOp).CompletedCount, counters.Counters[ledgerCompletedPrefix+SyncResourcesOp.String()])
}

func TestLedgerExistingSchedulerRejectsDuplicateBeforeCommit(t *testing.T) {
	s, f := newLedgerSchedulerFixture(t, 1)
	parent := s.run.pushAction(t.Context(), Action{Op: SyncResourcesOp, ResourceTypeID: "type"})
	child := Action{Op: SyncResourcesOp, ResourceTypeID: "type", PageToken: "child", Spawned: true}
	before := ledgerRawSnapshot(t, f.engine)
	s.testHooks.ledgerHandler = func(ctx context.Context, action *Action, page *ledgerPage) error {
		if err := page.writer.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "discard"}.Build()); err != nil {
			return err
		}
		return s.nextPageOrFinishAction(ctx, action, "", child, child)
	}
	f.audit.enter(ledgerHandler)
	_, err := runLedgerSchedulerBatch(t, s, SyncResourcesOp)
	f.audit.enter(ledgerLifecycle)
	require.ErrorContains(t, err, "duplicate or cyclic spawned cursor")
	require.Equal(t, parent, s.run.current())
	require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
	require.Zero(t, f.audit.writers)
}

func TestLedgerExistingSchedulerPreservesIndependentErrors(t *testing.T) {
	s, f := newLedgerSchedulerFixture(t, 2)
	first, second := errors.New("first failure"), errors.New("second failure")
	s.run.pushAction(t.Context(), Action{Op: SyncResourcesOp, ResourceTypeID: "first"})
	s.run.pushAction(t.Context(), Action{Op: SyncResourcesOp, ResourceTypeID: "second"})
	var arrived atomic.Int32
	ready := make(chan struct{})
	s.testHooks.ledgerHandler = func(_ context.Context, action *Action, _ *ledgerPage) error {
		if arrived.Add(1) == 2 {
			close(ready)
		}
		<-ready
		if action.ResourceTypeID == "first" {
			return first
		}
		return second
	}
	before := ledgerRawSnapshot(t, f.engine)
	f.audit.enter(ledgerHandler)
	_, err := runLedgerSchedulerBatch(t, s, SyncResourcesOp)
	f.audit.enter(ledgerLifecycle)
	require.ErrorIs(t, err, first)
	require.ErrorIs(t, err, second)
	require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
	require.Zero(t, f.audit.writers)
}

func TestLedgerExistingSchedulerWarningCommitsAccounting(t *testing.T) {
	s, f := newLedgerSchedulerFixture(t, 1)
	action := s.run.pushAction(t.Context(), Action{Op: SyncResourcesOp, ResourceTypeID: "type"})
	warning := status.Error(codes.NotFound, "resource no longer exists")
	s.testHooks.ledgerHandler = func(context.Context, *Action, *ledgerPage) error { return warning }
	f.audit.enter(ledgerHandler)
	warnings, err := runLedgerSchedulerBatch(t, s, SyncResourcesOp)
	f.audit.enter(ledgerLifecycle)
	require.NoError(t, err)
	require.Equal(t, []error{warning}, warnings)
	require.Nil(t, s.run.current())
	counters, err := f.ledger.LedgerCounters(t.Context())
	require.NoError(t, err)
	require.Equal(t, s.run.getActionCount(SyncResourcesOp).WarningCount, counters.Counters[ledgerWarningsPrefix+SyncResourcesOp.String()])
	require.EqualValues(t, 1, counters.Counters[ledgerCompletedActions])
	_, found, err := f.ledger.GetLedgerRow(t.Context(), ledgerIdentity(action))
	require.NoError(t, err)
	require.True(t, found)
}

func TestLedgerExistingSchedulerCommitFailureKeepsAction(t *testing.T) {
	s, f := newLedgerSchedulerFixture(t, 1)
	action := s.run.pushAction(t.Context(), Action{Op: SyncResourcesOp, ResourceTypeID: "type"})
	s.testHooks.ledgerHandler = func(ctx context.Context, action *Action, page *ledgerPage) error {
		if err := page.writer.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "discard"}.Build()); err != nil {
			return err
		}
		return s.nextPageOrFinishAction(ctx, action, "next")
	}
	s.ledger.store = ledgerFailingPageStore{PageLedgerStore: f.ledger, stage: "commit"}
	before := ledgerRawSnapshot(t, f.engine)
	f.audit.enter(ledgerHandler)
	_, err := runLedgerSchedulerBatch(t, s, SyncResourcesOp)
	f.audit.enter(ledgerLifecycle)
	require.Error(t, err)
	require.Equal(t, action, s.run.current())
	require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
}

func TestLedgerExistingSchedulerPublishesFactsAfterCommit(t *testing.T) {
	s, f := newLedgerSchedulerFixture(t, 1)
	s.run.pushAction(t.Context(), Action{Op: SyncResourcesOp, ResourceTypeID: "type"})
	s.testHooks.ledgerHandler = func(ctx context.Context, action *Action, page *ledgerPage) error {
		if err := page.setFact(factNeedsExpansion); err != nil {
			return err
		}
		if s.run.hasFact(factNeedsExpansion) {
			return errors.New("uncommitted fact was published")
		}
		if err := s.nextPageOrFinishAction(ctx, action, ""); err != nil {
			return err
		}
		if s.run.current() == nil {
			return errors.New("staged transition was published before handler returned")
		}
		return nil
	}
	f.audit.enter(ledgerHandler)
	_, err := runLedgerSchedulerBatch(t, s, SyncResourcesOp)
	f.audit.enter(ledgerLifecycle)
	require.NoError(t, err)
	require.True(t, s.run.hasFact(factNeedsExpansion))
	require.Nil(t, s.run.current())
}

type ledgerSchedulerCommitFaultStore struct {
	c1zstore.PageLedgerStore
	err error
}

func (s ledgerSchedulerCommitFaultStore) BeginPage() c1zstore.PageWriter {
	return ledgerSchedulerCommitFaultWriter{PageWriter: s.PageLedgerStore.BeginPage(), err: s.err}
}

type ledgerSchedulerCommitFaultWriter struct {
	c1zstore.PageWriter
	err error
}

func (w ledgerSchedulerCommitFaultWriter) Commit(context.Context, c1zstore.LedgerActionIdentity, *c1zstore.LedgerRow) error {
	return w.err
}

func TestLedgerExistingSchedulerCommitNotFoundIsNotWarning(t *testing.T) {
	s, f := newLedgerSchedulerFixture(t, 1)
	action := s.run.pushAction(t.Context(), Action{Op: SyncResourcesOp, ResourceTypeID: "type"})
	missing := status.Error(codes.NotFound, "commit target disappeared")
	s.ledger.store = ledgerSchedulerCommitFaultStore{PageLedgerStore: f.ledger, err: missing}
	s.testHooks.ledgerHandler = func(ctx context.Context, action *Action, _ *ledgerPage) error {
		return s.nextPageOrFinishAction(ctx, action, "")
	}
	before := ledgerRawSnapshot(t, f.engine)
	f.audit.enter(ledgerHandler)
	warnings, err := runLedgerSchedulerBatch(t, s, SyncResourcesOp)
	f.audit.enter(ledgerLifecycle)
	require.ErrorIs(t, err, missing)
	require.Empty(t, warnings)
	require.Equal(t, action, s.run.current())
	require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
}

func TestLedgerExistingSchedulerRootNotFoundDiscardsPage(t *testing.T) {
	s, f := newLedgerSchedulerFixture(t, 1)
	action := s.run.pushAction(t.Context(), Action{Op: SyncResourceTypesOp})
	missing := status.Error(codes.NotFound, "resource type listing unavailable")
	s.testHooks.ledgerHandler = func(context.Context, *Action, *ledgerPage) error { return missing }
	before := ledgerRawSnapshot(t, f.engine)
	f.audit.enter(ledgerHandler)
	_, err := s.parallelSync(t.Context(), t.Context(), nil)
	f.audit.enter(ledgerLifecycle)
	require.ErrorIs(t, err, missing)
	require.Equal(t, action, s.run.current())
	require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
}

func TestLedgerExistingSchedulerRejectsAssignedChildBeforeCommit(t *testing.T) {
	s, f := newLedgerSchedulerFixture(t, 1)
	action := s.run.pushAction(t.Context(), Action{Op: SyncResourcesOp, ResourceTypeID: "type"})
	s.testHooks.ledgerHandler = func(ctx context.Context, action *Action, _ *ledgerPage) error {
		return s.nextPageOrFinishAction(ctx, action, "", Action{ID: "already-assigned", Op: SyncResourcesOp, ResourceTypeID: "child"})
	}
	before := ledgerRawSnapshot(t, f.engine)
	f.audit.enter(ledgerHandler)
	_, err := runLedgerSchedulerBatch(t, s, SyncResourcesOp)
	f.audit.enter(ledgerLifecycle)
	require.ErrorContains(t, err, "action ID must be empty")
	require.Equal(t, action, s.run.current())
	require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
}
