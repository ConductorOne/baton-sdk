package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"errors"
	native_sync "sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/retry"
	"github.com/stretchr/testify/require"
)

// ledgerGatedPageStore runs before() ahead of every page commit and after()
// once the store has accepted it, on the committing goroutine.
type ledgerGatedPageStore struct {
	c1zstore.PageLedgerStore
	before func(c1zstore.LedgerActionIdentity)
	after  func(c1zstore.LedgerActionIdentity)
}

func (s ledgerGatedPageStore) BeginPage() c1zstore.PageWriter {
	return ledgerGatedPageWriter{PageWriter: s.PageLedgerStore.BeginPage(), before: s.before, after: s.after}
}

type ledgerGatedPageWriter struct {
	c1zstore.PageWriter
	before func(c1zstore.LedgerActionIdentity)
	after  func(c1zstore.LedgerActionIdentity)
}

// runActionByIdentity finds the in-memory action for an identity. Seeding
// reassigns action IDs from durable work IDs, so IDs from pushAction do not
// survive it; identities do.
func runActionByIdentity(s *syncer, id c1zstore.LedgerActionIdentity) *Action {
	s.run.mu.RLock()
	defer s.run.mu.RUnlock()
	for _, action := range s.run.actions {
		if ledgerIdentity(&action) == id {
			a := action
			return &a
		}
	}
	return nil
}

func (w ledgerGatedPageWriter) Commit(ctx context.Context, id c1zstore.LedgerActionIdentity, row *c1zstore.LedgerRow) error {
	if w.before != nil {
		w.before(id)
	}
	if err := w.PageWriter.Commit(ctx, id, row); err != nil {
		return err
	}
	if w.after != nil {
		w.after(id)
	}
	return nil
}

// The property the narrowing exists for: another worker can dequeue while a
// commit is in flight. With commitUnlocked off, next() blocks on q.mu until
// the commit returns; with it on, next() completes. Both are asserted so the
// test fails if the flag stops doing anything.
func TestLedgerQueueCommitDoesNotHoldQueueLock(t *testing.T) {
	// Real time, not synctest: a goroutine blocked on sync.Mutex is not a
	// durable block to synctest.Wait, and the held leg blocks on exactly that.
	for _, unlocked := range []bool{false, true} {
		t.Run(map[bool]string{false: "held", true: "released"}[unlocked], func(t *testing.T) {
			first := &Action{ID: "a", Op: SyncResourcesOp}
			second := &Action{ID: "b", Op: SyncResourcesOp}
			q := newParallelActionQueue([]*Action{first, second})
			q.commitUnlocked = unlocked
			taken, ok := q.next()
			require.True(t, ok)
			require.Same(t, first, taken)
			entered, release := make(chan struct{}), make(chan struct{})
			transitionDone := make(chan error, 1)
			go func() {
				transitionDone <- q.transition(t.Context(), SyncResourcesOp, first, "", nil, func(string, []Action) ([]*Action, error) {
					close(entered)
					<-release
					return nil, nil
				})
			}()
			<-entered
			dequeued := make(chan bool, 1)
			go func() {
				_, ok := q.next()
				dequeued <- ok
			}()
			duringCommit := false
			select {
			case ok := <-dequeued:
				require.True(t, ok)
				duringCommit = true
			case <-time.After(300 * time.Millisecond):
			}
			require.Equal(t, unlocked, duringCommit, "a sibling's next() completes during the commit only when the lock is released")
			close(release)
			require.NoError(t, <-transitionDone)
			if !duringCommit {
				select {
				case ok := <-dequeued:
					require.True(t, ok)
				case <-time.After(5 * time.Second):
					t.Fatal("next() never completed after the commit returned")
				}
			}
		})
	}
}

// The ledger commit runs outside q.mu, so a worker blocked in next() with the
// queue drained must still be woken when a sibling's commit lands a same-op
// child: the Broadcast is taken under the lock after the commit returns.
func TestLedgerCommitWakesWorkerWaitingForChildren(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, f := newLedgerSchedulerFixture(t, 2)
		parent := s.run.pushAction(t.Context(), Action{Op: SyncResourcesOp, ResourceTypeID: "type", ResourceID: "parent"})
		child := c1zstore.LedgerActionIdentity{Op: SyncResourcesOp.String(), ResourceTypeID: "type", ResourceID: "child"}
		var seenMu native_sync.Mutex
		workers := map[string]int{}
		s.testHooks.ledgerHandler = func(ctx context.Context, action *Action, page *ledgerPage) error {
			seenMu.Lock()
			workers[action.ResourceID] = ctx.Value(ledgerWorkerKey{}).(int)
			seenMu.Unlock()
			if err := page.writer.PutResourceTypes(ctx, v2.ResourceType_builder{Id: action.ResourceID}.Build()); err != nil {
				return err
			}
			if action.ResourceID == "parent" {
				return ledgerFixtureTransition(ctx, s, action, "", c1zstore.LedgerChild{Identity: child})
			}
			return s.nextPageOrFinishAction(ctx, action, "")
		}
		f.audit.enter(ledgerHandler)
		seedLedgerTestRun(t, s, parent)
		r := retry.NewRetryer(t.Context(), retry.RetryConfig{MaxAttempts: 1})
		_, err := s.syncParallel(t.Context(), r, s.run.peekMatchingActions(t.Context(), SyncResourcesOp), s.SyncResources)
		f.audit.enter(ledgerLifecycle)
		require.NoError(t, err)
		require.Len(t, workers, 2, "the child admitted through refill ran")
		require.Nil(t, s.run.current())
		for _, id := range []c1zstore.LedgerActionIdentity{ledgerIdentity(parent), child} {
			_, found, err := f.ledger.GetLedgerRow(t.Context(), id)
			require.NoError(t, err)
			require.True(t, found)
		}
	})
}

// A sibling's failure aborts the batch while another worker is committing.
// Two orderings, both required: if the abort's cancellation reaches the store
// before it accepts the batch, the page is refused and nothing lands; if the
// store has already accepted it, the commit publishes to memory and the batch
// reports only the sibling's error. In neither ordering does the committing
// worker's page end up durable but unpublished, or published but reported
// failed.
func TestLedgerCommitAcrossSiblingAbort(t *testing.T) {
	for _, stage := range []string{"before store commit", "after store commit"} {
		t.Run(stage, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s, f := newLedgerSchedulerFixture(t, 2)
				slow := s.run.pushAction(t.Context(), Action{Op: SyncResourcesOp, ResourceTypeID: "type", ResourceID: "slow"})
				s.run.pushAction(t.Context(), Action{Op: SyncResourcesOp, ResourceTypeID: "type", ResourceID: "failing"})
				injected := errors.New("sibling failure")
				slowEntered := make(chan struct{})
				failingReturned := make(chan struct{})
				gate := func(id c1zstore.LedgerActionIdentity) {
					if id.ResourceID != "slow" {
						return
					}
					close(slowEntered)
					<-failingReturned
					// Let the failing worker reach abort() and exit before the slow
					// commit resumes, so the abort precedes re-acquisition of q.mu.
					synctest.Wait()
				}
				gated := ledgerGatedPageStore{PageLedgerStore: f.ledger}
				if stage == "before store commit" {
					gated.before = gate
				} else {
					gated.after = gate
				}
				s.ledger.store = gated
				s.testHooks.ledgerHandler = func(ctx context.Context, action *Action, page *ledgerPage) error {
					if action.ResourceID == "failing" {
						<-slowEntered
						defer close(failingReturned)
						return injected
					}
					if err := page.writer.PutResourceTypes(ctx, v2.ResourceType_builder{Id: action.ResourceID}.Build()); err != nil {
						return err
					}
					return s.nextPageOrFinishAction(ctx, action, "")
				}
				f.audit.enter(ledgerHandler)
				seedLedgerTestRun(t, s, nil)
				r := retry.NewRetryer(t.Context(), retry.RetryConfig{MaxAttempts: 1})
				_, err := s.syncParallel(t.Context(), r, s.run.peekMatchingActions(t.Context(), SyncResourcesOp), s.SyncResources)
				f.audit.enter(ledgerLifecycle)
				require.ErrorIs(t, err, injected)
				_, found, rowErr := f.ledger.GetLedgerRow(t.Context(), ledgerIdentity(slow))
				require.NoError(t, rowErr)
				pending, _, pendingErr := f.ledger.PendingWork(t.Context(), 0, 10)
				require.NoError(t, pendingErr)
				if stage == "before store commit" {
					require.ErrorIs(t, err, context.Canceled, "the store refused the page on the aborted context")
					require.False(t, found, "nothing landed")
					require.NotNil(t, runActionByIdentity(s, ledgerIdentity(slow)), "memory did not advance")
					require.Len(t, pending, 2, "both actions remain pending for the next attempt")
					return
				}
				require.NotErrorIs(t, err, context.Canceled, "the accepted commit contributed no error")
				require.True(t, found, "the accepted commit is durable")
				require.Nil(t, runActionByIdentity(s, ledgerIdentity(slow)), "and was published to memory")
				require.Len(t, pending, 1)
				require.Equal(t, "failing", pending[0].Action.Identity.ResourceID)
			})
		})
	}
}

// Cancellation arriving after the store accepted a commit but before its
// in-memory publication must not turn the committed page into an error: the
// continuation's revision advances in memory to match the durable slot, so a
// later attempt would not retry a stale revision.
func TestLedgerCommitPublishesBeforeCancellation(t *testing.T) {
	s, f := newLedgerSchedulerFixture(t, 1)
	action := s.run.pushAction(t.Context(), Action{Op: SyncResourcesOp, ResourceTypeID: "type", ResourceID: "paged"})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var commits atomic.Int32
	s.ledger.store = ledgerGatedPageStore{PageLedgerStore: f.ledger, after: func(c1zstore.LedgerActionIdentity) {
		if commits.Add(1) == 1 {
			cancel()
		}
	}}
	s.testHooks.ledgerHandler = func(ctx context.Context, action *Action, page *ledgerPage) error {
		if err := page.writer.PutResourceTypes(ctx, v2.ResourceType_builder{Id: action.PageToken + "x"}.Build()); err != nil {
			return err
		}
		return s.nextPageOrFinishAction(ctx, action, "more")
	}
	f.audit.enter(ledgerHandler)
	seedLedgerTestRun(t, s, action)
	r := retry.NewRetryer(ctx, retry.RetryConfig{MaxAttempts: 1})
	_, err := s.syncParallel(ctx, r, s.run.peekMatchingActions(ctx, SyncResourcesOp), s.SyncResources)
	f.audit.enter(ledgerLifecycle)
	require.ErrorIs(t, err, context.Canceled)
	require.NotErrorIs(t, err, errLedgerPageTransition)
	require.EqualValues(t, 1, commits.Load(), "one page committed; the cancelled second attempt did not")
	updated := s.run.getAction(action.ID)
	require.NotNil(t, updated)
	require.Equal(t, "more", updated.PageToken, "publication reached memory after cancellation")
	pending, _, err := f.ledger.PendingWork(t.Context(), 0, 10)
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.Equal(t, updated.WorkRevision, pending[0].Revision, "memory and the durable slot agree on the revision")
	require.Equal(t, "more", pending[0].Action.Identity.PageToken)
}
