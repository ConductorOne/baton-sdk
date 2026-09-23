package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	native_sync "sync"
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type pendingNoHistoryStore struct{ c1zstore.PageLedgerStore }

func (s pendingNoHistoryStore) GetLedgerRow(context.Context, c1zstore.LedgerActionIdentity) (*c1zstore.LedgerRow, bool, error) {
	return nil, false, errors.New("resume must not read completed page history")
}

func TestPendingSyncResumesWithoutHistoryReads(t *testing.T) {
	f := openLedgerFixtureAt(t, filepath.Join(t.TempDir(), "pending-resume.c1z"), false)
	c := &ledgerTypesConnector{mockConnector: newMockConnector()}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	first, err := NewSyncer(t.Context(), c, WithConnectorStore(f.store), WithSkipEntitlementsAndGrants(true), WithProgressHandler(func(*Progress) {
		if len(c.calls) == 1 {
			cancel()
		}
	}))
	require.NoError(t, err)
	require.ErrorIs(t, first.Sync(ctx), context.Canceled)
	require.NoError(t, f.store.Close(t.Context()))
	f = openLedgerFixtureAt(t, f.path, false)
	c = &ledgerTypesConnector{mockConnector: newMockConnector()}
	resumed, err := NewSyncer(t.Context(), c, WithConnectorStore(f.store), WithSkipEntitlementsAndGrants(true))
	require.NoError(t, err)
	resumed.(*syncer).caps.pageLedger = pendingNoHistoryStore{f.ledger}
	require.NoError(t, resumed.Sync(t.Context()))
	require.Equal(t, []string{"page-2"}, c.calls)
}

type pendingRepeatedConnector struct {
	*mockConnector
	calls []string
}

func (c *pendingRepeatedConnector) ListResourceTypes(
	_ context.Context, req *v2.ResourceTypesServiceListResourceTypesRequest, _ ...grpc.CallOption,
) (*v2.ResourceTypesServiceListResourceTypesResponse, error) {
	c.calls = append(c.calls, req.GetPageToken())
	next := "A"
	if len(c.calls) == 3 {
		next = ""
	}
	return v2.ResourceTypesServiceListResourceTypesResponse_builder{NextPageToken: next}.Build(), nil
}

func TestPendingSyncRepeatedTokenReachesEmptyTerminalPage(t *testing.T) {
	f := openLedgerFixtureAt(t, filepath.Join(t.TempDir(), "repeated.c1z"), false)
	c := &pendingRepeatedConnector{mockConnector: newMockConnector()}
	s, err := NewSyncer(t.Context(), c, WithConnectorStore(f.store), WithSkipEntitlementsAndGrants(true))
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	require.NoError(t, s.Sync(ctx))
	require.Equal(t, []string{"", "A", "A"}, c.calls)
}

func TestPendingSyncBoundsExecutionWindow(t *testing.T) {
	f := newLedgerFixture(t)
	const count = 1001
	work := make([]c1zstore.LedgerWork, count)
	for i := range work {
		work[i].Action.Identity = c1zstore.LedgerActionIdentity{Op: SyncResourcesOp.String(), ResourceTypeID: "group"}
	}
	require.NoError(t, f.ledger.InitializePendingWork(t.Context(), work, ledgerFactIngestKnown))
	created, err := NewSyncer(t.Context(), newMockConnector(), WithConnectorStore(f.store), WithSyncID(f.engine.CurrentSyncID()), WithWorkerCount(4), WithSkipEntitlementsAndGrants(true))
	require.NoError(t, err)
	s := created.(*syncer)
	var mu native_sync.Mutex
	processed, peak := 0, 0
	s.testHooks.ledgerHandler = func(ctx context.Context, action *Action, _ *ledgerPage) error {
		s.run.mu.RLock()
		resident := len(s.run.actions)
		s.run.mu.RUnlock()
		mu.Lock()
		processed++
		peak = max(peak, resident)
		mu.Unlock()
		return s.nextPageOrFinishAction(ctx, action, "")
	}
	require.NoError(t, s.Sync(t.Context()))
	require.Equal(t, count, processed)
	require.LessOrEqual(t, peak, 2*maxPeekActionsCount+4)
	require.EqualValues(t, count, s.run.completedActionsCount())
}

func TestPendingSyncRefusesUnfinishedHistoryWithoutQueue(t *testing.T) {
	f := newLedgerFixture(t)
	writer := f.ledger.BeginPage()
	defer writer.Discard()
	require.NoError(t, writer.Commit(t.Context(), c1zstore.LedgerActionIdentity{Op: InitOp.String()}, nil))
	c := &ledgerTypesConnector{mockConnector: newMockConnector()}
	s, err := NewSyncer(t.Context(), c, WithConnectorStore(f.store), WithSyncID(f.engine.CurrentSyncID()))
	require.NoError(t, err)
	before := ledgerRawSnapshot(t, f.engine)
	require.ErrorContains(t, s.Sync(t.Context()), "completed history")
	require.Empty(t, c.calls)
	require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
}

func TestPendingSyncSpilledChildrenKeepOrder(t *testing.T) {
	for _, workers := range []int{1, 4} {
		t.Run(fmt.Sprint(workers), func(t *testing.T) {
			f := newLedgerFixture(t)
			const pages, children = 8, 100
			root := c1zstore.LedgerWork{Action: c1zstore.LedgerChild{Identity: c1zstore.LedgerActionIdentity{Op: SyncResourcesOp.String(), ResourceTypeID: "group", ResourceID: "root"}}}
			require.NoError(t, f.ledger.InitializePendingWork(t.Context(), []c1zstore.LedgerWork{root}, ledgerFactIngestKnown))
			created, err := NewSyncer(t.Context(), newMockConnector(), WithConnectorStore(f.store), WithSyncID(f.engine.CurrentSyncID()), WithWorkerCount(workers), WithSkipEntitlementsAndGrants(true))
			require.NoError(t, err)
			s := created.(*syncer)
			audit := attachQueueAudit(t, created)
			var mu native_sync.Mutex
			var order []string
			peak := 0
			s.testHooks.ledgerHandler = func(ctx context.Context, action *Action, _ *ledgerPage) error {
				s.run.mu.RLock()
				resident := len(s.run.actions)
				s.run.mu.RUnlock()
				mu.Lock()
				peak = max(peak, resident)
				mu.Unlock()
				if action.ResourceID == "root" {
					page := 0
					if action.PageToken != "" {
						var err error
						page, err = strconv.Atoi(action.PageToken)
						if err != nil {
							return err
						}
					}
					next := ""
					if page+1 < pages {
						next = strconv.Itoa(page + 1)
					}
					spawned := make([]Action, children)
					for i := range spawned {
						spawned[i] = Action{Op: SyncResourcesOp, ResourceTypeID: "group", ResourceID: fmt.Sprintf("child-%04d", page*children+i)}
					}
					return s.nextPageOrFinishAction(ctx, action, next, spawned...)
				}
				mu.Lock()
				order = append(order, action.ResourceID)
				mu.Unlock()
				return s.nextPageOrFinishAction(ctx, action, "")
			}
			require.NoError(t, s.Sync(t.Context()))
			verifyQueueAudit(t, audit)
			require.LessOrEqual(t, peak, 2*maxPeekActionsCount+workers)
			expected := make([]string, pages*children)
			for i := range expected {
				expected[i] = fmt.Sprintf("child-%04d", i)
			}
			if workers == 1 {
				require.Equal(t, expected, order)
			} else {
				require.ElementsMatch(t, expected, order)
			}
			require.EqualValues(t, 1+pages*children, s.run.completedActionsCount())
		})
	}
}

func TestPendingSyncCompletesLocalPhaseWithoutPageHistory(t *testing.T) {
	f := newLedgerFixture(t)
	work := c1zstore.LedgerWork{Action: c1zstore.LedgerChild{Identity: c1zstore.LedgerActionIdentity{Op: SyncGrantExpansionOp.String()}}}
	require.NoError(t, f.ledger.InitializePendingWork(t.Context(), []c1zstore.LedgerWork{work}, ledgerFactIngestKnown))
	created, err := NewSyncer(t.Context(), newMockConnector(), WithConnectorStore(f.store), WithSyncID(f.engine.CurrentSyncID()), WithLedgerDebug(true))
	require.NoError(t, err)
	s := created.(*syncer)
	require.NoError(t, s.Sync(t.Context()))
	require.EqualValues(t, 1, s.run.completedActionsCount())
	_, found, err := f.ledger.GetLedgerRow(t.Context(), work.Action.Identity)
	require.NoError(t, err)
	require.False(t, found)
	counts, err := f.ledger.LedgerCounters(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 1, counts.Counters[ledgerCompletedPrefix+SyncGrantExpansionOp.String()])
}

type pendingLocalFailure struct{ c1zstore.PageLedgerStore }

func (s pendingLocalFailure) CompletePendingWork(context.Context, c1zstore.LedgerWork, string, c1zstore.LedgerCounters) error {
	return errLedgerInjectedPage
}

func TestPendingLocalCompletionFailureAndStopAccounting(t *testing.T) {
	s, f := newLedgerSchedulerFixture(t, 1)
	action := s.run.pushAction(t.Context(), Action{Op: SyncGrantExpansionOp})
	seedLedgerTestRun(t, s, action)
	complete := func() error { s.finishAction(t.Context(), action); return nil }
	s.caps.pageLedger = pendingLocalFailure{f.ledger}
	require.ErrorIs(t, s.runPendingLocalStep(t.Context(), action, complete), errLedgerInjectedPage)
	require.Equal(t, action, s.run.current())
	require.Zero(t, s.run.completedActionsCount())
	s.caps.pageLedger = f.ledger
	require.NoError(t, s.runPendingLocalStep(t.Context(), action, complete))
	require.EqualValues(t, 1, s.run.completedActionsCount())
	s.checkpointLedgerOnStop(t.Context())
	totals, err := f.ledger.LedgerCounters(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 1, totals.Counters[ledgerCompletedActions])
	require.EqualValues(t, 1, totals.Counters[ledgerCompletedPrefix+SyncGrantExpansionOp.String()])
}

type pendingRefillFailure struct{ c1zstore.PageLedgerStore }

func (s pendingRefillFailure) PendingWorkAfter(ctx context.Context, after uint64, limit int) ([]c1zstore.LedgerWork, bool, error) {
	work, initialized, err := s.PageLedgerStore.PendingWorkAfter(ctx, after, limit)
	if err == nil && len(work) > 0 {
		return nil, initialized, errLedgerInjectedPage
	}
	return work, initialized, err
}

func TestPendingRefillFailureLeavesChildrenForColdResume(t *testing.T) {
	f := newLedgerFixture(t)
	syncID := f.engine.CurrentSyncID()
	root := c1zstore.LedgerActionIdentity{Op: SyncResourcesOp.String(), ResourceTypeID: "group", ResourceID: "root"}
	require.NoError(t, f.ledger.InitializePendingWork(t.Context(), []c1zstore.LedgerWork{{Action: c1zstore.LedgerChild{Identity: root}}}, ledgerFactIngestKnown))
	created, err := NewSyncer(t.Context(), newMockConnector(), WithConnectorStore(f.store), WithSyncID(syncID), WithWorkerCount(4), WithSkipEntitlementsAndGrants(true))
	require.NoError(t, err)
	s := created.(*syncer)
	s.caps.pageLedger = pendingRefillFailure{f.ledger}
	s.testHooks.ledgerHandler = func(ctx context.Context, action *Action, _ *ledgerPage) error {
		require.Equal(t, "root", action.ResourceID)
		return s.nextPageOrFinishAction(ctx, action, "", Action{Op: SyncResourcesOp, ResourceTypeID: "group", ResourceID: "child"})
	}
	require.ErrorIs(t, s.Sync(t.Context()), errLedgerInjectedPage)
	work, initialized, err := f.ledger.PendingWork(t.Context(), 0, 64)
	require.NoError(t, err)
	require.True(t, initialized)
	require.Len(t, work, 1)
	require.Equal(t, "child", work[0].Action.Identity.ResourceID)
	require.NoError(t, f.store.Close(t.Context()))
	f = openLedgerFixtureAt(t, f.path, false)
	created, err = NewSyncer(t.Context(), newMockConnector(), WithConnectorStore(f.store), WithSyncID(syncID), WithSkipEntitlementsAndGrants(true))
	require.NoError(t, err)
	resumed := created.(*syncer)
	calls := 0
	resumed.testHooks.ledgerHandler = func(ctx context.Context, action *Action, _ *ledgerPage) error {
		calls++
		require.Equal(t, "child", action.ResourceID)
		return resumed.nextPageOrFinishAction(ctx, action, "")
	}
	require.NoError(t, resumed.Sync(t.Context()))
	require.Equal(t, 1, calls)
}
