package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSyncParallelFiltersSpawnedActionsFromOtherOperations(t *testing.T) {
	ctx := t.Context()
	s, f := newLedgerSchedulerFixture(t, 2)
	s.run.pushAction(ctx, Action{Op: SyncGrantsOp, ResourceID: "group-1"})
	seedLedgerTestRun(t, s, nil)
	batch := s.run.peekMatchingActions(ctx, SyncGrantsOp)
	require.Len(t, batch, 1)
	parent := ledgerIdentity(batch[0])

	var calls atomic.Int32
	warnings, err := s.syncParallel(ctx, newTestRetryer(ctx), batch, func(ctx context.Context, action *Action) error {
		calls.Add(1)
		return s.nextPageOrFinishAction(ctx, action, "", Action{Op: SyncEntitlementsOp, ResourceID: "group-1"})
	})
	require.NoError(t, err)
	require.Empty(t, warnings)
	require.EqualValues(t, 1, calls.Load())

	row, found, err := f.ledger.GetLedgerRow(ctx, parent)
	require.NoError(t, err)
	require.True(t, found)
	require.Empty(t, row.NextPageToken)
	require.Len(t, row.Children, 1)
	require.Equal(t, SyncEntitlementsOp.String(), row.Children[0].Identity.Op)
	require.Equal(t, "group-1", row.Children[0].Identity.ResourceID)
	require.NoError(t, s.refreshPendingWindow(ctx))
	require.NotNil(t, s.run.current())
	require.Equal(t, SyncEntitlementsOp, s.run.current().Op)
	counters, err := f.ledger.LedgerCounters(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 1, counters.Counters[ledgerCompletedPrefix+SyncGrantsOp.String()])
	require.Zero(t, counters.Counters[ledgerCompletedPrefix+SyncEntitlementsOp.String()])
}

func TestSyncParallelEmptyBatchWithIdleWorkers(t *testing.T) {
	ctx := t.Context()
	s, _ := newLedgerSchedulerFixture(t, 8)
	seedLedgerTestRun(t, s, nil)

	var calls atomic.Int32
	warnings, err := s.syncParallel(ctx, newTestRetryer(ctx), nil, func(context.Context, *Action) error {
		calls.Add(1)
		return errors.New("empty batch invoked worker function")
	})
	require.NoError(t, err)
	require.Empty(t, warnings)
	require.Zero(t, calls.Load())
}

func TestSyncParallelAggregatesWarningAndContinues(t *testing.T) {
	ctx := t.Context()
	s, f := newLedgerSchedulerFixture(t, 2)
	s.run.pushAction(ctx, Action{Op: SyncGrantsOp, ResourceID: "missing"})
	s.run.pushAction(ctx, Action{Op: SyncGrantsOp, ResourceID: "present"})
	seedLedgerTestRun(t, s, nil)
	batch := s.run.peekMatchingActions(ctx, SyncGrantsOp)
	require.Len(t, batch, 2)
	identities := make([]c1zstore.LedgerActionIdentity, 0, len(batch))
	for _, action := range batch {
		identities = append(identities, ledgerIdentity(action))
	}

	var calls atomic.Int32
	warnings, err := s.syncParallel(ctx, newTestRetryer(ctx), batch, func(ctx context.Context, action *Action) error {
		calls.Add(1)
		if action.ResourceID == "missing" {
			return status.Error(codes.NotFound, "resource disappeared")
		}
		return s.nextPageOrFinishAction(ctx, action, "")
	})
	require.NoError(t, err)
	require.Len(t, warnings, 1)
	require.Equal(t, codes.NotFound, status.Code(warnings[0]))
	require.EqualValues(t, 2, calls.Load())
	require.Nil(t, s.run.current())
	for _, id := range identities {
		_, found, err := f.ledger.GetLedgerRow(ctx, id)
		require.NoError(t, err)
		require.True(t, found)
	}
	counters, err := f.ledger.LedgerCounters(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 2, counters.Counters[ledgerCompletedActions])
	require.EqualValues(t, 1, counters.Counters[ledgerWarningsPrefix+SyncGrantsOp.String()])
}

func TestFailedSiblingAdmissionDoesNotAdvanceParentCursor(t *testing.T) {
	ctx := t.Context()
	s, f := newLedgerSchedulerFixture(t, 1)
	s.run.pushAction(ctx, Action{
		Op:             SyncGrantsOp,
		ResourceTypeID: "group",
		ResourceID:     "group-1",
		PageToken:      "origin",
	})
	seedLedgerTestRun(t, s, nil)
	batch := s.run.peekMatchingActions(ctx, SyncGrantsOp)
	require.Len(t, batch, 1)
	origin := ledgerIdentity(batch[0])

	var calls atomic.Int32
	_, err := s.syncParallel(ctx, newTestRetryer(ctx), batch, func(ctx context.Context, action *Action) error {
		if calls.Add(1) > 1 {
			return errors.New("test safety stop: duplicate within one response was accepted")
		}
		child := Action{
			Op:             SyncGrantsOp,
			ResourceTypeID: action.ResourceTypeID,
			ResourceID:     action.ResourceID,
			PageToken:      "dup-child",
			Spawned:        true,
		}
		return s.nextPageOrFinishAction(ctx, action, "next", child, child)
	})
	require.ErrorContains(t, err, "duplicate or cyclic spawned cursor")
	require.EqualValues(t, 1, calls.Load())
	persisted := s.run.current()
	require.NotNil(t, persisted)
	require.Equal(t, "origin", persisted.PageToken)
	require.Len(t, s.run.peekMatchingActions(ctx, SyncGrantsOp), 1)
	_, found, err := f.ledger.GetLedgerRow(ctx, origin)
	require.NoError(t, err)
	require.False(t, found)
}

func TestContinuationReconvergenceFinishesParent(t *testing.T) {
	ctx := t.Context()
	s, f := newLedgerSchedulerFixture(t, 1)
	s.run.pushAction(ctx, Action{
		Op:             SyncGrantsOp,
		ResourceTypeID: "group",
		ResourceID:     "group-1",
		PageToken:      "walker",
	})
	s.run.pushAction(ctx, Action{
		Op:             SyncGrantsOp,
		ResourceTypeID: "group",
		ResourceID:     "group-1",
		PageToken:      "held-token",
		Spawned:        true,
	})
	seedLedgerTestRun(t, s, nil)
	batch := s.run.peekMatchingActions(ctx, SyncGrantsOp)
	require.Len(t, batch, 2)
	var walkerID string
	var walkerRow c1zstore.LedgerActionIdentity
	for _, action := range batch {
		if action.PageToken == "walker" {
			walkerID = action.ID
			walkerRow = ledgerIdentity(action)
		}
	}
	require.NotEmpty(t, walkerID)

	processed := map[string]int{}
	warnings, err := s.syncParallel(ctx, newTestRetryer(ctx), batch, func(ctx context.Context, action *Action) error {
		processed[action.PageToken]++
		if action.PageToken == "walker" {
			return s.nextPageOrFinishAction(ctx, action, "held-token")
		}
		return s.nextPageOrFinishAction(ctx, action, "")
	})
	require.NoError(t, err)
	require.Empty(t, warnings)
	require.Equal(t, map[string]int{"walker": 1, "held-token": 2}, processed)
	require.Nil(t, s.run.getAction(walkerID))
	require.Nil(t, s.run.current())
	row, found, err := f.ledger.GetLedgerRow(ctx, walkerRow)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "held-token", row.NextPageToken)
	counters, err := f.ledger.LedgerCounters(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 2, counters.Counters[ledgerCompletedPrefix+SyncGrantsOp.String()])
}
