package sync //nolint:revive,nolintlint // backwards-compatible package name

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/conductorone/baton-sdk/pkg/retry"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func newEmptySchedulerState(t *testing.T) *state {
	t.Helper()
	st := newState()
	require.NoError(t, st.Unmarshal(""))
	st.FinishAction(t.Context(), st.Current())
	return st
}

func newTestRetryer(ctx context.Context) *retry.Retryer {
	return retry.NewRetryer(ctx, retry.RetryConfig{
		MaxAttempts:  1,
		InitialDelay: time.Millisecond,
		MaxDelay:     time.Millisecond,
	})
}

func TestTooManyWarningsThreshold(t *testing.T) {
	require.False(t, tooManyWarnings(10, 1), "requires more than ten warnings")
	require.False(t, tooManyWarnings(11, 0), "requires completed actions")
	require.False(t, tooManyWarnings(11, 110), "exactly ten percent is allowed")
	require.True(t, tooManyWarnings(11, 109), "more than ten percent must stop the sync")
}

func TestCollectionProgressAccounting(t *testing.T) {
	tests := []struct {
		name          string
		action        *Action
		itemCount     int
		hasNextPage   bool
		wantIncrement int
		wantCountOnly bool
	}{
		{
			name:          "per-resource origin counts once on final page",
			action:        &Action{},
			itemCount:     25,
			wantIncrement: 1,
		},
		{
			name:        "per-resource origin does not count intermediate page",
			action:      &Action{},
			itemCount:   25,
			hasNextPage: true,
		},
		{
			name:      "spawned cursor does not count resource",
			action:    &Action{Spawned: true},
			itemCount: 25,
		},
		{
			name:          "type-scoped cursor counts collected rows",
			action:        &Action{TypeScoped: true},
			itemCount:     25,
			hasNextPage:   true,
			wantIncrement: 25,
			wantCountOnly: true,
		},
		{
			name:          "spawned type-scoped cursor still counts collected rows",
			action:        &Action{Spawned: true, TypeScoped: true},
			itemCount:     10,
			wantIncrement: 10,
			wantCountOnly: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			increment, countOnly := collectionProgressIncrement(tt.action, tt.itemCount, tt.hasNextPage)
			require.Equal(t, tt.wantIncrement, increment)
			require.Equal(t, tt.wantCountOnly, countOnly)
		})
	}
}

func TestSyncParallelDrainsMultipleSpawnedCursors(t *testing.T) {
	ctx := t.Context()
	st := newEmptySchedulerState(t)
	origin := st.pushAction(ctx, Action{
		Op:             SyncGrantsOp,
		ResourceTypeID: "group",
		ResourceID:     "group-1",
	})
	s := &syncer{state: st, workerCount: 3}

	var mu sync.Mutex
	processed := make(map[string]int)
	f := func(ctx context.Context, action *Action) error {
		mu.Lock()
		processed[action.PageToken]++
		mu.Unlock()
		if action.PageToken == "" {
			return s.nextPageOrFinishAction(ctx, action, "",
				Action{Op: SyncGrantsOp, ResourceTypeID: "group", ResourceID: "group-1", PageToken: "a", Spawned: true},
				Action{Op: SyncGrantsOp, ResourceTypeID: "group", ResourceID: "group-1", PageToken: "b", Spawned: true},
				Action{Op: SyncGrantsOp, ResourceTypeID: "group", ResourceID: "group-1", PageToken: "c", Spawned: true},
			)
		}
		s.state.FinishAction(ctx, action)
		return nil
	}

	warnings, err := s.syncParallel(ctx, newTestRetryer(ctx), []*Action{origin}, f)
	require.NoError(t, err)
	require.Empty(t, warnings)
	require.Equal(t, map[string]int{"": 1, "a": 1, "b": 1, "c": 1}, processed)
	require.Nil(t, st.Current())
}

func TestSyncParallelErrorAbortsQueuedWorkAndCancelsPeer(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	st := newEmptySchedulerState(t)
	fail := st.pushAction(ctx, Action{Op: SyncGrantsOp, ResourceID: "fail"})
	slow := st.pushAction(ctx, Action{Op: SyncGrantsOp, ResourceID: "slow"})
	queued := st.pushAction(ctx, Action{Op: SyncGrantsOp, ResourceID: "queued"})
	s := &syncer{state: st, workerCount: 2}

	slowStarted := make(chan struct{})
	var queuedRan bool
	var mu sync.Mutex
	f := func(ctx context.Context, action *Action) error {
		switch action.ResourceID {
		case "fail":
			select {
			case <-slowStarted:
				return errors.New("permanent failure")
			case <-ctx.Done():
				return context.Cause(ctx)
			}
		case "slow":
			close(slowStarted)
			<-ctx.Done()
			return context.Cause(ctx)
		case "queued":
			mu.Lock()
			queuedRan = true
			mu.Unlock()
			return nil
		default:
			return fmt.Errorf("unexpected action %q", action.ResourceID)
		}
	}

	warnings, err := s.syncParallel(ctx, newTestRetryer(ctx), []*Action{fail, slow, queued}, f)
	require.ErrorContains(t, err, "permanent failure")
	require.Empty(t, warnings)
	mu.Lock()
	require.False(t, queuedRan)
	mu.Unlock()
	require.NotNil(t, st.GetAction(queued.ID))
}

func TestSyncParallelAggregatesWarningAndContinues(t *testing.T) {
	ctx := t.Context()
	st := newEmptySchedulerState(t)
	warningAction := st.pushAction(ctx, Action{Op: SyncGrantsOp, ResourceID: "missing"})
	successAction := st.pushAction(ctx, Action{Op: SyncGrantsOp, ResourceID: "present"})
	s := &syncer{state: st, workerCount: 2}

	f := func(ctx context.Context, action *Action) error {
		if action.ResourceID == "missing" {
			return status.Error(codes.NotFound, "resource disappeared")
		}
		s.state.FinishAction(ctx, action)
		return nil
	}

	warnings, err := s.syncParallel(ctx, newTestRetryer(ctx), []*Action{warningAction, successAction}, f)
	require.NoError(t, err)
	require.Len(t, warnings, 1)
	require.Equal(t, codes.NotFound, status.Code(warnings[0]))
	require.Nil(t, st.Current())
}

func TestSyncParallelRetriesActionWithinWorker(t *testing.T) {
	ctx := t.Context()
	st := newEmptySchedulerState(t)
	action := st.pushAction(ctx, Action{Op: SyncGrantsOp, ResourceID: "group-1"})
	s := &syncer{state: st, workerCount: 1}

	calls := 0
	f := func(ctx context.Context, action *Action) error {
		calls++
		if calls == 1 {
			return status.Error(codes.Unavailable, "transient")
		}
		s.state.FinishAction(ctx, action)
		return nil
	}

	warnings, err := s.syncParallel(ctx, newTestRetryer(ctx), []*Action{action}, f)
	require.NoError(t, err)
	require.Empty(t, warnings)
	require.Equal(t, 2, calls)
	require.Nil(t, st.Current())
}

func TestSyncParallelFiltersSpawnedActionsFromOtherOperations(t *testing.T) {
	ctx := t.Context()
	st := newEmptySchedulerState(t)
	origin := st.pushAction(ctx, Action{Op: SyncGrantsOp, ResourceID: "group-1"})
	s := &syncer{state: st, workerCount: 2}

	calls := 0
	f := func(ctx context.Context, action *Action) error {
		calls++
		return s.nextPageOrFinishAction(ctx, action, "", Action{
			Op:         SyncEntitlementsOp,
			ResourceID: "group-1",
		})
	}

	warnings, err := s.syncParallel(ctx, newTestRetryer(ctx), []*Action{origin}, f)
	require.NoError(t, err)
	require.Empty(t, warnings)
	require.Equal(t, 1, calls)
	require.NotNil(t, st.Current())
	require.Equal(t, SyncEntitlementsOp, st.Current().Op)
}

func TestSyncParallelEmptyBatchWithIdleWorkers(t *testing.T) {
	ctx := t.Context()
	st := newEmptySchedulerState(t)
	s := &syncer{state: st, workerCount: 8}

	warnings, err := s.syncParallel(ctx, newTestRetryer(ctx), nil, func(context.Context, *Action) error {
		panic("empty batch invoked worker function")
	})
	require.NoError(t, err)
	require.Empty(t, warnings)
}

func TestSpawnedCursorsResumeAfterPartialCompletion(t *testing.T) {
	ctx := t.Context()
	st := newEmptySchedulerState(t)
	for _, token := range []string{"a", "b", "c"} {
		st.PushAction(ctx, Action{
			Op:             SyncGrantsOp,
			ResourceTypeID: "group",
			PageToken:      token,
			Spawned:        true,
			TypeScoped:     true,
		})
	}
	st.FinishAction(ctx, st.Current())

	token, err := st.Marshal()
	require.NoError(t, err)
	resumed := newState()
	require.NoError(t, resumed.Unmarshal(token))
	s := &syncer{state: resumed, workerCount: 2}

	var mu sync.Mutex
	var processed []string
	f := func(ctx context.Context, action *Action) error {
		mu.Lock()
		processed = append(processed, action.PageToken)
		mu.Unlock()
		s.state.FinishAction(ctx, action)
		return nil
	}

	warnings, err := s.syncParallel(ctx, newTestRetryer(ctx), resumed.PeekMatchingActions(ctx, SyncGrantsOp), f)
	require.NoError(t, err)
	require.Empty(t, warnings)
	require.ElementsMatch(t, []string{"a", "b"}, processed)
	require.Nil(t, resumed.Current())
}
