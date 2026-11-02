package sync //nolint:revive,nolintlint // backwards-compatible package name

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/conductorone/baton-sdk/pkg/retry"
	"github.com/stretchr/testify/require"
)

func newEmptySchedulerState(t *testing.T) *runState {
	t.Helper()
	st := newRunState()
	st.seedInitAction()
	st.finishAction(t.Context(), st.current())
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
	require.False(t, tooManyWarnings(10, 1, 0.1), "requires more than ten warnings")
	require.False(t, tooManyWarnings(11, 0, 0.1), "requires completed actions")
	require.False(t, tooManyWarnings(11, 110, 0.1), "exactly ten percent is allowed")
	require.True(t, tooManyWarnings(11, 109, 0.1), "more than ten percent must stop the sync")

	require.False(t, tooManyWarnings(11, 220, 0.05), "exactly five percent is allowed")
	require.True(t, tooManyWarnings(11, 219, 0.05), "more than five percent must stop the sync")
	require.False(t, tooManyWarnings(11, 0, 0.05), "empty list-resource counts must not trip the five percent check")
}

func TestTooManyListResourceWarnings(t *testing.T) {
	bad := ActionCount{CompletedCount: 20, WarningCount: 11}

	require.False(t, tooManyListResourceWarnings(bad, 0),
		"resumed counts must not stop a run that has not completed a list-resource action")
	require.False(t, tooManyListResourceWarnings(bad, 10),
		"ten completions this run are not enough to re-arm the durable ratio")
	require.True(t, tooManyListResourceWarnings(bad, 11),
		"more than ten completions this run re-arm the durable ratio")
	require.False(t, tooManyListResourceWarnings(ActionCount{CompletedCount: 220, WarningCount: 11}, 11),
		"exactly five percent is allowed")
	require.False(t, tooManyListResourceWarnings(ActionCount{CompletedCount: 20, WarningCount: 10}, 11),
		"requires more than ten warnings")
	require.False(t, tooManyListResourceWarnings(ActionCount{}, 11),
		"a run with no list-resource warnings never trips")
}

func TestRecordListResourceCompletedThisRun(t *testing.T) {
	s := &syncer{}
	s.recordListResourceCompletedThisRun(&Action{Op: SyncGrantsOp})
	require.Equal(t, uint64(0), s.listResourceActionsCompletedThisRun.Load())
	s.recordListResourceCompletedThisRun(&Action{Op: SyncResourcesOp})
	require.Equal(t, uint64(1), s.listResourceActionsCompletedThisRun.Load())
	s.recordListResourceCompletedThisRun(nil)
	require.Equal(t, uint64(1), s.listResourceActionsCompletedThisRun.Load())
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

func TestParallelActionKeyDoesNotRetainCursorStrings(t *testing.T) {
	key := makeParallelActionKey(&Action{
		Op:             SyncGrantsOp,
		ResourceTypeID: "group",
		ResourceID:     "group-1",
		PageToken:      string(make([]byte, maxEnqueuedPageTokenBytes)),
	})
	require.LessOrEqual(t, reflect.TypeOf(key).Size(), uintptr(32))
}

func TestAbortedQueueRejectsInFlightTransitionWithoutStateMutation(t *testing.T) {
	queue := newParallelActionQueue(nil)
	queue.abort()
	committed := false

	err := queue.transition(t.Context(), SyncGrantsOp, nil, "", []Action{{
		Op:        SyncGrantsOp,
		PageToken: "child",
	}}, func(string, []Action) ([]*Action, error) {
		committed = true
		return []*Action{{Op: SyncGrantsOp, PageToken: "child"}}, nil
	})
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, committed)
	require.Zero(t, queue.outstanding)
	require.Empty(t, queue.actions)
}

// TestParallelActionQueueRejectsCursorLimitBeforeCommit was deleted with the
// batch-lifetime cursor cap it pinned (RFC 0007 phase 1,
// docs/rfcs/0007-scheduler-cursor-accounting.md): the cap bounded CUMULATIVE
// unique cursors per batch, not in-flight width, so legitimate large fan-outs
// failed deterministically mid-sync. Phase 2's bound on OUTSTANDING actions
// is a different property and gets its own test.

func TestTransitionActionValidationFailureIsAtomic(t *testing.T) {
	ctx := t.Context()
	st := newEmptySchedulerState(t)
	parent := st.pushAction(ctx, Action{
		Op:         SyncGrantsOp,
		ResourceID: "group-1",
		PageToken:  "current",
	})
	beforeOrder := append([]string(nil), st.actionOrder...)
	beforeCompleted := st.completedActionsCount()

	_, err := st.transitionAction(ctx, parent, "next", []Action{
		{Op: SyncGrantsOp, ResourceID: "group-1", PageToken: "valid"},
		{ID: "already-assigned", Op: SyncGrantsOp, ResourceID: "group-1", PageToken: "invalid"},
	})
	require.ErrorContains(t, err, "action ID must be empty")
	require.Equal(t, beforeOrder, st.actionOrder)
	require.Equal(t, beforeCompleted, st.completedActionsCount())
	require.Len(t, st.actions, 1)
	persisted := st.getAction(parent.ID)
	require.NotNil(t, persisted)
	require.Equal(t, "current", persisted.PageToken)
}

func TestCheckpointVersionStampsTypeScopedTokens(t *testing.T) {
	ctx := t.Context()

	plain := newEmptySchedulerState(t)
	plain.pushAction(ctx, Action{Op: SyncGrantsOp, ResourceTypeID: "group", ResourceID: "g1"})
	plainToken := encodeTestRun(t, plain, newRunStats())
	require.Contains(t, plainToken, `"version":1`)

	scoped := newEmptySchedulerState(t)
	scoped.pushAction(ctx, Action{Op: SyncGrantsOp, ResourceTypeID: "group", TypeScoped: true})
	scopedToken := encodeTestRun(t, scoped, newRunStats())
	require.Contains(t, scopedToken, `"version":2`)

	spawned := newEmptySchedulerState(t)
	spawned.pushAction(ctx, Action{Op: SyncGrantsOp, ResourceTypeID: "group", ResourceID: "g1", PageToken: "p", Spawned: true})
	spawnedToken := encodeTestRun(t, spawned, newRunStats())
	require.Contains(t, spawnedToken, `"version":2`)

	planned := newEmptySchedulerState(t)
	planned.pushAction(ctx, Action{Op: SyncGrantsOp, TypeScopedPlanned: true})
	plannedToken := encodeTestRun(t, planned, newRunStats())
	require.Contains(t, plannedToken, `"version":2`)

	// This SDK accepts both versions losslessly.
	resumedScoped, _, _ := decodeTestRun(t, scopedToken)
	require.True(t, resumedScoped.current().TypeScoped)
	resumedPlain, _, _ := decodeTestRun(t, plainToken)
	require.NotNil(t, resumedPlain.current())

	// An older SDK rejects version 2 and reparses via the V0 format, which
	// carries no actions_map — the state comes back empty and the old
	// syncer restarts collection from Init.
	v0Fallback, err := unmarshalTokenV0(scopedToken)
	require.NoError(t, err)
	require.Empty(t, v0Fallback.ActionsMap)
}
