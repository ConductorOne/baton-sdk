package sync //nolint:revive,nolintlint // backwards-compatible package name

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/retry"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type legacyPaginatedCheckpointStore struct {
	c1zstore.Store
	resourceType     *v2.ResourceType
	nextPageToken    string
	listResourcesErr error
}

func (s *legacyPaginatedCheckpointStore) ListResourceTypes(
	context.Context,
	*v2.ResourceTypesServiceListResourceTypesRequest,
) (*v2.ResourceTypesServiceListResourceTypesResponse, error) {
	return v2.ResourceTypesServiceListResourceTypesResponse_builder{
		List: []*v2.ResourceType{s.resourceType},
	}.Build(), nil
}

func (s *legacyPaginatedCheckpointStore) ListResources(
	context.Context,
	*v2.ResourcesServiceListResourcesRequest,
) (*v2.ResourcesServiceListResourcesResponse, error) {
	if s.listResourcesErr != nil {
		return nil, s.listResourcesErr
	}
	return v2.ResourcesServiceListResourcesResponse_builder{
		NextPageToken: s.nextPageToken,
	}.Build(), nil
}

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

func TestSyncParallelRejectsCyclicSpawnedCursor(t *testing.T) {
	ctx := t.Context()
	st := newEmptySchedulerState(t)
	origin := st.pushAction(ctx, Action{
		Op:             SyncGrantsOp,
		ResourceTypeID: "group",
		ResourceID:     "group-1",
		PageToken:      "loop",
	})
	s := &syncer{state: st, workerCount: 1}

	calls := 0
	f := func(ctx context.Context, action *Action) error {
		calls++
		if calls > 3 {
			return errors.New("test safety stop: cyclic cursor was accepted")
		}
		return s.nextPageOrFinishAction(ctx, action, "", Action{
			Op:             SyncGrantsOp,
			ResourceTypeID: action.ResourceTypeID,
			ResourceID:     action.ResourceID,
			PageToken:      "loop",
			Spawned:        true,
		})
	}

	_, err := s.syncParallel(ctx, newTestRetryer(ctx), []*Action{origin}, f)
	require.ErrorContains(t, err, "duplicate or cyclic spawned cursor")
	require.LessOrEqual(t, calls, 2)
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

func TestFailedSiblingAdmissionDoesNotAdvanceParentCursor(t *testing.T) {
	ctx := t.Context()
	st := newEmptySchedulerState(t)
	origin := st.pushAction(ctx, Action{
		Op:             SyncGrantsOp,
		ResourceTypeID: "group",
		ResourceID:     "group-1",
		PageToken:      "origin",
	})
	s := &syncer{state: st, workerCount: 1}

	f := func(ctx context.Context, action *Action) error {
		return s.nextPageOrFinishAction(
			ctx,
			action,
			"next",
			Action{
				Op:             SyncGrantsOp,
				ResourceTypeID: action.ResourceTypeID,
				ResourceID:     action.ResourceID,
				PageToken:      "unique-child",
				Spawned:        true,
			},
			Action{
				Op:             SyncGrantsOp,
				ResourceTypeID: action.ResourceTypeID,
				ResourceID:     action.ResourceID,
				PageToken:      "origin",
				Spawned:        true,
			},
		)
	}

	_, err := s.syncParallel(ctx, newTestRetryer(ctx), []*Action{origin}, f)
	require.ErrorContains(t, err, "duplicate or cyclic spawned cursor")
	persisted := st.GetAction(origin.ID)
	require.NotNil(t, persisted)
	require.Equal(t, "origin", persisted.PageToken)
	require.Equal(t, []string{origin.ID}, st.actionOrder)
	require.Len(t, st.PeekMatchingActions(ctx, SyncGrantsOp), 1)
}

func TestSpawnedCursorCannotCollideWithParentContinuation(t *testing.T) {
	ctx := t.Context()
	st := newEmptySchedulerState(t)
	origin := st.pushAction(ctx, Action{
		Op:             SyncGrantsOp,
		ResourceTypeID: "group",
		ResourceID:     "group-1",
		PageToken:      "current",
	})
	s := &syncer{state: st, workerCount: 1}

	calls := 0
	f := func(ctx context.Context, action *Action) error {
		calls++
		if calls > 1 {
			return errors.New("test safety stop: parent/spawn collision was accepted")
		}
		return s.nextPageOrFinishAction(ctx, action, "same-next-token", Action{
			Op:             SyncGrantsOp,
			ResourceTypeID: action.ResourceTypeID,
			ResourceID:     action.ResourceID,
			PageToken:      "same-next-token",
			Spawned:        true,
		})
	}

	_, err := s.syncParallel(ctx, newTestRetryer(ctx), []*Action{origin}, f)
	require.ErrorContains(t, err, "duplicate or cyclic spawned cursor")
	require.Equal(t, 1, calls)
	require.Equal(t, "current", st.GetAction(origin.ID).PageToken)
	require.Equal(t, []string{origin.ID}, st.actionOrder)
}

func TestAbortedQueueRejectsInFlightTransitionWithoutStateMutation(t *testing.T) {
	queue := newParallelActionQueue(nil)
	queue.abort()
	committed := false

	err := queue.transition(SyncGrantsOp, nil, "", []Action{{
		Op:        SyncGrantsOp,
		PageToken: "child",
	}}, func() ([]*Action, error) {
		committed = true
		return []*Action{{Op: SyncGrantsOp, PageToken: "child"}}, nil
	})
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, committed)
	require.Zero(t, queue.outstanding)
	require.Empty(t, queue.actions)
}

func TestNextPageOrFinishActionStateTransitions(t *testing.T) {
	child := Action{
		Op:             SyncGrantsOp,
		ResourceTypeID: "group",
		ResourceID:     "group-1",
		PageToken:      "child",
		Spawned:        true,
	}
	tests := []struct {
		name             string
		nextPageToken    string
		children         []Action
		wantParent       bool
		wantParentToken  string
		wantCurrentToken string
		wantActions      int
	}{
		{
			name:            "continuation keeps parent",
			nextPageToken:   "next",
			wantParent:      true,
			wantParentToken: "next",
			wantActions:     1,
		},
		{
			name:             "continuation commits child and keeps parent",
			nextPageToken:    "next",
			children:         []Action{child},
			wantParent:       true,
			wantParentToken:  "next",
			wantCurrentToken: "child",
			wantActions:      2,
		},
		{
			name:        "final page removes parent",
			wantActions: 0,
		},
		{
			name:             "final page commits child then removes parent",
			children:         []Action{child},
			wantCurrentToken: "child",
			wantActions:      1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			st := newEmptySchedulerState(t)
			parent := st.pushAction(ctx, Action{
				Op:             SyncGrantsOp,
				ResourceTypeID: "group",
				ResourceID:     "group-1",
				PageToken:      "current",
			})
			s := &syncer{state: st}

			require.NoError(t, s.nextPageOrFinishAction(ctx, parent, tt.nextPageToken, tt.children...))
			require.Len(t, st.actions, tt.wantActions)
			persistedParent := st.GetAction(parent.ID)
			if tt.wantParent {
				require.NotNil(t, persistedParent)
				require.Equal(t, tt.wantParentToken, persistedParent.PageToken)
			} else {
				require.Nil(t, persistedParent)
			}
			if tt.wantCurrentToken == "" {
				if tt.wantActions == 0 {
					require.Nil(t, st.Current())
				} else {
					require.Equal(t, parent.ID, st.Current().ID)
				}
			} else {
				require.NotNil(t, st.Current())
				require.Equal(t, tt.wantCurrentToken, st.Current().PageToken)
				require.NotEqual(t, parent.ID, st.Current().ID)
			}
		})
	}
}

func TestTransitionActionValidationFailureIsAtomic(t *testing.T) {
	ctx := t.Context()
	st := newEmptySchedulerState(t)
	parent := st.pushAction(ctx, Action{
		Op:         SyncGrantsOp,
		ResourceID: "group-1",
		PageToken:  "current",
	})
	beforeOrder := append([]string(nil), st.actionOrder...)
	beforeCompleted := st.GetCompletedActionsCount()

	_, err := st.transitionAction(ctx, parent, "next", []Action{
		{Op: SyncGrantsOp, ResourceID: "group-1", PageToken: "valid"},
		{ID: "already-assigned", Op: SyncGrantsOp, ResourceID: "group-1", PageToken: "invalid"},
	})
	require.ErrorContains(t, err, "action ID must be empty")
	require.Equal(t, beforeOrder, st.actionOrder)
	require.Equal(t, beforeCompleted, st.GetCompletedActionsCount())
	require.Len(t, st.actions, 1)
	persisted := st.GetAction(parent.ID)
	require.NotNil(t, persisted)
	require.Equal(t, "current", persisted.PageToken)
}

func TestLegacyPaginatedCheckpointPlansTypeScopedCollection(t *testing.T) {
	tests := []struct {
		name       string
		op         ActionOp
		annotation *v2.ResourceType
		sync       func(*syncer, context.Context, *Action) error
	}{
		{
			name: "entitlements",
			op:   SyncEntitlementsOp,
			annotation: v2.ResourceType_builder{
				Id:          "group",
				DisplayName: "Group",
				Annotations: annotations.New(&v2.TypeScopedEntitlements{}),
			}.Build(),
			sync: (*syncer).SyncEntitlements,
		},
		{
			name: "grants",
			op:   SyncGrantsOp,
			annotation: v2.ResourceType_builder{
				Id:          "group",
				DisplayName: "Group",
				Annotations: annotations.New(&v2.TypeScopedGrants{}),
			}.Build(),
			sync: (*syncer).SyncGrants,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			st := newEmptySchedulerState(t)
			root := st.pushAction(ctx, Action{Op: tt.op, PageToken: "legacy-page-2"})
			s := &syncer{
				state: st,
				store: &legacyPaginatedCheckpointStore{resourceType: tt.annotation},
			}

			require.NoError(t, tt.sync(s, ctx, root))
			planned := st.Current()
			require.NotNil(t, planned)
			require.Equal(t, tt.op, planned.Op)
			require.Equal(t, "group", planned.ResourceTypeID)
			require.True(t, planned.TypeScoped)
		})
	}
}

func TestTypeScopedPlanningFailureDoesNotCommitMarker(t *testing.T) {
	ctx := t.Context()
	st := newEmptySchedulerState(t)
	root := st.pushAction(ctx, Action{Op: SyncGrantsOp, PageToken: "legacy-page-2"})
	store := &legacyPaginatedCheckpointStore{
		resourceType: v2.ResourceType_builder{
			Id:          "group",
			DisplayName: "Group",
			Annotations: annotations.New(&v2.TypeScopedGrants{}),
		}.Build(),
		listResourcesErr: errors.New("injected list-resources failure"),
	}
	s := &syncer{state: st, store: store}

	require.ErrorContains(t, s.SyncGrants(ctx, root), "injected list-resources failure")
	persisted := st.GetAction(root.ID)
	require.NotNil(t, persisted)
	require.False(t, persisted.TypeScopedPlanned)

	store.listResourcesErr = nil
	require.NoError(t, s.SyncGrants(ctx, persisted))
	planned := st.Current()
	require.NotNil(t, planned)
	require.True(t, planned.TypeScoped)
	require.Equal(t, "group", planned.ResourceTypeID)
}

func TestTypeScopedPlanningMarkerSurvivesCheckpoint(t *testing.T) {
	ctx := t.Context()
	st := newEmptySchedulerState(t)
	root := st.pushAction(ctx, Action{Op: SyncGrantsOp, PageToken: "legacy-page-2"})
	store := &legacyPaginatedCheckpointStore{
		resourceType: v2.ResourceType_builder{
			Id:          "group",
			DisplayName: "Group",
			Annotations: annotations.New(&v2.TypeScopedGrants{}),
		}.Build(),
		nextPageToken: "page-3",
	}
	s := &syncer{state: st, store: store}

	require.NoError(t, s.SyncGrants(ctx, root))
	planned := st.Current()
	require.NotNil(t, planned)
	require.True(t, planned.TypeScoped)
	st.FinishAction(ctx, planned)
	resumedRoot := st.Current()
	require.NotNil(t, resumedRoot)
	require.True(t, resumedRoot.TypeScopedPlanned)
	require.Equal(t, "page-3", resumedRoot.PageToken)

	token, err := st.Marshal()
	require.NoError(t, err)
	resumed := newState()
	require.NoError(t, resumed.Unmarshal(token))
	require.True(t, resumed.Current().TypeScopedPlanned)
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
