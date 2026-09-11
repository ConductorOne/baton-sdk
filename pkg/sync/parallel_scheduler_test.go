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

// SyncMeta reports "no sync-metadata sub-store". syncer.setStore resolves the
// store's optional capabilities once at attach (store_caps.go), which asks
// every store for its SyncMeta; a double with a nil embedded interface has to
// answer rather than panic. nil means the verification capability is absent,
// which is what this double intends.
func (s *legacyPaginatedCheckpointStore) SyncMeta() c1zstore.SyncMeta { return nil }

// Grants reports "no grant sub-store", for the same reason as SyncMeta above:
// capability resolution at attach asks for it.
func (s *legacyPaginatedCheckpointStore) Grants() c1zstore.GrantStore { return nil }

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

func TestSyncParallelDrainsMultipleSpawnedCursors(t *testing.T) {
	ctx := t.Context()
	st := newEmptySchedulerState(t)
	origin := st.pushAction(ctx, Action{
		Op:             SyncGrantsOp,
		ResourceTypeID: "group",
		ResourceID:     "group-1",
	})
	s := &syncer{run: st, stats: newRunStats(), graph: newExpansionGraph(), cfg: syncConfig{workerCount: 3}}

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
		s.run.finishAction(ctx, action)
		return nil
	}

	warnings, err := s.syncParallel(ctx, newTestRetryer(ctx), []*Action{origin}, f)
	require.NoError(t, err)
	require.Empty(t, warnings)
	require.Equal(t, map[string]int{"": 1, "a": 1, "b": 1, "c": 1}, processed)
	require.Nil(t, st.current())
}

func TestSyncParallelBreaksCyclicSpawnedCursorIdempotently(t *testing.T) {
	t.Skip("pinned the queue's batch-lifetime seen-set cycle break, removed by RFC 0007 phase 1 " +
		"(docs/rfcs/0007-scheduler-cursor-accounting.md): the queue keeps no identity history; " +
		"spawn-cycle termination is owned by runState.transitionAction's spawnedAdmitted guard " +
		"(one extra idempotent re-run of the first re-mention), and queue-level detection " +
		"returns with phase 2's working set")
	// A cursor that re-mentions its own identity (the tightest cycle) is
	// skipped, not fatal: the identical work is scheduled exactly once,
	// so failing would wedge the sync deterministically — while skipping
	// terminates the cycle with the batch fully drained.
	ctx := t.Context()
	st := newEmptySchedulerState(t)
	origin := st.pushAction(ctx, Action{
		Op:             SyncGrantsOp,
		ResourceTypeID: "group",
		ResourceID:     "group-1",
		PageToken:      "loop",
	})
	s := &syncer{run: st, stats: newRunStats(), graph: newExpansionGraph(), cfg: syncConfig{workerCount: 1}}

	calls := 0
	f := func(ctx context.Context, action *Action) error {
		calls++
		if calls > 3 {
			return errors.New("test safety stop: cyclic cursor spawned unbounded copies")
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
	require.NoError(t, err)
	require.Equal(t, 1, calls, "the cycle must be broken at admission, not by re-running the cursor")
	require.Nil(t, st.current(), "the batch must drain completely")
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
	// A WITHIN-CALL duplicate (the same cursor twice in one response) is
	// a genuine connector protocol violation and stays fatal — and the
	// rejection must be atomic: the parent's cursor must not advance and
	// nothing may be admitted.
	ctx := t.Context()
	st := newEmptySchedulerState(t)
	origin := st.pushAction(ctx, Action{
		Op:             SyncGrantsOp,
		ResourceTypeID: "group",
		ResourceID:     "group-1",
		PageToken:      "origin",
	})
	s := &syncer{run: st, stats: newRunStats(), graph: newExpansionGraph(), cfg: syncConfig{workerCount: 1}}

	calls := 0
	f := func(ctx context.Context, action *Action) error {
		calls++
		if calls > 1 {
			return errors.New("test safety stop: duplicate within one response was accepted")
		}
		return s.nextPageOrFinishAction(
			ctx,
			action,
			"next",
			Action{
				Op:             SyncGrantsOp,
				ResourceTypeID: action.ResourceTypeID,
				ResourceID:     action.ResourceID,
				PageToken:      "dup-child",
				Spawned:        true,
			},
			Action{
				Op:             SyncGrantsOp,
				ResourceTypeID: action.ResourceTypeID,
				ResourceID:     action.ResourceID,
				PageToken:      "dup-child",
				Spawned:        true,
			},
		)
	}

	_, err := s.syncParallel(ctx, newTestRetryer(ctx), []*Action{origin}, f)
	require.ErrorContains(t, err, "duplicate or cyclic spawned cursor")
	persisted := st.getAction(origin.ID)
	require.NotNil(t, persisted)
	require.Equal(t, "origin", persisted.PageToken)
	require.Equal(t, []string{origin.ID}, st.actionOrder)
	require.Len(t, st.peekMatchingActions(ctx, SyncGrantsOp), 1)
}

func TestContinuationReconvergenceFinishesParent(t *testing.T) {
	t.Skip("pinned seen-set re-convergence (finish a parent whose continuation lands on an " +
		"already-scheduled identity), removed by RFC 0007 phase 1 " +
		"(docs/rfcs/0007-scheduler-cursor-accounting.md): with no identity history the " +
		"continuation proceeds and re-walks the pages idempotently; prompt re-convergence " +
		"returns with phase 2's working set")
	// A parent whose NEXT PAGE TOKEN re-converges on an identity already
	// scheduled in the batch (e.g. a resumed cursor persisted with that
	// exact token, re-mentioned because the API's answers shifted across
	// a crash) must FINISH instead of erroring: the action owning that
	// identity performs exactly the work the continuation would have.
	// Pre-fix this was fatal, and deterministically so on every retry —
	// a permanently wedged sync.
	ctx := t.Context()
	st := newEmptySchedulerState(t)
	walker := st.pushAction(ctx, Action{
		Op:             SyncGrantsOp,
		ResourceTypeID: "group",
		ResourceID:     "group-1",
		PageToken:      "walker",
	})
	holder := st.pushAction(ctx, Action{
		Op:             SyncGrantsOp,
		ResourceTypeID: "group",
		ResourceID:     "group-1",
		PageToken:      "held-token",
		Spawned:        true,
	})
	s := &syncer{run: st, stats: newRunStats(), graph: newExpansionGraph(), cfg: syncConfig{workerCount: 1}}

	processed := map[string]int{}
	f := func(ctx context.Context, action *Action) error {
		processed[action.PageToken]++
		if action.PageToken == "walker" {
			// The connector's answer says "continue at held-token" —
			// which the sibling seed already owns.
			return s.nextPageOrFinishAction(ctx, action, "held-token")
		}
		s.run.finishAction(ctx, action)
		return nil
	}

	warnings, err := s.syncParallel(ctx, newTestRetryer(ctx), []*Action{walker, holder}, f)
	require.NoError(t, err)
	require.Empty(t, warnings)
	require.Equal(t, map[string]int{"walker": 1, "held-token": 1}, processed,
		"the held token must be walked exactly once, by its owner")
	require.Nil(t, st.getAction(walker.ID), "the re-converged parent must finish, not continue")
	require.Nil(t, st.current(), "the batch must drain completely")
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
	s := &syncer{run: st, stats: newRunStats(), graph: newExpansionGraph(), cfg: syncConfig{workerCount: 1}}

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
	require.Equal(t, "current", st.getAction(origin.ID).PageToken)
	require.Equal(t, []string{origin.ID}, st.actionOrder)
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
			s := &syncer{run: st, stats: newRunStats(), graph: newExpansionGraph()}

			require.NoError(t, s.nextPageOrFinishAction(ctx, parent, tt.nextPageToken, tt.children...))
			require.Len(t, st.actions, tt.wantActions)
			persistedParent := st.getAction(parent.ID)
			if tt.wantParent {
				require.NotNil(t, persistedParent)
				require.Equal(t, tt.wantParentToken, persistedParent.PageToken)
			} else {
				require.Nil(t, persistedParent)
			}
			if tt.wantCurrentToken == "" {
				if tt.wantActions == 0 {
					require.Nil(t, st.current())
				} else {
					require.Equal(t, parent.ID, st.current().ID)
				}
			} else {
				require.NotNil(t, st.current())
				require.Equal(t, tt.wantCurrentToken, st.current().PageToken)
				require.NotEqual(t, parent.ID, st.current().ID)
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
			s := &syncer{run: st, stats: newRunStats(), graph: newExpansionGraph()}
			s.setStore(&legacyPaginatedCheckpointStore{resourceType: tt.annotation})

			require.NoError(t, tt.sync(s, ctx, root))
			planned := st.current()
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
	s := &syncer{run: st, stats: newRunStats(), graph: newExpansionGraph()}
	s.setStore(store)

	require.ErrorContains(t, s.SyncGrants(ctx, root), "injected list-resources failure")
	persisted := st.getAction(root.ID)
	require.NotNil(t, persisted)
	require.False(t, persisted.TypeScopedPlanned)

	store.listResourcesErr = nil
	require.NoError(t, s.SyncGrants(ctx, persisted))
	planned := st.current()
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
	s := &syncer{run: st, stats: newRunStats(), graph: newExpansionGraph()}
	s.setStore(store)

	require.NoError(t, s.SyncGrants(ctx, root))
	planned := st.current()
	require.NotNil(t, planned)
	require.True(t, planned.TypeScoped)
	st.finishAction(ctx, planned)
	resumedRoot := st.current()
	require.NotNil(t, resumedRoot)
	require.True(t, resumedRoot.TypeScopedPlanned)
	require.Equal(t, "page-3", resumedRoot.PageToken)

	token := encodeTestRun(t, st, newRunStats())
	resumed, _, _ := decodeTestRun(t, token)
	require.True(t, resumed.current().TypeScopedPlanned)
}

func TestSyncParallelErrorAbortsQueuedWorkAndCancelsPeer(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	st := newEmptySchedulerState(t)
	fail := st.pushAction(ctx, Action{Op: SyncGrantsOp, ResourceID: "fail"})
	slow := st.pushAction(ctx, Action{Op: SyncGrantsOp, ResourceID: "slow"})
	queued := st.pushAction(ctx, Action{Op: SyncGrantsOp, ResourceID: "queued"})
	s := &syncer{run: st, stats: newRunStats(), graph: newExpansionGraph(), cfg: syncConfig{workerCount: 2}}

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
	require.NotNil(t, st.getAction(queued.ID))
}

func TestSyncParallelAggregatesWarningAndContinues(t *testing.T) {
	ctx := t.Context()
	st := newEmptySchedulerState(t)
	warningAction := st.pushAction(ctx, Action{Op: SyncGrantsOp, ResourceID: "missing"})
	successAction := st.pushAction(ctx, Action{Op: SyncGrantsOp, ResourceID: "present"})
	s := &syncer{run: st, stats: newRunStats(), graph: newExpansionGraph(), cfg: syncConfig{workerCount: 2}}

	f := func(ctx context.Context, action *Action) error {
		if action.ResourceID == "missing" {
			return status.Error(codes.NotFound, "resource disappeared")
		}
		s.run.finishAction(ctx, action)
		return nil
	}

	warnings, err := s.syncParallel(ctx, newTestRetryer(ctx), []*Action{warningAction, successAction}, f)
	require.NoError(t, err)
	require.Len(t, warnings, 1)
	require.Equal(t, codes.NotFound, status.Code(warnings[0]))
	require.Nil(t, st.current())
}

func TestSyncParallelRetriesActionWithinWorker(t *testing.T) {
	ctx := t.Context()
	st := newEmptySchedulerState(t)
	action := st.pushAction(ctx, Action{Op: SyncGrantsOp, ResourceID: "group-1"})
	s := &syncer{run: st, stats: newRunStats(), graph: newExpansionGraph(), cfg: syncConfig{workerCount: 1}}

	calls := 0
	f := func(ctx context.Context, action *Action) error {
		calls++
		if calls == 1 {
			return status.Error(codes.Unavailable, "transient")
		}
		s.run.finishAction(ctx, action)
		return nil
	}

	warnings, err := s.syncParallel(ctx, newTestRetryer(ctx), []*Action{action}, f)
	require.NoError(t, err)
	require.Empty(t, warnings)
	require.Equal(t, 2, calls)
	require.Nil(t, st.current())
}

func TestSyncParallelFiltersSpawnedActionsFromOtherOperations(t *testing.T) {
	ctx := t.Context()
	st := newEmptySchedulerState(t)
	origin := st.pushAction(ctx, Action{Op: SyncGrantsOp, ResourceID: "group-1"})
	s := &syncer{run: st, stats: newRunStats(), graph: newExpansionGraph(), cfg: syncConfig{workerCount: 2}}

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
	require.NotNil(t, st.current())
	require.Equal(t, SyncEntitlementsOp, st.current().Op)
}

func TestSyncParallelEmptyBatchWithIdleWorkers(t *testing.T) {
	ctx := t.Context()
	st := newEmptySchedulerState(t)
	s := &syncer{run: st, stats: newRunStats(), graph: newExpansionGraph(), cfg: syncConfig{workerCount: 8}}

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
		st.pushAction(ctx, Action{
			Op:             SyncGrantsOp,
			ResourceTypeID: "group",
			PageToken:      token,
			Spawned:        true,
			TypeScoped:     true,
		})
	}
	st.finishAction(ctx, st.current())

	token := encodeTestRun(t, st, newRunStats())
	resumed, _, _ := decodeTestRun(t, token)
	s := &syncer{run: resumed, stats: newRunStats(), graph: newExpansionGraph(), cfg: syncConfig{workerCount: 2}}

	var mu sync.Mutex
	var processed []string
	f := func(ctx context.Context, action *Action) error {
		mu.Lock()
		processed = append(processed, action.PageToken)
		mu.Unlock()
		s.run.finishAction(ctx, action)
		return nil
	}

	warnings, err := s.syncParallel(ctx, newTestRetryer(ctx), resumed.peekMatchingActions(ctx, SyncGrantsOp), f)
	require.NoError(t, err)
	require.Empty(t, warnings)
	require.ElementsMatch(t, []string{"a", "b"}, processed)
	require.Nil(t, resumed.current())
}

// Checkpoint tokens carrying type-scoped or spawned markers must be stamped
// with StateTokenVersionTypeScoped: an older SDK's parser silently drops the
// marker fields and dead-ends the cursors against store pagination, sealing
// the sync as complete with missing data. The version bump makes the old
// parser fall back to V0 (empty action state), restarting collection from
// Init instead — redone work, never silent loss. Plain tokens keep version 1
// so downgrades resume seamlessly.
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

func TestOriginContinuationAndSiblingsResumeExactlyOnce(t *testing.T) {
	ctx := t.Context()
	st := newEmptySchedulerState(t)
	origin := st.pushAction(ctx, Action{
		Op:             SyncGrantsOp,
		ResourceTypeID: "group",
		ResourceID:     "group-1",
		PageToken:      "origin-current",
	})
	_, err := st.transitionAction(ctx, origin, "origin-next", []Action{
		{Op: SyncGrantsOp, ResourceTypeID: "group", ResourceID: "group-1", PageToken: "sibling-a", Spawned: true},
		{Op: SyncGrantsOp, ResourceTypeID: "group", ResourceID: "group-1", PageToken: "sibling-b", Spawned: true},
	})
	require.NoError(t, err)
	st.finishAction(ctx, st.current())

	token := encodeTestRun(t, st, newRunStats())
	resumed, _, _ := decodeTestRun(t, token)
	s := &syncer{run: resumed, stats: newRunStats(), graph: newExpansionGraph(), cfg: syncConfig{workerCount: 2}}

	var mu sync.Mutex
	processed := make(map[string]int)
	f := func(ctx context.Context, action *Action) error {
		mu.Lock()
		processed[action.PageToken]++
		mu.Unlock()
		s.run.finishAction(ctx, action)
		return nil
	}

	warnings, err := s.syncParallel(ctx, newTestRetryer(ctx), resumed.peekMatchingActions(ctx, SyncGrantsOp), f)
	require.NoError(t, err)
	require.Empty(t, warnings)
	require.Equal(t, map[string]int{"origin-next": 1, "sibling-a": 1}, processed)
	require.Nil(t, resumed.current())
}
