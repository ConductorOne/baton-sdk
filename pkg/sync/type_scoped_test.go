package sync //nolint:revive,nolintlint // backwards-compatible package name

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/anypb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/retry"
	et "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	gt "github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
)

type targetedTypeScopedConnector struct {
	*mockConnector
	entitlementCalls int
	grantCalls       int
}

type emptyIDConnector struct {
	*mockConnector
	entitlementCalls int
	grantCalls       int
	sawTypeMarker    bool
}

func (c *emptyIDConnector) ListEntitlements(
	_ context.Context,
	in *v2.EntitlementsServiceListEntitlementsRequest,
	_ ...grpc.CallOption,
) (*v2.EntitlementsServiceListEntitlementsResponse, error) {
	c.entitlementCalls++
	reqAnnos := annotations.Annotations(in.GetAnnotations())
	c.sawTypeMarker = c.sawTypeMarker || reqAnnos.Contains(&v2.TypeScopedEntitlements{})
	return v2.EntitlementsServiceListEntitlementsResponse_builder{}.Build(), nil
}

func (c *emptyIDConnector) ListGrants(
	_ context.Context,
	in *v2.GrantsServiceListGrantsRequest,
	_ ...grpc.CallOption,
) (*v2.GrantsServiceListGrantsResponse, error) {
	c.grantCalls++
	reqAnnos := annotations.Annotations(in.GetAnnotations())
	c.sawTypeMarker = c.sawTypeMarker || reqAnnos.Contains(&v2.TypeScopedGrants{})
	return v2.GrantsServiceListGrantsResponse_builder{}.Build(), nil
}

type coldTypeScopedConnector struct {
	*mockConnector
	entitlementsByToken         map[string]*v2.Entitlement
	grantsByToken               map[string]*v2.Grant
	entitlementPlannerCalls     int
	grantPlannerCalls           int
	perResourceEntitlementCalls int
	perResourceGrantCalls       int
}

type countingEntitlementsConnector struct {
	*mockConnector
	calls atomic.Int64
}

func (c *countingEntitlementsConnector) ListEntitlements(
	_ context.Context,
	_ *v2.EntitlementsServiceListEntitlementsRequest,
	_ ...grpc.CallOption,
) (*v2.EntitlementsServiceListEntitlementsResponse, error) {
	c.calls.Add(1)
	return v2.EntitlementsServiceListEntitlementsResponse_builder{}.Build(), nil
}

type resumableTypeScopedConnector struct {
	*coldTypeScopedConnector
	failGrantToken string
	failedOnce     bool
}

// onReached, on this and the two fakes below, runs at the blocking point
// before the call starts waiting. It is how the run-duration tests expire
// the run from inside the in-flight call instead of hoping a real timer
// fires during it; see runDurationExpiry.
type blockingTypeScopedGrantConnector struct {
	*mockConnector
	onReached func()
	reached   atomic.Bool
	cancelled atomic.Bool
}

// blockingRootListResourcesStore blocks the FIRST root ListResources call
// until its context dies. Deadline-presence on the context is NOT a valid
// oracle here: parallelSync deliberately derives workerCtx from the
// parent (no deadline) and propagates expiry manually via
// context.AfterFunc(runCtx), preserving the DeadlineExceeded cause. The
// proof that expiry can interrupt root planner IO is cancellation itself.
type blockingRootListResourcesStore struct {
	c1zstore.Store
	onReached func()
	reached   atomic.Bool
	cancelled atomic.Bool
}

type countingTargetConnector struct {
	*mockConnector
	getResourceCalls atomic.Int64
}

func (c *countingTargetConnector) GetResource(
	ctx context.Context,
	in *v2.ResourceGetterServiceGetResourceRequest,
	opts ...grpc.CallOption,
) (*v2.ResourceGetterServiceGetResourceResponse, error) {
	c.getResourceCalls.Add(1)
	return c.mockConnector.GetResource(ctx, in, opts...)
}

type blockingValidateConnector struct {
	*mockConnector
	onReached func()
	blocked   atomic.Bool
	cancelled atomic.Bool
}

func (c *blockingValidateConnector) Validate(
	ctx context.Context,
	_ *v2.ConnectorServiceValidateRequest,
	_ ...grpc.CallOption,
) (*v2.ConnectorServiceValidateResponse, error) {
	c.blocked.Store(true)
	if c.onReached != nil {
		c.onReached()
	}
	select {
	case <-ctx.Done():
		c.cancelled.Store(true)
		return nil, context.Cause(ctx)
	case <-time.After(30 * time.Second):
		// Only guards an outright hang if cancellation is broken; must
		// exceed the test's run duration by a wide margin so slow CI
		// runners never race it.
		return nil, errors.New("test safety stop: SkipSync ignored run duration")
	}
}

func (s *blockingRootListResourcesStore) ListResources(
	ctx context.Context,
	req *v2.ResourcesServiceListResourcesRequest,
) (*v2.ResourcesServiceListResourcesResponse, error) {
	if s.reached.CompareAndSwap(false, true) {
		if s.onReached != nil {
			s.onReached()
		}
		select {
		case <-ctx.Done():
			s.cancelled.Store(true)
			return nil, context.Cause(ctx)
		case <-time.After(30 * time.Second):
			// Only guards an outright hang if cancellation is broken;
			// must exceed the test's run duration by a wide margin so
			// slow CI runners never race it.
			return nil, errors.New("test safety stop: root planner ignored run duration")
		}
	}
	return s.Store.ListResources(ctx, req)
}

func (c *blockingTypeScopedGrantConnector) ListGrants(
	ctx context.Context,
	in *v2.GrantsServiceListGrantsRequest,
	_ ...grpc.CallOption,
) (*v2.GrantsServiceListGrantsResponse, error) {
	reqAnnos := annotations.Annotations(in.GetAnnotations())
	if !reqAnnos.Contains(&v2.TypeScopedGrants{}) {
		return v2.GrantsServiceListGrantsResponse_builder{}.Build(), nil
	}
	if in.GetPageToken() == "" {
		return v2.GrantsServiceListGrantsResponse_builder{
			Annotations: annotations.New(v2.EnqueuePageTokens_builder{
				PageTokens: []string{"slow"},
			}.Build()),
		}.Build(), nil
	}
	c.reached.Store(true)
	if c.onReached != nil {
		c.onReached()
	}
	select {
	case <-ctx.Done():
		c.cancelled.Store(true)
		return nil, context.Cause(ctx)
	case <-time.After(30 * time.Second):
		// Only guards an outright hang if cancellation is broken; must
		// exceed the test's run duration by a wide margin so slow CI
		// runners never race it.
		return nil, errors.New("test safety stop: active batch ignored run duration")
	}
}

func (c *resumableTypeScopedConnector) ListGrants(
	ctx context.Context,
	in *v2.GrantsServiceListGrantsRequest,
	opts ...grpc.CallOption,
) (*v2.GrantsServiceListGrantsResponse, error) {
	if in.GetPageToken() == c.failGrantToken && !c.failedOnce {
		c.failedOnce = true
		return nil, fmt.Errorf("injected grant cursor failure")
	}
	return c.coldTypeScopedConnector.ListGrants(ctx, in, opts...)
}

func (c *coldTypeScopedConnector) ListEntitlements(
	_ context.Context,
	in *v2.EntitlementsServiceListEntitlementsRequest,
	_ ...grpc.CallOption,
) (*v2.EntitlementsServiceListEntitlementsResponse, error) {
	reqAnnos := annotations.Annotations(in.GetAnnotations())
	if !reqAnnos.Contains(&v2.TypeScopedEntitlements{}) {
		c.perResourceEntitlementCalls++
		return v2.EntitlementsServiceListEntitlementsResponse_builder{}.Build(), nil
	}
	if in.GetPageToken() == "" {
		c.entitlementPlannerCalls++
		return v2.EntitlementsServiceListEntitlementsResponse_builder{
			Annotations: annotations.New(v2.EnqueuePageTokens_builder{
				PageTokens: []string{"ent-1", "ent-2"},
			}.Build()),
		}.Build(), nil
	}
	return v2.EntitlementsServiceListEntitlementsResponse_builder{
		List: []*v2.Entitlement{c.entitlementsByToken[in.GetPageToken()]},
	}.Build(), nil
}

func (c *coldTypeScopedConnector) ListGrants(_ context.Context, in *v2.GrantsServiceListGrantsRequest, _ ...grpc.CallOption) (*v2.GrantsServiceListGrantsResponse, error) {
	reqAnnos := annotations.Annotations(in.GetAnnotations())
	if !reqAnnos.Contains(&v2.TypeScopedGrants{}) {
		c.perResourceGrantCalls++
		return v2.GrantsServiceListGrantsResponse_builder{}.Build(), nil
	}
	if in.GetPageToken() == "" {
		c.grantPlannerCalls++
		return v2.GrantsServiceListGrantsResponse_builder{
			Annotations: annotations.New(v2.EnqueuePageTokens_builder{
				PageTokens: []string{"grant-1", "grant-2"},
			}.Build()),
		}.Build(), nil
	}
	return v2.GrantsServiceListGrantsResponse_builder{
		List: []*v2.Grant{c.grantsByToken[in.GetPageToken()]},
	}.Build(), nil
}

func (c *targetedTypeScopedConnector) ListEntitlements(
	ctx context.Context,
	in *v2.EntitlementsServiceListEntitlementsRequest,
	opts ...grpc.CallOption,
) (*v2.EntitlementsServiceListEntitlementsResponse, error) {
	c.entitlementCalls++
	return c.mockConnector.ListEntitlements(ctx, in, opts...)
}

func (c *targetedTypeScopedConnector) ListGrants(ctx context.Context, in *v2.GrantsServiceListGrantsRequest, opts ...grpc.CallOption) (*v2.GrantsServiceListGrantsResponse, error) {
	c.grantCalls++
	return c.mockConnector.ListGrants(ctx, in, opts...)
}

func TestCollectEnqueuedPageTokens(t *testing.T) {
	s := &syncer{}
	origin := &Action{
		Op:             SyncGrantsOp,
		ResourceTypeID: "group",
		ResourceID:     "group-1",
	}
	spawned, err := s.collectEnqueuedPageTokens(
		context.Background(),
		"sync-grants-for-resource",
		SyncGrantsOp,
		origin,
		annotations.New(v2.EnqueuePageTokens_builder{
			PageTokens: []string{"page-2", "page-3"},
		}.Build()),
	)
	require.NoError(t, err)
	require.Equal(t, []Action{
		{Op: SyncGrantsOp, ResourceTypeID: "group", ResourceID: "group-1", PageToken: "page-2", Spawned: true},
		{Op: SyncGrantsOp, ResourceTypeID: "group", ResourceID: "group-1", PageToken: "page-3", Spawned: true},
	}, spawned)
}

func TestCollectEnqueuedPageTokensPreservesTypeScopedMarker(t *testing.T) {
	spawned, err := (&syncer{}).collectEnqueuedPageTokens(
		t.Context(),
		"sync-grants-for-type",
		SyncGrantsOp,
		&Action{ResourceTypeID: "group", TypeScoped: true},
		annotations.New(v2.EnqueuePageTokens_builder{
			PageTokens: []string{"page-2"},
		}.Build()),
	)
	require.NoError(t, err)
	require.Len(t, spawned, 1)
	require.True(t, spawned[0].TypeScoped)
	require.True(t, spawned[0].Spawned)
}

func TestCollectEnqueuedPageTokensRejectsInvalidFanout(t *testing.T) {
	s := &syncer{}
	action := &Action{ResourceTypeID: "group"}

	_, err := s.collectEnqueuedPageTokens(context.Background(), "sync-grants", SyncGrantsOp, action,
		annotations.New(v2.EnqueuePageTokens_builder{PageTokens: []string{""}}.Build()))
	require.ErrorContains(t, err, "empty page token")

	_, err = s.collectEnqueuedPageTokens(context.Background(), "sync-grants", SyncGrantsOp, action,
		annotations.New(v2.EnqueuePageTokens_builder{PageTokens: []string{strings.Repeat("x", maxEnqueuedPageTokenBytes+1)}}.Build()))
	require.ErrorContains(t, err, "page token is")

	tooMany := make([]string, maxEnqueuePageTokensPerResponse+1)
	for i := range tooMany {
		tooMany[i] = "token"
	}
	_, err = s.collectEnqueuedPageTokens(context.Background(), "sync-grants", SyncGrantsOp, action,
		annotations.New(v2.EnqueuePageTokens_builder{PageTokens: tooMany}.Build()))
	require.ErrorContains(t, err, "max 1024")

	tooLargeInAggregate := make([]string, maxEnqueuedPageTokenTotalBytes/maxEnqueuedPageTokenBytes+1)
	for i := range tooLargeInAggregate {
		tooLargeInAggregate[i] = strings.Repeat("x", maxEnqueuedPageTokenBytes)
	}
	_, err = s.collectEnqueuedPageTokens(context.Background(), "sync-grants", SyncGrantsOp, action,
		annotations.New(v2.EnqueuePageTokens_builder{PageTokens: tooLargeInAggregate}.Build()))
	require.ErrorContains(t, err, "total page-token bytes")

	malformed, err := anypb.New(&v2.EnqueuePageTokens{})
	require.NoError(t, err)
	malformed.Value = []byte{0xff}
	_, err = s.collectEnqueuedPageTokens(context.Background(), "sync-grants", SyncGrantsOp, action,
		annotations.Annotations{malformed})
	require.ErrorContains(t, err, "error parsing enqueue-page-tokens annotation")

	first, err := anypb.New(v2.EnqueuePageTokens_builder{PageTokens: []string{"page-1"}}.Build())
	require.NoError(t, err)
	second, err := anypb.New(v2.EnqueuePageTokens_builder{PageTokens: []string{"page-2"}}.Build())
	require.NoError(t, err)
	_, err = s.collectEnqueuedPageTokens(context.Background(), "sync-grants", SyncGrantsOp, action,
		annotations.Annotations{first, second})
	require.ErrorContains(t, err, "multiple EnqueuePageTokens annotations")
}

func TestCollectEnqueuedPageTokensAcceptsMaximumCount(t *testing.T) {
	tokens := make([]string, maxEnqueuePageTokensPerResponse)
	for i := range tokens {
		tokens[i] = fmt.Sprintf("token-%d", i)
	}
	spawned, err := (&syncer{}).collectEnqueuedPageTokens(
		context.Background(),
		"sync-entitlements",
		SyncEntitlementsOp,
		&Action{ResourceTypeID: "group"},
		annotations.New(v2.EnqueuePageTokens_builder{PageTokens: tokens}.Build()),
	)
	require.NoError(t, err)
	require.Len(t, spawned, maxEnqueuePageTokensPerResponse)
}

func TestSpawnedActionsCoexistWithOriginContinuation(t *testing.T) {
	ctx := context.Background()
	st := newState()
	require.NoError(t, st.Unmarshal(""))
	st.FinishAction(ctx, st.Current())
	st.PushAction(ctx, Action{Op: SyncGrantsOp, ResourceTypeID: "group", ResourceID: "group-1"})
	origin := st.Current()
	s := &syncer{state: st}
	require.NoError(t, s.nextPageOrFinishAction(ctx, origin, "origin-next",
		Action{Op: SyncGrantsOp, ResourceTypeID: "group", ResourceID: "group-1", PageToken: "sibling-1", Spawned: true},
		Action{Op: SyncGrantsOp, ResourceTypeID: "group", ResourceID: "group-1", PageToken: "sibling-2", Spawned: true},
	))

	token, err := st.Marshal()
	require.NoError(t, err)
	resumed := newState()
	require.NoError(t, resumed.Unmarshal(token))

	seen := map[string]Action{}
	for resumed.Current() != nil {
		action := *resumed.Current()
		seen[action.PageToken] = action
		resumed.FinishAction(ctx, &action)
	}
	require.Len(t, seen, 3)
	require.False(t, seen["origin-next"].Spawned)
	require.True(t, seen["sibling-1"].Spawned)
	require.True(t, seen["sibling-2"].Spawned)
}

func TestSpawnedActionsSurviveCheckpoint(t *testing.T) {
	ctx := context.Background()
	st := newState()
	require.NoError(t, st.Unmarshal(""))
	st.FinishAction(ctx, st.Current())
	st.PushAction(ctx, Action{
		Op:             SyncEntitlementsOp,
		ResourceTypeID: "group",
		PageToken:      "chunk-1",
		Spawned:        true,
		TypeScoped:     true,
	})

	token, err := st.Marshal()
	require.NoError(t, err)
	resumed := newState()
	require.NoError(t, resumed.Unmarshal(token))
	require.NotNil(t, resumed.Current())
	require.True(t, resumed.Current().Spawned)
	require.True(t, resumed.Current().TypeScoped)
	require.Equal(t, "chunk-1", resumed.Current().PageToken)
}

func TestSpawnedCursorJoinsActiveParallelBatch(t *testing.T) {
	ctx := t.Context()
	st := newState()
	require.NoError(t, st.Unmarshal(""))
	st.FinishAction(ctx, st.Current())
	origin := st.pushAction(ctx, Action{
		Op:             SyncGrantsOp,
		ResourceTypeID: "group",
		ResourceID:     "group-1",
	})
	s := &syncer{state: st, cfg: syncConfig{workerCount: 2}}
	childStarted := make(chan struct{})

	f := func(ctx context.Context, action *Action) error {
		switch action.PageToken {
		case "":
			return s.nextPageOrFinishAction(ctx, action, "origin-next", Action{
				Op:             SyncGrantsOp,
				ResourceTypeID: "group",
				ResourceID:     "group-1",
				PageToken:      "sibling",
				Spawned:        true,
			})
		case "sibling":
			close(childStarted)
			s.state.FinishAction(ctx, action)
			return nil
		case "origin-next":
			select {
			case <-childStarted:
				s.state.FinishAction(ctx, action)
				return nil
			case <-time.After(2 * time.Second):
				return fmt.Errorf("spawned cursor did not join the active worker batch")
			}
		default:
			return fmt.Errorf("unexpected page token %q", action.PageToken)
		}
	}

	_, err := s.syncParallel(ctx, retry.NewRetryer(ctx, retry.RetryConfig{}), []*Action{origin}, f)
	require.NoError(t, err)
	require.Nil(t, st.Current())
}

func TestParallelActionQueueReleasesDequeuedStorage(t *testing.T) {
	actions := make([]*Action, 2048)
	for i := range actions {
		actions[i] = &Action{ID: fmt.Sprintf("action-%d", i)}
	}
	queue := newParallelActionQueue(actions)

	for range actions {
		action, ok := queue.next()
		require.True(t, ok)
		require.NotNil(t, action)
		queue.done()
	}

	require.Empty(t, queue.actions)
	require.Zero(t, queue.head)
	require.Zero(t, queue.outstanding)
}

func TestEmptyResourceIDIsSkippedBeforePerResourceCalls(t *testing.T) {
	ctx := t.Context()
	tmpDir := t.TempDir()
	resourceType := v2.ResourceType_builder{
		Id:          "untyped",
		DisplayName: "Untyped",
	}.Build()
	resource := v2.Resource_builder{
		Id:          v2.ResourceId_builder{ResourceType: "untyped"}.Build(),
		DisplayName: "Malformed but historically tolerated",
	}.Build()
	connector := &emptyIDConnector{mockConnector: newMockConnector()}
	connector.rtDB = append(connector.rtDB, resourceType)
	connector.resourceDB["untyped"] = append(connector.resourceDB["untyped"], resource)

	s, err := NewSyncer(ctx, connector,
		WithC1ZPath(filepath.Join(tmpDir, "empty-id.c1z")),
		WithTmpDir(tmpDir),
	)
	require.NoError(t, err)
	require.NoError(t, s.Sync(ctx))
	require.NoError(t, s.Close(ctx))
	require.Zero(t, connector.entitlementCalls)
	require.Zero(t, connector.grantCalls)
	require.False(t, connector.sawTypeMarker)
}

func TestTargetedSyncSkipsPerResourceCallsForTypeScopedType(t *testing.T) {
	ctx := t.Context()
	tmpDir := t.TempDir()
	resourceType := v2.ResourceType_builder{
		Id:          "group",
		DisplayName: "Group",
		Annotations: annotations.New(&v2.TypeScopedEntitlements{}, &v2.TypeScopedGrants{}),
	}.Build()
	resource := v2.Resource_builder{
		Id:          v2.ResourceId_builder{ResourceType: "group", Resource: "group-1"}.Build(),
		DisplayName: "Group 1",
	}.Build()
	connector := &targetedTypeScopedConnector{mockConnector: newMockConnector()}
	connector.rtDB = append(connector.rtDB, resourceType)
	connector.resourceDB["group"] = append(connector.resourceDB["group"], resource)

	s, err := NewSyncer(ctx, connector,
		WithC1ZPath(filepath.Join(tmpDir, "targeted.c1z")),
		WithTmpDir(tmpDir),
		WithTargetedSyncResources([]*v2.Resource{resource}),
	)
	require.NoError(t, err)
	require.NoError(t, s.Sync(ctx))
	require.NoError(t, s.Close(ctx))

	require.Zero(t, connector.entitlementCalls)
	require.Zero(t, connector.grantCalls)
}

func TestTargetedResourceSchedulingFailureLeavesParentAndNoFollowups(t *testing.T) {
	ctx := t.Context()
	tmpDir := t.TempDir()
	resourceType := v2.ResourceType_builder{
		Id:          "group",
		DisplayName: "Group",
	}.Build()
	malformedChildType, err := anypb.New(&v2.ChildResourceType{})
	require.NoError(t, err)
	malformedChildType.Value = []byte{0xff}
	resource := v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: "group",
			Resource:     "group-1",
		}.Build(),
		DisplayName: "Group 1",
		Annotations: []*anypb.Any{malformedChildType},
	}.Build()
	connector := newMockConnector()
	connector.rtDB = append(connector.rtDB, resourceType)
	connector.resourceDB["group"] = append(connector.resourceDB["group"], resource)

	s, err := NewSyncer(ctx, connector,
		WithC1ZPath(filepath.Join(tmpDir, "targeted-transition.c1z")),
		WithTmpDir(tmpDir),
		WithTargetedSyncResources([]*v2.Resource{resource}),
		WithWorkerCount(2),
	)
	require.NoError(t, err)
	require.Error(t, s.Sync(ctx))

	internalState := s.(*syncer).state.(*state)
	var targetedActions, followupActions int
	for _, action := range internalState.actions {
		switch action.Op {
		case SyncTargetedResourceOp:
			targetedActions++
			require.Equal(t, "group-1", action.ResourceID)
		case SyncEntitlementsOp, SyncGrantsOp, SyncResourcesOp:
			followupActions++
		default:
		}
	}
	require.Equal(t, 1, targetedActions)
	require.Zero(t, followupActions)
	require.NoError(t, s.Close(ctx))
}

func TestDuplicateTargetedResourcesAreScheduledOnce(t *testing.T) {
	ctx := t.Context()
	tmpDir := t.TempDir()
	resourceType := v2.ResourceType_builder{
		Id:          "group",
		DisplayName: "Group",
	}.Build()
	resource := v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: "group",
			Resource:     "group-1",
		}.Build(),
		DisplayName: "Group 1",
	}.Build()
	connector := &countingTargetConnector{mockConnector: newMockConnector()}
	connector.rtDB = append(connector.rtDB, resourceType)
	connector.resourceDB["group"] = append(connector.resourceDB["group"], resource)

	s, err := NewSyncer(ctx, connector,
		WithC1ZPath(filepath.Join(tmpDir, "duplicate-targets.c1z")),
		WithTmpDir(tmpDir),
		WithTargetedSyncResources([]*v2.Resource{resource, resource}),
		WithWorkerCount(2),
	)
	require.NoError(t, err)
	require.NoError(t, s.Sync(ctx))
	require.NoError(t, s.Close(ctx))
	require.EqualValues(t, 1, connector.getResourceCalls.Load())
}

func TestTypeScopedColdCollectionEndToEnd(t *testing.T) {
	for _, workers := range []int{1, 4} {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			ctx := t.Context()
			tmpDir := t.TempDir()
			groupType := v2.ResourceType_builder{
				Id:          "group",
				DisplayName: "Group",
				Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_GROUP},
				Annotations: annotations.New(&v2.TypeScopedEntitlements{}, &v2.TypeScopedGrants{}),
			}.Build()
			userType := v2.ResourceType_builder{
				Id:          "user",
				DisplayName: "User",
				Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER},
				Annotations: annotations.New(&v2.SkipEntitlementsAndGrants{}),
			}.Build()
			group1, err := rs.NewGroupResource("group-1", groupType, "Group 1", nil)
			require.NoError(t, err)
			group2, err := rs.NewGroupResource("group-2", groupType, "Group 2", nil)
			require.NoError(t, err)
			user, err := rs.NewUserResource("user-1", userType, "User 1", nil)
			require.NoError(t, err)
			ent1 := et.NewAssignmentEntitlement(group1, "member", et.WithGrantableTo(userType))
			ent2 := et.NewAssignmentEntitlement(group2, "member", et.WithGrantableTo(userType))
			grant1 := gt.NewGrant(group1, "member", user.GetId())
			grant2 := gt.NewGrant(group2, "member", user.GetId())

			connector := &coldTypeScopedConnector{
				mockConnector:       newMockConnector(),
				entitlementsByToken: map[string]*v2.Entitlement{"ent-1": ent1, "ent-2": ent2},
				grantsByToken:       map[string]*v2.Grant{"grant-1": grant1, "grant-2": grant2},
			}
			connector.rtDB = append(connector.rtDB, groupType, userType)
			connector.resourceDB["group"] = append(connector.resourceDB["group"], group1, group2)
			connector.resourceDB["user"] = append(connector.resourceDB["user"], user)

			store, err := dotc1z.NewStore(ctx, filepath.Join(tmpDir, "type-scoped.c1z"),
				dotc1z.WithEngine(c1zstore.EnginePebble),
				dotc1z.WithTmpDir(tmpDir),
			)
			require.NoError(t, err)
			s, err := NewSyncer(ctx, connector,
				WithConnectorStore(store),
				WithTmpDir(tmpDir),
				WithWorkerCount(workers),
			)
			require.NoError(t, err)
			require.NoError(t, s.Sync(ctx))

			entResp, err := store.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{}.Build())
			require.NoError(t, err)
			require.Len(t, entResp.GetList(), 2)
			grantResp, err := store.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
			require.NoError(t, err)
			require.Len(t, grantResp.GetList(), 2)
			require.NoError(t, s.Close(ctx))

			require.Equal(t, 1, connector.entitlementPlannerCalls)
			require.Equal(t, 1, connector.grantPlannerCalls)
			require.Zero(t, connector.perResourceEntitlementCalls)
			require.Zero(t, connector.perResourceGrantCalls)
		})
	}
}

func TestSyncDrainsMoreThanOneSchedulerBatch(t *testing.T) {
	ctx := t.Context()
	tmpDir := t.TempDir()
	resourceType := v2.ResourceType_builder{
		Id:          "item",
		DisplayName: "Item",
	}.Build()
	connector := &countingEntitlementsConnector{mockConnector: newMockConnector()}
	connector.rtDB = append(connector.rtDB, resourceType)
	for i := 0; i < maxPeekActionsCount+5; i++ {
		connector.resourceDB["item"] = append(connector.resourceDB["item"], v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: "item",
				Resource:     fmt.Sprintf("item-%03d", i),
			}.Build(),
			DisplayName: fmt.Sprintf("Item %03d", i),
		}.Build())
	}

	s, err := NewSyncer(ctx, connector,
		WithC1ZPath(filepath.Join(tmpDir, "multi-batch.c1z")),
		WithTmpDir(tmpDir),
		WithWorkerCount(4),
		WithSkipGrants(true),
	)
	require.NoError(t, err)
	require.NoError(t, s.Sync(ctx))
	require.NoError(t, s.Close(ctx))
	require.EqualValues(t, maxPeekActionsCount+5, connector.calls.Load())
}

func TestTypeScopedFanoutResumesAfterStoredCursorFailure(t *testing.T) {
	ctx := t.Context()
	tmpDir := t.TempDir()
	groupType := v2.ResourceType_builder{
		Id:          "group",
		DisplayName: "Group",
		Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_GROUP},
		Annotations: annotations.New(&v2.TypeScopedEntitlements{}, &v2.TypeScopedGrants{}),
	}.Build()
	userType := v2.ResourceType_builder{
		Id:          "user",
		DisplayName: "User",
		Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER},
		Annotations: annotations.New(&v2.SkipEntitlementsAndGrants{}),
	}.Build()
	group1, err := rs.NewGroupResource("group-1", groupType, "Group 1", nil)
	require.NoError(t, err)
	group2, err := rs.NewGroupResource("group-2", groupType, "Group 2", nil)
	require.NoError(t, err)
	user, err := rs.NewUserResource("user-1", userType, "User 1", nil)
	require.NoError(t, err)
	entitlement1 := et.NewAssignmentEntitlement(group1, "member", et.WithGrantableTo(userType))
	entitlement2 := et.NewAssignmentEntitlement(group2, "member", et.WithGrantableTo(userType))
	grant1 := gt.NewGrant(group1, "member", user.GetId())
	grant2 := gt.NewGrant(group2, "member", user.GetId())

	baseConnector := &coldTypeScopedConnector{
		mockConnector:       newMockConnector(),
		entitlementsByToken: map[string]*v2.Entitlement{"ent-1": entitlement1, "ent-2": entitlement2},
		grantsByToken:       map[string]*v2.Grant{"grant-1": grant1, "grant-2": grant2},
	}
	baseConnector.rtDB = append(baseConnector.rtDB, groupType, userType)
	baseConnector.resourceDB["group"] = append(baseConnector.resourceDB["group"], group1, group2)
	baseConnector.resourceDB["user"] = append(baseConnector.resourceDB["user"], user)
	connector := &resumableTypeScopedConnector{
		coldTypeScopedConnector: baseConnector,
		failGrantToken:          "grant-2",
	}

	store, err := dotc1z.NewStore(ctx, filepath.Join(tmpDir, "resume-fanout.c1z"),
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithTmpDir(tmpDir),
	)
	require.NoError(t, err)
	s, err := NewSyncer(ctx, connector,
		WithConnectorStore(store),
		WithTmpDir(tmpDir),
		WithWorkerCount(1),
	)
	require.NoError(t, err)

	require.ErrorContains(t, s.Sync(ctx), "injected grant cursor failure")
	require.NoError(t, s.Sync(ctx))

	grantResp, err := store.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, grantResp.GetList(), 2)
	require.NoError(t, s.Close(ctx))
	require.True(t, connector.failedOnce)
}

// runDurationExpiry returns a context to sync under, and a func that makes
// the syncer see the run duration as expired.
//
// Expiry is consumed as a cause, never as a timer: parallelSync hands it to
// its workers as cancelWorkers(context.Cause(runCtx)), and both Sync and
// SkipSync decide ErrSyncNotComplete by testing that cause against
// context.DeadlineExceeded. Cancelling with that cause is therefore the same
// event, delivered where the test asks for it instead of whenever a real
// timer wins a race against setup. Racing it is what these tests used to do,
// through budgets of 50ms, then 300ms, then 8s, and the 8s still lost on a
// Windows runner — the failure is a false negative (setup did not reach the
// call, so nothing was exercised), which is why the assertions below pin
// reached as well as cancelled.
//
// What cause injection cannot test is the timer plumbing itself: it cancels
// the parent of runCtx and workerCtx alike, so every call site sees it
// whether or not its context descends from runCtx, and the tests using this
// helper would pass with WithRunDuration ignored outright. The
// TestRunDurationTimer* tests below keep the real timer under test, one per
// call-site family.
func runDurationExpiry(t *testing.T, parent context.Context) (context.Context, func()) {
	t.Helper()
	ctx, cancel := context.WithCancelCause(parent)
	t.Cleanup(func() { cancel(context.Canceled) })
	return ctx, func() { cancel(context.DeadlineExceeded) }
}

// requireCheckpointedOnExpiry asserts that the checkpoint taken when the
// run duration expires actually succeeded — the resumability guarantee.
//
// ErrSyncNotComplete on its own proves nothing: Sync joins it onto
// whatever error it is about to return once the deadline has fired, and
// handleOperationError joins it onto a failed checkpoint's error too, so
// ErrorIs passes whether or not the checkpoint was written. What makes
// the joined tree an oracle is that handleOperationError drops the batch
// error — the cancellation that ended the run — so on this path a healthy
// expiry has nothing else to report. Every leaf being ErrSyncNotComplete
// is therefore the same claim as "the checkpoint returned nil", and it
// holds for a checkpoint that failed on a dead context, a sealed engine,
// or a disk error alike, rather than only the context-flavored subset.
//
// Only valid for the timer tests. Not for SkipSync, which never
// checkpoints and returns its cancelled call's error verbatim; and not
// for the cause-injected tests, which cancel the caller's context — the
// one the expiry checkpoint writes under — so their checkpoint fails by
// construction. Real expiry leaves the caller's context alive, which is
// the very thing these leaves prove the checkpoint relied on.
func requireCheckpointedOnExpiry(t *testing.T, err error) {
	t.Helper()
	leaves := flattenJoined(err)
	require.NotEmpty(t, leaves, "expected an error on run-duration expiry")
	for _, leaf := range leaves {
		require.ErrorIs(t, leaf, ErrSyncNotComplete,
			"run-duration expiry returned %q next to ErrSyncNotComplete. handleOperationError joins only the "+
				"checkpoint's error, so this is the checkpoint failing — the sync cannot resume from where it stopped", leaf)
	}
}

// flattenJoined returns the leaves of an errors.Join tree, so a caller
// can assert about each one instead of about the flattened chain, where
// errors.Is on the whole reports a match no matter which branch it came
// from. Single-error wrappers (fmt.Errorf %w) are peeled while looking
// for the join: if a layer above handleOperationError ever adds context
// around it, requiring the join to be outermost would silently degrade
// requireCheckpointedOnExpiry to a plain errors.Is, which passes whether
// or not the checkpoint failed.
func flattenJoined(err error) []error {
	if err == nil {
		return nil
	}
	for unwrapped := err; unwrapped != nil; unwrapped = errors.Unwrap(unwrapped) {
		joined, ok := unwrapped.(interface{ Unwrap() []error })
		if !ok {
			continue
		}
		var leaves []error
		for _, child := range joined.Unwrap() {
			leaves = append(leaves, flattenJoined(child)...)
		}
		return leaves
	}
	return []error{err}
}

// Validates the oracle above: a planted checkpoint failure must stay a
// separate leaf even when a wrapper hides the join, or
// requireCheckpointedOnExpiry would report a healthy expiry.
func TestFlattenJoinedSeesWrappedJoins(t *testing.T) {
	checkpointErr := errors.New("checkpoint failed")
	wrapped := fmt.Errorf("sync: %w", errors.Join(checkpointErr, ErrSyncNotComplete))
	leaves := flattenJoined(wrapped)
	require.Len(t, leaves, 2, "a single-error wrapper hid the errors.Join tree from flattenJoined")
	require.ErrorIs(t, leaves[0], checkpointErr)
	require.ErrorIs(t, leaves[1], ErrSyncNotComplete)
	require.NotErrorIs(t, leaves[0], ErrSyncNotComplete,
		"the checkpoint leaf must not satisfy the ErrSyncNotComplete check, or the oracle cannot fail")
}

func TestRunDurationCancelsActiveSpawnedCursorBatch(t *testing.T) {
	// Run-duration expiry must cancel a spawned cursor's connector call
	// while it is in flight rather than wait it out, and surface
	// ErrSyncNotComplete. Expiry is triggered from inside that call, so a
	// run that never reached it fails on reached instead of passing without
	// having exercised anything.
	ctx, expire := runDurationExpiry(t, t.Context())
	tmpDir := t.TempDir()
	resourceType := v2.ResourceType_builder{
		Id:          "group",
		DisplayName: "Group",
		Annotations: annotations.New(&v2.TypeScopedGrants{}),
	}.Build()
	connector := &blockingTypeScopedGrantConnector{mockConnector: newMockConnector(), onReached: expire}
	connector.rtDB = append(connector.rtDB, resourceType)

	s, err := NewSyncer(ctx, connector,
		WithC1ZPath(filepath.Join(tmpDir, "run-duration.c1z")),
		WithTmpDir(tmpDir),
		WithWorkerCount(2),
		// Long enough that the timer never fires: expiry arrives from
		// onReached, while the option still puts a real deadline on runCtx
		// so the context plumbing under test is the production one.
		WithRunDuration(5*time.Minute),
	)
	require.NoError(t, err)
	err = s.Sync(ctx)
	require.ErrorIs(t, err, ErrSyncNotComplete)
	require.True(t, connector.reached.Load(),
		"the sync ended before the spawned-cursor batch was reached, so nothing was exercised")
	require.True(t, connector.cancelled.Load(),
		"the in-flight spawned cursor call was not cancelled by run-duration expiry")
	// Close after expiry: ctx is cancelled by now, and Close writes.
	require.NoError(t, s.Close(context.WithoutCancel(ctx)))
}

func TestRunDurationCancelsRootPlannerIO(t *testing.T) {
	// Root planning steps (entitlements/grants planning, not the parallel
	// batch workers) are a separate call-site family. Expiry must cancel
	// the first blocked root store ListResources, and cancellation is the
	// only valid oracle here since workerCtx carries no deadline by
	// design. Catching this family being rewired off workerCtx is
	// TestRunDurationTimerCancelsRootPlannerIO's job — cause injection
	// cancels the parent of every context here, so a rewired call site
	// would still be cancelled.
	ctx, expire := runDurationExpiry(t, t.Context())
	tmpDir := t.TempDir()
	resourceType := v2.ResourceType_builder{
		Id:          "group",
		DisplayName: "Group",
	}.Build()
	resource := v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: "group",
			Resource:     "group-1",
		}.Build(),
		DisplayName: "Group 1",
	}.Build()
	connector := newMockConnector()
	connector.rtDB = append(connector.rtDB, resourceType)
	connector.resourceDB["group"] = append(connector.resourceDB["group"], resource)

	baseStore, err := dotc1z.NewStore(ctx, filepath.Join(tmpDir, "root-run-duration.c1z"),
		dotc1z.WithTmpDir(tmpDir),
	)
	require.NoError(t, err)
	store := &blockingRootListResourcesStore{Store: baseStore, onReached: expire}
	s, err := NewSyncer(ctx, connector,
		WithConnectorStore(store),
		WithTmpDir(tmpDir),
		WithWorkerCount(2),
		WithRunDuration(5*time.Minute),
	)
	require.NoError(t, err)

	err = s.Sync(ctx)
	require.ErrorIs(t, err, ErrSyncNotComplete)
	require.True(t, store.reached.Load(),
		"the sync ended before root planner IO was reached, so nothing was exercised")
	require.True(t, store.cancelled.Load(),
		"the blocked root planner IO was not cancelled by run-duration expiry")
	require.NoError(t, s.Close(context.WithoutCancel(ctx)))
}

func TestSkipSyncHonorsRunDuration(t *testing.T) {
	// Same shape as the spawned-cursor test above, for the SkipFullSync
	// path: expiry must cancel a blocked Validate. SkipSync builds its own
	// runCtx and converts the cause in its own defer, so this is separate
	// code from the full sync's.
	ctx, expire := runDurationExpiry(t, t.Context())
	tmpDir := t.TempDir()
	connector := &blockingValidateConnector{mockConnector: newMockConnector(), onReached: expire}
	s, err := NewSyncer(ctx, connector,
		WithC1ZPath(filepath.Join(tmpDir, "skip-run-duration.c1z")),
		WithTmpDir(tmpDir),
		WithSkipFullSync(),
		WithRunDuration(5*time.Minute),
	)
	require.NoError(t, err)

	err = s.Sync(ctx)
	require.ErrorIs(t, err, ErrSyncNotComplete)
	require.True(t, connector.blocked.Load(),
		"the sync ended before Validate was reached, so nothing was exercised")
	require.True(t, connector.cancelled.Load(),
		"the blocked Validate call was not cancelled by run-duration expiry")
	require.NoError(t, s.Close(context.WithoutCancel(ctx)))
}

// The three TestRunDurationTimer* tests cover what the cause-injected tests
// above give up by delivering expiry themselves: that the real timer fires
// and reaches in-flight work through the production plumbing. Nothing a test
// can cancel from outside distinguishes those paths — runCtx and workerCtx
// are both children of the context passed to Sync, so cancelling that
// reaches every call site whether or not it descends from runCtx, and the
// cause-injected tests would pass with WithRunDuration ignored outright.
// There is one timer test per call-site family (spawned-cursor batch, root
// planner, SkipSync's Validate) because each family could be rewired off
// runCtx independently.
//
// Each pays for its wall-clock wait the same two ways. The store is created
// before the syncer, keeping c1z creation — the slowest setup step — out of
// the window the duration has to cover. For the spawned-cursor and SkipSync
// families that is a change: their old tests created the c1z inside the
// window via WithC1ZPath, and the spawned-cursor one is the 8s test that
// failed on a loaded Windows runner. The root-planner test always created
// its store first, and that 8s configuration has held in CI since it
// landed, which is why 8s is kept: the window covers only the sync steps up
// to the blocked call, well inside the fakes' 30s safety stop. And losing
// the race is a loud failure on reached, never a quiet pass.
func TestRunDurationTimerCancelsInFlightWork(t *testing.T) {
	// Spawned-cursor batch family: the timer cancels runCtx only, so the
	// blocked ListGrants sees expiry exclusively through the
	// context.AfterFunc(runCtx) hop into workerCtx. Gutting that hop
	// leaves the cause-injected tests passing and this one failing.
	ctx := t.Context()
	tmpDir := t.TempDir()
	resourceType := v2.ResourceType_builder{
		Id:          "group",
		DisplayName: "Group",
		Annotations: annotations.New(&v2.TypeScopedGrants{}),
	}.Build()
	connector := &blockingTypeScopedGrantConnector{mockConnector: newMockConnector()}
	connector.rtDB = append(connector.rtDB, resourceType)

	store, err := dotc1z.NewStore(ctx, filepath.Join(tmpDir, "run-duration-timer.c1z"),
		dotc1z.WithTmpDir(tmpDir),
	)
	require.NoError(t, err)
	s, err := NewSyncer(ctx, connector,
		WithConnectorStore(store),
		WithTmpDir(tmpDir),
		WithWorkerCount(2),
		WithRunDuration(8*time.Second),
	)
	require.NoError(t, err)

	err = s.Sync(ctx)
	require.ErrorIs(t, err, ErrSyncNotComplete)
	requireCheckpointedOnExpiry(t, err)
	require.True(t, connector.reached.Load(),
		"the timer expired before the spawned-cursor batch was reached, so nothing was exercised")
	require.True(t, connector.cancelled.Load(),
		"the real run-duration timer did not reach the in-flight call")
	require.NoError(t, ctx.Err(), "the run duration must end the sync without cancelling the caller's context")
	require.NoError(t, s.Close(ctx))
}

func TestRunDurationTimerCancelsRootPlannerIO(t *testing.T) {
	// Root-planner family: this is the test that fails if a root
	// planning call site is rewired from workerCtx to ctx, because the
	// timer cancels only runCtx and a rewired call would block until the
	// fake's safety stop.
	ctx := t.Context()
	tmpDir := t.TempDir()
	resourceType := v2.ResourceType_builder{
		Id:          "group",
		DisplayName: "Group",
	}.Build()
	resource := v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: "group",
			Resource:     "group-1",
		}.Build(),
		DisplayName: "Group 1",
	}.Build()
	connector := newMockConnector()
	connector.rtDB = append(connector.rtDB, resourceType)
	connector.resourceDB["group"] = append(connector.resourceDB["group"], resource)

	baseStore, err := dotc1z.NewStore(ctx, filepath.Join(tmpDir, "root-run-duration-timer.c1z"),
		dotc1z.WithTmpDir(tmpDir),
	)
	require.NoError(t, err)
	store := &blockingRootListResourcesStore{Store: baseStore}
	s, err := NewSyncer(ctx, connector,
		WithConnectorStore(store),
		WithTmpDir(tmpDir),
		WithWorkerCount(2),
		WithRunDuration(8*time.Second),
	)
	require.NoError(t, err)

	err = s.Sync(ctx)
	require.ErrorIs(t, err, ErrSyncNotComplete)
	requireCheckpointedOnExpiry(t, err)
	require.True(t, store.reached.Load(),
		"the timer expired before root planner IO was reached, so nothing was exercised")
	require.True(t, store.cancelled.Load(),
		"the real run-duration timer did not cancel the blocked root planner IO")
	require.NoError(t, ctx.Err(), "the run duration must end the sync without cancelling the caller's context")
	require.NoError(t, s.Close(ctx))
}

func TestRunDurationTimerCancelsSkipSyncValidate(t *testing.T) {
	// SkipSync family: SkipSync builds its own runCtx and hands it to
	// Validate directly, with no AfterFunc hop. This is the test that
	// fails if Validate's context stops descending from that runCtx. No
	// checkpoint oracle: SkipSync never checkpoints and returns the
	// cancelled call's error verbatim.
	ctx := t.Context()
	tmpDir := t.TempDir()
	connector := &blockingValidateConnector{mockConnector: newMockConnector()}
	store, err := dotc1z.NewStore(ctx, filepath.Join(tmpDir, "skip-run-duration-timer.c1z"),
		dotc1z.WithTmpDir(tmpDir),
	)
	require.NoError(t, err)
	s, err := NewSyncer(ctx, connector,
		WithConnectorStore(store),
		WithTmpDir(tmpDir),
		WithSkipFullSync(),
		WithRunDuration(8*time.Second),
	)
	require.NoError(t, err)

	err = s.Sync(ctx)
	require.ErrorIs(t, err, ErrSyncNotComplete)
	require.True(t, connector.blocked.Load(),
		"the timer expired before Validate was reached, so nothing was exercised")
	require.True(t, connector.cancelled.Load(),
		"the real run-duration timer did not cancel the blocked Validate call")
	require.NoError(t, ctx.Err(), "the run duration must end the sync without cancelling the caller's context")
	require.NoError(t, s.Close(ctx))
}
