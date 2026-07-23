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

type blockingTypeScopedGrantConnector struct {
	*mockConnector
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
	blocked   atomic.Bool
	cancelled atomic.Bool
}

func (c *blockingValidateConnector) Validate(
	ctx context.Context,
	_ *v2.ConnectorServiceValidateRequest,
	_ ...grpc.CallOption,
) (*v2.ConnectorServiceValidateResponse, error) {
	c.blocked.Store(true)
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
	s := &syncer{state: st, workerCount: 2}
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

func TestEmptyResourceIDRemainsPerResource(t *testing.T) {
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
	require.Equal(t, 1, connector.entitlementCalls)
	require.Equal(t, 1, connector.grantCalls)
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

func TestRunDurationCancelsActiveSpawnedCursorBatch(t *testing.T) {
	// The REAL run-duration timer must fire while a spawned cursor's
	// connector call is in flight, cancel that call via its context (not
	// wait it out), and surface ErrSyncNotComplete. The duration is
	// deliberately generous: it must not expire before setup reaches the
	// grants batch on a slow CI runner, or the test never exercises the
	// in-flight call at all — the previous 50ms raced setup and was
	// vacuous whenever it lost (and its 500ms wall bound failed on
	// Windows FS latency). reached/cancelled make the claim mechanical;
	// the wall bound only guards outright hangs.
	ctx := t.Context()
	tmpDir := t.TempDir()
	resourceType := v2.ResourceType_builder{
		Id:          "group",
		DisplayName: "Group",
		Annotations: annotations.New(&v2.TypeScopedGrants{}),
	}.Build()
	connector := &blockingTypeScopedGrantConnector{mockConnector: newMockConnector()}
	connector.rtDB = append(connector.rtDB, resourceType)

	s, err := NewSyncer(ctx, connector,
		WithC1ZPath(filepath.Join(tmpDir, "run-duration.c1z")),
		WithTmpDir(tmpDir),
		WithWorkerCount(2),
		WithRunDuration(8*time.Second),
	)
	require.NoError(t, err)
	started := time.Now()
	err = s.Sync(ctx)
	require.ErrorIs(t, err, ErrSyncNotComplete)
	require.True(t, connector.reached.Load(),
		"run duration expired before the spawned-cursor batch was reached; the duration must comfortably exceed setup time")
	require.True(t, connector.cancelled.Load(),
		"the in-flight spawned cursor call was not cancelled by the run-duration deadline")
	require.Less(t, time.Since(started), time.Minute)
	require.NoError(t, s.Close(ctx))
}

func TestRunDurationCancelsRootPlannerIO(t *testing.T) {
	// Root planning steps (entitlements/grants planning, not the
	// parallel batch workers) are a separate call-site family: both
	// receive workerCtx today, but a regression could rewire one family
	// without the other. The real run-duration timer must expire while
	// the first root store ListResources is blocked and cancel it via
	// context.AfterFunc propagation (workerCtx carries no deadline by
	// design, so cancellation is the only valid oracle). The duration is
	// deliberately generous: it must not expire before setup reaches the
	// root call on a slow CI runner — the previous 300ms raced setup and
	// failed on Windows, where setup was slower than the whole deadline.
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

	baseStore, err := dotc1z.NewStore(ctx, filepath.Join(tmpDir, "root-run-duration.c1z"),
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

	started := time.Now()
	err = s.Sync(ctx)
	require.ErrorIs(t, err, ErrSyncNotComplete)
	require.True(t, store.reached.Load(),
		"run duration expired before root planner IO was reached; the duration must comfortably exceed setup time")
	require.True(t, store.cancelled.Load(),
		"the blocked root planner IO was not cancelled by the run-duration expiry")
	require.Less(t, time.Since(started), time.Minute)
	require.NoError(t, s.Close(ctx))
}

func TestSkipSyncHonorsRunDuration(t *testing.T) {
	// Same shape as the spawned-cursor test above, for the SkipFullSync
	// path: the real timer must expire while Validate is blocked and
	// cancel it via context. The duration must comfortably exceed store
	// setup on slow CI runners or the deadline fires before Validate is
	// reached and the test asserts nothing; the wall bound only guards
	// outright hangs.
	ctx := t.Context()
	tmpDir := t.TempDir()
	connector := &blockingValidateConnector{mockConnector: newMockConnector()}
	s, err := NewSyncer(ctx, connector,
		WithC1ZPath(filepath.Join(tmpDir, "skip-run-duration.c1z")),
		WithTmpDir(tmpDir),
		WithSkipFullSync(),
		WithRunDuration(5*time.Second),
	)
	require.NoError(t, err)

	started := time.Now()
	err = s.Sync(ctx)
	require.ErrorIs(t, err, ErrSyncNotComplete)
	require.True(t, connector.blocked.Load(),
		"run duration expired before Validate was reached; the duration must comfortably exceed setup time")
	require.True(t, connector.cancelled.Load(),
		"the blocked Validate call was not cancelled by the run-duration deadline")
	require.Less(t, time.Since(started), time.Minute)
	require.NoError(t, s.Close(ctx))
}
