package sync //nolint:revive,nolintlint // backwards-compatible package name

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/anypb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	et "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	gt "github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
)

type targetedTypeScopedConnector struct {
	*mockConnector
	entitlementCalls int
	grantCalls       int
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
	})

	token, err := st.Marshal()
	require.NoError(t, err)
	resumed := newState()
	require.NoError(t, resumed.Unmarshal(token))
	require.NotNil(t, resumed.Current())
	require.True(t, resumed.Current().Spawned)
	require.Equal(t, "chunk-1", resumed.Current().PageToken)
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
