package dotc1z_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// TestSyncMetaStatsV2Parity locks in that SyncMeta.StatsV2 returns the
// same success shape and the same error codes the v1 engine returned.
//
// Historically Pebble quietly returned empty stats (nil error) for a
// missing sync ID or a sync-type mismatch, while the v1 engine returned
// codes.NotFound / codes.InvalidArgument. Callers that branch on status
// codes depend on the latter.
func TestSyncMetaStatsV2Parity(t *testing.T) {
	ctx := context.Background()

	for _, engine := range []c1zstore.Engine{c1zstore.EnginePebble} {
		t.Run(string(engine), func(t *testing.T) {
			store, syncID := openStatsV2Fixture(t, ctx, engine)
			defer func() { _ = store.Close(ctx) }()

			meta := store.SyncMeta()

			t.Run("happy path", func(t *testing.T) {
				stats, err := meta.StatsV2(ctx, connectorstore.SyncTypeFull, syncID)
				require.NoError(t, err)
				require.NotNil(t, stats)
				require.Equal(t, int64(2), stats.GetResourceTypes())
				require.Equal(t, int64(2), stats.GetResources())
				require.Equal(t, int64(1), stats.GetEntitlements())
				require.Equal(t, int64(1), stats.GetGrants())
				require.Equal(t, int64(1), stats.GetAssets())
				require.Equal(t, int64(1), stats.GetResourcesByResourceType()["user"])
				require.Equal(t, int64(1), stats.GetResourcesByResourceType()["app"])
				require.Equal(t, int64(1), stats.GetEntitlementsByResourceType()["app"])
				require.Equal(t, int64(1), stats.GetGrantsByResourceType()["app"])
			})

			t.Run("SyncTypeAny accepts full sync", func(t *testing.T) {
				stats, err := meta.StatsV2(ctx, connectorstore.SyncTypeAny, syncID)
				require.NoError(t, err)
				require.NotNil(t, stats)
				require.Equal(t, int64(1), stats.GetGrants())
			})

			t.Run("wrong sync type returns InvalidArgument", func(t *testing.T) {
				stats, err := meta.StatsV2(ctx, connectorstore.SyncTypePartial, syncID)
				require.Nil(t, stats)
				require.Error(t, err)
				require.Equal(t, codes.InvalidArgument, status.Code(err),
					"wrong sync type: got status %v (%v), want InvalidArgument", status.Code(err), err)
				require.Equal(t,
					status.Errorf(codes.InvalidArgument, "sync '%s' is not of type '%s'", syncID, connectorstore.SyncTypePartial).Error(),
					err.Error(),
				)
			})

			t.Run("sync not found returns NotFound", func(t *testing.T) {
				missingID := "sync_does_not_exist"
				stats, err := meta.StatsV2(ctx, connectorstore.SyncTypeAny, missingID)
				require.Nil(t, stats)
				require.Error(t, err)
				require.Equal(t, codes.NotFound, status.Code(err),
					"missing sync: got status %v (%v), want NotFound", status.Code(err), err)
			})

			t.Run("empty syncID uses latest finished sync", func(t *testing.T) {
				explicit, err := meta.StatsV2(ctx, connectorstore.SyncTypeFull, syncID)
				require.NoError(t, err)
				empty, err := meta.StatsV2(ctx, connectorstore.SyncTypeFull, "")
				require.NoError(t, err)
				require.True(t, proto.Equal(explicit, empty),
					"empty syncID must resolve to the finished sync\nexplicit=%v\nempty=%v", explicit, empty)
			})

			t.Run("empty syncID with no finished sync returns NotFound", func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "open-only.c1z")
				openStore, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(engine))
				require.NoError(t, err)
				defer func() { _ = openStore.Close(ctx) }()

				_, err = openStore.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
				require.NoError(t, err)

				stats, err := openStore.SyncMeta().StatsV2(ctx, connectorstore.SyncTypeFull, "")
				require.Nil(t, stats)
				require.Equal(t, codes.NotFound, status.Code(err),
					"empty syncID with only an in-progress sync: got %v (%v)", status.Code(err), err)
			})
		})
	}
}

// TestSyncMetaStatsV2InProgressExplicitIDReturnsPartialStats locks in that
// naming an in-progress sync ID returns live counts of whatever has been
// written so far — not NotFound, and not a stale/empty snapshot. Empty
// syncID still refuses to bind to an in-progress sync (see
// TestSyncMetaStatsV2EmptyPrefersFinishedOverInProgress).
func TestSyncMetaStatsV2InProgressExplicitIDReturnsPartialStats(t *testing.T) {
	ctx := context.Background()

	for _, engine := range []c1zstore.Engine{c1zstore.EnginePebble} {
		t.Run(string(engine), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), string(engine)+"-partial.c1z")
			store, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(engine))
			require.NoError(t, err)
			defer func() { _ = store.Close(ctx) }()

			syncID, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
			require.NoError(t, err)

			require.NoError(t, store.PutResourceTypes(ctx,
				v2.ResourceType_builder{Id: "user"}.Build(),
				v2.ResourceType_builder{Id: "group"}.Build(),
			))
			require.NoError(t, store.PutResources(ctx,
				v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build()}.Build(),
				v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u2"}.Build()}.Build(),
				v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build()}.Build(),
			))
			// Deliberately no EndSync — sync is still in progress.

			stats, err := store.SyncMeta().StatsV2(ctx, connectorstore.SyncTypeFull, syncID)
			require.NoError(t, err)
			require.NotNil(t, stats)
			require.Equal(t, int64(2), stats.GetResourceTypes())
			require.Equal(t, int64(3), stats.GetResources())
			require.Equal(t, int64(2), stats.GetResourcesByResourceType()["user"])
			require.Equal(t, int64(1), stats.GetResourcesByResourceType()["group"])
			require.Equal(t, int64(0), stats.GetEntitlements())
			require.Equal(t, int64(0), stats.GetGrants())

			// Empty syncID must still refuse the in-progress sync.
			_, err = store.SyncMeta().StatsV2(ctx, connectorstore.SyncTypeFull, "")
			require.Equal(t, codes.NotFound, status.Code(err))
		})
	}
}

// TestSyncMetaStatsV2EmptyPrefersFinishedOverInProgress locks in that an
// empty syncID never binds to the in-progress current sync. Pebble replaces
// the finished sync on StartNewSync, so the empty-ID path returns NotFound
// once only an in-progress sync remains.
func TestSyncMetaStatsV2EmptyPrefersFinishedOverInProgress(t *testing.T) {
	ctx := context.Background()

	t.Run("pebble", func(t *testing.T) {
		store, _ := openStatsV2Fixture(t, ctx, c1zstore.EnginePebble)
		defer func() { _ = store.Close(ctx) }()

		_, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)

		stats, err := store.SyncMeta().StatsV2(ctx, connectorstore.SyncTypeFull, "")
		require.Nil(t, stats)
		require.Equal(t, codes.NotFound, status.Code(err),
			"pebble single-sync: StartNewSync replaces the finished sync, so empty syncID must NotFound")
	})
}

func openStatsV2Fixture(t *testing.T, ctx context.Context, engine c1zstore.Engine) (c1zstore.Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), string(engine)+".c1z")
	store, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(engine))
	require.NoError(t, err)

	syncID, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	require.NoError(t, store.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "user", DisplayName: "User", Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER}}.Build(),
		v2.ResourceType_builder{Id: "app", DisplayName: "App", Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_APP}}.Build(),
	))

	user := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(),
	}.Build()
	app := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "app", Resource: "gh"}.Build(),
	}.Build()
	require.NoError(t, store.PutResources(ctx, user, app))

	ent := v2.Entitlement_builder{
		Id:       "ent-member",
		Resource: app,
		Purpose:  v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT,
		Slug:     "member",
	}.Build()
	require.NoError(t, store.PutEntitlements(ctx, ent))

	require.NoError(t, store.PutGrants(ctx, v2.Grant_builder{
		Id:          "g1",
		Entitlement: ent,
		Principal:   user,
	}.Build()))
	require.NoError(t, store.PutAsset(ctx, v2.AssetRef_builder{Id: "asset-1"}.Build(), "text/plain", []byte("hello")))
	require.NoError(t, store.EndSync(ctx))

	return store, syncID
}
