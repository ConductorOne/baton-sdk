package dotc1z_test

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
)

// TestToPebbleRoundTrip seeds a SQLite .c1z with a finished full sync
// (resource types, resources, entitlements, grants, and an asset), converts it
// to a v3/Pebble .c1z via ToPebble, and asserts the converted store reads back
// the same data.
func TestToPebbleRoundTrip(t *testing.T) {
	ctx := context.Background()
	require.NoError(t, pebble.Register())

	dir := t.TempDir()
	srcPath := filepath.Join(dir, "source.c1z")

	src, err := dotc1z.NewC1ZFile(ctx, srcPath, dotc1z.WithTmpDir(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, src.Close(ctx)) }()

	syncID, err := src.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	require.NoError(t, src.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build(),
		v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build(),
	))

	const userCount = 25
	users := make([]*v2.Resource, userCount)
	for i := 0; i < userCount; i++ {
		users[i] = v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u" + strconv.Itoa(i)}.Build(),
		}.Build()
	}
	group := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(),
	}.Build()
	require.NoError(t, src.PutResources(ctx, append(users, group)...))

	ent := v2.Entitlement_builder{Id: "ent1", Resource: group}.Build()
	require.NoError(t, src.PutEntitlements(ctx, ent))

	grants := make([]*v2.Grant, userCount)
	for i := 0; i < userCount; i++ {
		grants[i] = v2.Grant_builder{
			Id:          "grant-" + strconv.Itoa(i),
			Entitlement: ent,
			Principal:   users[i],
		}.Build()
	}
	require.NoError(t, src.PutGrants(ctx, grants...))

	assetData := []byte("hello-asset-bytes")
	require.NoError(t, src.PutAsset(ctx, v2.AssetRef_builder{Id: "asset-1"}.Build(), "text/plain", assetData))

	require.NoError(t, src.EndSync(ctx))

	// Convert the finished sync into a new Pebble .c1z.
	outPath := filepath.Join(dir, "out.c1z")
	stats, err := src.ToPebble(ctx, outPath, syncID)
	require.NoError(t, err)
	require.Equal(t, syncID, stats.SourceSyncID)
	require.NotEmpty(t, stats.DestSyncID)
	require.Equal(t, int64(2), stats.ResourceTypes.Rows)
	require.Equal(t, int64(userCount+1), stats.Resources.Rows)
	require.Equal(t, int64(1), stats.Entitlements.Rows)
	require.Equal(t, int64(userCount), stats.Grants.Rows)
	require.Equal(t, int64(1), stats.Assets.Rows)
	require.Equal(t, int64(len(assetData)), stats.AssetBytes)

	// Open the converted Pebble store and verify the data round-tripped.
	dst, err := dotc1z.NewStore(ctx, outPath, dotc1z.WithEngine(dotc1z.EnginePebble), dotc1z.WithTmpDir(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, dst.Close(ctx)) }()
	require.NoError(t, dst.SetCurrentSync(ctx, stats.DestSyncID))

	rtResp, err := dst.ListResourceTypes(ctx, v2.ResourceTypesServiceListResourceTypesRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, rtResp.GetList(), 2)

	resCount := countResources(ctx, t, dst)
	require.Equal(t, userCount+1, resCount)

	entResp, err := dst.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, entResp.GetList(), 1)

	grantCount := countGrants(ctx, t, dst)
	require.Equal(t, userCount, grantCount)

	// Verify the fast grant copy path populated Pebble's secondary indexes, not
	// just the primary grant keyspace.
	resourceFiltered, err := dst.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{
		Resource: group,
	}.Build())
	require.NoError(t, err)
	require.Len(t, resourceFiltered.GetList(), userCount)

	byEntitlement, err := dst.ListGrantsForEntitlement(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
		Entitlement: v2.Entitlement_builder{Id: "ent1"}.Build(),
		PageSize:    1000,
	}.Build())
	require.NoError(t, err)
	require.Len(t, byEntitlement.GetList(), userCount)

	byPrincipalRT, err := dst.ListGrantsForResourceType(ctx, reader_v2.GrantsReaderServiceListGrantsForResourceTypeRequest_builder{
		ResourceTypeId: "user",
		PageSize:       1000,
	}.Build())
	require.NoError(t, err)
	require.Len(t, byPrincipalRT.GetList(), userCount)

	contentType, r, err := dst.GetAsset(ctx, v2.AssetServiceGetAssetRequest_builder{
		Asset: v2.AssetRef_builder{Id: "asset-1"}.Build(),
	}.Build())
	require.NoError(t, err)
	require.Equal(t, "text/plain", contentType)
	gotData, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, assetData, gotData)
}

// TestToPebbleErrors exercises ToPebble's guard clauses: output path must not
// exist, the sync must exist, and the sync must be ended.
func TestToPebbleErrors(t *testing.T) {
	ctx := context.Background()
	require.NoError(t, pebble.Register())

	dir := t.TempDir()
	src, err := dotc1z.NewC1ZFile(ctx, filepath.Join(dir, "source.c1z"), dotc1z.WithTmpDir(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, src.Close(ctx)) }()

	// A finished full sync to use for the "output exists" case.
	syncID, err := src.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, src.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "user"}.Build()))
	require.NoError(t, src.EndSync(ctx))

	t.Run("output path exists", func(t *testing.T) {
		outPath := filepath.Join(dir, "exists.c1z")
		require.NoError(t, os.WriteFile(outPath, []byte("x"), 0600))
		_, err := src.ToPebble(ctx, outPath, syncID)
		require.Error(t, err)
	})

	t.Run("sync not found", func(t *testing.T) {
		_, err := src.ToPebble(ctx, filepath.Join(dir, "not-found.c1z"), "nonexistent-sync-id")
		require.Error(t, err)
	})

	t.Run("sync not ended", func(t *testing.T) {
		unfinished, err := src.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)
		_, err = src.ToPebble(ctx, filepath.Join(dir, "unfinished.c1z"), unfinished)
		require.Error(t, err)
	})
}

func countResources(ctx context.Context, t *testing.T, store connectorstore.Reader) int {
	t.Helper()
	total := 0
	pageToken := ""
	for {
		resp, err := store.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{PageToken: pageToken}.Build())
		require.NoError(t, err)
		total += len(resp.GetList())
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			return total
		}
	}
}

func countGrants(ctx context.Context, t *testing.T, store connectorstore.Reader) int {
	t.Helper()
	total := 0
	pageToken := ""
	for {
		resp, err := store.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{PageToken: pageToken}.Build())
		require.NoError(t, err)
		total += len(resp.GetList())
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			return total
		}
	}
}
