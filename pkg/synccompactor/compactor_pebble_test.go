package synccompactor

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
)

// buildPebbleInput writes a minimal Pebble (v3) c1z at path with the
// given sync type and one grant per id (a fixed user→member graph),
// then ends + closes it. Returns the sync id.
func buildPebbleInput(t *testing.T, ctx context.Context, path string, st connectorstore.SyncType, grantIDs ...string) string {
	t.Helper()
	require.NoError(t, ensurePebbleRegistered())

	w, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(dotc1z.EnginePebble))
	require.NoError(t, err)
	store, ok := w.(dotc1z.C1ZStore)
	require.True(t, ok)

	syncID, err := store.StartNewSync(ctx, st, "")
	require.NoError(t, err)

	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()
	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	require.NoError(t, store.PutResourceTypes(ctx, userRT, groupRT))

	group := v2.Resource_builder{
		Id:          v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(),
		DisplayName: "Group One",
	}.Build()
	user := v2.Resource_builder{
		Id:          v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(),
		DisplayName: "User One",
	}.Build()
	require.NoError(t, store.PutResources(ctx, group, user))

	member := v2.Entitlement_builder{
		Id:       "member",
		Resource: group,
		Purpose:  v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT,
	}.Build()
	require.NoError(t, store.PutEntitlements(ctx, member))

	for _, id := range grantIDs {
		g := v2.Grant_builder{Id: id, Principal: user, Entitlement: member}.Build()
		require.NoError(t, store.PutGrants(ctx, g))
	}

	require.NoError(t, store.EndSync(ctx))
	require.NoError(t, store.Close(ctx))
	return syncID
}

func verifyCompacted(t *testing.T, ctx context.Context, path, syncID string) (int, string) {
	t.Helper()
	w, err := dotc1z.NewStore(ctx, path, dotc1z.WithReadOnly(true))
	require.NoError(t, err)
	store, ok := w.(dotc1z.C1ZStore)
	require.True(t, ok)
	defer store.Close(ctx)

	require.NoError(t, store.SetCurrentSync(ctx, syncID))

	count := 0
	pageToken := ""
	for {
		resp, err := store.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{
			PageSize:  1000,
			PageToken: pageToken,
		}.Build())
		require.NoError(t, err)
		count += len(resp.GetList())
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}

	syncResp, err := store.GetLatestFinishedSync(ctx, reader_v2.SyncsReaderServiceGetLatestFinishedSyncRequest_builder{}.Build())
	require.NoError(t, err)
	return count, syncResp.GetSync().GetSyncType()
}

// TestCompactPebbleEndToEnd is the wired-in-option integration: two v3
// Pebble inputs compacted with WithEngine(EnginePebble) yield ONE
// merged Pebble sync whose grants are the union (overlapping ids
// deduped), with the union sync type.
func TestCompactPebbleEndToEnd(t *testing.T) {
	ctx := context.Background()
	inDir := t.TempDir()
	outDir := t.TempDir()

	p1 := filepath.Join(inDir, "in1.c1z")
	p2 := filepath.Join(inDir, "in2.c1z")
	s1 := buildPebbleInput(t, ctx, p1, connectorstore.SyncTypeFull, "g-shared", "g-only1")
	s2 := buildPebbleInput(t, ctx, p2, connectorstore.SyncTypePartial, "g-shared", "g-only2")

	entries := []*CompactableSync{{FilePath: p1, SyncID: s1}, {FilePath: p2, SyncID: s2}}
	c, cleanup, err := NewCompactor(ctx, outDir, entries, WithTmpDir(t.TempDir()), WithEngine(dotc1z.EnginePebble))
	require.NoError(t, err)
	defer func() { _ = cleanup() }()

	out, err := c.Compact(ctx)
	require.NoError(t, err)
	require.NotNil(t, out)

	count, st := verifyCompacted(t, ctx, out.FilePath, out.SyncID)
	require.Equal(t, 3, count, "union of {g-shared,g-only1} and {g-shared,g-only2} deduped = 3 grants")
	require.Equal(t, string(connectorstore.SyncTypeFull), st, "union of full + partial = full")
}
