package local

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	enginepkg "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	"github.com/conductorone/baton-sdk/pkg/synccompactor"
)

func TestLocalCompactorPassesStorageEngine(t *testing.T) {
	ctx := context.Background()
	inDir := t.TempDir()
	outDir := t.TempDir()

	p1 := filepath.Join(inDir, "in1.c1z")
	p2 := filepath.Join(inDir, "in2.c1z")
	s1 := buildLocalCompactorSQLiteInput(t, ctx, p1, connectorstore.SyncTypeFull, "g-shared", "g-only1")
	s2 := buildLocalCompactorSQLiteInput(t, ctx, p2, connectorstore.SyncTypePartial, "g-shared", "g-only2")

	mgr := NewLocalCompactor(ctx, outDir, []*synccompactor.CompactableSync{
		{FilePath: p1, SyncID: s1},
		{FilePath: p2, SyncID: s2},
	}, t.TempDir(), WithCompactorStorageEngine(dotc1z.EnginePebble))

	require.NoError(t, mgr.Process(ctx, nil, nil))

	entries, err := os.ReadDir(outDir)
	require.NoError(t, err)
	require.Len(t, entries, 1)

	store, err := dotc1z.NewStore(ctx, filepath.Join(outDir, entries[0].Name()), dotc1z.WithReadOnly(true))
	require.NoError(t, err)
	defer func() { _ = store.Close(ctx) }()

	_, ok := enginepkg.AsEngine(store)
	require.True(t, ok, "local compactor must pass requested pebble engine to synccompactor")
}

func buildLocalCompactorSQLiteInput(t *testing.T, ctx context.Context, path string, st connectorstore.SyncType, grantIDs ...string) string {
	t.Helper()

	store, err := dotc1z.NewC1ZFile(ctx, path)
	require.NoError(t, err)

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
		grant := v2.Grant_builder{
			Id:          id,
			Principal:   user,
			Entitlement: member,
		}.Build()
		require.NoError(t, store.PutGrants(ctx, grant))
	}

	require.NoError(t, store.EndSync(ctx))
	require.NoError(t, store.Close(ctx))
	return syncID
}
