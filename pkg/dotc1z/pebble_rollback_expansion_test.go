package dotc1z

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

func buildPebbleRollbackFixture(t *testing.T, ctx context.Context, path string) string {
	t.Helper()
	store, err := NewStore(ctx, path, WithEngine(c1zstore.EnginePebble), WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	syncID, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	immutable, err := anypb.New(v2.GrantImmutable_builder{}.Build())
	require.NoError(t, err)
	derived := mkV2Grant("", "ent-A", "user", "alice")
	derived.SetAnnotations([]*anypb.Any{immutable})
	derived.SetSources(v2.GrantSources_builder{Sources: map[string]*v2.GrantSources_GrantSource{
		"app:github:ent-B": v2.GrantSources_GrantSource_builder{}.Build(),
	}}.Build())
	require.NoError(t, store.PutGrants(ctx, derived, mkV2Grant("", "ent-B", "user", "alice")))
	require.NoError(t, store.EndSync(ctx))
	require.NoError(t, store.Close(ctx))
	return syncID
}

func pebbleGrantIDs(t *testing.T, ctx context.Context, path, syncID string) []string {
	t.Helper()
	ro, err := NewStore(ctx, path, WithReadOnly(true), WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	defer func() { require.NoError(t, ro.Close(ctx)) }()
	var ids []string
	for g, err := range ro.(connectorstore.StreamingReader).StreamGrants(ctx, syncID, connectorstore.StreamGrantsOptions{}) {
		require.NoError(t, err)
		ids = append(ids, g.GetId())
	}
	return ids
}

func TestPebbleStoreRollbackExpansionPersistsWithoutReseal(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "rollback.c1z")
	syncID := buildPebbleRollbackFixture(t, ctx, path)

	store, err := NewStore(ctx, path, WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	res, err := store.(*pebbleStore).RollbackExpansion(ctx, syncID, false)
	require.NoError(t, err)
	require.Equal(t, 1, res.GrantsDeleted)
	require.NoError(t, store.Close(ctx))

	require.Equal(t, []string{mkV2GrantID("ent-B", "user", "alice")}, pebbleGrantIDs(t, ctx, path, syncID))
}

func TestPebbleStoreRollbackExpansionDryRunAndRefusalLeaveFile(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "rollback.c1z")
	syncID := buildPebbleRollbackFixture(t, ctx, path)
	before, err := os.ReadFile(path)
	require.NoError(t, err)

	store, err := NewStore(ctx, path, WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	res, err := store.(*pebbleStore).RollbackExpansion(ctx, syncID, true)
	require.NoError(t, err)
	require.True(t, res.DryRun)
	_, err = store.(*pebbleStore).RollbackExpansion(ctx, "no-such-sync", false)
	require.Error(t, err)
	require.NoError(t, store.Close(ctx))

	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, before, after)
}
