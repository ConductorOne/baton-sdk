package dotc1z

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// convert-open path tests: selectStoreDriver → convertExistingV1C1ZFile →
// ToPebble → rename → reopen-as-pebble.

func TestNewStoreConvertsNeverSyncedSQLiteToPebble(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	c1zPath := filepath.Join(dir, "never-synced.c1z")

	src, err := NewC1ZFile(ctx, c1zPath, WithTmpDir(dir), WithEngine(c1zstore.EngineSQLite))
	require.NoError(t, err)
	// Close skips save when nothing dirtied the db; force a schema-only
	// v1 envelope onto disk so convert-open has a real artifact to rewrite.
	src.dbUpdated.Store(true)
	require.NoError(t, src.Close(ctx))
	require.Equal(t, C1ZFormatV1, mustReadC1ZFormat(t, c1zPath))

	store, err := NewStore(ctx, c1zPath, WithEngine(c1zstore.EnginePebble), WithTmpDir(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close(ctx)) }()

	require.Equal(t, C1ZFormatV3, mustReadC1ZFormat(t, c1zPath))
	require.Equal(t, string(c1zstore.EnginePebble), store.Metadata().Engine)
	latest, err := store.SyncMeta().LatestFinishedSyncOfAnyType(ctx)
	require.NoError(t, err)
	require.Nil(t, latest)
}

func TestNewStoreConvertsUnfinishedOnlySQLiteToPebble(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	c1zPath := filepath.Join(dir, "unfinished-only.c1z")

	src, err := NewC1ZFile(ctx, c1zPath, WithTmpDir(dir), WithEngine(c1zstore.EngineSQLite))
	require.NoError(t, err)
	syncID, err := src.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, src.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "user"}.Build()))
	require.NoError(t, src.Close(ctx))
	require.Equal(t, C1ZFormatV1, mustReadC1ZFormat(t, c1zPath))

	store, err := NewStore(ctx, c1zPath, WithEngine(c1zstore.EnginePebble), WithTmpDir(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close(ctx)) }()

	require.Equal(t, C1ZFormatV3, mustReadC1ZFormat(t, c1zPath))
	latest, err := store.SyncMeta().LatestFinishedSyncOfAnyType(ctx)
	require.NoError(t, err)
	require.Nil(t, latest, "unfinished source sync must stay unfinished after convert-open")
	got, err := store.GetSync(ctx, reader_v2.SyncsReaderServiceGetSyncRequest_builder{SyncId: syncID}.Build())
	require.NoError(t, err)
	require.NotNil(t, got.GetSync())
	require.Nil(t, got.GetSync().GetEndedAt())
}

func mustReadC1ZFormat(t *testing.T, path string) C1ZFormat {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer func() { require.NoError(t, f.Close()) }()
	format, err := ReadHeaderFormat(f)
	require.NoError(t, err)
	return format
}
