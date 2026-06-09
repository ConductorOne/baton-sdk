package dotc1z_test

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
)

func TestNewStoreConvertsExistingSQLiteToPebble(t *testing.T) {
	ctx := context.Background()

	dir := t.TempDir()
	c1zPath := filepath.Join(dir, "source.c1z")

	src, err := dotc1z.NewC1ZFile(ctx, c1zPath, dotc1z.WithTmpDir(dir))
	require.NoError(t, err)
	require.NoError(t, seedFinishedSQLiteSync(ctx, t, src))
	require.NoError(t, src.Close(ctx))

	store, err := dotc1z.NewStore(ctx, c1zPath,
		dotc1z.WithEngine(dotc1z.EnginePebble),
		dotc1z.WithTmpDir(dir),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close(ctx)) }()

	_, ok := store.(*dotc1z.C1File)
	require.False(t, ok)

	format, err := dotc1z.ReadHeaderFormat(mustOpenFile(t, c1zPath))
	require.NoError(t, err)
	require.Equal(t, dotc1z.C1ZFormatV3, format)

	md := store.Metadata()
	require.Equal(t, string(dotc1z.EnginePebble), md.Engine)
	require.Equal(t, dotc1z.C1ZFormatV3.String(), md.Format)

	rtResp, err := store.ListResourceTypes(ctx, v2.ResourceTypesServiceListResourceTypesRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, rtResp.GetList(), 2)
}

func TestNewC1ZFileRejectsPebbleEngineOnExistingSQLite(t *testing.T) {
	ctx := context.Background()

	dir := t.TempDir()
	c1zPath := filepath.Join(dir, "source.c1z")

	src, err := dotc1z.NewC1ZFile(ctx, c1zPath, dotc1z.WithTmpDir(dir))
	require.NoError(t, err)
	require.NoError(t, seedFinishedSQLiteSync(ctx, t, src))
	require.NoError(t, src.Close(ctx))

	_, err = dotc1z.NewC1ZFile(ctx, c1zPath,
		dotc1z.WithEngine(dotc1z.EnginePebble),
		dotc1z.WithTmpDir(dir),
	)
	require.Error(t, err)

	format, err := dotc1z.ReadHeaderFormat(mustOpenFile(t, c1zPath))
	require.NoError(t, err)
	require.Equal(t, dotc1z.C1ZFormatV3, format, "NewC1File conversion should land before NewC1ZFile returns")

	store, err := dotc1z.NewStore(ctx, c1zPath,
		dotc1z.WithEngine(dotc1z.EnginePebble),
		dotc1z.WithTmpDir(dir),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close(ctx)) }()

	rtResp, err := store.ListResourceTypes(ctx, v2.ResourceTypesServiceListResourceTypesRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, rtResp.GetList(), 2)
}

func TestNewC1ZFileDoesNotConvertNewSQLiteFile(t *testing.T) {
	ctx := context.Background()

	dir := t.TempDir()
	c1zPath := filepath.Join(dir, "new.c1z")

	f, err := dotc1z.NewC1ZFile(ctx, c1zPath,
		dotc1z.WithEngine(dotc1z.EnginePebble),
		dotc1z.WithTmpDir(dir),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, f.Close(ctx)) }()

	require.Equal(t, string(dotc1z.EnginePebble), f.Metadata().Engine)
}

func TestNewC1ZFileDoesNotConvertExistingSQLiteWhenEngineSQLite(t *testing.T) {
	ctx := context.Background()

	dir := t.TempDir()
	c1zPath := filepath.Join(dir, "source.c1z")

	src, err := dotc1z.NewC1ZFile(ctx, c1zPath, dotc1z.WithTmpDir(dir))
	require.NoError(t, err)
	require.NoError(t, seedFinishedSQLiteSync(ctx, t, src))
	require.NoError(t, src.Close(ctx))

	f, err := dotc1z.NewC1ZFile(ctx, c1zPath, dotc1z.WithTmpDir(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, f.Close(ctx)) }()

	format, err := dotc1z.ReadHeaderFormat(mustOpenFile(t, c1zPath))
	require.NoError(t, err)
	require.Equal(t, dotc1z.C1ZFormatV1, format)
	require.Equal(t, string(dotc1z.EngineSQLite), f.Metadata().Engine)
}

func TestNewC1ZFileReadOnlyDoesNotConvertExistingSQLite(t *testing.T) {
	ctx := context.Background()

	dir := t.TempDir()
	c1zPath := filepath.Join(dir, "source.c1z")

	src, err := dotc1z.NewC1ZFile(ctx, c1zPath, dotc1z.WithTmpDir(dir))
	require.NoError(t, err)
	require.NoError(t, seedFinishedSQLiteSync(ctx, t, src))
	require.NoError(t, src.Close(ctx))

	f, err := dotc1z.NewC1ZFile(ctx, c1zPath,
		dotc1z.WithEngine(dotc1z.EnginePebble),
		dotc1z.WithReadOnly(true),
		dotc1z.WithTmpDir(dir),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, f.Close(ctx)) }()

	format, err := dotc1z.ReadHeaderFormat(mustOpenFile(t, c1zPath))
	require.NoError(t, err)
	require.Equal(t, dotc1z.C1ZFormatV1, format)
}

func seedFinishedSQLiteSync(ctx context.Context, t *testing.T, store connectorstore.Writer) error {
	t.Helper()

	syncID, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	if err != nil {
		return err
	}
	require.NotEmpty(t, syncID)

	if err := store.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build(),
		v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build(),
	); err != nil {
		return err
	}

	for i := 0; i < 3; i++ {
		if err := store.PutResources(ctx, v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u" + strconv.Itoa(i)}.Build(),
		}.Build()); err != nil {
			return err
		}
	}

	return store.EndSync(ctx)
}

func mustOpenFile(t *testing.T, path string) *os.File {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })
	return f
}
