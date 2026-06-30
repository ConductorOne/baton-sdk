package dotc1z

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// seedDiffC1Z creates a c1z with one fully-backfilled, diff-capable sync that
// contains a resource type and one resource. Returns the sync ID.
func seedDiffC1Z(t *testing.T, ctx context.Context, path string) string {
	t.Helper()
	f, err := newC1ZFile(ctx, path)
	require.NoError(t, err)

	syncID, _, err := f.StartOrResumeSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	require.NoError(t, f.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "user"}.Build()))
	require.NoError(t, f.PutResources(ctx, v2.Resource_builder{
		Id:          v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(),
		DisplayName: "User One",
	}.Build()))

	require.NoError(t, f.EndSync(ctx))
	// Mark diff-capable. New syncs have grants_backfilled=1 by default.
	require.NoError(t, f.SetSupportsDiff(ctx, syncID))

	require.NoError(t, f.Close(ctx))
	return syncID
}

// TestGenerateSyncDiffFromFilesV1RoundTrip creates two SQLite c1z files,
// calls GenerateSyncDiffFromFiles, and verifies that the returned sync IDs
// are non-empty and the applied file can be opened and has the new syncs.
func TestGenerateSyncDiffFromFilesV1RoundTrip(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	basePath := filepath.Join(dir, "base.c1z")
	appliedPath := filepath.Join(dir, "applied.c1z")

	baseSync := seedDiffC1Z(t, ctx, basePath)

	// Applied has the same base data plus one extra resource.
	appliedSync := seedDiffC1Z(t, ctx, appliedPath)

	result, err := GenerateSyncDiffFromFiles(ctx, basePath, appliedPath, baseSync, appliedSync)
	require.NoError(t, err)
	require.NotEmpty(t, result.UpsertsSyncID, "UpsertsSyncID must be set")
	require.NotEmpty(t, result.DeletionsSyncID, "DeletionsSyncID must be set")
	require.NotEqual(t, result.UpsertsSyncID, result.DeletionsSyncID)

	// The applied c1z must still be openable and contain the new syncs.
	applied, err := OpenStore(ctx, appliedPath, WithReadOnly(true))
	require.NoError(t, err)
	defer func() { _ = applied.Close(ctx) }()

	resp, err := applied.ListSyncs(ctx, nil)
	require.NoError(t, err)
	syncIDs := make(map[string]bool)
	for _, s := range resp.GetSyncs() {
		syncIDs[s.GetId()] = true
	}
	require.True(t, syncIDs[result.UpsertsSyncID], "applied c1z must contain UpsertsSyncID")
	require.True(t, syncIDs[result.DeletionsSyncID], "applied c1z must contain DeletionsSyncID")
}

// TestGenerateSyncDiffFromFilesUnsupportedEngine verifies that passing a
// Pebble (v3) file returns ErrDiffEngineUnsupported.
func TestGenerateSyncDiffFromFilesUnsupportedEngine(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	sqlitePath := filepath.Join(dir, "sqlite.c1z")
	pebblePath := filepath.Join(dir, "pebble.c1z")

	_ = seedDiffC1Z(t, ctx, sqlitePath)
	writeV3EnvelopeForEngine(t, pebblePath, EnginePebble)

	_, err := GenerateSyncDiffFromFiles(ctx, pebblePath, sqlitePath, "s1", "s2")
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrDiffEngineUnsupported),
		"Pebble base must return ErrDiffEngineUnsupported; got %v", err)

	_, err = GenerateSyncDiffFromFiles(ctx, sqlitePath, pebblePath, "s1", "s2")
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrDiffEngineUnsupported),
		"Pebble applied must return ErrDiffEngineUnsupported; got %v", err)
}

// TestGenerateSyncDiffFromFilesMissingBase verifies that a missing base path
// returns ErrStoreNotFound.
func TestGenerateSyncDiffFromFilesMissingBase(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	appliedPath := filepath.Join(dir, "applied.c1z")
	appliedSync := seedDiffC1Z(t, ctx, appliedPath)

	_, err := GenerateSyncDiffFromFiles(ctx, filepath.Join(dir, "missing.c1z"), appliedPath, "s1", appliedSync)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrStoreNotFound), "missing base must return ErrStoreNotFound; got %v", err)
}

// TestGenerateSyncDiffFromFilesEmptyBase verifies that an empty base path
// returns ErrStoreEmpty.
func TestGenerateSyncDiffFromFilesEmptyBase(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	basePath := filepath.Join(dir, "empty-base.c1z")
	f, err := os.Create(basePath)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	appliedPath := filepath.Join(dir, "applied.c1z")
	appliedSync := seedDiffC1Z(t, ctx, appliedPath)

	_, err = GenerateSyncDiffFromFiles(ctx, basePath, appliedPath, "s1", appliedSync)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrStoreEmpty), "empty base must return ErrStoreEmpty; got %v", err)
}

// TestGenerateSyncDiffFromFilesDiffOptions verifies that DiffOption knobs
// (TmpDir, SyncLimit, SkipVacuum) are accepted without error.
func TestGenerateSyncDiffFromFilesDiffOptions(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	basePath := filepath.Join(dir, "base.c1z")
	appliedPath := filepath.Join(dir, "applied.c1z")

	baseSync := seedDiffC1Z(t, ctx, basePath)
	appliedSync := seedDiffC1Z(t, ctx, appliedPath)

	result, err := GenerateSyncDiffFromFiles(ctx, basePath, appliedPath, baseSync, appliedSync,
		WithDiffTmpDir(dir),
		WithDiffSyncLimit(1),
		WithDiffSkipVacuum(true),
	)
	require.NoError(t, err)
	require.NotEmpty(t, result.UpsertsSyncID)
	require.NotEmpty(t, result.DeletionsSyncID)
}
