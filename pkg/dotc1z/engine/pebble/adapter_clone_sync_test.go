package pebble_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	formatv3 "github.com/conductorone/baton-sdk/pkg/dotc1z/format/v3"
	"github.com/stretchr/testify/require"
)

// testCloneSyncRoundtrip writes a small sync to a Pebble-backed
// c1z, calls FileOps().CloneSync to materialize it at a new path,
// then re-opens the cloned file and verifies the grants land
// intact. Covers the basic byte-level copy + envelope-write path.

func TestCloneSyncRoundtripReadOnly(t *testing.T) {
	testCloneSyncRoundtrip(t, true)
}

func TestCloneSyncRoundtrip(t *testing.T) {
	testCloneSyncRoundtrip(t, false)
}

func testCloneSyncRoundtrip(t *testing.T, readOnlyClone bool) {
	ctx := context.Background()
	tmp := t.TempDir()
	srcPath := filepath.Join(tmp, "src.c1z")

	src, err := dotc1z.NewStore(ctx, srcPath, dotc1z.WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err, "NewStore src")
	syncID, err := src.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	err = src.PutGrants(ctx,
		mkV2Grant("g1", "ent-A", "user", "alice"),
		mkV2Grant("g2", "ent-B", "user", "bob"),
	)
	require.NoError(t, err, "PutGrants")
	err = src.EndSync(ctx)
	require.NoError(t, err, "EndSync")
	err = src.Close(ctx)
	require.NoError(t, err, "close src")

	openOpts := []dotc1z.C1ZOption{dotc1z.WithEngine(c1zstore.EnginePebble)}
	if readOnlyClone {
		openOpts = append(openOpts, dotc1z.WithReadOnly(true))
	}
	src, err = dotc1z.NewStore(ctx, srcPath, openOpts...)
	require.NoError(t, err, "NewStore src for clone")

	tmp2 := t.TempDir()
	clonePath := filepath.Join(tmp2, "clone.c1z")
	err = src.FileOps().CloneSync(ctx, clonePath, syncID)
	require.NoError(t, err, "CloneSync")
	err = src.Close(ctx)
	require.NoError(t, err, "close src")
	_, err = os.Stat(clonePath)
	require.NoError(t, err, "clone file missing")
	f, err := os.Open(clonePath)
	require.NoError(t, err, "open clone file")
	manifest, err := formatv3.ReadManifestHeader(f)
	// Close BEFORE the assertions: a leaked handle keeps Windows from
	// deleting clone.c1z in TempDir cleanup, which fails the test there.
	require.NoError(t, f.Close(), "close clone file")
	require.NoError(t, err, "ReadManifestHeader clone")
	runs := manifest.GetSyncRuns()
	require.Equal(t, 1, len(runs), "clone manifest sync_runs = %d, want 1", len(runs))
	require.NotNil(t, runs[0].GetStats(), "clone manifest sync_run stats is nil")

	// Re-open the clone and confirm the grants are present.
	cloneStore, err := dotc1z.NewStore(ctx, clonePath,
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithReadOnly(true),
	)
	require.NoError(t, err, "NewStore clone")
	defer cloneStore.Close(ctx)

	latest, ok := cloneStore.(connectorstore.LatestFinishedSyncIDFetcher)
	require.True(t, ok, "clone is %T, want LatestFinishedSyncIDFetcher", cloneStore)
	latestSyncID, err := latest.LatestFinishedSyncID(ctx, connectorstore.SyncTypeFull)
	require.NoError(t, err, "LatestFinishedSyncID")
	require.NotEmpty(t, latestSyncID, "clone has no finished sync")
	err = cloneStore.SetCurrentSync(ctx, latestSyncID)
	require.NoError(t, err, "SetCurrentSync")
	resp, err := cloneStore.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
	require.NoError(t, err, "ListGrants")
	require.Equal(t, 2, len(resp.GetList()), "clone ListGrants = %d, want 2", len(resp.GetList()))
}

// TestCloneSyncRefusesExistingOutPath validates the "must not
// exist" precondition on outPath — clobbering an existing file
// silently is a hazard.
func TestCloneSyncRefusesExistingOutPath(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	srcPath := filepath.Join(tmp, "src.c1z")

	src, err := dotc1z.NewStore(ctx, srcPath, dotc1z.WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	_, err = src.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	err = src.PutGrants(ctx, mkV2Grant("g1", "ent", "user", "alice"))
	require.NoError(t, err)
	err = src.EndSync(ctx)
	require.NoError(t, err)
	defer src.Close(ctx)

	existing := filepath.Join(tmp, "exists.c1z")
	err = os.WriteFile(existing, []byte("blocker"), 0o600)
	require.NoError(t, err)
	err = src.FileOps().CloneSync(ctx, existing, "")
	require.Error(t, err, "CloneSync over existing file: want error, got nil")
}

// TestCloneSyncRefusesUnfinishedSync validates the "sync must be
// ended" precondition. A still-running sync can't be cloned
// because its records may be in flux.
func TestCloneSyncRefusesUnfinishedSync(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	srcPath := filepath.Join(tmp, "src.c1z")

	src, err := dotc1z.NewStore(ctx, srcPath, dotc1z.WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	syncID, err := src.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	defer src.Close(ctx)

	// No EndSync — sync is still running.
	out := filepath.Join(tmp, "clone.c1z")
	err = src.FileOps().CloneSync(ctx, out, syncID)
	require.Error(t, err, "CloneSync of unfinished sync: want error, got nil")
}
