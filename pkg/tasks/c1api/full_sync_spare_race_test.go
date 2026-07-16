package c1api

// Regression tests for the spare-slot lifecycle race: concurrent
// full-sync handlers share ONE deterministic spare path, so a failing
// task's replay-integrity retirement can interleave with a successful
// sibling's promotion. Retirement must therefore be identity-checked —
// it may only remove the exact artifact the failing sync replayed from,
// never a fresh spare promoted underneath it — and both mutations are
// serialized by the manager's spare mutex.

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/logging"
)

// writeSpareC1Z writes a minimal finished pebble c1z at path and returns
// its sync id (readable via c1zSyncIDBestEffort — the same identity the
// production retirement compares).
func writeSpareC1Z(ctx context.Context, t *testing.T, path, tmpDir string) string {
	t.Helper()
	store, err := dotc1z.NewStore(ctx, path,
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithTmpDir(tmpDir),
	)
	require.NoError(t, err)
	syncID, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, store.EndSync(ctx))
	require.NoError(t, store.Close(ctx))
	require.Equal(t, syncID, c1zSyncIDBestEffort(path),
		"fixture: the spare's manifest header must carry the sync id the identity check reads")
	return syncID
}

func TestRetireSpareIfStillImplicated(t *testing.T) {
	ctx, err := logging.Init(context.Background())
	require.NoError(t, err)
	tmpDir := t.TempDir()
	sparePath := filepath.Join(tmpDir, "baton-previous-sync-test.c1z")

	t.Run("rotated spare survives a stale task's retirement", func(t *testing.T) {
		// Task A opens spare X, syncs for a long time. Meanwhile task B
		// finishes and promotes a FRESH spare Y onto the same path. A's
		// replay-integrity failure implicates X — not Y.
		implicatedID := writeSpareC1Z(ctx, t, sparePath, tmpDir)

		freshPath := filepath.Join(tmpDir, "fresh.c1z")
		freshID := writeSpareC1Z(ctx, t, freshPath, tmpDir)
		require.NotEqual(t, implicatedID, freshID)
		require.NoError(t, os.Rename(freshPath, sparePath)) // B's promotion

		retireSpareIfStillImplicated(ctx, sparePath, implicatedID)

		require.FileExists(t, sparePath,
			"a fresh spare promoted by a concurrent task is not implicated and must survive")
		require.Equal(t, freshID, c1zSyncIDBestEffort(sparePath))
	})

	t.Run("unrotated spare is removed", func(t *testing.T) {
		implicatedID := writeSpareC1Z(ctx, t, sparePath, tmpDir)
		retireSpareIfStillImplicated(ctx, sparePath, implicatedID)
		require.NoFileExists(t, sparePath,
			"the artifact the failing sync replayed from must be retired")
	})

	t.Run("unknown opened identity degrades to removal", func(t *testing.T) {
		// The failing sync could not identify what it opened (unreadable
		// header): conservative direction is retirement — an extra cold
		// sync is safe, replaying from an implicated spare is not.
		writeSpareC1Z(ctx, t, sparePath, tmpDir)
		retireSpareIfStillImplicated(ctx, sparePath, "")
		require.NoFileExists(t, sparePath)
	})

	t.Run("unreadable current spare degrades to removal", func(t *testing.T) {
		// The slot holds something the identity check can't read (e.g. a
		// truncated file): it can't be positively identified as a fresh
		// promotion, so it is retired.
		require.NoError(t, os.WriteFile(sparePath, []byte("not a c1z"), 0o600))
		retireSpareIfStillImplicated(ctx, sparePath, "some-sync-id")
		require.NoFileExists(t, sparePath)
	})

	t.Run("missing spare is a no-op", func(t *testing.T) {
		retireSpareIfStillImplicated(ctx, filepath.Join(tmpDir, "ghost.c1z"), "some-sync-id")
	})
}

// TestSpareRetentionEligibility pins the engine gate on spare rotation:
// only Pebble artifacts can serve source-cache replay (SQLite replay is
// an explicit non-goal), so a keep-previous-sync-c1z opt-in running on
// the DEFAULT engine (empty = SQLite) must not retain a spare — a full
// c1z of disk per rotation for a file every future sync would refuse.
func TestSpareRetentionEligibility(t *testing.T) {
	require.True(t, spareRetentionEligible(c1zstore.EnginePebble))
	require.False(t, spareRetentionEligible(c1zstore.EngineSQLite))
	require.False(t, spareRetentionEligible(""), "the empty engine resolves to the SQLite default and must not retain")

	// Engine resolution precedence: runner config > task > default.
	h := &fullSyncTaskHandler{task: fullSyncTask(t, "")}
	require.Equal(t, c1zstore.Engine(""), h.resolveStorageEngine(), "no config, no task engine: SDK default (SQLite)")
	h = &fullSyncTaskHandler{task: fullSyncTask(t, "pebble")}
	require.Equal(t, c1zstore.EnginePebble, h.resolveStorageEngine(), "task-supplied engine applies")
	h = &fullSyncTaskHandler{task: fullSyncTask(t, "sqlite"), storageEngine: c1zstore.EnginePebble}
	require.Equal(t, c1zstore.EnginePebble, h.resolveStorageEngine(), "runner config wins over the task")
}

// fullSyncTask builds a minimal full-sync task carrying storageEngine.
func fullSyncTask(t *testing.T, storageEngine string) *v1.Task {
	t.Helper()
	syncFull := &v1.Task_SyncFullTask{}
	syncFull.SetStorageEngine(storageEngine)
	return v1.Task_builder{SyncFull: syncFull}.Build()
}
