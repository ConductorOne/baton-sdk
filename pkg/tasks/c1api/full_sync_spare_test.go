package c1api

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPromoteOrRemoveC1Z pins the spare-rotation lifecycle's safety
// properties: the disk footprint is bounded at one spare (promotion
// replaces, never accumulates), opting out always removes, and a failed
// promotion falls back to removal so nothing leaks.
func TestPromoteOrRemoveC1Z(t *testing.T) {
	ctx := context.Background()

	write := func(t *testing.T, path, contents string) {
		t.Helper()
		require.NoError(t, os.WriteFile(path, []byte(contents), 0o600))
	}

	spareName := func(dir string) string {
		return filepath.Base(previousSyncSparePath(dir, "client-a"))
	}

	t.Run("keep promotes current into the spare slot", func(t *testing.T) {
		dir := t.TempDir()
		current := filepath.Join(dir, "current.c1z")
		spare := filepath.Join(dir, spareName(dir))
		write(t, current, "current-bytes")

		promoteOrRemoveC1Z(ctx, current, spare, true)

		require.NoFileExists(t, current, "current must move, not copy")
		b, err := os.ReadFile(spare)
		require.NoError(t, err)
		require.Equal(t, "current-bytes", string(b))
	})

	t.Run("keep replaces an existing spare atomically (bounded at one)", func(t *testing.T) {
		dir := t.TempDir()
		current := filepath.Join(dir, "current.c1z")
		spare := filepath.Join(dir, spareName(dir))
		write(t, spare, "old-spare")
		write(t, current, "new-spare")

		promoteOrRemoveC1Z(ctx, current, spare, true)

		require.NoFileExists(t, current)
		b, err := os.ReadFile(spare)
		require.NoError(t, err)
		require.Equal(t, "new-spare", string(b), "old spare must be replaced by the promotion")
		entries, err := os.ReadDir(dir)
		require.NoError(t, err)
		require.Len(t, entries, 1, "exactly one file may remain after rotation")
	})

	t.Run("no keep removes current and leaves an existing spare alone", func(t *testing.T) {
		dir := t.TempDir()
		current := filepath.Join(dir, "current.c1z")
		spare := filepath.Join(dir, spareName(dir))
		write(t, spare, "old-spare")
		write(t, current, "current-bytes")

		promoteOrRemoveC1Z(ctx, current, spare, false)

		require.NoFileExists(t, current)
		b, err := os.ReadFile(spare)
		require.NoError(t, err)
		require.Equal(t, "old-spare", string(b), "opting out must not touch the spare")
	})

	t.Run("failed promotion falls back to removal (no leak)", func(t *testing.T) {
		dir := t.TempDir()
		current := filepath.Join(dir, "current.c1z")
		write(t, current, "current-bytes")
		// Spare path inside a missing directory makes the rename fail.
		spare := filepath.Join(dir, "no-such-dir", spareName(dir))

		promoteOrRemoveC1Z(ctx, current, spare, true)

		require.NoFileExists(t, current, "current must be removed when promotion fails")
	})

	t.Run("missing current is a no-op", func(t *testing.T) {
		dir := t.TempDir()
		promoteOrRemoveC1Z(ctx, filepath.Join(dir, "ghost.c1z"), filepath.Join(dir, spareName(dir)), false)
	})
}

// TestPreviousSyncSparePath pins the spare's identity properties: the
// path is deterministic (a restarted instance finds the spare its last
// upload left), namespaced by client id (instances with different
// credentials sharing a temp dir can never see each other's spare), of
// constant shape (prefix + digest + .c1z — the bound on what rotation
// may touch), and falls back to the OS temp dir the way os.CreateTemp
// does.
func TestPreviousSyncSparePath(t *testing.T) {
	dir := t.TempDir()

	a1 := previousSyncSparePath(dir, "client-a")
	a2 := previousSyncSparePath(dir, "client-a")
	b := previousSyncSparePath(dir, "client-b")

	require.Equal(t, a1, a2, "spare path must be deterministic across restarts")
	require.NotEqual(t, a1, b, "different client ids must map to different spares")
	require.Equal(t, dir, filepath.Dir(a1))

	base := filepath.Base(a1)
	require.Regexp(t, `^baton-previous-sync-[0-9a-f]{16}\.c1z$`, base,
		"spare filename must have the constant prefix+digest shape")

	fallback := previousSyncSparePath("", "client-a")
	require.Equal(t, filepath.Clean(os.TempDir()), filepath.Dir(fallback))
}
