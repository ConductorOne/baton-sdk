package dotc1z

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// TestSecurity_SaveStagesExclusively pins the c1z publish path's staging
// contract. The staged file holds the complete sync artifact — PII plus the
// access graph — so before the fix a deterministic "<out>.tmp" name opened
// with O_CREATE|O_TRUNC, 0644, let any local account sharing the output
// directory (adversary 4 of the threat model, default kernel settings):
//
//   - mkdir <out>.tmp/x — fail every save deterministically (the sync
//     artifact is discarded; the maintainers' own retry test induced exactly
//     this failure), or
//   - read the finished c1z, published 0644 where the c1api handler's
//     placeholder was 0600.
//
// The staging must instead be created exclusively under an unpredictable name
// and stay private until the atomic rename.
func TestSecurity_SaveStagesExclusively(t *testing.T) {
	ctx := context.Background()

	t.Run("planted entry at the old predictable name cannot block or receive the save", func(t *testing.T) {
		dir := t.TempDir()
		c1zPath := filepath.Join(dir, "sync.c1z")

		// Squat the OLD staging name with a non-empty directory — the
		// pre-fix poison pill. The save must not touch it.
		planted := c1zPath + ".tmp"
		require.NoError(t, os.MkdirAll(filepath.Join(planted, "blocker"), 0o755))
		t.Cleanup(func() { _ = os.RemoveAll(planted) })

		store, err := NewStore(ctx, c1zPath, WithTmpDir(dir))
		require.NoError(t, err)
		t.Cleanup(func() { _ = store.Close(ctx) })
		_, err = store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)
		require.NoError(t, store.EndSync(ctx))

		require.NoError(t, store.Close(ctx), "a planted entry at <out>.tmp must not fail the save")

		// The planted directory is untouched (the save never opened it).
		fi, err := os.Stat(filepath.Join(planted, "blocker"))
		require.NoError(t, err)
		require.True(t, fi.IsDir())
		// And the published artifact exists beside it.
		_, err = os.Stat(c1zPath)
		require.NoError(t, err)
	})

	t.Run("published c1z is private, replacing a 0600 placeholder", func(t *testing.T) {
		dir := t.TempDir()
		// The production shape: c1api creates the output via os.CreateTemp
		// (0600, random suffix) in a shared directory.
		placeholder, err := os.CreateTemp(dir, "baton-sdk-sync-upload")
		require.NoError(t, err)
		require.NoError(t, placeholder.Close())
		c1zPath := placeholder.Name()

		store, err := NewStore(ctx, c1zPath, WithTmpDir(dir))
		require.NoError(t, err)
		t.Cleanup(func() { _ = store.Close(ctx) })
		_, err = store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)
		require.NoError(t, store.EndSync(ctx))
		require.NoError(t, store.Close(ctx))

		fi, err := os.Stat(c1zPath)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o600), fi.Mode().Perm(),
			"the published c1z must be no more permissive than the 0600 placeholder it replaces")
	})

	t.Run("no staging debris after a failed save", func(t *testing.T) {
		dir := t.TempDir()
		c1zPath := filepath.Join(dir, "retry.c1z")

		store, err := NewStore(ctx, c1zPath, WithTmpDir(dir))
		require.NoError(t, err)
		_, err = store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)
		require.NoError(t, store.PutGrants(ctx, mkV2Grant("g1", "ent", "user", "alice")))
		require.NoError(t, store.EndSync(ctx))

		// Break the output path so the save fails after staging exists:
		// the output's parent cannot be renamed into once it is a file's
		// target through a read-only directory.
		outDir := filepath.Join(dir, "out")
		require.NoError(t, os.MkdirAll(outDir, 0o555))
		blockedPath := filepath.Join(outDir, "sync.c1z")
		blockedStore, err := NewStore(ctx, blockedPath, WithTmpDir(dir))
		require.NoError(t, err)
		_, err = blockedStore.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)
		require.NoError(t, blockedStore.PutGrants(ctx, mkV2Grant("g1", "ent", "user", "alice")))
		require.NoError(t, blockedStore.EndSync(ctx))
		closeErr := blockedStore.Close(ctx)
		require.Error(t, closeErr, "read-only output dir must fail the save")

		// Only entries the failed save itself created may be cleaned; no
		// *.tmp-* staging debris may remain.
		entries, err := os.ReadDir(outDir)
		require.NoError(t, err)
		require.Empty(t, entries, "failed save must leave no staging debris")
	})

	t.Run("AtomicFile unit contract", func(t *testing.T) {
		dir := t.TempDir()
		out := filepath.Join(dir, "out.bin")

		af, err := NewAtomicFile(out)
		require.NoError(t, err)
		_, err = af.File.Write([]byte("payload"))
		require.NoError(t, err)
		require.NoError(t, af.Commit())
		// Commit consumed the file.
		require.Nil(t, af.File)
		require.Error(t, af.Commit())

		got, err := os.ReadFile(out)
		require.NoError(t, err)
		require.Equal(t, "payload", string(got))
		fi, err := os.Stat(out)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o600), fi.Mode().Perm())
		// No staging siblings remain.
		entries, err := os.ReadDir(dir)
		require.NoError(t, err)
		require.Len(t, entries, 1)

		// Abort removes only the file this process created.
		af2, err := NewAtomicFile(out)
		require.NoError(t, err)
		staged := af2.path
		require.NotEqual(t, filepath.Base(out)+".tmp", filepath.Base(staged), "staging name must not be the old predictable sibling")
		af2.Abort()
		af2.Abort() // idempotent
		_, err = os.Stat(staged)
		require.True(t, os.IsNotExist(err))
	})
}