package dotc1z_test

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// TestArtifactVerdictOnSaveFailure is the storage-verdict injection test
// (RFC 0009 §5): a save/finalize failure with uncommitted mutations must
// carry ErrArtifactUnusable out of Close, for both engines. Both engines
// write the c1z atomically (temp file + rename), so the injection plants a
// directory at the temp path — the save fails, the previous artifact (none,
// here) is untouched, and the verdict names exactly that: the output c1z
// does not reflect this run's progress.
func TestArtifactVerdictOnSaveFailure(t *testing.T) {
	for _, engine := range []c1zstore.Engine{c1zstore.EngineSQLite, c1zstore.EnginePebble} {
		t.Run(string(engine), func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			c1zPath := filepath.Join(dir, "verdict.c1z")

			store, err := dotc1z.NewStore(ctx, c1zPath, dotc1z.WithTmpDir(dir), dotc1z.WithEngine(engine))
			require.NoError(t, err)

			_, err = store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
			require.NoError(t, err)
			require.NoError(t, store.PutResourceTypes(ctx,
				v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build(),
			))

			// Plant a directory at the atomic-save temp path.
			tmpTarget := c1zPath + ".tmp"
			require.NoError(t, os.Mkdir(tmpTarget, 0o755))

			// Unconditional release: a failing require unwinds via FailNow,
			// and on Windows live engine handles fail t.TempDir()'s cleanup,
			// burying the real assertion message.
			t.Cleanup(func() {
				_ = os.Remove(tmpTarget)
				_ = store.Close(ctx)
			})

			closeErr := store.Close(ctx)
			require.Error(t, closeErr)
			require.ErrorIs(t, closeErr, dotc1z.ErrArtifactUnusable,
				"a failed save with uncommitted mutations must carry the storage verdict")

			// The verdict wrapper is message-preserving: operator-facing
			// text (and any out-of-repo log matching) stays byte-identical.
			require.NotContains(t, closeErr.Error(), dotc1z.ErrArtifactUnusable.Error())

			// "Stale, never torn": the atomic save failed before the rename,
			// so the output path holds exactly what it held before — nothing.
			require.NoFileExists(t, c1zPath,
				"a failed save must leave the previous artifact untouched")

			// Pebble stays open after a failed save (the unpacked DB is the
			// only copy of the data) and Windows cannot delete files with
			// live handles, which fails t.TempDir()'s cleanup. Closing again
			// releases the engine and pins the advertised recovery path in
			// one step. Sqlite needs no retry: it closes its handle before
			// saving and its finalize removes the working dir even on the
			// failed-save path, so nothing is held open or recoverable.
			require.NoError(t, os.Remove(tmpTarget))
			if engine == c1zstore.EnginePebble {
				require.NoError(t, store.Close(ctx),
					"pebble advertises recovery: fix the condition and Close again")
				require.FileExists(t, c1zPath)
			}
		})
	}
}

// TestArtifactVerdictAbsentOnPostSaveTeardownFailure executes the other half
// of the verdict's iff: a failure AFTER a successful save must not carry the
// verdict — the c1z on disk is a faithful commit at that point, and wrapping
// teardown errors would tell a runner to discard a good artifact. The clean-
// close test cannot pin this (its Close returns nil), so this one poisons the
// pebble store's unpacked temp dir with an unreadable, non-empty subdirectory:
// the save — which packages the sibling "checkpoint" path — succeeds, then
// the post-save os.RemoveAll of the temp dir fails.
func TestArtifactVerdictAbsentOnPostSaveTeardownFailure(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the plant relies on unix directory-permission semantics")
	}
	if os.Getuid() == 0 {
		t.Skip("root bypasses permission bits; the plant would not fire")
	}
	ctx := context.Background()
	dir := t.TempDir()
	c1zPath := filepath.Join(dir, "teardown.c1z")

	store, err := dotc1z.NewStore(ctx, c1zPath, dotc1z.WithTmpDir(dir), dotc1z.WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	_, err = store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, store.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build(),
	))
	require.NoError(t, store.EndSync(ctx))

	// The store's unpacked dir is created as c1z-pebble* under the base dir.
	matches, err := filepath.Glob(filepath.Join(dir, "c1z-pebble*"))
	require.NoError(t, err)
	require.Len(t, matches, 1, "premise: exactly one unpacked pebble dir")
	poison := filepath.Join(matches[0], "poison")
	require.NoError(t, os.Mkdir(poison, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(poison, "f"), []byte("x"), 0o600))
	require.NoError(t, os.Chmod(poison, 0o000))
	// Restore permissions so t.TempDir()'s cleanup can clear the leftovers.
	t.Cleanup(func() { _ = os.Chmod(poison, 0o700) })

	closeErr := store.Close(ctx)
	// If the plant ever broke the save itself, the verdict WOULD attach and
	// the NotErrorIs below fails loudly — the premise is self-checking.
	require.Error(t, closeErr, "premise: the post-save teardown must fail")
	require.NotErrorIs(t, closeErr, dotc1z.ErrArtifactUnusable,
		"a failure after a successful save must not carry the discard verdict")
	require.FileExists(t, c1zPath,
		"the save preceded the teardown failure: the c1z is a faithful commit")
}

// TestArtifactVerdictAbsentOnCleanClose is the negative control: a healthy
// commit carries no verdict, and the artifact it wrote reopens.
func TestArtifactVerdictAbsentOnCleanClose(t *testing.T) {
	for _, engine := range []c1zstore.Engine{c1zstore.EngineSQLite, c1zstore.EnginePebble} {
		t.Run(string(engine), func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			c1zPath := filepath.Join(dir, "clean.c1z")

			store, err := dotc1z.NewStore(ctx, c1zPath, dotc1z.WithTmpDir(dir), dotc1z.WithEngine(engine))
			require.NoError(t, err)
			_, err = store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
			require.NoError(t, err)
			require.NoError(t, store.PutResourceTypes(ctx,
				v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build(),
			))
			require.NoError(t, store.EndSync(ctx))
			require.NoError(t, store.Close(ctx))

			reopened, err := dotc1z.NewStore(ctx, c1zPath,
				dotc1z.WithTmpDir(dir), dotc1z.WithEngine(engine), dotc1z.WithReadOnly(true))
			require.NoError(t, err)
			require.NoError(t, reopened.Close(ctx))
		})
	}
}

// TestArtifactVerdictClearsOnRetriedClose pins the pebble store's advertised
// recovery contract: a failed save leaves the store open with data
// preserved; fixing the condition and closing again commits cleanly, with
// no verdict on the retried close.
func TestArtifactVerdictClearsOnRetriedClose(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	c1zPath := filepath.Join(dir, "retry.c1z")

	store, err := dotc1z.NewStore(ctx, c1zPath, dotc1z.WithTmpDir(dir), dotc1z.WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	_, err = store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, store.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build(),
	))

	tmpTarget := c1zPath + ".tmp"
	require.NoError(t, os.Mkdir(tmpTarget, 0o755))
	// Same unconditional release as above: if the assertion below fails, the
	// store stays open and Windows cannot clear t.TempDir().
	t.Cleanup(func() {
		_ = os.Remove(tmpTarget)
		_ = store.Close(ctx)
	})

	closeErr := store.Close(ctx)
	require.ErrorIs(t, closeErr, dotc1z.ErrArtifactUnusable)

	require.NoError(t, os.Remove(tmpTarget))
	require.NoError(t, store.Close(ctx))
	require.FileExists(t, c1zPath)
}
