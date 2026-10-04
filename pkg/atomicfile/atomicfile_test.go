package atomicfile

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestAtomicFileContract pins the helper's own behavior: exclusive creation,
// private mode after publish, atomic rename, and failure cleanup that only
// ever removes the file this process created.
func TestAtomicFileContract(t *testing.T) {
	t.Run("commit publishes atomically and privately", func(t *testing.T) {
		dir := t.TempDir()
		out := filepath.Join(dir, "out.bin")

		af, err := New(out)
		require.NoError(t, err)
		_, err = af.File.Write([]byte("payload"))
		require.NoError(t, err)
		require.NoError(t, af.Commit())
		// Commit consumed the file handle.
		require.Nil(t, af.File)
		require.Error(t, af.Commit(), "second Commit must fail")

		got, err := os.ReadFile(out)
		require.NoError(t, err)
		require.Equal(t, "payload", string(got))
		fi, err := os.Stat(out)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o600), fi.Mode().Perm(), "published file must be private")
		// No staging siblings remain.
		entries, err := os.ReadDir(dir)
		require.NoError(t, err)
		require.Len(t, entries, 1)
	})

	t.Run("staging name is not the predictable sibling", func(t *testing.T) {
		dir := t.TempDir()
		out := filepath.Join(dir, "out.bin")
		af, err := New(out)
		require.NoError(t, err)
		require.NotEqual(t, filepath.Base(out)+".tmp", filepath.Base(af.path),
			"staging name must not be guessable from the output name")
		af.Abort()
	})

	t.Run("abort removes only this process's file", func(t *testing.T) {
		dir := t.TempDir()
		out := filepath.Join(dir, "out.bin")

		af, err := New(out)
		require.NoError(t, err)
		staged := af.path
		af.Abort()
		af.Abort() // idempotent
		require.Nil(t, af.File)
		_, err = os.Stat(staged)
		require.True(t, os.IsNotExist(err))

		// A pre-existing foreign entry is never touched by Abort: it only
		// removes the staged file New created, whose name no other entry
		// shares (exclusive CreateTemp).
		foreign := filepath.Join(dir, "foreign")
		require.NoError(t, os.WriteFile(foreign, []byte("x"), 0o600))
		af2, err := New(out)
		require.NoError(t, err)
		af2.Abort()
		_, err = os.Stat(foreign)
		require.NoError(t, err, "Abort must not remove entries it did not create")
	})
}
