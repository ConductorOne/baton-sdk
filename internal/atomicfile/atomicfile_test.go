package atomicfile

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReplaceKeepsExistingPrivateMode(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "out.c1z")
	require.NoError(t, os.WriteFile(target, []byte("old"), 0o600))
	// The old deterministic staging name, planted by another account.
	planted := filepath.Join(target+".tmp", "blocker")
	require.NoError(t, os.MkdirAll(planted, 0o755))

	f, err := Create(target)
	require.NoError(t, err)
	defer f.Cleanup()
	_, err = f.WriteString("new")
	require.NoError(t, err)
	require.NoError(t, f.CloseAtomicallyReplace())
	f.Cleanup()

	got, err := os.ReadFile(target)
	require.NoError(t, err)
	require.Equal(t, "new", string(got))
	require.DirExists(t, planted)
	require.ElementsMatch(t, []string{"out.c1z", "out.c1z.tmp"}, dirNames(t, dir))
	if runtime.GOOS != "windows" {
		fi, err := os.Stat(target)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o600), fi.Mode().Perm())
	}
}

func TestCleanupAfterFailedReplaceLeavesTargetAlone(t *testing.T) {
	dir := t.TempDir()
	// A non-empty directory at the target makes the rename fail on every
	// platform.
	target := filepath.Join(dir, "out.c1z")
	require.NoError(t, os.MkdirAll(filepath.Join(target, "blocker"), 0o755))

	f, err := Create(target)
	require.NoError(t, err)
	_, err = f.WriteString("new")
	require.NoError(t, err)
	require.Error(t, f.CloseAtomicallyReplace())
	f.Cleanup()

	require.Equal(t, []string{"out.c1z"}, dirNames(t, dir))
	require.DirExists(t, filepath.Join(target, "blocker"))
}

func dirNames(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name()
	}
	return names
}
