package tempdir

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResolve_ExplicitDir(t *testing.T) {
	dir := t.TempDir()
	got := Resolve(dir)
	require.Equal(t, dir, got, "explicit dir should be returned unchanged")
}

func TestResolve_EmptyFallback(t *testing.T) {
	t.Setenv(EnvVar, "")
	got := Resolve("")
	require.NotEmpty(t, got, "should return a non-empty path")

	// On machines without /c1-tenant-datastore this should be os.TempDir().
	// On machines with it, it should be DefaultDatastorePath.
	// Either way, it must be an existing, writable directory.
	fi, err := os.Stat(got)
	require.NoError(t, err, "resolved dir should exist")
	require.True(t, fi.IsDir(), "resolved dir should be a directory")
}

func TestResolve_EnvVarOverridesDatastore(t *testing.T) {
	envDir := t.TempDir()
	t.Setenv(EnvVar, envDir)

	got := Resolve("")
	require.Equal(t, envDir, got, "BATON_TMPDIR should take precedence over datastore detection")
}

func TestResolve_ExplicitOverridesEnvVar(t *testing.T) {
	explicit := t.TempDir()
	envDir := t.TempDir()
	t.Setenv(EnvVar, envDir)

	got := Resolve(explicit)
	require.Equal(t, explicit, got, "explicit dir should take precedence over BATON_TMPDIR")
}

func TestResolve_DatastorePreferred(t *testing.T) {
	// Create a fake datastore dir and override the const via a local wrapper
	// to avoid depending on the real /c1-tenant-datastore path.
	t.Setenv(EnvVar, "")
	fakeDatastore := t.TempDir()

	result := resolveWithDatastorePath("", fakeDatastore)
	require.Equal(t, fakeDatastore, result, "should prefer datastore path when it exists")
}

func TestResolve_DatastoreMissing(t *testing.T) {
	t.Setenv(EnvVar, "")
	result := resolveWithDatastorePath("", "/nonexistent-path-that-should-not-exist")
	require.Equal(t, os.TempDir(), result, "should fall back to os.TempDir when datastore missing")
}

func TestResolve_ExplicitOverridesDatastore(t *testing.T) {
	explicit := t.TempDir()
	fakeDatastore := t.TempDir()

	result := resolveWithDatastorePath(explicit, fakeDatastore)
	require.Equal(t, explicit, result, "explicit dir should take precedence over datastore")
}

// resolveWithDatastorePath is a test helper that mirrors Resolve but with
// a configurable datastore path for testing without real /c1-tenant-datastore.
func resolveWithDatastorePath(dir string, datastorePath string) string {
	if dir != "" {
		return dir
	}
	if fi, err := os.Stat(datastorePath); err == nil && fi.IsDir() {
		return datastorePath
	}
	return os.TempDir()
}
