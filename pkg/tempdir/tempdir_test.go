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
	require.Equal(t, os.TempDir(), got, "should fall back to os.TempDir")
}

func TestResolve_EnvVar(t *testing.T) {
	envDir := t.TempDir()
	t.Setenv(EnvVar, envDir)

	got := Resolve("")
	require.Equal(t, envDir, got, "BATON_TMPDIR should be used when no explicit dir")
}

func TestResolve_ExplicitOverridesEnvVar(t *testing.T) {
	explicit := t.TempDir()
	envDir := t.TempDir()
	t.Setenv(EnvVar, envDir)

	got := Resolve(explicit)
	require.Equal(t, explicit, got, "explicit dir should take precedence over BATON_TMPDIR")
}
