package c1zsanitize

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadOrGenerateSecretRequiresSecretForExistingOutput(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "output.c1z")
	require.NoError(t, os.WriteFile(out, []byte("existing"), 0o600))

	_, _, err := LoadOrGenerateSecret("", out)
	require.ErrorContains(t, err, "missing for existing output")
	require.NoFileExists(t, out+".secret")
}

func TestLoadOrGenerateSecretRejectsOrphanedDefaultSecret(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "output.c1z")
	require.NoError(t, os.WriteFile(out+".secret", make([]byte, MinSecretBytes), 0o600))

	_, _, err := LoadOrGenerateSecret("", out)
	require.ErrorContains(t, err, "exists without output")
}
