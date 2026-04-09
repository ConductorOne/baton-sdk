package local

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSaveC1ZOverwritesViaStagingFile(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "sync.c1z")
	tmpPath := filepath.Join(tmpDir, "sync.tmp.c1z")

	err := os.WriteFile(filePath, []byte("old-data"), 0o600)
	require.NoError(t, err)
	err = os.WriteFile(tmpPath, []byte("new-data"), 0o600)
	require.NoError(t, err)

	m := &localManager{
		filePath: filePath,
		tmpPath:  tmpPath,
		tmpDir:   tmpDir,
	}

	err = m.SaveC1Z(context.Background())
	require.NoError(t, err)

	got, err := os.ReadFile(filePath)
	require.NoError(t, err)
	require.Equal(t, []byte("new-data"), got)

	matches, err := filepath.Glob(filepath.Join(tmpDir, "sync.c1z.tmp-*"))
	require.NoError(t, err)
	require.Empty(t, matches)
}

func TestSaveC1ZRemovesStagingFileOnCopyError(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "sync.c1z")
	tmpPath := filepath.Join(tmpDir, "source-dir")

	err := os.Mkdir(tmpPath, 0o700)
	require.NoError(t, err)

	m := &localManager{
		filePath: filePath,
		tmpPath:  tmpPath,
		tmpDir:   tmpDir,
	}

	err = m.SaveC1Z(context.Background())
	require.Error(t, err)

	matches, globErr := filepath.Glob(filepath.Join(tmpDir, "sync.c1z.tmp-*"))
	require.NoError(t, globErr)
	require.Empty(t, matches)
}

func TestSaveC1ZPreservesExistingDestinationOnFailure(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "sync.c1z")
	tmpPath := filepath.Join(tmpDir, "source-dir")

	err := os.WriteFile(filePath, []byte("old-data"), 0o600)
	require.NoError(t, err)
	err = os.Mkdir(tmpPath, 0o700)
	require.NoError(t, err)

	m := &localManager{
		filePath: filePath,
		tmpPath:  tmpPath,
		tmpDir:   tmpDir,
	}

	err = m.SaveC1Z(context.Background())
	require.Error(t, err)

	got, readErr := os.ReadFile(filePath)
	require.NoError(t, readErr)
	require.Equal(t, []byte("old-data"), got)
}

func TestSaveC1ZRemovesStagingFileOnRenameError(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "existing-dir")
	tmpPath := filepath.Join(tmpDir, "sync.tmp.c1z")

	err := os.Mkdir(filePath, 0o700)
	require.NoError(t, err)
	err = os.WriteFile(tmpPath, []byte("new-data"), 0o600)
	require.NoError(t, err)

	m := &localManager{
		filePath: filePath,
		tmpPath:  tmpPath,
		tmpDir:   tmpDir,
	}

	err = m.SaveC1Z(context.Background())
	require.Error(t, err)

	matches, globErr := filepath.Glob(filepath.Join(tmpDir, "existing-dir.tmp-*"))
	require.NoError(t, globErr)
	require.Empty(t, matches)
}
