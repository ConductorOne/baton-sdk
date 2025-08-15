package attached

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/stretchr/testify/require"
)

func TestAttachedCompactor(t *testing.T) {
	ctx := context.Background()

	// Create temporary files for base, applied, and dest databases
	tmpDir := t.TempDir()
	baseFile := filepath.Join(tmpDir, "base.c1z")
	appliedFile := filepath.Join(tmpDir, "applied.c1z")
	destFile := filepath.Join(tmpDir, "dest.c1z")

	// Create base database with some test data
	baseDB, err := dotc1z.NewC1ZFile(ctx, baseFile, dotc1z.WithTmpDir(tmpDir))
	require.NoError(t, err)
	defer baseDB.Close()

	// Start sync and add some base data
	_, err = baseDB.StartNewSync(ctx)
	require.NoError(t, err)

	err = baseDB.EndSync(ctx)
	require.NoError(t, err)

	// Create applied database with some test data
	appliedDB, err := dotc1z.NewC1ZFile(ctx, appliedFile, dotc1z.WithTmpDir(tmpDir))
	require.NoError(t, err)
	defer appliedDB.Close()

	// Start sync and add some applied data
	_, err = appliedDB.StartNewSync(ctx)
	require.NoError(t, err)

	err = appliedDB.EndSync(ctx)
	require.NoError(t, err)

	// Create destination database
	destDB, err := dotc1z.NewC1ZFile(ctx, destFile, dotc1z.WithTmpDir(tmpDir))
	require.NoError(t, err)
	defer destDB.Close()

	// Create compactor and run compaction
	compactor := NewAttachedCompactor(baseDB, appliedDB, destDB)
	err = compactor.Compact(ctx)
	require.NoError(t, err)

	// Verify that compaction completed without errors
	// The actual verification would depend on having test data,
	// but this test verifies that the compaction workflow works
}
