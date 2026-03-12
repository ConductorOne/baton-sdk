package dotc1z_test

import (
	"path/filepath"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1ztest"
	"github.com/stretchr/testify/require"
)

func TestCleanupVacuum(t *testing.T) {
	ctx := t.Context()
	tmpDir := t.TempDir()

	testFilePath := filepath.Join(tmpDir, "test.c1z")

	f, err := dotc1z.NewC1ZFile(ctx, testFilePath)
	require.NoError(t, err)

	_, err = c1ztest.CreateTestSync(ctx, t, f, c1ztest.C1ZCounts{
		ResourceTypeCount: 3,
		ResourceCount:     10,
		UserCount:         10,
		EntitlementCount:  10,
		GrantCount:        25,
	})
	require.NoError(t, err)

	var pageCount int
	row := f.RawDB().QueryRowContext(ctx, "PRAGMA page_count")
	require.NoError(t, row.Scan(&pageCount))
	require.Greater(t, pageCount, 0)

	var freelistCount int
	row = f.RawDB().QueryRowContext(ctx, "PRAGMA freelist_count")
	require.NoError(t, row.Scan(&freelistCount))
	require.Greater(t, freelistCount, 0)

	err = f.Cleanup(ctx)
	require.NoError(t, err)

	// Vacuum should have run, so page_count should be lower and freelist_count should be zero.
	var cleanupPageCount int
	row = f.RawDB().QueryRowContext(ctx, "PRAGMA page_count")
	require.NoError(t, row.Scan(&cleanupPageCount))
	require.Less(t, cleanupPageCount, pageCount, "page_count should be lower")

	var cleanupFreelistCount int
	row = f.RawDB().QueryRowContext(ctx, "PRAGMA freelist_count")
	require.NoError(t, row.Scan(&cleanupFreelistCount))
	require.Equal(t, 0, cleanupFreelistCount, "freelist_count should be zero")

	// Close the file.
	err = f.Close(ctx)
	require.NoError(t, err)
}

func TestCleanupVacuumWAL(t *testing.T) {
	ctx := t.Context()
	tmpDir := t.TempDir()

	testFilePath := filepath.Join(tmpDir, "test.c1z")

	f, err := dotc1z.NewC1ZFile(ctx, testFilePath, dotc1z.WithPragma("journal_mode", "WAL"))
	require.NoError(t, err)

	_, err = c1ztest.CreateTestSync(ctx, t, f, c1ztest.C1ZCounts{
		ResourceTypeCount: 3,
		ResourceCount:     10,
		UserCount:         10,
		EntitlementCount:  10,
		GrantCount:        25,
	})
	require.NoError(t, err)

	var pageCount int
	row := f.RawDB().QueryRowContext(ctx, "PRAGMA page_count")
	require.NoError(t, row.Scan(&pageCount))
	require.Greater(t, pageCount, 0)

	var freelistCount int
	row = f.RawDB().QueryRowContext(ctx, "PRAGMA freelist_count")
	require.NoError(t, row.Scan(&freelistCount))
	require.Greater(t, freelistCount, 0)

	err = f.Cleanup(ctx)
	require.NoError(t, err)

	// Vacuum should have run, so page_count and freelist_count should be lower.
	var cleanupPageCount int
	row = f.RawDB().QueryRowContext(ctx, "PRAGMA page_count")
	require.NoError(t, row.Scan(&cleanupPageCount))
	require.Less(t, cleanupPageCount, pageCount, "page_count should be lower")

	var cleanupFreelistCount int
	row = f.RawDB().QueryRowContext(ctx, "PRAGMA freelist_count")
	require.NoError(t, row.Scan(&cleanupFreelistCount))
	require.Equal(t, 0, cleanupFreelistCount, "freelist_count should be zero")

	// Close the file.
	err = f.Close(ctx)
	require.NoError(t, err)
}

func TestCleanupSyncLimit(t *testing.T) {
	ctx := t.Context()
	tmpDir := t.TempDir()

	testFilePath := filepath.Join(tmpDir, "test.c1z")

	f, err := dotc1z.NewC1ZFile(ctx, testFilePath)
	require.NoError(t, err)

	for range 10 {
		_, err = c1ztest.CreateTestSync(ctx, t, f, c1ztest.C1ZCounts{
			ResourceTypeCount: 3,
			ResourceCount:     10,
			UserCount:         10,
			EntitlementCount:  10,
			GrantCount:        25,
		})
		require.NoError(t, err)
	}

	err = f.Cleanup(ctx)
	require.NoError(t, err)

	// Check that we only have two syncs left.
	syncs, _, err := f.ListSyncRuns(ctx, "", 100)
	require.NoError(t, err)
	require.Len(t, syncs, 2)

	// Close the file.
	err = f.Close(ctx)
	require.NoError(t, err)
}

func TestCleanupSyncLimitCurrentSync(t *testing.T) {
	ctx := t.Context()
	tmpDir := t.TempDir()

	testFilePath := filepath.Join(tmpDir, "test.c1z")

	f, err := dotc1z.NewC1ZFile(ctx, testFilePath, dotc1z.WithSyncLimit(1))
	require.NoError(t, err)

	for range 10 {
		_, err = c1ztest.CreateTestSync(ctx, t, f, c1ztest.C1ZCounts{
			ResourceTypeCount: 3,
			ResourceCount:     10,
			UserCount:         10,
			EntitlementCount:  10,
			GrantCount:        25,
		})
		require.NoError(t, err)
	}

	syncID, err := f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	err = f.Cleanup(ctx)
	require.NoError(t, err)

	// Check that we only have two syncs left.
	syncs, _, err := f.ListSyncRuns(ctx, "", 100)
	require.NoError(t, err)
	require.Len(t, syncs, 1)
	require.Equal(t, syncID, syncs[0].ID)

	// Close the file.
	err = f.Close(ctx)
	require.NoError(t, err)
}
