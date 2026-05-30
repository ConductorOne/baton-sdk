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

// TestCleanupSkipVacuum verifies that WithSkipVacuum prevents the VACUUM step
// from running inside Cleanup() while keeping the old-sync delete behavior
// intact. With vacuum skipped, freed pages stay on the freelist — page_count
// stays the same and freelist_count remains non-zero. Counter-test to
// TestCleanupVacuum which asserts both go down.
//
// The test creates 3 finished syncs so DeleteSyncRun actually runs (default
// syncLimit=2 keeps the latest 2 and deletes the oldest 1). After Cleanup,
// the file is closed, re-opened, and ListSyncRuns is asserted to return 2
// entries — proving that the delete happened correctly even with vacuum off.
func TestCleanupSkipVacuum(t *testing.T) {
	ctx := t.Context()
	tmpDir := t.TempDir()

	testFilePath := filepath.Join(tmpDir, "test.c1z")

	f, err := dotc1z.NewC1ZFile(ctx, testFilePath, dotc1z.WithSkipVacuum(true))
	require.NoError(t, err)

	counts := c1ztest.C1ZCounts{
		ResourceTypeCount: 3,
		ResourceCount:     10,
		UserCount:         10,
		EntitlementCount:  10,
		GrantCount:        25,
	}
	// Three syncs > default syncLimit (2) → DeleteSyncRun will actually run during
	// Cleanup. This is what we need to verify the "delete still happens with vacuum
	// skipped" invariant; a single sync would short-circuit the delete loop.
	for range 3 {
		_, err = c1ztest.CreateTestSync(ctx, t, f, counts)
		require.NoError(t, err)
	}

	// Note: we don't assert freelist_count > 0 here pre-Cleanup. Across multiple
	// sequential syncs, SQLite reuses freelist pages from earlier syncs to satisfy
	// new INSERTs, so by the third sync the freelist may have been consumed back
	// to zero. The signal we care about is post-Cleanup: DeleteSyncRun should free
	// pages that, with vacuum skipped, stay on the freelist instead of being
	// reclaimed.
	var pageCount int
	row := f.RawDB().QueryRowContext(ctx, "PRAGMA page_count")
	require.NoError(t, row.Scan(&pageCount))
	require.Greater(t, pageCount, 0)

	err = f.Cleanup(ctx)
	require.NoError(t, err)

	// Vacuum was skipped, so page_count should NOT have decreased and freelist_count
	// should be non-zero (pages freed by DeleteSyncRun stay on the freelist).
	var afterPageCount int
	row = f.RawDB().QueryRowContext(ctx, "PRAGMA page_count")
	require.NoError(t, row.Scan(&afterPageCount))
	require.Equal(t, pageCount, afterPageCount, "page_count should be unchanged when vacuum is skipped")

	var afterFreelistCount int
	row = f.RawDB().QueryRowContext(ctx, "PRAGMA freelist_count")
	require.NoError(t, row.Scan(&afterFreelistCount))
	require.Greater(t, afterFreelistCount, 0, "freelist_count should be non-zero post-Cleanup when vacuum is skipped (proves DeleteSyncRun freed pages and they stayed)")

	err = f.Close(ctx)
	require.NoError(t, err)

	// Re-open and verify the c1z is still readable post-Cleanup (file got re-saved
	// on Close even though vacuum was skipped) and that exactly syncLimit (2) syncs
	// remain — proving DeleteSyncRun ran.
	reopened, err := dotc1z.NewC1ZFile(ctx, testFilePath)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = reopened.Close(ctx)
	})
	syncRuns, _, err := reopened.ListSyncRuns(ctx, "", 100)
	require.NoError(t, err)
	require.Len(t, syncRuns, 2, "Cleanup should have deleted the oldest sync, leaving syncLimit (2) remaining")
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

	for _, sync := range syncs {
		stats, err := f.Stats(ctx, connectorstore.SyncTypeAny, sync.ID)
		require.NoError(t, err)
		require.Equal(t, int64(3), stats["resource_types"])
		require.Equal(t, int64(10), stats["entitlements"])
	}

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
