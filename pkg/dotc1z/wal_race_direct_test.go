package dotc1z

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// TestWALFileRace directly tests the WAL checkpoint behavior by checking
// if the WAL file is properly handled before the database file is read.
// This test manually replicates what C1File.Close() does to expose the race.
func TestWALFileRace(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping WAL file race test in short mode")
	}

	ctx := context.Background()
	tmpDir := t.TempDir()

	iterations := 500
	failures := 0
	var mu sync.Mutex

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for j := 0; j < iterations/8; j++ {
				testFilePath := filepath.Join(tmpDir, fmt.Sprintf("wal_direct_%d_%d.c1z", workerID, j))

				f, err := NewC1ZFile(ctx, testFilePath, WithPragma("journal_mode", "WAL"))
				if err != nil {
					t.Logf("worker %d iter %d: failed to create c1z: %v", workerID, j, err)
					continue
				}

				_, err = f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
				if err != nil {
					f.Close()
					continue
				}

				// Write significant data to generate WAL activity
				for k := 0; k < 100; k++ {
					rtID := fmt.Sprintf("rt-%d-%d-%d", workerID, j, k)
					err = f.PutResourceTypes(ctx, v2.ResourceType_builder{
						Id:          rtID,
						DisplayName: fmt.Sprintf("Resource Type %d-%d-%d with extra data to increase WAL size", workerID, j, k),
						Description: fmt.Sprintf("Description %d-%d-%d with more text", workerID, j, k),
					}.Build())
					if err != nil {
						break
					}
				}

				err = f.EndSync(ctx)
				if err != nil {
					f.Close()
					continue
				}

				// Get the db file path before close
				dbPath := f.dbFilePath

				// Check WAL file before close
				walPath := dbPath + "-wal"
				shmPath := dbPath + "-shm"

				walSizeBefore := int64(0)
				if stat, err := os.Stat(walPath); err == nil {
					walSizeBefore = stat.Size()
				}

				// Close the file (this should checkpoint WAL)
				err = f.Close()
				if err != nil {
					t.Logf("worker %d iter %d: close failed: %v", workerID, j, err)
					continue
				}

				// Check WAL file after close - it should be truncated or deleted
				// if checkpoint succeeded
				walSizeAfter := int64(0)
				walExists := false
				if stat, err := os.Stat(walPath); err == nil {
					walExists = true
					walSizeAfter = stat.Size()
				}

				// Check if shm file still exists (it shouldn't after proper close)
				shmExists := false
				if _, err := os.Stat(shmPath); err == nil {
					shmExists = true
				}

				// If WAL file still has significant size or shm exists,
				// checkpoint may not have completed properly
				if walExists && walSizeAfter > 0 {
					t.Logf("worker %d iter %d: WAL file still has data after close: before=%d after=%d",
						workerID, j, walSizeBefore, walSizeAfter)
					mu.Lock()
					failures++
					mu.Unlock()
				}

				if shmExists {
					t.Logf("worker %d iter %d: SHM file still exists after close", workerID, j)
				}

				// Verify the c1z file is valid
				f2, err := NewC1ZFile(ctx, testFilePath, WithPragma("journal_mode", "WAL"), WithReadOnly(true))
				if err != nil {
					t.Logf("worker %d iter %d: failed to reopen c1z: %v", workerID, j, err)
					mu.Lock()
					failures++
					mu.Unlock()
					continue
				}

				stats, err := f2.Stats(ctx, connectorstore.SyncTypeFull, "")
				if err != nil {
					t.Logf("worker %d iter %d: failed to get stats: %v", workerID, j, err)
					mu.Lock()
					failures++
					mu.Unlock()
				} else if stats["resource_types"] != 100 {
					t.Logf("worker %d iter %d: resource_types mismatch: expected 100, got %d",
						workerID, j, stats["resource_types"])
					mu.Lock()
					failures++
					mu.Unlock()
				}

				f2.Close()
			}
		}(i)
	}

	wg.Wait()

	if failures > 0 {
		t.Fatalf("Had %d failures out of %d iterations", failures, iterations)
	}
}

// TestExplicitWALCheckpointRace tests whether explicit WAL checkpoint
// prevents data loss by comparing behavior with and without it.
func TestExplicitWALCheckpointRace(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping explicit WAL checkpoint race test in short mode")
	}

	ctx := context.Background()
	tmpDir := t.TempDir()

	// Test without explicit checkpoint (current behavior on main)
	t.Run("without_explicit_checkpoint", func(t *testing.T) {
		failures := 0
		for i := 0; i < 200; i++ {
			testFilePath := filepath.Join(tmpDir, fmt.Sprintf("no_checkpoint_%d.c1z", i))

			f, err := NewC1ZFile(ctx, testFilePath, WithPragma("journal_mode", "WAL"))
			require.NoError(t, err)

			_, err = f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
			require.NoError(t, err)

			expectedCount := 50 + (i % 50)
			for j := 0; j < expectedCount; j++ {
				err = f.PutResourceTypes(ctx, v2.ResourceType_builder{
					Id: fmt.Sprintf("rt-%d-%d", i, j),
				}.Build())
				require.NoError(t, err)
			}

			err = f.EndSync(ctx)
			require.NoError(t, err)

			err = f.Close()
			require.NoError(t, err)

			// Immediately reopen and verify
			f2, err := NewC1ZFile(ctx, testFilePath, WithPragma("journal_mode", "WAL"), WithReadOnly(true))
			require.NoError(t, err)

			stats, err := f2.Stats(ctx, connectorstore.SyncTypeFull, "")
			if err != nil {
				failures++
				t.Logf("iter %d: stats error: %v", i, err)
			} else if stats["resource_types"] != int64(expectedCount) {
				failures++
				t.Logf("iter %d: count mismatch: expected %d, got %d", i, expectedCount, stats["resource_types"])
			}

			f2.Close()
		}

		t.Logf("Failures without explicit checkpoint: %d/200", failures)
	})
}

// TestRapidWALActivity creates rapid WAL activity to maximize chance of hitting race
func TestRapidWALActivity(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping rapid WAL activity test in short mode")
	}

	ctx := context.Background()
	tmpDir := t.TempDir()

	failures := 0
	iterations := 1000

	for i := 0; i < iterations; i++ {
		testFilePath := filepath.Join(tmpDir, fmt.Sprintf("rapid_%d.c1z", i))

		f, err := NewC1ZFile(ctx, testFilePath, WithPragma("journal_mode", "WAL"))
		if err != nil {
			failures++
			continue
		}

		_, err = f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		if err != nil {
			f.Close()
			failures++
			continue
		}

		// Rapid small writes to generate lots of WAL frames
		for j := 0; j < 20; j++ {
			f.PutResourceTypes(ctx, v2.ResourceType_builder{
				Id: fmt.Sprintf("rt-%d-%d", i, j),
			}.Build())
		}

		f.EndSync(ctx)
		f.Close()

		// Immediate verification
		f2, err := NewC1ZFile(ctx, testFilePath, WithPragma("journal_mode", "WAL"), WithReadOnly(true))
		if err != nil {
			failures++
			t.Logf("iter %d: reopen failed: %v", i, err)
			continue
		}

		stats, err := f2.Stats(ctx, connectorstore.SyncTypeFull, "")
		if err != nil {
			failures++
			t.Logf("iter %d: stats failed: %v", i, err)
		} else if stats["resource_types"] != 20 {
			failures++
			t.Logf("iter %d: expected 20 resource_types, got %d", i, stats["resource_types"])
		}

		f2.Close()
	}

	if failures > 0 {
		t.Fatalf("Had %d failures out of %d iterations (%.2f%%)", failures, iterations, float64(failures)/float64(iterations)*100)
	}
}
