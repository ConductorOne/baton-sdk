package dotc1z

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// TestWALCheckpointRace tests for a race condition where the WAL file
// may not be fully checkpointed back to the main database file before
// saveC1z() reads it. This can manifest on filesystems with aggressive
// caching like ZFS with a large ARC cache.
//
// The issue: In C1File.Close(), rawDb.Close() triggers a WAL checkpoint,
// but the checkpoint writes may still be in kernel buffers when saveC1z()
// immediately reads the database file. On fast storage with aggressive
// caching (ZFS ARC), reads can be served from a different cache tier
// that doesn't yet have the checkpoint data.
func TestWALCheckpointRace(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping race condition test in short mode")
	}

	ctx := context.Background()
	tmpDir := t.TempDir()

	// Number of iterations - increase for more thorough testing
	iterations := 100

	for i := 0; i < iterations; i++ {
		t.Run(fmt.Sprintf("iteration_%d", i), func(t *testing.T) {
			testFilePath := filepath.Join(tmpDir, fmt.Sprintf("wal_race_%d.c1z", i))

			// Create a c1z file with WAL mode and write significant data
			f, err := NewC1ZFile(ctx, testFilePath, WithPragma("journal_mode", "WAL"))
			require.NoError(t, err)

			syncID, err := f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
			require.NoError(t, err)

			// Create multiple resource types to generate WAL activity
			resourceTypeIDs := make([]string, 50)
			for j := 0; j < 50; j++ {
				resourceTypeIDs[j] = fmt.Sprintf("resource-type-%d-%d", i, j)
				err = f.PutResourceTypes(ctx, v2.ResourceType_builder{
					Id:          resourceTypeIDs[j],
					DisplayName: fmt.Sprintf("Resource Type %d-%d with some extra data to make it larger", i, j),
					Description: fmt.Sprintf("Description for resource type %d-%d with additional text to increase WAL activity", i, j),
				}.Build())
				require.NoError(t, err)

				// Add resources for each type
				for k := 0; k < 10; k++ {
					err = f.PutResources(ctx, v2.Resource_builder{
						Id: v2.ResourceId_builder{
							ResourceType: resourceTypeIDs[j],
							Resource:     fmt.Sprintf("resource-%d-%d-%d", i, j, k),
						}.Build(),
						DisplayName: fmt.Sprintf("Resource %d-%d-%d", i, j, k),
						Description: fmt.Sprintf("Description for resource %d-%d-%d with more data", i, j, k),
					}.Build())
					require.NoError(t, err)
				}
			}

			err = f.EndSync(ctx)
			require.NoError(t, err)

			// Close the file - this should checkpoint WAL and save
			err = f.Close()
			require.NoError(t, err)

			// CRITICAL: Immediately reopen and verify ALL data is present
			// This is where the race manifests - if WAL wasn't fully checkpointed,
			// some data will be missing
			f2, err := NewC1ZFile(ctx, testFilePath, WithPragma("journal_mode", "WAL"), WithReadOnly(true))
			require.NoError(t, err)

			// Verify sync exists
			latestSyncID, err := f2.LatestSyncID(ctx, connectorstore.SyncTypeFull)
			require.NoError(t, err, "failed to get latest sync ID on iteration %d", i)
			require.Equal(t, syncID, latestSyncID, "sync ID mismatch on iteration %d", i)

			// Verify all resource types are present
			stats, err := f2.Stats(ctx, connectorstore.SyncTypeFull, syncID)
			require.NoError(t, err)
			require.Equal(t, int64(50), stats["resource_types"],
				"resource type count mismatch on iteration %d: expected 50, got %d", i, stats["resource_types"])

			// Verify each resource type and its resources
			for j := 0; j < 50; j++ {
				rtID := fmt.Sprintf("resource-type-%d-%d", i, j)
				count, ok := stats[rtID]
				require.True(t, ok, "resource type %s not found in stats on iteration %d", rtID, i)
				require.Equal(t, int64(10), count,
					"resource count for %s mismatch on iteration %d: expected 10, got %d", rtID, i, count)
			}

			err = f2.Close()
			require.NoError(t, err)
		})
	}
}

// TestRapidOpenClose tests rapid open/close cycles which can expose
// race conditions in file handle management and buffer flushing.
func TestRapidOpenClose(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping rapid open/close test in short mode")
	}

	ctx := context.Background()
	tmpDir := t.TempDir()
	testFilePath := filepath.Join(tmpDir, "rapid_open_close.c1z")

	// First create a file with some data
	f, err := NewC1ZFile(ctx, testFilePath, WithPragma("journal_mode", "WAL"))
	require.NoError(t, err)

	_, err = f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	err = f.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "initial-type"}.Build())
	require.NoError(t, err)

	err = f.EndSync(ctx)
	require.NoError(t, err)

	err = f.Close()
	require.NoError(t, err)

	// Now rapidly open and close the file many times
	for i := 0; i < 500; i++ {
		f, err := NewC1ZFile(ctx, testFilePath, WithPragma("journal_mode", "WAL"), WithReadOnly(true))
		require.NoError(t, err, "failed to open on iteration %d", i)

		// Quick read to ensure file is valid
		_, err = f.LatestSyncID(ctx, connectorstore.SyncTypeFull)
		require.NoError(t, err, "failed to read sync ID on iteration %d", i)

		err = f.Close()
		require.NoError(t, err, "failed to close on iteration %d", i)
	}
}

// TestIncrementalSyncRace tests incremental syncs which have been suspected
// to be more prone to corruption. Each incremental sync involves more WAL
// activity and complex database operations.
func TestIncrementalSyncRace(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping incremental sync race test in short mode")
	}

	ctx := context.Background()
	tmpDir := t.TempDir()
	testFilePath := filepath.Join(tmpDir, "incremental_race.c1z")

	var prevSyncID string
	totalResources := 0

	// Perform multiple incremental syncs
	for i := 0; i < 20; i++ {
		t.Run(fmt.Sprintf("sync_%d", i), func(t *testing.T) {
			f, err := NewC1ZFile(ctx, testFilePath, WithPragma("journal_mode", "WAL"))
			require.NoError(t, err)

			var syncID string
			var syncType connectorstore.SyncType

			if i == 0 {
				// First sync is full
				syncType = connectorstore.SyncTypeFull
				syncID, err = f.StartNewSync(ctx, syncType, "")
			} else {
				// Subsequent syncs are partial/incremental
				syncType = connectorstore.SyncTypePartial
				syncID, err = f.StartNewSync(ctx, syncType, prevSyncID)
			}
			require.NoError(t, err)

			// Add resource type
			rtID := fmt.Sprintf("resource-type-%d", i)
			err = f.PutResourceTypes(ctx, v2.ResourceType_builder{Id: rtID}.Build())
			require.NoError(t, err)

			// Add multiple resources in this sync
			resourceCount := 25 + i // Varying count to create different WAL patterns
			for j := 0; j < resourceCount; j++ {
				err = f.PutResources(ctx, v2.Resource_builder{
					Id: v2.ResourceId_builder{
						ResourceType: rtID,
						Resource:     fmt.Sprintf("resource-%d-%d", i, j),
					}.Build(),
				}.Build())
				require.NoError(t, err)
			}
			totalResources += resourceCount

			err = f.EndSync(ctx)
			require.NoError(t, err)

			err = f.Close()
			require.NoError(t, err)

			// Immediately verify the sync was saved correctly
			f2, err := NewC1ZFile(ctx, testFilePath, WithPragma("journal_mode", "WAL"), WithReadOnly(true))
			require.NoError(t, err)

			// Verify sync
			latestID, err := f2.LatestSyncID(ctx, syncType)
			require.NoError(t, err)
			require.Equal(t, syncID, latestID, "sync ID mismatch after sync %d", i)

			// Verify resource count for this sync
			stats, err := f2.Stats(ctx, syncType, syncID)
			require.NoError(t, err)
			require.Equal(t, int64(resourceCount), stats[rtID],
				"resource count mismatch for sync %d: expected %d, got %d", i, resourceCount, stats[rtID])

			err = f2.Close()
			require.NoError(t, err)

			prevSyncID = syncID
		})
	}
}

// TestConcurrentReadWrite tests concurrent read and write operations
// which can expose race conditions in file locking and buffer management.
func TestConcurrentReadWrite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping concurrent read/write test in short mode")
	}

	ctx := context.Background()
	tmpDir := t.TempDir()

	numWriters := 4
	numReaders := 8
	iterations := 50

	var wg sync.WaitGroup
	errors := make(chan error, numWriters*iterations+numReaders*iterations)

	// Writer goroutines - each creates its own file
	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()

			testFilePath := filepath.Join(tmpDir, fmt.Sprintf("concurrent_%d.c1z", writerID))

			for i := 0; i < iterations; i++ {
				f, err := NewC1ZFile(ctx, testFilePath, WithPragma("journal_mode", "WAL"))
				if err != nil {
					errors <- fmt.Errorf("writer %d iter %d open: %w", writerID, i, err)
					continue
				}

				var syncID string
				if i == 0 {
					syncID, err = f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
				} else {
					syncID, _, err = f.StartOrResumeSync(ctx, connectorstore.SyncTypeFull, "")
				}
				if err != nil {
					f.Close()
					errors <- fmt.Errorf("writer %d iter %d start sync: %w", writerID, i, err)
					continue
				}

				// Write data
				rtID := fmt.Sprintf("rt-%d-%d", writerID, i)
				err = f.PutResourceTypes(ctx, v2.ResourceType_builder{Id: rtID}.Build())
				if err != nil {
					f.Close()
					errors <- fmt.Errorf("writer %d iter %d put rt: %w", writerID, i, err)
					continue
				}

				for j := 0; j < 20; j++ {
					err = f.PutResources(ctx, v2.Resource_builder{
						Id: v2.ResourceId_builder{
							ResourceType: rtID,
							Resource:     fmt.Sprintf("r-%d-%d-%d", writerID, i, j),
						}.Build(),
					}.Build())
					if err != nil {
						f.Close()
						errors <- fmt.Errorf("writer %d iter %d put resource %d: %w", writerID, i, j, err)
						break
					}
				}

				if err == nil {
					err = f.EndSync(ctx)
					if err != nil {
						errors <- fmt.Errorf("writer %d iter %d end sync: %w", writerID, i, err)
					}
				}

				err = f.Close()
				if err != nil {
					errors <- fmt.Errorf("writer %d iter %d close: %w", writerID, i, err)
					continue
				}

				// Immediately verify
				f2, err := NewC1ZFile(ctx, testFilePath, WithPragma("journal_mode", "WAL"), WithReadOnly(true))
				if err != nil {
					errors <- fmt.Errorf("writer %d iter %d verify open: %w", writerID, i, err)
					continue
				}

				latestID, err := f2.LatestSyncID(ctx, connectorstore.SyncTypeFull)
				if err != nil {
					f2.Close()
					errors <- fmt.Errorf("writer %d iter %d verify sync: %w", writerID, i, err)
					continue
				}

				if latestID != syncID && i == 0 {
					// On first iteration, sync should match
					errors <- fmt.Errorf("writer %d iter %d sync mismatch: expected %s, got %s", writerID, i, syncID, latestID)
				}

				f2.Close()
			}
		}(w)
	}

	wg.Wait()
	close(errors)

	// Collect all errors
	var errs []error
	for err := range errors {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		for _, err := range errs {
			t.Logf("Error: %v", err)
		}
		t.Fatalf("Got %d errors during concurrent test", len(errs))
	}
}

// TestC1ZIntegrity verifies the c1z file can be decoded correctly after save.
// This tests the full encode/decode cycle to catch corruption.
func TestC1ZIntegrity(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integrity test in short mode")
	}

	ctx := context.Background()
	tmpDir := t.TempDir()

	for i := 0; i < 50; i++ {
		t.Run(fmt.Sprintf("iteration_%d", i), func(t *testing.T) {
			testFilePath := filepath.Join(tmpDir, fmt.Sprintf("integrity_%d.c1z", i))

			// Create file with data
			f, err := NewC1ZFile(ctx, testFilePath, WithPragma("journal_mode", "WAL"))
			require.NoError(t, err)

			_, err = f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
			require.NoError(t, err)

			// Write random amount of data
			numTypes := 10 + (i % 20)
			for j := 0; j < numTypes; j++ {
				rtID := fmt.Sprintf("rt-%d-%d", i, j)
				err = f.PutResourceTypes(ctx, v2.ResourceType_builder{Id: rtID}.Build())
				require.NoError(t, err)

				numResources := 5 + (j % 15)
				for k := 0; k < numResources; k++ {
					err = f.PutResources(ctx, v2.Resource_builder{
						Id: v2.ResourceId_builder{
							ResourceType: rtID,
							Resource:     fmt.Sprintf("r-%d-%d-%d", i, j, k),
						}.Build(),
					}.Build())
					require.NoError(t, err)
				}
			}

			err = f.EndSync(ctx)
			require.NoError(t, err)

			// Get the dbFilePath before close
			dbPath := f.dbFilePath

			err = f.Close()
			require.NoError(t, err)

			// Verify the c1z file can be decoded
			c1zFile, err := os.Open(testFilePath)
			require.NoError(t, err)

			// Check magic header
			header := make([]byte, len(C1ZFileHeader))
			_, err = io.ReadFull(c1zFile, header)
			require.NoError(t, err)
			require.Equal(t, C1ZFileHeader, header, "invalid c1z header on iteration %d", i)

			// Reset and decode full file
			_, err = c1zFile.Seek(0, 0)
			require.NoError(t, err)

			decoder, err := NewDecoder(c1zFile)
			require.NoError(t, err)

			// Read all decoded data
			decoded := &bytes.Buffer{}
			_, err = io.Copy(decoded, decoder)
			require.NoError(t, err, "failed to decode c1z on iteration %d", i)

			err = decoder.Close()
			require.NoError(t, err)

			c1zFile.Close()

			// Verify decoded data is a valid SQLite database by checking header
			decodedBytes := decoded.Bytes()
			require.True(t, len(decodedBytes) >= 16, "decoded data too small on iteration %d: %d bytes", i, len(decodedBytes))

			// SQLite file header starts with "SQLite format 3\x00"
			sqliteHeader := []byte("SQLite format 3\x00")
			require.Equal(t, sqliteHeader, decodedBytes[:16],
				"decoded data is not a valid SQLite database on iteration %d", i)

			_ = dbPath // unused but captured
		})
	}
}

// TestHighConcurrencyStress performs high-concurrency stress testing
// to maximize the chance of hitting race conditions.
func TestHighConcurrencyStress(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping high concurrency stress test in short mode")
	}

	ctx := context.Background()
	tmpDir := t.TempDir()

	// Use all available CPUs
	numGoroutines := runtime.GOMAXPROCS(0) * 2
	iterations := 25

	var wg sync.WaitGroup
	errCh := make(chan error, numGoroutines*iterations)

	start := time.Now()

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()

			for i := 0; i < iterations; i++ {
				testFilePath := filepath.Join(tmpDir, fmt.Sprintf("stress_%d_%d.c1z", gid, i))

				f, err := NewC1ZFile(ctx, testFilePath, WithPragma("journal_mode", "WAL"))
				if err != nil {
					errCh <- fmt.Errorf("g%d i%d open: %w", gid, i, err)
					continue
				}

				syncID, err := f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
				if err != nil {
					f.Close()
					errCh <- fmt.Errorf("g%d i%d start: %w", gid, i, err)
					continue
				}

				// Generate random data
				randomData := make([]byte, 32)
				rand.Read(randomData)

				rtID := fmt.Sprintf("rt-%d-%d-%x", gid, i, randomData[:8])
				err = f.PutResourceTypes(ctx, v2.ResourceType_builder{
					Id:          rtID,
					DisplayName: fmt.Sprintf("Type %x", randomData),
				}.Build())
				if err != nil {
					f.Close()
					errCh <- fmt.Errorf("g%d i%d put rt: %w", gid, i, err)
					continue
				}

				for j := 0; j < 30; j++ {
					err = f.PutResources(ctx, v2.Resource_builder{
						Id: v2.ResourceId_builder{
							ResourceType: rtID,
							Resource:     fmt.Sprintf("r-%d-%d-%d", gid, i, j),
						}.Build(),
					}.Build())
					if err != nil {
						break
					}
				}

				if err == nil {
					err = f.EndSync(ctx)
				}

				closeErr := f.Close()
				if closeErr != nil {
					errCh <- fmt.Errorf("g%d i%d close: %w", gid, i, closeErr)
					continue
				}

				if err != nil {
					errCh <- fmt.Errorf("g%d i%d ops: %w", gid, i, err)
					continue
				}

				// Verify immediately
				f2, err := NewC1ZFile(ctx, testFilePath, WithPragma("journal_mode", "WAL"), WithReadOnly(true))
				if err != nil {
					errCh <- fmt.Errorf("g%d i%d verify open: %w", gid, i, err)
					continue
				}

				verifyID, err := f2.LatestSyncID(ctx, connectorstore.SyncTypeFull)
				if err != nil {
					f2.Close()
					errCh <- fmt.Errorf("g%d i%d verify sync: %w", gid, i, err)
					continue
				}

				if verifyID != syncID {
					f2.Close()
					errCh <- fmt.Errorf("g%d i%d sync mismatch: %s != %s", gid, i, verifyID, syncID)
					continue
				}

				stats, err := f2.Stats(ctx, connectorstore.SyncTypeFull, syncID)
				if err != nil {
					f2.Close()
					errCh <- fmt.Errorf("g%d i%d stats: %w", gid, i, err)
					continue
				}

				if stats[rtID] != 30 {
					f2.Close()
					errCh <- fmt.Errorf("g%d i%d count mismatch: %d != 30", gid, i, stats[rtID])
					continue
				}

				f2.Close()
			}
		}(g)
	}

	wg.Wait()
	close(errCh)

	elapsed := time.Since(start)
	t.Logf("Completed %d goroutines x %d iterations in %v", numGoroutines, iterations, elapsed)

	var errs []error
	for err := range errCh {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		for i, err := range errs {
			if i < 20 { // Only show first 20 errors
				t.Logf("Error: %v", err)
			}
		}
		t.Fatalf("Got %d errors during stress test", len(errs))
	}
}

// TestLoadC1zSync verifies that loadC1z properly syncs the decompressed database
// before it's used. Without sync, reads could get stale data on fast filesystems.
func TestLoadC1zSync(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping loadC1z sync test in short mode")
	}

	ctx := context.Background()
	tmpDir := t.TempDir()

	for i := 0; i < 100; i++ {
		testFilePath := filepath.Join(tmpDir, fmt.Sprintf("load_sync_%d.c1z", i))

		// Create a c1z with data
		f, err := NewC1ZFile(ctx, testFilePath, WithPragma("journal_mode", "WAL"))
		require.NoError(t, err)

		syncID, err := f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)

		// Write enough data to make WAL activity significant
		for j := 0; j < 20; j++ {
			rtID := fmt.Sprintf("rt-%d-%d", i, j)
			err = f.PutResourceTypes(ctx, v2.ResourceType_builder{
				Id:          rtID,
				DisplayName: fmt.Sprintf("Type %d-%d with extra data", i, j),
			}.Build())
			require.NoError(t, err)
		}

		err = f.EndSync(ctx)
		require.NoError(t, err)

		err = f.Close()
		require.NoError(t, err)

		// Load and verify multiple times rapidly
		for v := 0; v < 5; v++ {
			f2, err := NewC1ZFile(ctx, testFilePath, WithPragma("journal_mode", "WAL"), WithReadOnly(true))
			require.NoError(t, err, "failed to load c1z on iteration %d verify %d", i, v)

			latestID, err := f2.LatestSyncID(ctx, connectorstore.SyncTypeFull)
			require.NoError(t, err)
			require.Equal(t, syncID, latestID)

			stats, err := f2.Stats(ctx, connectorstore.SyncTypeFull, syncID)
			require.NoError(t, err)
			require.Equal(t, int64(20), stats["resource_types"],
				"iteration %d verify %d: expected 20 resource types, got %d", i, v, stats["resource_types"])

			err = f2.Close()
			require.NoError(t, err)
		}
	}
}
