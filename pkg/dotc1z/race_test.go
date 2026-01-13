package dotc1z

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

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

	ctx := t.Context()
	tmpDir := t.TempDir()

	// Number of iterations - increase for more thorough testing
	iterations := 100

	for i := range iterations {
		t.Run(fmt.Sprintf("iteration_%d", i), func(t *testing.T) {
			testFilePath := filepath.Join(tmpDir, fmt.Sprintf("wal_race_%d.c1z", i))

			// Create a c1z file with WAL mode and write significant data
			f, err := NewC1ZFile(ctx, testFilePath, WithPragma("journal_mode", "WAL"))
			require.NoError(t, err)

			syncID, err := f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
			require.NoError(t, err)

			// Create multiple resource types to generate WAL activity
			resourceTypeIDs := make([]string, 50)
			for j := range 50 {
				resourceTypeIDs[j] = fmt.Sprintf("resource-type-%d-%d", i, j)
				err = f.PutResourceTypes(ctx, v2.ResourceType_builder{
					Id:          resourceTypeIDs[j],
					DisplayName: fmt.Sprintf("Resource Type %d-%d with some extra data to make it larger", i, j),
					Description: fmt.Sprintf("Description for resource type %d-%d with additional text to increase WAL activity", i, j),
				}.Build())
				require.NoError(t, err)

				// Add resources for each type
				for k := range 10 {
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
			err = f.Close(ctx)
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
			for j := range 50 {
				rtID := fmt.Sprintf("resource-type-%d-%d", i, j)
				count, ok := stats[rtID]
				require.True(t, ok, "resource type %s not found in stats on iteration %d", rtID, i)
				require.Equal(t, int64(10), count,
					"resource count for %s mismatch on iteration %d: expected 10, got %d", rtID, i, count)
			}

			err = f2.Close(ctx)
			require.NoError(t, err)
		})
	}
}

// TestC1ZIntegrity verifies the c1z file can be decoded correctly after save.
// This tests the full encode/decode cycle to catch corruption.
func TestC1ZIntegrity(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integrity test in short mode")
	}

	ctx := t.Context()
	tmpDir := t.TempDir()

	for i := range 50 {
		t.Run(fmt.Sprintf("iteration_%d", i), func(t *testing.T) {
			testFilePath := filepath.Join(tmpDir, fmt.Sprintf("integrity_%d.c1z", i))

			// Create file with data
			f, err := NewC1ZFile(ctx, testFilePath, WithPragma("journal_mode", "WAL"))
			require.NoError(t, err)

			_, err = f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
			require.NoError(t, err)

			// Write random amount of data
			numTypes := 10 + (i % 20)
			for j := range numTypes {
				rtID := fmt.Sprintf("rt-%d-%d", i, j)
				err = f.PutResourceTypes(ctx, v2.ResourceType_builder{Id: rtID}.Build())
				require.NoError(t, err)

				numResources := 5 + (j % 15)
				for k := range numResources {
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

			err = f.Close(ctx)
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
		})
	}
}
