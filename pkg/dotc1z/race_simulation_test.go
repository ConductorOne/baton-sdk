package dotc1z

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// TestSimulateWALRace simulates the race condition by manually controlling
// the sequence of operations that C1File.Close() performs, but reading the
// database file BEFORE the WAL is checkpointed.
//
// This demonstrates what happens when saveC1z reads stale data.
func TestSimulateWALRace(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	testFilePath := filepath.Join(tmpDir, "simulate_race.c1z")

	// Create a c1z file with WAL mode
	f, err := NewC1ZFile(ctx, testFilePath, WithPragma("journal_mode", "WAL"))
	require.NoError(t, err)

	_, err = f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	// Write data
	for i := 0; i < 50; i++ {
		err = f.PutResourceTypes(ctx, v2.ResourceType_builder{
			Id:          fmt.Sprintf("rt-%d", i),
			DisplayName: fmt.Sprintf("Resource Type %d", i),
		}.Build())
		require.NoError(t, err)
	}

	err = f.EndSync(ctx)
	require.NoError(t, err)

	// Now we'll manually simulate what happens in Close()
	// but READ THE DB FILE BEFORE closing the database (which checkpoints WAL)

	dbPath := f.dbFilePath
	walPath := dbPath + "-wal"

	// Check WAL file exists and has data
	walInfo, err := os.Stat(walPath)
	require.NoError(t, err)
	t.Logf("WAL file size before close: %d bytes", walInfo.Size())
	require.Greater(t, walInfo.Size(), int64(0), "WAL should have data before checkpoint")

	// Read the database file BEFORE closing (simulating the race)
	dbDataBeforeClose, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	t.Logf("DB file size before close: %d bytes", len(dbDataBeforeClose))

	// Now close the database (this checkpoints WAL)
	err = f.rawDb.Close()
	require.NoError(t, err)
	f.rawDb = nil
	f.db = nil

	// Read the database file AFTER closing
	dbDataAfterClose, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	t.Logf("DB file size after close: %d bytes", len(dbDataAfterClose))

	// Check if WAL was checkpointed
	walInfoAfter, err := os.Stat(walPath)
	if err != nil {
		t.Logf("WAL file deleted after close (good)")
	} else {
		t.Logf("WAL file size after close: %d bytes", walInfoAfter.Size())
	}

	// THE KEY TEST: Are the before and after files different?
	if bytes.Equal(dbDataBeforeClose, dbDataAfterClose) {
		t.Logf("DB file unchanged after close - WAL data was already in main DB or not checkpointed")
	} else {
		t.Logf("DB file CHANGED after close - WAL checkpoint wrote %d new bytes",
			len(dbDataAfterClose)-len(dbDataBeforeClose))

		// This is the race! If we had used dbDataBeforeClose to create the c1z,
		// we would have lost the WAL data!
		t.Logf("*** THIS DEMONSTRATES THE RACE CONDITION ***")
		t.Logf("If saveC1z() read the file before checkpoint completed,")
		t.Logf("it would create a c1z missing %d bytes of data",
			len(dbDataAfterClose)-len(dbDataBeforeClose))
	}

	// Compress both versions and compare
	beforeC1z := filepath.Join(tmpDir, "before.c1z")
	afterC1z := filepath.Join(tmpDir, "after.c1z")

	// Write the "before" data to a file and compress it
	beforeDB := filepath.Join(tmpDir, "before.db")
	err = os.WriteFile(beforeDB, dbDataBeforeClose, 0644)
	require.NoError(t, err)
	err = saveC1z(beforeDB, beforeC1z, 1)
	require.NoError(t, err)

	// Write the "after" data to a file and compress it
	afterDB := filepath.Join(tmpDir, "after.db")
	err = os.WriteFile(afterDB, dbDataAfterClose, 0644)
	require.NoError(t, err)
	err = saveC1z(afterDB, afterC1z, 1)
	require.NoError(t, err)

	// Now try to open each as a c1z and count resource types
	beforeFile, err := NewC1ZFile(ctx, beforeC1z, WithPragma("journal_mode", "WAL"), WithReadOnly(true))
	require.NoError(t, err)
	beforeStats, err := beforeFile.Stats(ctx, connectorstore.SyncTypeFull, "")
	if err != nil {
		t.Logf("Before c1z stats error: %v", err)
	} else {
		t.Logf("Before c1z resource_types: %d", beforeStats["resource_types"])
	}
	beforeFile.Close()

	afterFile, err := NewC1ZFile(ctx, afterC1z, WithPragma("journal_mode", "WAL"), WithReadOnly(true))
	require.NoError(t, err)
	afterStats, err := afterFile.Stats(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	t.Logf("After c1z resource_types: %d", afterStats["resource_types"])
	afterFile.Close()

	// If before has fewer resource types than after, we've demonstrated the race
	if beforeStats != nil && beforeStats["resource_types"] < afterStats["resource_types"] {
		t.Logf("*** RACE DEMONSTRATED: before=%d, after=%d ***",
			beforeStats["resource_types"], afterStats["resource_types"])
	}

	// Clean up by deleting the original temp db
	os.RemoveAll(filepath.Dir(dbPath))
}

// TestWALCheckpointTiming measures the timing between Close() and data visibility
func TestWALCheckpointTiming(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	testFilePath := filepath.Join(tmpDir, "timing.c1z")

	f, err := NewC1ZFile(ctx, testFilePath, WithPragma("journal_mode", "WAL"))
	require.NoError(t, err)

	_, err = f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	for i := 0; i < 100; i++ {
		err = f.PutResourceTypes(ctx, v2.ResourceType_builder{
			Id: fmt.Sprintf("rt-%d", i),
		}.Build())
		require.NoError(t, err)
	}

	err = f.EndSync(ctx)
	require.NoError(t, err)

	dbPath := f.dbFilePath

	// Get DB size before checkpoint
	dbStatBefore, _ := os.Stat(dbPath)
	t.Logf("DB size before explicit checkpoint: %d", dbStatBefore.Size())

	// Force WAL checkpoint manually (this is what the fix does)
	_, err = f.rawDb.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
	require.NoError(t, err)

	// Get DB size after checkpoint
	dbStatAfter, _ := os.Stat(dbPath)
	t.Logf("DB size after explicit checkpoint: %d", dbStatAfter.Size())

	// The difference shows how much data was in WAL
	if dbStatAfter.Size() > dbStatBefore.Size() {
		t.Logf("Checkpoint wrote %d bytes to main DB file",
			dbStatAfter.Size()-dbStatBefore.Size())
	}

	f.rawDb.Close()
	f.rawDb = nil
	f.db = nil

	os.RemoveAll(filepath.Dir(dbPath))
}

// TestDirectDBReadRace opens the SQLite database directly and demonstrates
// that reading before checkpoint gives different results
func TestDirectDBReadRace(t *testing.T) {
	tmpDir := t.TempDir()

	dbPath := filepath.Join(tmpDir, "direct.db")

	// Create a SQLite database in WAL mode
	db, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)

	_, err = db.Exec("PRAGMA journal_mode=WAL")
	require.NoError(t, err)

	_, err = db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
	require.NoError(t, err)

	// Insert data
	for i := 0; i < 100; i++ {
		_, err = db.Exec("INSERT INTO test (value) VALUES (?)", fmt.Sprintf("value-%d", i))
		require.NoError(t, err)
	}

	// Check WAL file
	walPath := dbPath + "-wal"
	walStat, err := os.Stat(walPath)
	require.NoError(t, err)
	t.Logf("WAL size after inserts: %d bytes", walStat.Size())

	// Read main DB file - it may not have all the data yet
	dbData, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	t.Logf("Main DB size before close: %d bytes", len(dbData))

	// Now do explicit checkpoint
	_, err = db.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
	require.NoError(t, err)

	// Read main DB file again
	dbDataAfter, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	t.Logf("Main DB size after checkpoint: %d bytes", len(dbDataAfter))

	if len(dbDataAfter) > len(dbData) {
		t.Logf("Checkpoint added %d bytes to main DB", len(dbDataAfter)-len(dbData))
		t.Logf("*** Without explicit checkpoint, saveC1z would miss this data ***")
	}

	// Verify WAL is truncated
	walStatAfter, err := os.Stat(walPath)
	if err != nil {
		t.Logf("WAL file removed after checkpoint")
	} else {
		t.Logf("WAL size after checkpoint: %d bytes", walStatAfter.Size())
	}

	db.Close()
}

// TestCompareC1ZWithAndWithoutCheckpoint creates two c1z files:
// one using current Close() behavior, one with explicit checkpoint first
func TestCompareC1ZWithAndWithoutCheckpoint(t *testing.T) {
	tmpDir := t.TempDir()

	// First: WITHOUT explicit checkpoint (simulate old code)
	t.Run("without_checkpoint", func(t *testing.T) {
		dbPath := filepath.Join(tmpDir, "no_checkpoint.db")
		c1zPath := filepath.Join(tmpDir, "no_checkpoint.c1z")

		db, err := sql.Open("sqlite", dbPath)
		require.NoError(t, err)

		_, err = db.Exec("PRAGMA journal_mode=WAL")
		require.NoError(t, err)

		// Create schema matching c1z tables
		_, err = db.Exec(`CREATE TABLE IF NOT EXISTS resource_types (
			id TEXT PRIMARY KEY,
			sync_id TEXT,
			data BLOB
		)`)
		require.NoError(t, err)

		// Insert data
		for i := 0; i < 50; i++ {
			_, err = db.Exec("INSERT INTO resource_types (id, sync_id, data) VALUES (?, ?, ?)",
				fmt.Sprintf("rt-%d", i), "sync-1", []byte(fmt.Sprintf("data-%d", i)))
			require.NoError(t, err)
		}

		// Close WITHOUT explicit checkpoint (old behavior)
		db.Close()

		// Read DB and save to c1z - might read stale data on ZFS
		err = saveC1z(dbPath, c1zPath, 1)
		require.NoError(t, err)

		// Verify
		c1zFile, err := os.Open(c1zPath)
		require.NoError(t, err)
		decoder, err := NewDecoder(c1zFile)
		require.NoError(t, err)
		decoded, _ := io.ReadAll(decoder)
		t.Logf("C1Z without checkpoint: %d bytes decoded", len(decoded))
		decoder.Close()
		c1zFile.Close()
	})

	// Second: WITH explicit checkpoint (new code)
	t.Run("with_checkpoint", func(t *testing.T) {
		dbPath := filepath.Join(tmpDir, "with_checkpoint.db")
		c1zPath := filepath.Join(tmpDir, "with_checkpoint.c1z")

		db, err := sql.Open("sqlite", dbPath)
		require.NoError(t, err)

		_, err = db.Exec("PRAGMA journal_mode=WAL")
		require.NoError(t, err)

		_, err = db.Exec(`CREATE TABLE IF NOT EXISTS resource_types (
			id TEXT PRIMARY KEY,
			sync_id TEXT,
			data BLOB
		)`)
		require.NoError(t, err)

		for i := 0; i < 50; i++ {
			_, err = db.Exec("INSERT INTO resource_types (id, sync_id, data) VALUES (?, ?, ?)",
				fmt.Sprintf("rt-%d", i), "sync-1", []byte(fmt.Sprintf("data-%d", i)))
			require.NoError(t, err)
		}

		// EXPLICIT checkpoint before close (new behavior)
		_, err = db.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
		require.NoError(t, err)

		db.Close()

		err = saveC1z(dbPath, c1zPath, 1)
		require.NoError(t, err)

		c1zFile, err := os.Open(c1zPath)
		require.NoError(t, err)
		decoder, err := NewDecoder(c1zFile)
		require.NoError(t, err)
		decoded, _ := io.ReadAll(decoder)
		t.Logf("C1Z with checkpoint: %d bytes decoded", len(decoded))
		decoder.Close()
		c1zFile.Close()
	})
}
