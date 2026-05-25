package dotc1z

import (
	"bytes"
	"fmt"
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
	ctx := t.Context()
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
	err = os.WriteFile(beforeDB, dbDataBeforeClose, 0600)
	require.NoError(t, err)
	err = saveC1z(beforeDB, beforeC1z, 1)
	require.NoError(t, err)

	// Write the "after" data to a file and compress it
	afterDB := filepath.Join(tmpDir, "after.db")
	err = os.WriteFile(afterDB, dbDataAfterClose, 0600)
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
	require.NoError(t, beforeFile.Close(ctx))

	afterFile, err := NewC1ZFile(ctx, afterC1z, WithPragma("journal_mode", "WAL"), WithReadOnly(true))
	require.NoError(t, err)
	afterStats, err := afterFile.Stats(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	t.Logf("After c1z resource_types: %d", afterStats["resource_types"])
	require.NoError(t, afterFile.Close(ctx))

	// If before has fewer resource types than after, we've demonstrated the race
	if beforeStats != nil && beforeStats["resource_types"] < afterStats["resource_types"] {
		t.Logf("*** RACE DEMONSTRATED: before=%d, after=%d ***",
			beforeStats["resource_types"], afterStats["resource_types"])
	}

	// Clean up by deleting the original temp db
	os.RemoveAll(filepath.Dir(dbPath))
}
