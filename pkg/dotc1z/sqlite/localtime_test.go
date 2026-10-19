package sqlite_test

import (
	"context"
	"database/sql"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	sqlitereader "github.com/conductorone/baton-sdk/pkg/dotc1z/sqlite"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	_ "modernc.org/sqlite"
)

const nonUTCChild = "BATON_SQLITE_READER_NON_UTC_CHILD"

var nonUTCTests = []string{
	"TestToPebbleStartedAtIsAbsoluteInstant",
	"TestToPebbleEndedAtIsAbsoluteInstant",
	"TestToPebbleDiscoveredAtIsAbsoluteInstant",
	"TestToPebbleDiscardedSyncTimestampsAreAbsoluteInstants",
}

func TestConversionTimestampsUnderNonUTCZone(t *testing.T) {
	if os.Getenv(nonUTCChild) != "" {
		t.Skip("child process runs timestamp cases")
	}
	pattern := "^(" + strings.Join(nonUTCTests, "|") + ")$"
	cmd := exec.CommandContext(context.Background(), os.Args[0], "-test.run="+pattern, "-test.count=1", "-test.v") //nolint:gosec // Arguments use test-owned constants.
	cmd.Env = append(os.Environ(), nonUTCChild+"=1", "TZ=America/Los_Angeles")
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s", output)
	for _, name := range nonUTCTests {
		require.Contains(t, string(output), "--- PASS: "+name)
	}
}

func TestToPebbleStartedAtIsAbsoluteInstant(t *testing.T) {
	sourcePath := filepath.Join("testdata", "unfinished_partial.c1z")
	assertConvertedSyncTime(t, sourcePath, true)
}

func TestToPebbleEndedAtIsAbsoluteInstant(t *testing.T) {
	sourcePath := filepath.Join("testdata", "finished_full.c1z")
	assertConvertedSyncTime(t, sourcePath, false)
}

func assertConvertedSyncTime(t *testing.T, sourcePath string, started bool) {
	t.Helper()
	ctx := context.Background()
	source, err := sqlitereader.Open(ctx, sourcePath, sqlitereader.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	runs, err := source.SyncRuns(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, runs)
	run := runs[0]
	outPath := filepath.Join(t.TempDir(), "out.c1z")
	stats, err := source.ToPebble(ctx, outPath, run.ID)
	require.NoError(t, err)
	require.NoError(t, source.Close(ctx))
	record := syncRecord(t, outPath, stats.DestSyncID)
	if started {
		require.True(t, record.GetStartedAt().AsTime().Equal(localWallClock(*run.StartedAt)))
		return
	}
	require.True(t, record.GetEndedAt().AsTime().Equal(localWallClock(*run.EndedAt)))
}

func TestToPebbleDiscoveredAtIsAbsoluteInstant(t *testing.T) {
	ctx := context.Background()
	sourcePath := filepath.Join("testdata", "bare.db")
	db, err := sql.Open("sqlite", sourcePath)
	require.NoError(t, err)
	var scanned time.Time
	require.NoError(t, db.QueryRowContext(ctx, "SELECT discovered_at FROM v1_resource_types ORDER BY id LIMIT 1").Scan(&scanned))
	require.NoError(t, db.Close())

	source, err := sqlitereader.Open(ctx, sourcePath)
	require.NoError(t, err)
	outPath := filepath.Join(t.TempDir(), "out.c1z")
	_, err = source.ToPebble(ctx, outPath, "")
	require.NoError(t, err)
	require.NoError(t, source.Close(ctx))

	store, err := dotc1z.NewStore(ctx, outPath, dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close(ctx)) }()
	engine, ok := pebble.AsEngine(store)
	require.True(t, ok)
	iter, err := engine.NewIter(nil)
	require.NoError(t, err)
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		if len(key) < 2 || key[0] != 0x03 || key[1] != 0x01 {
			continue
		}
		record := &v3.ResourceTypeRecord{}
		require.NoError(t, proto.Unmarshal(iter.Value(), record))
		require.True(t, record.GetDiscoveredAt().AsTime().Equal(localWallClock(scanned)))
		return
	}
	t.Fatal("resource type record not found")
}

func TestToPebbleDiscardedSyncTimestampsAreAbsoluteInstants(t *testing.T) {
	ctx := context.Background()
	source, err := sqlitereader.Open(ctx, filepath.Join("testdata", "multi_sync.c1z"), sqlitereader.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	runs, err := source.SyncRuns(ctx)
	require.NoError(t, err)
	require.Len(t, runs, 3)
	output := filepath.Join(t.TempDir(), "out.c1z")
	stats, err := source.ToPebble(ctx, output, runs[1].ID)
	require.NoError(t, err)
	require.NoError(t, source.Close(ctx))
	require.Len(t, stats.DiscardedSyncs, 2)
	sourceByID := make(map[string]*time.Time, len(runs))
	for _, run := range runs {
		sourceByID[run.ID] = run.StartedAt
	}
	for _, discarded := range stats.DiscardedSyncs {
		require.True(t, discarded.StartedAt.Equal(localWallClock(*sourceByID[discarded.ID])))
	}
}

func localWallClock(value time.Time) time.Time {
	return time.Date(
		value.Year(), value.Month(), value.Day(),
		value.Hour(), value.Minute(), value.Second(), value.Nanosecond(),
		time.Local,
	)
}
