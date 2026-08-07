package dotc1z

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
)

// Conversion turns SQLite wall clocks into absolute instants, and the writers
// store local time with no zone while the driver parses it as UTC. The tests
// listed in nonUTCTestNames only tell the fixed and the broken apart when
// time.Local is not UTC, since under parse-as-UTC the two readings coincide
// exactly on a UTC host — which is what CI runs.
//
// The zone is therefore supplied by re-running those tests in a child test
// binary with TZ set, rather than by assigning time.Local: that variable is read
// by every time.Now() in the process, including the ones in Pebble's background
// goroutines, so writing it mid-run is a data race that `make race-check` can
// report.
const (
	nonUTCTestZone     = "America/Los_Angeles"
	nonUTCTestChildEnv = "BATON_DOTC1Z_NON_UTC_CHILD"
)

var nonUTCTestNames = []string{
	"TestToPebbleStartedAtIsAbsoluteInstant",
	"TestToPebbleEndedAtIsAbsoluteInstant",
	"TestToPebbleDiscoveredAtIsAbsoluteInstant",
	"TestToPebbleDiscardedSyncTimestampsAreAbsoluteInstants",
}

func TestConversionTimestampsUnderNonUTCZone(t *testing.T) {
	if os.Getenv(nonUTCTestChildEnv) != "" {
		t.Skip("child run executes the timestamp tests directly")
	}

	loc, err := time.LoadLocation(nonUTCTestZone)
	if err != nil {
		t.Skipf("zone database unavailable, cannot run the conversion in a non-UTC zone: %v", err)
	}
	// A zone that happens to sit at UTC would make the child run vacuous.
	_, offset := time.Now().In(loc).Zone()
	require.NotZero(t, offset, "%s must be offset from UTC for the child run to discriminate", nonUTCTestZone)

	pattern := "^(" + strings.Join(nonUTCTestNames, "|") + ")$"
	//nolint:gosec // re-runs this test binary with a pattern built from constants.
	cmd := exec.CommandContext(context.Background(), os.Args[0], "-test.run="+pattern, "-test.count=1", "-test.v")
	cmd.Env = append(os.Environ(), nonUTCTestChildEnv+"=1", "TZ="+nonUTCTestZone)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "timestamp tests must pass with TZ=%s:\n%s", nonUTCTestZone, out)

	// A rename would leave this test passing on an empty child run, so require
	// each one by name.
	for _, name := range nonUTCTestNames {
		require.Contains(t, string(out), "--- PASS: "+name, "child run must have executed %s", name)
	}
}

// TestLocalizeSQLiteTimestamp pins the reinterpretation itself, in an explicit
// zone, so the arithmetic is covered on a UTC host too.
func TestLocalizeSQLiteTimestamp(t *testing.T) {
	// Fixed zones keep the arithmetic cases independent of the zone database.
	minus7 := time.FixedZone("TEST-7", -7*60*60)
	plus9 := time.FixedZone("TEST+9", 9*60*60)

	t.Run("scanned wall clock keeps its clock and gains the zone", func(t *testing.T) {
		scanned := time.Date(2026, time.March, 4, 5, 6, 7, 89, time.UTC)
		for _, loc := range []*time.Location{minus7, plus9} {
			got := localizeSQLiteTimestamp(scanned, loc)
			require.Equal(t, loc, got.Location())
			require.Equal(t, "2026-03-04 05:06:07.000000089", got.Format(sqliteTimeFormat),
				"the wall clock the writer stored must survive")
			require.Equal(t, scanned.Add(-zoneOffset(t, loc)), got.UTC(),
				"the instant must move by the zone's offset")
		}
	})

	t.Run("value that already carries a zone is left alone", func(t *testing.T) {
		// Either a zone-ful layout or a future _timezone DSN: unambiguous
		// already, so relabelling it would corrupt it.
		zoned := time.Date(2026, time.March, 4, 5, 6, 7, 0, plus9)
		require.Equal(t, zoned, localizeSQLiteTimestamp(zoned, minus7))
	})

	t.Run("zero time stays zero so callers can read it as unset", func(t *testing.T) {
		require.True(t, localizeSQLiteTimestamp(time.Time{}, minus7).IsZero())
	})

	t.Run("dst fall back resolves to one of the two candidate instants", func(t *testing.T) {
		// The documented hole: 01:30 happens twice in Los Angeles on
		// 2024-11-03 and the stored text cannot say which, so Go picks one.
		// Pinned as "one of", not as a value, since the choice is unspecified.
		loc, err := time.LoadLocation(nonUTCTestZone)
		if err != nil {
			t.Skipf("zone database unavailable: %v", err)
		}
		got := localizeSQLiteTimestamp(time.Date(2024, time.November, 3, 1, 30, 0, 0, time.UTC), loc)
		pdt := time.Date(2024, time.November, 3, 8, 30, 0, 0, time.UTC)
		pst := time.Date(2024, time.November, 3, 9, 30, 0, 0, time.UTC)
		require.Contains(t, []time.Time{pdt, pst}, got.UTC())
	})
}

func zoneOffset(t *testing.T, loc *time.Location) time.Duration {
	t.Helper()
	_, offset := time.Now().In(loc).Zone()
	return time.Duration(offset) * time.Second
}

// TestToPebbleStartedAtIsAbsoluteInstant pins that a converted sync's started_at
// is the instant the sync actually started, not that wall clock relabelled UTC.
//
// Pebble's LatestUnfinishedSyncRecord compares started_at against
// time.Now().Add(-unfinishedSyncMaxAge), so an offset here silently moves the
// resume window: on a UTC-7 host an unfinished sync would drop out of resume
// seven hours early, and the syncer starting fresh wipes the converted data via
// ResetForNewSync.
func TestToPebbleStartedAtIsAbsoluteInstant(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	src, err := NewC1ZFile(ctx, filepath.Join(dir, "src.c1z"), WithTmpDir(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, src.Close(ctx)) }()

	syncID, err := src.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, src.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "user"}.Build()))
	require.NoError(t, src.CheckpointSync(ctx, "resume-token"))

	// Stamp started_at the way the writers do: local wall clock, no zone.
	startedAt := time.Now().Add(-time.Hour).Truncate(time.Second)
	_, err = src.db.ExecContext(ctx,
		`UPDATE `+syncRuns.Name()+` SET started_at = ? WHERE sync_id = ?`,
		startedAt.Format(sqliteTimeFormat), syncID,
	)
	require.NoError(t, err)

	outPath := filepath.Join(dir, "out.c1z")
	_, err = src.ToPebble(ctx, outPath, syncID)
	require.NoError(t, err)

	dst, err := NewStore(ctx, outPath, WithEngine(c1zstore.EnginePebble), WithTmpDir(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, dst.Close(ctx)) }()

	eng, ok := pebble.AsEngine(dst)
	require.True(t, ok)
	rec, err := eng.GetSyncRunRecord(ctx, syncID)
	require.NoError(t, err)
	require.WithinDuration(t, startedAt, rec.GetStartedAt().AsTime(), time.Second,
		"converted started_at must be the same instant, not the wall clock relabelled UTC")

	// The sync is an hour old, so it has to still be a resume target. Without
	// the localization the recorded instant is seven hours early, which on a
	// smaller cutoff would put it out of range entirely.
	unfinished, err := eng.LatestUnfinishedSyncRecord(ctx, nil)
	require.NoError(t, err)
	require.NotNil(t, unfinished, "a sync started an hour ago must still be resumable")
	require.Equal(t, syncID, unfinished.GetSyncId())
}

// TestToPebbleEndedAtIsAbsoluteInstant is the finished-sync half: ended_at is
// what compaction and read-side recency use to order snapshots.
func TestToPebbleEndedAtIsAbsoluteInstant(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	src, err := NewC1ZFile(ctx, filepath.Join(dir, "src.c1z"), WithTmpDir(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, src.Close(ctx)) }()

	syncID, err := src.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, src.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "user"}.Build()))
	require.NoError(t, src.EndSync(ctx))

	endedAt := time.Now().Add(-time.Hour).Truncate(time.Second)
	_, err = src.db.ExecContext(ctx,
		`UPDATE `+syncRuns.Name()+` SET ended_at = ? WHERE sync_id = ?`,
		endedAt.Format(sqliteTimeFormat), syncID,
	)
	require.NoError(t, err)

	outPath := filepath.Join(dir, "out.c1z")
	_, err = src.ToPebble(ctx, outPath, syncID)
	require.NoError(t, err)

	dst, err := NewStore(ctx, outPath, WithEngine(c1zstore.EnginePebble), WithTmpDir(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, dst.Close(ctx)) }()

	eng, ok := pebble.AsEngine(dst)
	require.True(t, ok)
	rec, err := eng.GetSyncRunRecord(ctx, syncID)
	require.NoError(t, err)
	require.WithinDuration(t, endedAt, rec.GetEndedAt().AsTime(), time.Second,
		"converted ended_at must be the same instant, not the wall clock relabelled UTC")
}

// TestToPebbleDiscoveredAtIsAbsoluteInstant covers the per-record stamp. Every
// Pebble merge strategy picks winners by newest discovered_at, so an offset here
// lets a converted record lose to (or beat) a genuinely newer one during
// multi-engine compaction.
func TestToPebbleDiscoveredAtIsAbsoluteInstant(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	src, err := NewC1ZFile(ctx, filepath.Join(dir, "src.c1z"), WithTmpDir(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, src.Close(ctx)) }()

	syncID, err := src.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, src.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "user"}.Build()))

	discoveredAt := time.Now().Add(-3 * time.Hour).Truncate(time.Second)
	_, err = src.rawDb.ExecContext(ctx,
		"UPDATE "+resourceTypes.Name()+" SET discovered_at = ?", //nolint:gosec // fixed internal table name.
		discoveredAt.Format(sqliteTimeFormat),
	)
	require.NoError(t, err)
	require.NoError(t, src.EndSync(ctx))

	outPath := filepath.Join(dir, "out.c1z")
	_, err = src.ToPebble(ctx, outPath, syncID)
	require.NoError(t, err)

	dst, err := NewStore(ctx, outPath, WithEngine(c1zstore.EnginePebble), WithTmpDir(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, dst.Close(ctx)) }()

	eng, ok := pebble.AsEngine(dst)
	require.True(t, ok)

	iter, err := eng.NewIter(nil)
	require.NoError(t, err)
	defer iter.Close()

	checked := 0
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		// Primary record keyspace (0x03), resource_type bucket (0x01).
		if len(key) < 2 || key[0] != 0x03 || key[1] != 0x01 {
			continue
		}
		rec := &v3.ResourceTypeRecord{}
		require.NoError(t, proto.Unmarshal(iter.Value(), rec))
		require.WithinDuration(t, discoveredAt, rec.GetDiscoveredAt().AsTime(), time.Second,
			"converted discovered_at must be the same instant, not the wall clock relabelled UTC")
		checked++
	}
	require.NoError(t, iter.Error())
	require.Equal(t, 1, checked)
}
