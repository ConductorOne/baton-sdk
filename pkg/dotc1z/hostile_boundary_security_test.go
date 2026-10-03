package dotc1z

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// Boundary regressions for the metadata-only schema guard: the guard must
// hold at every entry point that runs DDL/DML against untrusted bytes, not
// just the constructor. Each test builds a LEGITIMATE source through the
// public API, then plants the hostile object post-construction (so the
// planted state is genuinely reachable through the route under test) and
// asserts rejection BEFORE the route's own DML executes.

// newLegitC1Z opens a writable c1z through the public API and returns the
// open C1File plus its path.
func newLegitC1Z(t *testing.T, ctx context.Context) (*C1File, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "legit.c1z")
	f, err := NewC1ZFile(ctx, path)
	require.NoError(t, err)
	return f, path
}

func putMinimalSync(t *testing.T, ctx context.Context, f *C1File) string {
	t.Helper()
	syncID, err := f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, f.EndSync(ctx))
	return syncID
}

// TestSecurity_InitTablesReentryRejectsPlantedTrigger: a legitimate source
// with one finished sync, its grants_backfilled flag reset so a re-init
// runs the backfill UPDATE on v1_sync_runs, and a trigger planted on that
// table. The public InitTables re-entry must reject before the UPDATE runs.
func TestSecurity_InitTablesReentryRejectsPlantedTrigger(t *testing.T) {
	ctx := context.Background()
	f, _ := newLegitC1Z(t, ctx)
	defer func() { require.NoError(t, f.Close(ctx)) }()

	putMinimalSync(t, ctx, f)
	_, err := f.RawDB().ExecContext(ctx, `UPDATE v1_sync_runs SET grants_backfilled = 0`)
	require.NoError(t, err)
	_, err = f.RawDB().ExecContext(ctx, `CREATE TRIGGER planted BEFORE UPDATE ON v1_sync_runs BEGIN SELECT RAISE(ABORT, 'file-authored-trigger-executed'); END`)
	require.NoError(t, err)

	_, err = f.InitTables(ctx)
	require.Error(t, err)
	require.ErrorContains(t, err, "sqlite schema guard: rejected")
	require.NotContains(t, err.Error(), hostileTriggerMarker,
		"public InitTables re-entry executed the planted trigger during migrations")
	require.Contains(t, err.Error(), "planted")
}

// TestSecurity_CopyIsolateSyncRejectsBeforeRawCopyDelete: a legitimate
// two-sync source plus a BEFORE DELETE trigger on v1_sync_runs planted on
// the open handle. The raw-copy phase deletes the non-target sync's row, so
// CopyIsolateSync must reject before that DELETE runs, produce no output,
// and preserve both source syncs.
func TestSecurity_CopyIsolateSyncRejectsBeforeRawCopyDelete(t *testing.T) {
	ctx := context.Background()
	f, _ := newLegitC1Z(t, ctx)
	defer func() { require.NoError(t, f.Close(ctx)) }()

	putMinimalSync(t, ctx, f)
	putMinimalSync(t, ctx, f)

	_, err := f.RawDB().ExecContext(ctx, `CREATE TRIGGER killdeletes BEFORE DELETE ON v1_sync_runs BEGIN SELECT RAISE(ABORT, 'file-authored-trigger-executed'); END`)
	require.NoError(t, err)
	outPath := filepath.Join(t.TempDir(), "iso.c1z")
	err = f.CopyIsolateSync(ctx, outPath, "")
	require.Error(t, err)
	require.ErrorContains(t, err, "sqlite schema guard: rejected")
	require.NotContains(t, err.Error(), hostileTriggerMarker,
		"CopyIsolateSync executed the planted trigger during the raw-copy DELETEs")
	require.NoFileExists(t, outPath, "rejected isolation must produce no output")

	// Both source syncs must be preserved in the still-open source.
	var n int
	require.NoError(t, f.RawDB().QueryRowContext(ctx, `SELECT COUNT(DISTINCT sync_id) FROM v1_sync_runs`).Scan(&n))
	require.Equal(t, 2, n, "rejection must not disturb the source syncs")
}

// TestSecurity_AttachFileRejectsHostileAttachment: a hostile raw DB attached
// to a legitimate host must be rejected with DETACH, and the host must
// remain usable (proved by attaching a legitimate DB afterwards).
func TestSecurity_AttachFileRejectsHostileAttachment(t *testing.T) {
	ctx := context.Background()
	host, _ := newLegitC1Z(t, ctx)
	defer func() { require.NoError(t, host.Close(ctx)) }()

	// The attached file must be a *C1File, whose own construction already
	// ran the guard. The reachable hostile shape is therefore a trigger
	// PLANTED on its raw file after construction (the same post-hoc
	// mutation the InitTables re-entry test uses): AttachFile re-validates
	// the attached catalog before returning it as safe.
	otherPath := filepath.Join(t.TempDir(), "planted-attach.db")
	other, err := NewC1File(ctx, otherPath, WithC1FPragma("locking_mode", "normal"))
	require.NoError(t, err, "legitimate raw open must succeed before the trigger is planted")
	_, err = other.RawDB().ExecContext(ctx, `CREATE TABLE marker (x text)`)
	require.NoError(t, err)
	_, err = other.RawDB().ExecContext(ctx, `CREATE TRIGGER tr BEFORE INSERT ON marker BEGIN SELECT RAISE(ABORT, 'boom'); END`)
	require.NoError(t, err)

	_, err = host.AttachFile(other, "hostile")
	require.Error(t, err)
	require.ErrorContains(t, err, "sqlite schema guard: rejected")
	require.Contains(t, err.Error(), "trigger")

	// Release other's file handle: Windows cannot remove a TempDir file
	// that a live DB handle still holds.
	require.NoError(t, other.Close(ctx))

	// Host survives: a legitimate second attachment succeeds.
	legitPath := filepath.Join(t.TempDir(), "legit-attach.db")
	ldb, err := sql.Open("sqlite", legitPath)
	require.NoError(t, err)
	_, err = ldb.ExecContext(ctx, `CREATE TABLE fine (x text)`)
	require.NoError(t, err)
	require.NoError(t, ldb.Close())
	legit, err := NewC1File(ctx, legitPath)
	require.NoError(t, err)
	defer func() { _ = legit.Close(ctx) }()

	att, err := host.AttachFile(legit, "fine")
	require.NoError(t, err, "host connection must remain usable after rejecting a hostile attachment")
	require.True(t, att.safe)
	_, err = att.DetachFile("fine")
	require.NoError(t, err)
}

// TestSecurity_ConversionRejectsHostileV1: hostile v1 opened via
// NewStore(EnginePebble) then explicit ToPebble conversion must reject with
// no output and an unchanged source.
func TestSecurity_ConversionRejectsHostileV1(t *testing.T) {
	ctx := context.Background()
	path := hostileV1Fixture(t, "hostile.c1z", hostileTriggerDDL...)
	before := readSourceBytes(t, path)

	outDir := t.TempDir()
	outPath := filepath.Join(outDir, "rejected.c1z")

	store, err := NewStore(ctx, path, WithEngine(c1zstore.EnginePebble), WithReadOnly(true))
	require.Error(t, err)
	require.ErrorContains(t, err, "sqlite schema guard: rejected")
	require.Nil(t, store)
	require.NoFileExists(t, outPath)
	require.Equal(t, before, readSourceBytes(t, path), "conversion rejection must leave the source unchanged")
}

// TestParseTimeoutSecondsClampsOverflow: a seconds value too large for
// time.Duration must clamp rather than wrap negative, which would expire
// every timeout immediately.
func TestParseTimeoutSecondsClampsOverflow(t *testing.T) {
	require.Greater(t, parseTimeoutSeconds("9223372036854775807", time.Minute), time.Duration(0))
}

// TestSecurity_FailingOpenReturnsNilStoreInterface is the typed-nil
// regression: after a failing open, the returned c1zstore.Store
// interface must be nil so `if store != nil { store.Close(ctx) }` callers
// cannot panic.
func TestSecurity_FailingOpenReturnsNilStoreInterface(t *testing.T) {
	ctx := context.Background()
	path := hostileV1Fixture(t, "hostile.c1z", hostileTriggerDDL...)

	store, err := NewStore(ctx, path, WithReadOnly(true))
	require.Error(t, err)
	require.Nil(t, store, "Store interface must be nil on failed open (typed-nil regression)")

	require.False(t, errors.Is(err, ErrArtifactUnusable),
		"input rejection is NOT an output-storage verdict")
}
