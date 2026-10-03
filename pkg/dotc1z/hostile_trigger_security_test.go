package dotc1z

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite" // sqlite driver for the raw fixture DB

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// hostileV1Fixture builds a raw SQLite database from ddl statements, closes
// it, reads the bytes back, and frames them as a v1 .c1z (C1ZFileHeader +
// zstd with WithEncoderConcurrency(1)) under t.TempDir(). This is the same
// framing pattern as hostile_index_name_security_test.go: the fixture must
// be byte-deterministic so a rejected source can be compared before/after.
// Returns the c1z path and the raw (uncompressed) bytes digest source.
func hostileV1Fixture(t *testing.T, name string, ddl ...string) string {
	t.Helper()
	dir := t.TempDir()
	rawPath := filepath.Join(dir, "raw.db")
	db, err := sql.Open("sqlite", rawPath)
	require.NoError(t, err)
	for _, q := range ddl {
		_, err := db.ExecContext(t.Context(), q)
		require.NoError(t, err, "fixture DDL failed: %s", q)
	}
	require.NoError(t, db.Close())

	rawBytes, err := os.ReadFile(rawPath)
	require.NoError(t, err)

	c1zPath := filepath.Join(dir, name)
	out, err := os.Create(c1zPath)
	require.NoError(t, err)
	_, err = out.Write(C1ZFileHeader)
	require.NoError(t, err)
	enc, err := zstd.NewWriter(out, zstd.WithEncoderConcurrency(1))
	require.NoError(t, err)
	_, err = enc.Write(rawBytes)
	require.NoError(t, err)
	require.NoError(t, enc.Close())
	require.NoError(t, out.Close())
	return c1zPath
}

// hostileTriggerDDL is the primary hostile fixture from the v1 open-time
// trigger finding: an old-shape v1_sync_runs, one pending finished full sync
// (grants_backfilled absent/0), ZERO grants, plus a file-authored
// BEFORE UPDATE trigger on v1_sync_runs whose body raises the marker abort
// 'file-authored-trigger-executed'. The grant-backfill migration updates
// v1_sync_runs when any pending sync exists even with zero grants, so
// pre-fix the trigger executes during read-only open.
var hostileTriggerDDL = []string{
	`CREATE TABLE v1_resource_types (id integer primary key, external_id text not null, data blob not null, sync_id text not null, discovered_at datetime not null)`,
	`CREATE TABLE v1_resources (id integer primary key, resource_type_id text not null, external_id text not null, parent_resource_type_id text, parent_resource_id text,` +
		`data blob not null, sync_id text not null, discovered_at datetime not null)`,
	`CREATE TABLE v1_entitlements (id integer primary key, resource_type_id text not null, resource_id text not null, external_id text not null, data blob not null,` +
		`sync_id text not null, discovered_at datetime not null)`,
	`CREATE TABLE v1_grants (id integer primary key, resource_type_id text not null, resource_id text not null, entitlement_id text not null, principal_resource_type_id text not null,` +
		`principal_resource_id text not null, external_id text not null, data blob not null, sync_id text not null, discovered_at datetime not null)`,
	`CREATE TABLE v1_sync_runs (id integer primary key, sync_id text not null, started_at datetime not null, ended_at datetime, sync_token text not null,` +
		`sync_type text not null default 'full', parent_sync_id text not null default '', supports_diff integer not null default 0, grants_backfilled integer not null default 0, stats text)`,
	`CREATE TABLE v1_assets (id integer primary key, external_id text not null, content_type text not null, data blob not null, sync_id text not null, discovered_at datetime not null)`,
	`CREATE TABLE v1_connector_sessions (id integer primary key, sync_id text NOT NULL, key TEXT NOT NULL, value BLOB NOT NULL)`,
	`INSERT INTO v1_sync_runs (sync_id, started_at, ended_at, sync_token, sync_type, grants_backfilled) VALUES ('hostile-sync', '2024-01-01T00:00:00Z', '2024-01-01T00:01:00Z', 'tok', 'full', 0)`,
	`CREATE TRIGGER file_authored_update BEFORE UPDATE ON v1_sync_runs BEGIN SELECT RAISE(ABORT, 'file-authored-trigger-executed'); END`,
}

const hostileTriggerMarker = "file-authored-trigger-executed"

// readSourceBytes reads the c1z envelope bytes for before/after comparison.
func readSourceBytes(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	return b
}

// TestSecurity_HostileTriggerRejectedOnEveryRoute proves the v1 open-time
// trigger-execution finding is closed: a read-only v1 open of a catalog
// carrying a file-authored BEFORE UPDATE trigger on v1_sync_runs must fail
// with a deterministic data rejection BEFORE any file-authored SQL executes
// (the marker text must NEVER appear), on every public entry route.
func TestSecurity_HostileTriggerRejectedOnEveryRoute(t *testing.T) {
	ctx := context.Background()

	newFixture := func(t *testing.T) (string, []byte) {
		t.Helper()
		path := hostileV1Fixture(t, "hostile.c1z", hostileTriggerDDL...)
		return path, readSourceBytes(t, path)
	}

	t.Run("NewStore read-only sqlite", func(t *testing.T) {
		path, before := newFixture(t)
		store, err := NewStore(ctx, path, WithReadOnly(true))
		require.Error(t, err)
		require.ErrorContains(t, err, "sqlite schema guard: rejected")
		require.NotContains(t, err.Error(), hostileTriggerMarker,
			"file-authored SQL executed during open: the trigger fired")
		require.Contains(t, err.Error(), "trigger")
		require.Nil(t, store, "failing open must return a nil Store interface (typed-nil fix)")
		require.Equal(t, before, readSourceBytes(t, path), "source bytes must be unchanged by rejection")
	})

	t.Run("NewStore read-only pebble engine", func(t *testing.T) {
		path, before := newFixture(t)
		store, err := NewStore(ctx, path, WithReadOnly(true), WithEngine(c1zstore.EnginePebble))
		require.Error(t, err)
		require.ErrorContains(t, err, "sqlite schema guard: rejected")
		require.NotContains(t, err.Error(), hostileTriggerMarker)
		require.Nil(t, store)
		require.Equal(t, before, readSourceBytes(t, path))
	})

	t.Run("NewC1ZFile read-only", func(t *testing.T) {
		path, before := newFixture(t)
		f, err := NewC1ZFile(ctx, path, WithReadOnly(true))
		require.Error(t, err)
		require.ErrorContains(t, err, "sqlite schema guard: rejected")
		require.NotContains(t, err.Error(), hostileTriggerMarker)
		require.Nil(t, f)
		require.Equal(t, before, readSourceBytes(t, path))
	})

	t.Run("writable constructor", func(t *testing.T) {
		path, before := newFixture(t)
		f, err := NewC1ZFile(ctx, path)
		require.Error(t, err)
		require.ErrorContains(t, err, "sqlite schema guard: rejected")
		require.NotContains(t, err.Error(), hostileTriggerMarker)
		require.Nil(t, f)
		require.Equal(t, before, readSourceBytes(t, path))
	})

	t.Run("bulk-load constructor", func(t *testing.T) {
		path, before := newFixture(t)
		f, err := NewC1ZFile(ctx, path, WithBulkLoad(true), WithSkipVacuum(true))
		require.Error(t, err)
		require.ErrorContains(t, err, "sqlite schema guard: rejected")
		require.NotContains(t, err.Error(), hostileTriggerMarker)
		require.Nil(t, f)
		require.Equal(t, before, readSourceBytes(t, path))
	})

	t.Run("raw NewC1File", func(t *testing.T) {
		path, before := newFixture(t)
		// NewC1File takes a raw SQLite path, not a framed envelope:
		// decode the payload the way NewC1ZFile does (zstd frame after
		// the 5-byte C1ZFileHeader) so the low-level constructor is
		// exercised against the hostile raw bytes.
		rawPath := decodeV1FixtureToRaw(t, path)
		f, err := NewC1File(ctx, rawPath, WithC1FReadOnly(true))
		require.Error(t, err)
		require.ErrorContains(t, err, "sqlite schema guard: rejected")
		require.NotContains(t, err.Error(), hostileTriggerMarker)
		require.Nil(t, f)
		require.Equal(t, before, readSourceBytes(t, path), "source envelope must be unchanged")
	})
}

// decodeV1FixtureToRaw decompresses a v1 c1z fixture into a raw .db file
// under the same temp dir (mirrors the decode NewC1ZFile performs).
func decodeV1FixtureToRaw(t *testing.T, c1zPath string) string {
	t.Helper()
	f, err := os.Open(c1zPath)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()
	header := make([]byte, len(C1ZFileHeader))
	_, err = f.Read(header)
	require.NoError(t, err)
	require.Equal(t, C1ZFileHeader, header)
	dec, err := zstd.NewReader(f, zstd.WithDecoderConcurrency(1))
	require.NoError(t, err)
	defer dec.Close()
	rawPath := filepath.Join(filepath.Dir(c1zPath), "decoded.db")
	out, err := os.Create(rawPath)
	require.NoError(t, err)
	_, err = out.ReadFrom(dec)
	require.NoError(t, err)
	require.NoError(t, out.Close())
	return rawPath
}

// TestSecurity_HostileTriggerMarkerNeverExecutes reads the trigger's own
// marker row out of the raw fixture (proving the trigger IS armed in the
// bytes), then shows the SDK open path never lets it fire — the error
// names the trigger object instead.
func TestSecurity_HostileTriggerMarkerNeverExecutes(t *testing.T) {
	ctx := context.Background()
	path := hostileV1Fixture(t, "hostile.c1z", hostileTriggerDDL...)

	// Prove the trigger is armed in the raw bytes: an UPDATE on the raw
	// DB (outside the SDK) must fire the marker abort.
	dir := filepath.Dir(path)
	rawPath := filepath.Join(dir, "raw.db")
	db, err := sql.Open("sqlite", rawPath)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `UPDATE v1_sync_runs SET grants_backfilled = 1 WHERE sync_id = 'hostile-sync'`)
	require.Error(t, err)
	require.Contains(t, err.Error(), hostileTriggerMarker,
		"fixture self-check failed: the trigger is not armed in the raw bytes")
	require.NoError(t, db.Close())

	// The SDK open must reject before the migration UPDATE runs.
	_, err = NewStore(ctx, path, WithReadOnly(true))
	require.Error(t, err)
	require.ErrorContains(t, err, "sqlite schema guard: rejected")
	require.NotContains(t, err.Error(), hostileTriggerMarker)
	require.Contains(t, err.Error(), "file_authored_update",
		"the rejection must name the offending trigger object")
}
