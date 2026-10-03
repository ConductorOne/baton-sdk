package dotc1z

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "modernc.org/sqlite" // sqlite driver for the raw fixture DB

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
)

// TestSecurity_HostileIndexNameExecutesDDLDuringBulkLoadOpen proves that an
// attacker-controlled SQLite index NAME (definable in a hostile .c1z v1 payload
// schema) is interpolated unescaped into DROP INDEX IF EXISTS "%s" during
// NewC1ZFile(WithBulkLoad(true)) open, executing attacker DDL.
func TestSecurity_HostileIndexNameExecutesDDLDuringBulkLoadOpen(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	// The injected ATTACH writes a marker INSIDE the test's private temp
	// dir (not a shared global path): a t.TempDir() path may contain
	// apostrophes, so escape it for the SQL literal too.
	markerPath := filepath.ToSlash(filepath.Join(dir, "marker.db"))
	sqlMarker := strings.ReplaceAll(markerPath, "'", "''")

	rawPath := filepath.Join(dir, "raw.db")
	db, err := sql.Open("sqlite", rawPath)
	require.NoError(t, err)
	for _, q := range []string{
		`CREATE TABLE v1_resource_types (` +
			`id integer primary key, external_id text not null, data blob not null,` +
			` sync_id text not null, discovered_at datetime not null)`,
		`CREATE TABLE v1_resources (` +
			`id integer primary key, resource_type_id text not null, external_id text not null,` +
			` parent_resource_type_id text, parent_resource_id text, data blob not null,` +
			` sync_id text not null, discovered_at datetime not null)`,
		`CREATE TABLE v1_entitlements (` +
			`id integer primary key, resource_type_id text not null, resource_id text not null,` +
			` external_id text not null, data blob not null,` +
			` sync_id text not null, discovered_at datetime not null)`,
		`CREATE TABLE v1_grants (` +
			`id integer primary key, resource_type_id text not null, resource_id text not null,` +
			` entitlement_id text not null, principal_resource_type_id text not null,` +
			` principal_resource_id text not null, external_id text not null, expansion blob,` +
			` needs_expansion integer not null default 0, data blob not null,` +
			` sync_id text not null, discovered_at datetime not null)`,
		`CREATE TABLE v1_sync_runs (` +
			`id integer primary key, sync_id text not null, started_at datetime not null,` +
			` ended_at datetime, sync_token text not null,` +
			` sync_type text not null default 'full', parent_sync_id text not null default '',` +
			` linked_sync_id text not null default '',` +
			` supports_diff integer not null default 0,` +
			` grants_backfilled integer not null default 0, stats text,` +
			` ingest_invariant_generation text not null default '',` +
			` ingest_invariant_coverage text not null default '',` +
			` ingest_invariant_mode text not null default '')`,
		`CREATE TABLE v1_session_store (` +
			`id integer primary key, sync_id text NOT NULL, key TEXT NOT NULL,` +
			` value BLOB NOT NULL)`,
		`CREATE TABLE v1_assets (` +
			`id integer primary key, external_id text not null, content_type text not null,` +
			` data blob not null, sync_id text not null,` +
			` discovered_at datetime not null)`,
		// Hostile non-unique origin-'c' index whose NAME contains a raw
		// double-quote (written as a doubled quote in DDL). PRAGMA index_list
		// returns it verbatim; the wrapper's DROP INDEX IF EXISTS "%s"
		// interpolates it unescaped, closing the wrapper quote early and
		// running the attacker's trailing SQL script. The payload here
		// attaches a marker file to also prove arbitrary-file-write shape.
		`CREATE INDEX "idx""; ATTACH DATABASE '` + sqlMarker + `'` +
			` AS pwn; CREATE TABLE pwn.t (x text); --" ON v1_grants (sync_id)`,
	} {
		_, err := db.ExecContext(ctx, q)
		require.NoError(t, err, "setup DDL failed: %s", q)
	}
	require.NoError(t, db.Close())

	rawBytes, err := os.ReadFile(rawPath)
	require.NoError(t, err)
	c1zPath := filepath.Join(dir, "hostile.c1z")
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

	f, err := NewC1ZFile(ctx, c1zPath,
		WithBulkLoad(true),
		WithSkipVacuum(true),
		WithPragma("journal_mode", "OFF"),
		WithPragma("synchronous", "OFF"),
	)
	require.NoError(t, err,
		"open must succeed: a hostile index NAME alone is data, not executable schema — an open error here would mask the invariant")
	t.Cleanup(func() {
		if f != nil {
			_ = f.Close(ctx)
		}
	})
	require.NotNil(t, f, "open must succeed far enough to reach the DROP INDEX")

	// The hostile index NAME must be treated as data: unescaped, its
	// `"; ATTACH ...` tail ran through the driver's multi-statement exec and
	// created the marker file.
	require.NoFileExists(t, markerPath,
		"attacker SQL executed during open: hostile index name escaped the identifier and created the marker file")

	// The hostile-named index itself must still be dropped as a plain
	// quoted identifier. Match the crafted name exactly, as a bound
	// parameter.
	craftedName := `idx"; ATTACH DATABASE '` + sqlMarker + `' AS pwn; CREATE TABLE pwn.t (x text); --`
	var hostileIdxCount int
	require.NoError(t, f.RawDB().QueryRowContext(ctx,
		`SELECT count(*) FROM sqlite_master WHERE type='index' AND name = ?`,
		craftedName,
	).Scan(&hostileIdxCount))
	require.Equal(t, 0, hostileIdxCount,
		"hostile-named index should be dropped as a plain identifier during bulk-load deferral")
}
