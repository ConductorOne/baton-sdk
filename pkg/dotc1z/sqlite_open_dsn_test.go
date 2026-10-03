package dotc1z

import (
	"context"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSQLiteDSNPortablePathShapes pins sqliteDSN's path-to-URI escaping on
// every OS path shape the SDK's callers produce. The windows-latest CI
// failure this guards: filepath.Abs("db") on Windows yields "C:\...\db",
// and url.URL{Path: "C:/..."} without a leading slash renders "file:C:/...",
// which the sqlite driver parses with "C" as a URI AUTHORITY and rejects
// with "invalid uri authority" (every SQLite open failed on windows-latest).
// The fix forces the leading slash so the drive letter stays in the path.
func TestSQLiteDSNPortablePathShapes(t *testing.T) {
	ctx := context.Background()

	t.Run("plain relative path round-trips through a real open", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "plain.db")
		db, err := openSQLite(ctx, path)
		require.NoError(t, err)
		defer func() { _ = db.Close() }()
		var v int
		require.NoError(t, db.QueryRowContext(ctx, "PRAGMA trusted_schema").Scan(&v))
		require.Equal(t, 0, v, "hardened DSN must apply trusted_schema=OFF")
	})

	t.Run("reserved characters stay path bytes", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "we ir'd#100%.db")
		dsn, err := sqliteDSN(path)
		require.NoError(t, err)
		// The reserved characters must be percent-encoded in the URI
		// path (never query syntax), and a real open through the same
		// DSN must reach the database the path names.
		require.Contains(t, dsn, "we%20ir%27d%23100%25.db",
			"space/apostrophe/#/% must stay encoded path bytes, not raw or query syntax")
		db, err := openSQLite(ctx, path)
		require.NoError(t, err)
		defer func() { _ = db.Close() }()
		var v int
		require.NoError(t, db.QueryRowContext(ctx, "PRAGMA trusted_schema").Scan(&v))
		require.Equal(t, 0, v)
	})

	t.Run("posix absolute renders with authority-free scheme", func(t *testing.T) {
		dsn, err := sqliteDSN("/tmp/abs-check.db")
		if runtime.GOOS == "windows" {
			require.NoError(t, err)
			// On Windows /tmp/... resolves against the current drive:
			// still must render as a path, never an authority.
			require.Contains(t, dsn, "file:///")
			return
		}
		require.NoError(t, err)
		require.Contains(t, dsn, "file:///tmp/abs-check.db",
			"POSIX absolutes must render as file:///path, not file:path (authority)")
	})

	t.Run("memory passthrough keeps the pragma", func(t *testing.T) {
		dsn, err := sqliteDSN(":memory:")
		require.NoError(t, err)
		require.Contains(t, dsn, "memory")
		require.Contains(t, dsn, "_pragma=trusted_schema%28OFF%29")
	})

	t.Run("hostile trusted_schema pragma value in caller URI is refused", func(t *testing.T) {
		_, err := sqliteDSN("file:x.db?_pragma=trusted_schema(ON)")
		require.Error(t, err)
	})

	t.Run("explicit off pragma in caller URI passes through with canonical appended", func(t *testing.T) {
		dsn, err := sqliteDSN("file:x.db?_pragma=trusted_schema%28OFF%29")
		require.NoError(t, err)
		require.Contains(t, dsn, "trusted_schema")
	})
}
