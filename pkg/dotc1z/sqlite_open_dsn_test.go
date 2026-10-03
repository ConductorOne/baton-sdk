package dotc1z

import (
	"context"
	"net/url"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSQLiteDSNPortablePathShapes pins sqliteDSN's path-to-URI escaping on
// every OS path shape the SDK's callers produce. On Windows,
// filepath.Abs("db") yields "C:\...\db", and url.URL{Path: "C:/..."}
// without a leading slash renders "file:C:/...", which the sqlite driver
// parses with "C" as a URI authority and rejects. sqliteDSN forces the
// leading slash so the drive letter stays in the path.
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

	t.Run("caller trusted_schema pragma: only off spellings are accepted", func(t *testing.T) {
		for _, v := range []string{"trusted_schema(OFF)", "trusted_schema=off", "TRUSTED_SCHEMA = 0", "trusted_schema(false)", "trusted_schema=no"} {
			_, err := sqliteDSN("file:x.db?_pragma=" + url.QueryEscape(v))
			require.NoError(t, err, v)
		}
		for _, v := range []string{"trusted_schema(ON)", "trusted_schema=1", "trusted_schema=yes", "trusted_schema"} {
			_, err := sqliteDSN("file:x.db?_pragma=" + url.QueryEscape(v))
			require.Error(t, err, v)
		}
	})
}
