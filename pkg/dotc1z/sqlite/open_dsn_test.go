package sqlite

import (
	"net/url"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSQLiteDSNPortablePathShapes(t *testing.T) {
	t.Run("reserved characters stay path bytes", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "we ir'd#100%.db")
		dsn, err := sqliteDSN(path)
		require.NoError(t, err)
		require.Contains(t, dsn, "we%20ir%27d%23100%25.db")
		require.Contains(t, dsn, "mode=ro")
		require.Contains(t, dsn, "_pragma=query_only%28ON%29")
		require.Contains(t, dsn, "_pragma=trusted_schema%28OFF%29")
	})

	t.Run("absolute paths have no URI authority", func(t *testing.T) {
		dsn, err := sqliteDSN("/tmp/abs-check.db")
		require.NoError(t, err)
		if runtime.GOOS == "windows" {
			require.Contains(t, dsn, "file:///")
		} else {
			require.Contains(t, dsn, "file:///tmp/abs-check.db")
		}
	})

	t.Run("file URI keeps unrelated query values", func(t *testing.T) {
		input := "file:x.db?cache=shared&_pragma=" + url.QueryEscape("foreign_keys(ON)")
		dsn, err := sqliteDSN(input)
		require.NoError(t, err)
		require.Contains(t, dsn, "cache=shared")
		require.Contains(t, dsn, "foreign_keys%28ON%29")
		require.Contains(t, dsn, "mode=ro")
		require.Contains(t, dsn, "trusted_schema%28OFF%29")
		require.Contains(t, dsn, "query_only%28ON%29")
	})
}
