package dotc1z

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

// The SQLite driver applies every `_pragma` query parameter on EVERY new
// physical connection (vendor/modernc.org/sqlite/sqlite.go applyQueryParams),
// sorted with busy_timeout first and the rest lexicographically. A DSN pragma
// is therefore the only supported pool-wide hardening surface that does not
// rely on process-global state (RegisterConnectionHook mutates the driver
// singleton for every consumer in the process) or unsafe native handles.
const (
	trustedSchemaPragmaName = "trusted_schema"
	trustedSchemaOffPragma  = "trusted_schema(OFF)"

	// sqliteDriverName is the registered name of the vendored driver.
	sqliteDriverName = "sqlite"
)

// sqliteDSN builds a driver DSN that applies _pragma=trusted_schema(OFF) on
// every physical connection. An explicit "file:" URI keeps its query
// parameters; if the caller already set a trusted_schema _pragma, that
// setting is left as given. A plain filesystem path is escaped into a
// "file:" URI via net/url (filepath.Abs first; reserved characters survive
// as encoded path bytes, not query syntax). ":memory:" and "file::memory:"
// pass through with the pragma appended. Returns an error for an empty path
// or an unparsable file: URI.
func sqliteDSN(dbPathOrURI string) (string, error) {
	if dbPathOrURI == "" {
		return "", fmt.Errorf("c1z sqlite open: empty database path")
	}

	if strings.HasPrefix(dbPathOrURI, "file:") {
		u, err := url.Parse(dbPathOrURI)
		if err != nil {
			return "", fmt.Errorf("c1z sqlite open: invalid file: URI: %w", err)
		}
		q := u.Query()
		for _, v := range q["_pragma"] {
			if strings.HasPrefix(strings.ToLower(strings.TrimSpace(v)), trustedSchemaPragmaName) {
				return dbPathOrURI, nil
			}
		}
		q.Add("_pragma", trustedSchemaOffPragma)
		u.RawQuery = q.Encode()
		return u.String(), nil
	}

	if dbPathOrURI == ":memory:" {
		return "file::memory:?_pragma=" + url.QueryEscape(trustedSchemaOffPragma), nil
	}

	// Plain filesystem path (the shape every production call site passes).
	// Escape it into a file: URI so reserved characters (?, #, %, +, spaces)
	// stay path bytes rather than query syntax.
	abs, err := filepath.Abs(dbPathOrURI)
	if err != nil {
		return "", fmt.Errorf("c1z sqlite open: resolving path: %w", err)
	}
	slashPath := filepath.ToSlash(abs)
	// url.URL treats a "C:/..." path without a leading slash as a URI
	// AUTHORITY ("file:C:" parses with host "C"), which the driver
	// rejects ("invalid uri authority") — the windows-latest path shape.
	// Forcing the leading slash emits file:///C:/..., where the drive
	// letter stays in the path. POSIX absolutes already start with "/".
	if !strings.HasPrefix(slashPath, "/") {
		slashPath = "/" + slashPath
	}
	u := &url.URL{
		Scheme: "file",
		Path:   slashPath,
	}
	q := u.Query()
	q.Add("_pragma", trustedSchemaOffPragma)
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// openSQLite opens a hardened pool: sql.Open(sqliteDriverName, sqliteDSN(...))
// followed by PingContext(ctx). Pool sizing/caps are the caller's; the pool
// is closed on ping failure. Callers own the success-path Close.
//
// trusted_schema=OFF is defense-in-depth against unsafe schema-expression
// functions (it is NOT a trigger-disable switch; the metadata-only schema
// guard in sqlite_schema_guard.go rejects triggers before any file-authored
// SQL executes).
func openSQLite(ctx context.Context, dbPathOrURI string) (*sql.DB, error) {
	_, span := tracer.Start(ctx, "dotc1z.openSQLite")
	defer span.End()

	dsn, err := sqliteDSN(dbPathOrURI)
	if err != nil {
		return nil, err
	}

	rawDB, err := sql.Open(sqliteDriverName, dsn)
	if err != nil {
		return nil, fmt.Errorf("c1z sqlite open: error opening database: %w", err)
	}
	if err := rawDB.PingContext(ctx); err != nil {
		_ = rawDB.Close()
		return nil, fmt.Errorf("c1z sqlite open: error pinging database: %w", err)
	}
	ctxzap.Extract(ctx).Debug("c1z sqlite open: opened hardened pool",
		zap.String("db_file_path", dbPathOrURI))
	return rawDB, nil
}
