package dotc1z

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/uotel"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var validColumnNameRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// validateColumnName rejects column names that contain anything other than
// ASCII letters, digits, and underscores.  This prevents SQL injection via
// malicious column names embedded in a crafted .c1z database.
func validateColumnName(name string) error {
	if !validColumnNameRe.MatchString(name) {
		return fmt.Errorf("invalid column name: %q", name)
	}
	return nil
}

// quoteIdentifier wraps a SQLite identifier in double-quotes, escaping any
// embedded double-quote characters by doubling them per the SQL standard.
func quoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// cloneTableColumns returns the non-autoincrement column names for tableName
// by querying PRAGMA table_info on the given connection. The column names are
// returned in schema-definition order for the source table, which may differ
// from a freshly-created table when columns were added via ALTER TABLE.
func cloneTableColumns(ctx context.Context, conn *sql.Conn, tableName string) ([]string, error) {
	rows, err := conn.QueryContext(ctx, fmt.Sprintf("PRAGMA table_info(%s)", tableName))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var cid int
		var name, dataType string
		var notNull, pk int
		var defaultValue any

		if err := rows.Scan(&cid, &name, &dataType, &notNull, &defaultValue, &pk); err != nil {
			return nil, err
		}
		if name != "id" {
			if err := validateColumnName(name); err != nil {
				return nil, err
			}
			columns = append(columns, name)
		}
	}
	return columns, rows.Err()
}

// cloneTableQuery builds an INSERT ... SELECT that copies rows by explicit
// column name rather than relying on SELECT *, which is sensitive to the
// physical column order of the source vs destination tables.
func cloneTableQuery(tableName string, columns []string) string {
	quoted := make([]string, len(columns))
	for i, c := range columns {
		quoted[i] = quoteIdentifier(c)
	}
	colList := strings.Join(quoted, ", ")
	return fmt.Sprintf(
		"INSERT INTO clone.%s (%s) SELECT %s FROM %s WHERE sync_id=?",
		tableName, colList, colList, tableName,
	)
}

// cloneTableQueryAll is the all-rows sibling of cloneTableQuery: it omits the
// WHERE sync_id=? filter so every row of the table is copied verbatim. It is
// used by SnapshotTo, which reproduces the whole file — including un-ended
// syncs and the full sync_runs table (with each row's sync_token bytes intact).
func cloneTableQueryAll(tableName string, columns []string) string {
	quoted := make([]string, len(columns))
	for i, c := range columns {
		quoted[i] = quoteIdentifier(c)
	}
	colList := strings.Join(quoted, ", ")
	return fmt.Sprintf(
		"INSERT INTO clone.%s (%s) SELECT %s FROM %s",
		tableName, colList, colList, tableName,
	)
}

// CloneSync uses sqlite hackery to directly copy the pertinent rows into a new database.
// 1. Create a new empty sqlite database in a temp file
// 2. Open the c1z that we are cloning to get a db handle
// 3. Execute an ATTACH query to bring our empty sqlite db into the context of our db connection
// 4. Select directly from the cloned db and insert directly into the new database.
// 5. Close and save the new database as a c1z at the configured path.
func (c *C1File) CloneSync(ctx context.Context, outPath string, syncID string, opts ...C1FOption) error {
	ctx, span := tracer.Start(ctx, "C1File.CloneSync")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	defaultOpts := []C1FOption{
		WithC1FEncoderConcurrency(0),
		WithC1FSkipCleanup(true), // No need to clean up old syncs, as we only copied one sync into the new file.
	}
	opts = append(defaultOpts, opts...)

	// Be sure that the output path is empty else return an error. Checked here,
	// ahead of the sync preconditions, so an occupied output path is reported
	// before the target sync is inspected — the order callers have always seen.
	if _, statErr := os.Stat(outPath); statErr == nil || !errors.Is(statErr, fs.ErrNotExist) {
		return fmt.Errorf("clone-sync: output path (%s) must not exist for cloning to proceed", outPath)
	}

	if syncID == "" {
		syncID, err = c.LatestSyncID(ctx, connectorstore.SyncTypeFull)
		if err != nil {
			return err
		}
	}

	sync, err := c.getSync(ctx, syncID)
	if err != nil {
		return err
	}

	if sync == nil {
		return status.Errorf(codes.NotFound, "clone-sync: sync %s not found", syncID)
	}

	if sync.EndedAt == nil {
		return status.Errorf(codes.FailedPrecondition, "clone-sync: sync %s is not ended", syncID)
	}

	err = c.cloneCopy(ctx, outPath, syncID, false, "clone-sync", opts...)
	return err
}

// SnapshotTo writes a valid, openable .c1z of this file's CURRENT committed
// state to outPath, without closing the live handle and without
// re-decompressing the source. The live handle remains fully usable for reads
// and writes after SnapshotTo returns.
//
// Unlike CloneSync, which copies exactly one ended sync, SnapshotTo copies
// EVERY sync_runs row — ended and un-ended alike — and preserves each row's
// sync_token bytes exactly. A consumer that checkpoints resume progress into
// sync_token (e.g. c1zsanitize with Options.Resumable) can therefore reopen the
// snapshot with NewC1ZFile and resume from the checkpoint that was current when
// the snapshot was taken. All object tables (resources, entitlements, grants,
// assets, ...) are copied wholesale.
//
// outPath must not already exist. The write is atomic: a .tmp file is built and
// renamed into place only on full success, so a failure leaves no partial file
// at outPath (same guarantee as Close/saveC1z).
//
// Concurrency: the copy runs over the file's single SQLite connection
// (SetMaxOpenConns(1)), so it serializes against the live writer — writes block
// for the snapshot's row-copy window, then resume. The snapshot is a
// point-in-time view of committed rows; in-flight uncommitted work is excluded.
// There is no cancellation of the live sync and no phase redo. The compress
// half runs on a separate temp db and does not hold the live connection, so
// only the row copy stalls the writer, not the compress.
//
// For whale files, callers should also pass WithC1FBulkLoad(true) and
// WithC1FSkipVacuum(true) so the snapshot-side write defers secondary-index
// maintenance to a single rebuild pass at Close.
//
// SnapshotTo is a SQLite-engine (*C1File) method. It returns an error for the
// Pebble engine, which manages its own storage, and for a read-only handle.
func (c *C1File) SnapshotTo(ctx context.Context, outPath string, opts ...C1FOption) error {
	ctx, span := tracer.Start(ctx, "C1File.SnapshotTo")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	if c.engine == EnginePebble {
		err = fmt.Errorf("snapshot-to: unsupported for the %q engine; it manages its own storage", EnginePebble)
		return err
	}
	if c.readOnly {
		err = fmt.Errorf("snapshot-to: cannot snapshot a read-only handle")
		return err
	}

	// Mirror CloneSync's defaults: no old-sync cleanup (we are copying all syncs
	// deliberately) and GOMAXPROCS encoder concurrency for the compress.
	defaultOpts := []C1FOption{
		WithC1FEncoderConcurrency(0),
		WithC1FSkipCleanup(true),
	}
	opts = append(defaultOpts, opts...)

	err = c.cloneCopy(ctx, outPath, "", true, "snapshot-to", opts...)
	return err
}

// cloneCopy copies tables from the live connection into a fresh schema db in a
// temp dir, then compresses that db to outPath via a fresh C1File. It is the
// shared machinery behind CloneSync and SnapshotTo.
//
// When selectAll is false it copies only rows WHERE sync_id=syncID (CloneSync's
// single-sync clone). When selectAll is true it copies every row of every table
// verbatim — including the full sync_runs table with its sync_token bytes — and
// syncID is ignored (SnapshotTo's whole-file snapshot).
//
// The row copy runs over a single connection taken from the live pool
// (SetMaxOpenConns(1)), so it serializes against the live writer rather than
// racing it; the SELECTs read committed rows on the writer's own connection.
// The compress runs on the separate temp db and does not hold the live
// connection. cloneCopy never touches c.rawDb/c.db/c.currentSyncID/c.closed, so
// the live handle is left exactly as it was found (plus the row-copy stall).
// errPrefix is the caller's error-message namespace ("clone-sync" /
// "snapshot-to") so each entry point keeps its own observable error strings.
func (c *C1File) cloneCopy(ctx context.Context, outPath string, syncID string, selectAll bool, errPrefix string, opts ...C1FOption) error {
	// Be sure that the output path is empty else return an error
	_, err := os.Stat(outPath)
	if err == nil || !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("%s: output path (%s) must not exist", errPrefix, outPath)
	}

	tmpDir, err := os.MkdirTemp(c.tempDir, "c1zclone")
	if err != nil {
		return err
	}

	// Always clean up the temp dir and return an error if that fails
	defer func() {
		cleanupErr := os.RemoveAll(tmpDir)
		if cleanupErr != nil {
			err = errors.Join(err, fmt.Errorf("%s: error cleaning up temp dir: %w", errPrefix, cleanupErr))
		}
	}()

	dbPath := filepath.Join(tmpDir, "db")

	// Create a temporary C1File to initialize the schema in the new db.
	// NewC1File calls init() internally, creating all required tables.
	// We close only the rawDb to release the connection and file locks
	// without triggering C1File.Close()'s cleanupDbDir which would
	// remove the tmpDir we still need.
	initFile, err := NewC1File(ctx, dbPath)
	if err != nil {
		return err
	}
	if err = initFile.rawDb.Close(); err != nil {
		return err
	}
	initFile.rawDb = nil
	initFile.db = nil

	qCtx, canc := context.WithCancel(ctx)
	defer canc()

	// Copy the rows over a single connection. DETACH is deferred so it runs on
	// every path — including a mid-copy ExecContext failure — before the
	// connection is released and before the deferred temp-dir cleanup. Without
	// that, an early return leaves the clone db attached, the source pool keeps a
	// handle on the temp file, and os.RemoveAll fails (Windows), masking the real
	// error with a cleanup error.
	if copyErr := func() error {
		conn, err := c.rawDb.Conn(qCtx)
		if err != nil {
			return err
		}
		defer conn.Close()

		if _, err := conn.ExecContext(qCtx, fmt.Sprintf(`ATTACH '%s' AS clone`, dbPath)); err != nil {
			return err
		}
		defer func() {
			if _, derr := conn.ExecContext(qCtx, "DETACH clone"); derr != nil {
				ctxzap.Extract(ctx).Error("error detaching clone database", zap.Error(derr))
			}
		}()

		for _, t := range allTableDescriptors {
			columns, err := cloneTableColumns(qCtx, conn, t.Name())
			if err != nil {
				return fmt.Errorf("%s: error reading columns for %s: %w", errPrefix, t.Name(), err)
			}
			var q string
			var args []any
			if selectAll {
				q = cloneTableQueryAll(t.Name(), columns)
			} else {
				q = cloneTableQuery(t.Name(), columns)
				args = append(args, syncID)
			}
			if _, err := conn.ExecContext(qCtx, q, args...); err != nil {
				return err
			}
		}
		return nil
	}(); copyErr != nil {
		return copyErr
	}
	canc()

	// Open a fresh C1File to compress the populated db into a c1z.
	// No other connections are open on dbPath at this point.
	outFile, err := NewC1File(ctx, dbPath, opts...)
	if err != nil {
		return err
	}
	outFile.dbUpdated = true
	outFile.outputFilePath = outPath
	err = outFile.Close(ctx)
	if err != nil {
		return err
	}

	return err
}
