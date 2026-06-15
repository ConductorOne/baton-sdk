package dotc1z

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/uotel"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// copyDBFile streams srcPath to a new file at dstPath (which must not exist).
// The source is opened read-only and never written, preserving input
// immutability. Streaming (rather than os.ReadFile) keeps memory flat when the
// working database is whale-scale.
func copyDBFile(srcPath, dstPath string) (retErr error) {
	// #nosec G304 -- srcPath is the connector's own decompressed working DB.
	in, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := in.Close(); cerr != nil {
			retErr = errors.Join(retErr, cerr)
		}
	}()

	// O_EXCL: dstPath lives in a freshly created temp dir and must not
	// pre-exist; refuse to clobber.
	out, err := os.OpenFile(dstPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	defer func() {
		if out != nil {
			if cerr := out.Close(); cerr != nil {
				retErr = errors.Join(retErr, cerr)
			}
		}
	}()

	if _, err := io.Copy(out, in); err != nil {
		return err
	}

	// Sync before SQLite opens the copy: on aggressively-caching filesystems an
	// unsynced copy can be read with incomplete data — the same hazard
	// decompressC1z guards against.
	if err := out.Sync(); err != nil {
		return err
	}
	// Nil out before inspecting the Close error so the deferred close becomes a
	// no-op on both paths: if this explicit Close fails, a second close from the
	// defer would only join a benign already-closed error onto the real one.
	cerr := out.Close()
	out = nil
	if cerr != nil {
		return cerr
	}
	return nil
}

// CopyIsolateSync produces a standalone .c1z at outPath containing only the
// given sync, like CloneSync, but it copies the source working database and
// deletes the other syncs from the copy instead of rebuilding the target sync
// row-by-row with INSERT ... SELECT.
//
// For a whale c1z whose target sync is the bulk of the data, that rebuild reads
// and re-inserts every target row into a fresh file (maintaining every index
// per row) — the dominant cost of the ~5h/~99GB clone. Copy-isolation instead
// pays one sequential file copy plus a delete of the (usually minority)
// non-target rows, which is far less work.
//
// Correctness contract (mirrors what CloneSync got for free):
//
//   - Input immutability: the source .c1z is never rewritten and the copy is
//     the only durable output. The source working DB is WAL-checkpointed before
//     the copy so the byte-copy is complete, but that folds already-committed
//     frames into the main file and changes no logical data. A failure mid-op
//     leaves a discardable copy in a temp dir, not a half-mutated source.
//   - Sync isolation: a single-sync source is used as-is; a multi-sync source
//     has every non-target sync deleted from the copy. Either way the output
//     contains only the target sync.
//   - Schema normalization: the copy is opened through NewC1File, whose init()
//     runs the same migrations CloneSync's fresh-DB creation did, so an
//     older-schema source is normalized to the current schema.
//   - Close-time cleanup: the copy is opened WithC1FSkipCleanup(true) so the
//     old-sync cleanup that runs at close does not delete the isolated sync.
func (c *C1File) CopyIsolateSync(ctx context.Context, outPath string, syncID string, opts ...C1FOption) (err error) {
	ctx, span := tracer.Start(ctx, "C1File.CopyIsolateSync")
	defer func() { uotel.EndSpanWithError(span, err) }()

	if c.rawDb == nil {
		return ErrDbNotOpen
	}
	if c.dbFilePath == "" {
		return fmt.Errorf("copy-isolate-sync: source working database path is not set")
	}

	// Check the output path before inspecting the sync, matching CloneSync's
	// observable ordering.
	if _, statErr := os.Stat(outPath); statErr == nil || !errors.Is(statErr, fs.ErrNotExist) {
		return errOutPathExists("copy-isolate-sync", outPath)
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
		return status.Errorf(codes.NotFound, "copy-isolate-sync: sync %s not found", syncID)
	}
	if sync.EndedAt == nil {
		return status.Errorf(codes.FailedPrecondition, "copy-isolate-sync: sync %s is not ended", syncID)
	}

	tmpDir, err := os.MkdirTemp(c.tempDir, "c1zcopyiso")
	if err != nil {
		return err
	}
	// Always clean up the temp dir. A successful Close already removes it via
	// cleanupDbDir; RemoveAll on an absent dir is a no-op, so this stays correct
	// on both the success and error paths.
	defer func() {
		if cleanupErr := os.RemoveAll(tmpDir); cleanupErr != nil {
			err = errors.Join(err, fmt.Errorf("copy-isolate-sync: error cleaning up temp dir: %w", cleanupErr))
		}
	}()

	copyDbPath := filepath.Join(tmpDir, "db")

	// Step 1 — copy the decompressed working DB. copyDBFile reads only the main
	// database file, so any WAL frames left uncheckpointed would be silently
	// dropped from the copy. Checkpoint first to fold every committed frame into
	// the main file: this makes the byte-copy a complete, consistent snapshot by
	// construction, independent of the caller's journal mode rather than relying
	// on the current read-only caller. It is a no-op on a read-only handle
	// (journal_mode=OFF, no WAL) and relocates no logical data, so the source is
	// not observably mutated.
	if _, err = c.db.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		return fmt.Errorf("copy-isolate-sync: error checkpointing source before copy: %w", err)
	}
	if err = copyDBFile(c.dbFilePath, copyDbPath); err != nil {
		return fmt.Errorf("copy-isolate-sync: error copying working database: %w", err)
	}

	// Step 2 — on a multi-sync source, delete the non-target syncs from the copy
	// BEFORE opening it through NewC1File. NewC1File's init() runs the
	// schema-normalization migrations — including the per-row grant-expansion
	// backfill and the secondary-index builds — across every row present. Doing
	// the delete first keeps that work off the (usually majority) rows that are
	// about to be discarded. The delete filters on sync_id alone, which is in
	// every table's base schema and so predates any migration, and the
	// already-open source guarantees the copy carries every current table. A raw
	// handle with throwaway pragmas keeps the delete cheap; the copy is discarded
	// after recompress, so a crash here only loses the temp file the defer above
	// already removes.
	if err = func() error {
		rawCopy, openErr := sql.Open("sqlite", copyDbPath)
		if openErr != nil {
			return fmt.Errorf("error opening copy for pre-migration delete: %w", openErr)
		}
		defer func() { _ = rawCopy.Close() }()
		rawCopy.SetMaxOpenConns(1)
		for _, p := range []string{"PRAGMA journal_mode = OFF", "PRAGMA synchronous = OFF"} {
			if _, perr := rawCopy.ExecContext(ctx, p); perr != nil {
				return fmt.Errorf("error setting %q on copy: %w", p, perr)
			}
		}
		var syncCount int
		//nolint:gosec // table name is from the hardcoded syncRuns descriptor; no user input.
		countQuery := fmt.Sprintf("SELECT COUNT(DISTINCT sync_id) FROM %s", syncRuns.Name())
		if scanErr := rawCopy.QueryRowContext(ctx, countQuery).Scan(&syncCount); scanErr != nil {
			return fmt.Errorf("error counting syncs: %w", scanErr)
		}
		if syncCount <= 1 {
			return nil
		}
		for _, t := range allTableDescriptors {
			//nolint:gosec // table names are from the hardcoded allTableDescriptors list; sync_id is a bound parameter.
			delQuery := fmt.Sprintf("DELETE FROM %s WHERE sync_id != ?", t.Name())
			if _, delErr := rawCopy.ExecContext(ctx, delQuery, syncID); delErr != nil {
				return fmt.Errorf("error deleting other syncs from %s: %w", t.Name(), delErr)
			}
		}
		return nil
	}(); err != nil {
		return fmt.Errorf("copy-isolate-sync: %w", err)
	}

	// Step 3 — open the copy. NewC1File's init() runs migrations (schema
	// normalization) before our pragmas are applied, so the migrations stay
	// crash-safe under SQLite's default rollback journal. WithC1FSkipCleanup
	// keeps close-time old-sync cleanup from deleting the isolated sync. The
	// throwaway pragmas make the migrations and the recompress cheap: the copy is
	// discarded after recompress, so its durability does not matter.
	defaultOpts := []C1FOption{
		WithC1FEncoderConcurrency(0),
		WithC1FSkipCleanup(true),
		WithC1FPragma("journal_mode", "OFF"),
		WithC1FPragma("synchronous", "OFF"),
	}
	copyFile, err := NewC1File(ctx, copyDbPath, append(defaultOpts, opts...)...)
	if err != nil {
		return fmt.Errorf("copy-isolate-sync: error opening copy: %w", err)
	}

	// Release the copy handle on any path that does not reach a successful
	// Close. Set once Close is invoked so we never double-close its rawDb.
	finalized := false
	defer func() {
		if !finalized && copyFile.rawDb != nil {
			if closeErr := copyFile.closeRawDB(ctx); closeErr != nil {
				err = errors.Join(err, closeErr)
			}
		}
	}()

	// Step 4 — recompress the isolated copy to outPath. Mirrors cloneCopy's
	// finalize: dbUpdated + outputFilePath + Close() runs the
	// WAL-checkpoint -> close-raw-db -> saveC1z sequence.
	copyFile.dbUpdated = true
	copyFile.outputFilePath = outPath
	finalized = true
	if err = copyFile.Close(ctx); err != nil {
		return fmt.Errorf("copy-isolate-sync: error writing output: %w", err)
	}

	return nil
}
