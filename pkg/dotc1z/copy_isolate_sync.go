package dotc1z

import (
	"context"
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
	if err := out.Close(); err != nil {
		return err
	}
	out = nil
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
//   - Input immutability: only the copy is written; the source .c1z and its
//     decompressed working DB are read, never mutated. A failure mid-op leaves
//     a discardable copy in a temp dir, not a half-mutated source.
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

	// Step 1 — copy the decompressed working DB. The source handle is opened
	// read-only (init sets journal_mode=OFF, so there is no WAL), so its working
	// file is the complete committed image and a plain file copy is a consistent
	// snapshot without checkpointing — and without touching the source.
	if err = copyDBFile(c.dbFilePath, copyDbPath); err != nil {
		return fmt.Errorf("copy-isolate-sync: error copying working database: %w", err)
	}

	// Step 2 — open the copy. NewC1File's init() runs migrations (schema
	// normalization) before our pragmas are applied, so the migrations stay
	// crash-safe under SQLite's default rollback journal. WithC1FSkipCleanup
	// keeps close-time old-sync cleanup from deleting the isolated sync. The
	// throwaway pragmas make the multi-sync bulk delete cheap: the copy is
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

	// Step 3 — count syncs (a cheap single-row read).
	var syncCount int
	//nolint:gosec // table name is from the hardcoded syncRuns descriptor; no user input.
	countQuery := fmt.Sprintf("SELECT COUNT(DISTINCT sync_id) FROM %s", syncRuns.Name())
	if err = copyFile.rawDb.QueryRowContext(ctx, countQuery).Scan(&syncCount); err != nil {
		return fmt.Errorf("copy-isolate-sync: error counting syncs: %w", err)
	}

	// Step 4 — multiple syncs: delete the non-target syncs from every data table
	// on the copy. This removes the (usually minority) other-sync rows rather
	// than re-inserting the entire target sync. A single-sync source skips this
	// entirely and passes the copy through unchanged.
	if syncCount > 1 {
		for _, t := range allTableDescriptors {
			//nolint:gosec // table names are from the hardcoded allTableDescriptors list; sync_id is a bound parameter.
			delQuery := fmt.Sprintf("DELETE FROM %s WHERE sync_id != ?", t.Name())
			if _, err = copyFile.rawDb.ExecContext(ctx, delQuery, syncID); err != nil {
				return fmt.Errorf("copy-isolate-sync: error deleting other syncs from %s: %w", t.Name(), err)
			}
		}
	}

	// Step 5 — recompress the isolated copy to outPath. Mirrors cloneCopy's
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
