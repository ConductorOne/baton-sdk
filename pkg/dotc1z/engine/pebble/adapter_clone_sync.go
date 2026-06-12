package pebble

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/cockroachdb/pebble/v2"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	formatv3 "github.com/conductorone/baton-sdk/pkg/dotc1z/format/v3"
)

// cloneSync is the shared CloneSync implementation reached by
// pebbleFileOps.CloneSync. Materializes the named sync's data into
// a freshly-created Pebble-backed c1z at outPath.
//
// Strategy: the file holds exactly one sync and keys carry no
// sync_id, so cloning "the sync" is cloning the database. Stage a
// pebble Checkpoint of the source (hard links on the same
// filesystem — O(live files), not O(records)), open it, excise the
// engine-local keyspaces the clone contract excludes (sessions,
// counters — everything outside scopedRanges except the engine-meta
// stamp, which the clone keeps), then Checkpoint again and emit a
// c1z v3 envelope at outPath. No record is ever decoded, copied, or
// re-encoded. The excised keyspaces are usually empty; when they are
// not, their bytes may survive physically inside linked SSTs but are
// excised from the cloned LSM and unreachable.
//
// The raw db.Checkpoint (no flush) is used for the staging copy so
// read-only source engines work: any unflushed source writes ride
// along in the checkpointed WAL and replay when the stage opens. The
// stage engine is always writable, so the final CheckpointTo gets
// the usual flush + WAL-truncate envelope shape.
//
// Defaults syncID to the latest finished SyncTypeFull when empty.
// Errors if the sync isn't ended (mirrors the SQLite contract in
// clone_sync.go).
func cloneSync(
	ctx context.Context,
	a *Adapter,
	encoding c1zstore.PayloadEncoding,
	outPath, syncID string,
	opts ...c1zstore.CloneSyncOption,
) error {
	if _, err := os.Stat(outPath); err == nil || !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("clone-sync: output path (%s) must not exist for cloning to proceed", outPath)
	}

	cloneOpts := c1zstore.NewCloneSyncOptions(opts...)

	resolved := syncID
	if resolved == "" {
		latest, err := a.LatestFinishedSyncID(ctx, connectorstore.SyncTypeFull)
		if err != nil {
			return err
		}
		if latest == "" {
			return fmt.Errorf("clone-sync: no finished full sync available")
		}
		resolved = latest
	}

	srcRun, err := a.engine.GetSyncRunRecord(ctx, resolved)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return status.Errorf(codes.NotFound, "clone-sync: sync %q not found", resolved)
		}
		return err
	}
	if srcRun.GetEndedAt() == nil {
		return status.Errorf(codes.FailedPrecondition, "clone-sync: sync %q is not ended", resolved)
	}

	cloneTmp, err := os.MkdirTemp(cloneOpts.TmpDir, "c1z-clone")
	if err != nil {
		return err
	}
	defer func() {
		cleanupErr := os.RemoveAll(cloneTmp)
		if cleanupErr != nil {
			ctxzap.Extract(ctx).Warn("clone-sync: error cleaning up temp dir", zap.Error(cleanupErr))
		}
	}()
	destDBDir := filepath.Join(cloneTmp, "db")

	// Stage: checkpoint the whole source DB. Raw db.Checkpoint, not
	// Engine.CheckpointTo — the source may be opened read-only, where
	// the flush inside CheckpointTo is refused; the checkpoint carries
	// the live WALs instead, which replay on open below.
	if err := a.engine.DB().Checkpoint(destDBDir); err != nil {
		return fmt.Errorf("clone-sync: checkpoint source: %w", err)
	}

	dest, err := Open(ctx, destDBDir)
	if err != nil {
		return fmt.Errorf("clone-sync: open staged clone: %w", err)
	}
	defer func() { _ = dest.Close() }()

	// Drop the engine-local keyspaces a clone must not carry: counters
	// and sessions (adjacent type bytes, one span). Everything else in
	// the checkpoint is either sync-scoped data (scopedRanges) or the
	// engine-meta stamp, both of which the clone keeps.
	if err := dest.withWrite(func() error {
		span := pebble.KeyRange{
			Start: []byte{versionV3, typeCounter},
			End:   upperBoundOf([]byte{versionV3, typeSession}),
		}
		if err := dest.db.Excise(ctx, span); err != nil {
			return fmt.Errorf("excise [%x, %x): %w", span.Start, span.End, err)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("clone-sync: drop engine-local keyspaces: %w", err)
	}

	// Checkpoint the staged clone and emit the envelope at outPath.
	checkpointDir := filepath.Join(cloneTmp, "checkpoint")
	if err := dest.CheckpointTo(ctx, checkpointDir); err != nil {
		return fmt.Errorf("clone-sync: checkpoint: %w", err)
	}

	tmpPath := outPath + ".tmp"
	out, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	success := false
	defer func() {
		if out != nil {
			_ = out.Close()
		}
		if !success {
			_ = os.Remove(tmpPath)
		}
	}()

	manifest, err := BuildManifestWithSyncRuns(ctx, dest, encoding)
	if err != nil {
		return err
	}
	if err := formatv3.WriteEnvelope(out, manifest, checkpointDir); err != nil {
		return err
	}
	if err := out.Sync(); err != nil {
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	out = nil
	if err := os.Rename(tmpPath, outPath); err != nil {
		return err
	}
	success = true
	return nil
}
