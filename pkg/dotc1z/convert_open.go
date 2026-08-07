package dotc1z

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/uotel"
)

type pebbleOpenOptions struct {
	tmpDir             string
	pragmas            []pragma
	decoderOptions     []DecoderOption
	readOnly           bool
	encoderConcurrency int
	syncLimit          int
	skipCleanup        bool
	skipVacuum         bool
	v2GrantsWriter     bool
	payloadEncoding    c1zstore.PayloadEncoding
}

func pebbleOpenOptionsFromC1Z(options *c1zOptions) pebbleOpenOptions {
	return pebbleOpenOptions{
		tmpDir:             options.tmpDir,
		pragmas:            append([]pragma(nil), options.pragmas...),
		decoderOptions:     append([]DecoderOption(nil), options.decoderOptions...),
		readOnly:           options.readOnly,
		encoderConcurrency: options.encoderConcurrency,
		syncLimit:          options.syncLimit,
		skipCleanup:        options.skipCleanup,
		skipVacuum:         options.skipVacuum,
		v2GrantsWriter:     options.v2GrantsWriter,
		payloadEncoding:    options.payloadEncoding,
	}
}

// convertSQLiteC1ZToPebble converts src into a new v3/Pebble .c1z at outPath,
// which must already exist as a v1/SQLite .c1z. The converted file atomically
// replaces outPath on success.
//
// Uses ToPebble's default ConvertResolveBehaviorNewest so an unfinished or
// non-full convertible sync can still migrate (unlike baton to-pebble, which
// requests ConvertResolveBehaviorFullFinished).
//
// This is lossy by design, and the loss is not recoverable: a Pebble c1z holds
// exactly one sync, and the converted file replaces outPath, so every sync the
// source held other than the selected one is gone. A SQLite c1z that holds a
// finished full sync plus a newer partial (the shape a targeted sync leaves
// behind) converts to the partial alone — a subset of the snapshot SQLite
// readers were served, since SQLite's default read view prefers a finished
// full. The tradeoff is deliberate: the newest run is the work that cannot be
// reproduced from elsewhere, while an earlier finished sync has already been
// uploaded.
//
// Because the loss is permanent, every dropped sync is logged at WARN with its
// id, type, start time, and whether it had finished.
func convertSQLiteC1ZToPebble(ctx context.Context, src *C1File, outPath string) error {
	ctx, span := tracer.Start(ctx, "convertSQLiteC1ZToPebble")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	tmpOut := outPath + ".pebble-convert.tmp"
	if removeErr := os.Remove(tmpOut); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) { // #nosec G703 -- conversion output path is caller-controlled by API design.
		err = removeErr
		return err
	}

	var stats *ConvertStats
	if stats, err = src.ToPebble(ctx, tmpOut, ""); err != nil {
		_ = os.Remove(tmpOut) // #nosec G703 -- cleanup of caller-selected conversion temp output.
		return err
	}

	if err = src.closeWithoutSave(ctx); err != nil {
		_ = os.Remove(tmpOut) // #nosec G703 -- cleanup of caller-selected conversion temp output.
		return fmt.Errorf("convert-open: close sqlite source: %w", err)
	}

	if err = os.Rename(tmpOut, outPath); err != nil { // #nosec G703 -- conversion output path is caller-controlled by API design.
		_ = os.Remove(tmpOut) // #nosec G703 -- cleanup of caller-selected conversion temp output.
		return fmt.Errorf("convert-open: replace output c1z: %w", err)
	}

	l := ctxzap.Extract(ctx)
	// WARN, not Info: the rename above replaced the v1 artifact, so any sync
	// the conversion did not select is gone for good. This line is the only
	// record an operator gets of what the file used to hold.
	if len(stats.DiscardedSyncs) > 0 {
		l.Warn("convert-open: discarded syncs not carried into the pebble c1z",
			append([]zap.Field{
				zap.String("path", outPath),
				zap.String("kept_sync_id", stats.SourceSyncID),
			}, discardedSyncFields(stats.DiscardedSyncs)...)...,
		)
	}
	l.Info("convert-open: converted sqlite c1z to pebble",
		zap.String("path", outPath),
		zap.String("kept_sync_id", stats.SourceSyncID),
		zap.Int("discarded_sync_count", len(stats.DiscardedSyncs)),
	)

	return nil
}

// convertExistingV1C1ZFile converts an existing v1 .c1z to Pebble in place.
func convertExistingV1C1ZFile(ctx context.Context, c1zPath string, openOpts pebbleOpenOptions) error {
	tmpDir := openOpts.tmpDir
	if tmpDir == "" {
		tmpDir = os.TempDir()
	}

	dbFilePath, _, err := decompressC1z(c1zPath, tmpDir, openOpts.decoderOptions...)
	if err != nil {
		return err
	}

	srcFile, err := NewC1File(ctx, dbFilePath,
		WithC1FTmpDir(tmpDir),
		WithC1FReadOnly(true),
	)
	if err != nil {
		return cleanupDbDir(dbFilePath, err)
	}

	if err := convertSQLiteC1ZToPebble(ctx, srcFile, c1zPath); err != nil {
		return cleanupDbDir(dbFilePath, err)
	}

	return cleanupDbDir(dbFilePath, nil)
}

// closeWithoutSave releases the sqlite handle without writing the decompressed
// db back to the .c1z envelope.
func (c *C1File) closeWithoutSave(ctx context.Context) error {
	c.closedMu.Lock()
	defer c.closedMu.Unlock()
	if c.closed {
		return nil
	}
	if c.rawDb != nil {
		if err := c.closeRawDB(ctx); err != nil {
			return err
		}
	}
	c.closed = true
	return cleanupDbDir(c.dbFilePath, nil)
}
