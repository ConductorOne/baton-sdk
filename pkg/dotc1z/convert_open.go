package dotc1z

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"

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
	payloadEncoding    PayloadEncoding
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
func convertSQLiteC1ZToPebble(ctx context.Context, src *C1File, outPath string) error {
	ctx, span := tracer.Start(ctx, "convertSQLiteC1ZToPebble")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	tmpOut := outPath + ".pebble-convert.tmp"
	if removeErr := os.Remove(tmpOut); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
		err = removeErr
		return err
	}

	if _, err = src.ToPebble(ctx, tmpOut, ""); err != nil {
		_ = os.Remove(tmpOut)
		return err
	}

	if err = src.closeWithoutSave(ctx); err != nil {
		_ = os.Remove(tmpOut)
		return fmt.Errorf("convert-open: close sqlite source: %w", err)
	}

	if err = os.Rename(tmpOut, outPath); err != nil {
		_ = os.Remove(tmpOut)
		return fmt.Errorf("convert-open: replace output c1z: %w", err)
	}

	ctxzap.Extract(ctx).Info("convert-open: converted sqlite c1z to pebble",
		zap.String("path", outPath),
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
