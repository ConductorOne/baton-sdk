package local

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/conductorone/baton-sdk/pkg/dotc1z"
)

var tracer = otel.Tracer("baton-sdk/pkg.dotc1z.manager.local")

type localManager struct {
	filePath       string
	tmpPath        string
	tmpDir         string
	decoderOptions []dotc1z.DecoderOption
}

type Option func(*localManager)

func WithTmpDir(tmpDir string) Option {
	return func(o *localManager) {
		o.tmpDir = tmpDir
	}
}

func WithDecoderOptions(opts ...dotc1z.DecoderOption) Option {
	return func(o *localManager) {
		o.decoderOptions = opts
	}
}

func (l *localManager) copyFileToTmp(ctx context.Context) error {
	_, span := tracer.Start(ctx, "localManager.copyFileToTmp")
	defer span.End()

	tmp, err := os.CreateTemp(l.tmpDir, "sync-*.c1z")
	if err != nil {
		return err
	}
	defer tmp.Close()

	l.tmpPath = tmp.Name()

	if l.filePath == "" {
		return nil
	}

	if _, err = os.Stat(l.filePath); err == nil {
		f, err := os.Open(l.filePath)
		if err != nil {
			return err
		}
		defer f.Close()

		// Get source file size for verification
		sourceStat, err := f.Stat()
		if err != nil {
			return fmt.Errorf("failed to stat source file: %w", err)
		}
		expectedSize := sourceStat.Size()

		written, err := io.Copy(tmp, f)
		if err != nil {
			return err
		}

		// CRITICAL: Sync to ensure all data is written before file is used.
		// This is especially important on ZFS ARC where writes may be cached
		// and reads can happen before buffers are flushed to disk.
		if err := tmp.Sync(); err != nil {
			return fmt.Errorf("failed to sync temp file: %w", err)
		}

		// Verify file size matches what we wrote (defensive check)
		stat, err := tmp.Stat()
		if err != nil {
			return fmt.Errorf("failed to stat temp file: %w", err)
		}
		if stat.Size() != written {
			return fmt.Errorf("file size mismatch: wrote %d bytes but file is %d bytes", written, stat.Size())
		}
		if written != expectedSize {
			return fmt.Errorf("copy size mismatch: expected %d bytes from source but copied %d bytes", expectedSize, written)
		}
	}

	return nil
}

// LoadRaw returns an io.Reader of the bytes in the c1z file.
func (l *localManager) LoadRaw(ctx context.Context) (io.ReadCloser, error) {
	ctx, span := tracer.Start(ctx, "localManager.LoadRaw")
	defer span.End()

	err := l.copyFileToTmp(ctx)
	if err != nil {
		return nil, err
	}

	f, err := os.Open(l.tmpPath)
	if err != nil {
		return nil, err
	}

	return f, nil
}

// LoadC1Z loads the C1Z file from the local file system.
func (l *localManager) LoadC1Z(ctx context.Context) (*dotc1z.C1File, error) {
	ctx, span := tracer.Start(ctx, "localManager.LoadC1Z")
	defer span.End()

	log := ctxzap.Extract(ctx)

	err := l.copyFileToTmp(ctx)
	if err != nil {
		return nil, err
	}

	log.Debug(
		"successfully loaded c1z locally",
		zap.String("file_path", l.filePath),
		zap.String("temp_path", l.tmpPath),
		zap.String("tmp_dir", l.tmpDir),
	)

	opts := []dotc1z.C1ZOption{
		dotc1z.WithTmpDir(l.tmpDir),
	}
	if len(l.decoderOptions) > 0 {
		opts = append(opts, dotc1z.WithDecoderOptions(l.decoderOptions...))
	}
	return dotc1z.NewC1ZFile(ctx, l.tmpPath, opts...)
}

// SaveC1Z saves the C1Z file to the local file system.
func (l *localManager) SaveC1Z(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "localManager.SaveC1Z")
	defer span.End()

	log := ctxzap.Extract(ctx)

	if l.tmpPath == "" {
		return fmt.Errorf("unexpected state - missing temp file path")
	}

	if l.filePath == "" {
		return fmt.Errorf("unexpected state - missing file path")
	}

	tmpFile, err := os.Open(l.tmpPath)
	if err != nil {
		return err
	}
	defer tmpFile.Close()

	srcStat, err := tmpFile.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat source file: %w", err)
	}
	expectedSize := srcStat.Size()

	// Stage in the destination directory so the final rename stays on the same
	// filesystem and remains atomic.
	destDir := filepath.Dir(l.filePath)
	dstFile, err := os.CreateTemp(destDir, filepath.Base(l.filePath)+".tmp-*")
	if err != nil {
		return fmt.Errorf("failed to create staging file: %w", err)
	}
	stagingPath := dstFile.Name()

	success := false
	defer func() {
		if dstFile != nil {
			if closeErr := dstFile.Close(); closeErr != nil {
				log.Warn("failed to close staging file", zap.Error(closeErr))
			}
		}
		if !success {
			_ = os.Remove(stagingPath)
		}
	}()

	sourceHash := sha256.New()
	size, err := io.Copy(dstFile, io.TeeReader(tmpFile, sourceHash))
	if err != nil {
		return fmt.Errorf("failed to copy to staging file: %w", err)
	}
	if size != expectedSize {
		return fmt.Errorf("copy size mismatch: expected %d bytes from source but copied %d bytes", expectedSize, size)
	}

	// CRITICAL: Sync to ensure data is written before function returns.
	// This is especially important on ZFS ARC where writes may be cached.
	if err := dstFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync staging file: %w", err)
	}

	stagingDigest, err := sha256HexFromFile(dstFile)
	if err != nil {
		return fmt.Errorf("failed to hash staging file: %w", err)
	}
	sourceDigest := hex.EncodeToString(sourceHash.Sum(nil))
	if stagingDigest != sourceDigest {
		return fmt.Errorf("staging digest mismatch: source=%s staging=%s", sourceDigest, stagingDigest)
	}

	log.Info(
		"validated c1z staging file before rename",
		zap.String("source_path", l.tmpPath),
		zap.String("staging_path", stagingPath),
		zap.Int64("source_bytes", expectedSize),
		zap.Int64("staging_bytes", size),
		zap.String("source_sha256", sourceDigest),
		zap.String("staging_sha256", stagingDigest),
	)

	if err := dstFile.Close(); err != nil {
		dstFile = nil
		return fmt.Errorf("failed to close staging file: %w", err)
	}
	dstFile = nil

	if err := os.Rename(stagingPath, l.filePath); err != nil {
		return fmt.Errorf("failed to rename staging file to destination: %w", err)
	}
	if err := dotc1z.SyncParentDir(l.filePath); err != nil {
		return fmt.Errorf("failed to sync destination directory: %w", err)
	}
	finalStat, err := os.Stat(l.filePath)
	if err != nil {
		return fmt.Errorf("failed to stat destination file: %w", err)
	}
	if finalStat.Size() != size {
		return fmt.Errorf("destination file size mismatch: staged=%d destination=%d", size, finalStat.Size())
	}
	success = true

	log.Debug(
		"successfully saved c1z locally",
		zap.String("file_path", l.filePath),
		zap.String("temp_path", l.tmpPath),
		zap.String("tmp_dir", l.tmpDir),
		zap.Int64("bytes", finalStat.Size()),
		zap.String("sha256", stagingDigest),
	)

	return nil
}

func sha256HexFromFile(f *os.File) (string, error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return "", fmt.Errorf("seek staging file for hash: %w", err)
	}

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", fmt.Errorf("hash staging file: %w", err)
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}

func (l *localManager) Close(ctx context.Context) error {
	_, span := tracer.Start(ctx, "localManager.Close")
	defer span.End()

	err := os.Remove(l.tmpPath)
	if err != nil {
		return err
	}
	return nil
}

// New returns a new localManager that uses the given filePath.
func New(ctx context.Context, filePath string, opts ...Option) (*localManager, error) {
	ret := &localManager{
		filePath: filePath,
	}

	for _, opt := range opts {
		opt(ret)
	}

	return ret, nil
}
