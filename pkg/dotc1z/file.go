package dotc1z

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"

	"github.com/klauspost/compress/zstd"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func loadC1z(filePath string, tmpDir string, opts ...DecoderOption) (string, error) {
	var err error
	workingDir, err := os.MkdirTemp(tmpDir, "c1z")
	if err != nil {
		return "", err
	}
	defer func() {
		if err != nil {
			if removeErr := os.RemoveAll(workingDir); removeErr != nil {
				err = errors.Join(err, removeErr)
			}
		}
	}()
	dbFilePath := filepath.Join(workingDir, "db")
	dbFile, err := os.Create(dbFilePath)
	if err != nil {
		return "", err
	}
	defer dbFile.Close()

	if stat, err := os.Stat(filePath); err == nil && stat.Size() != 0 {
		c1zFile, err := os.Open(filePath)
		if err != nil {
			return "", err
		}
		defer c1zFile.Close()

		r, err := NewDecoder(c1zFile, opts...)
		if err != nil {
			return "", err
		}
		_, err = io.Copy(dbFile, r)
		if err != nil {
			return "", err
		}
		err = r.Close()
		if err != nil {
			return "", err
		}
	}

	return dbFilePath, nil
}

func saveC1z(dbFilePath string, outputFilePath string, encoderConcurrency int) error {
	if outputFilePath == "" {
		return status.Errorf(codes.InvalidArgument, "c1z: output file path not configured")
	}

	dbFile, err := os.Open(dbFilePath)
	if err != nil {
		return err
	}
	defer func() {
		if dbFile != nil {
			if closeErr := dbFile.Close(); closeErr != nil {
				zap.L().Error("failed to close db file", zap.Error(closeErr))
			}
		}
	}()

	// Write to temp file first, then atomic rename on success.
	// This ensures outputFilePath never contains partial/corrupt data.
	// Use CreateTemp for unique filename to prevent concurrent writer races.
	outputDir := filepath.Dir(outputFilePath)
	outputBase := filepath.Base(outputFilePath)
	outFile, err := os.CreateTemp(outputDir, outputBase+".tmp-*")
	if err != nil {
		return err
	}
	tmpPath := outFile.Name()

	// Clean up temp file on any failure
	defer func() {
		if outFile != nil {
			if closeErr := outFile.Close(); closeErr != nil {
				zap.L().Error("failed to close temp file", zap.Error(closeErr))
			}
		}
		// Remove temp file if it exists (no-op if rename succeeded)
		_ = os.Remove(tmpPath)
	}()

	// Write the magic file header
	_, err = outFile.Write(C1ZFileHeader)
	if err != nil {
		return err
	}

	// zstd.WithEncoderConcurrency does not work the same as WithDecoderConcurrency.
	// WithDecoderConcurrency uses GOMAXPROCS if set to 0.
	// WithEncoderConcurrency errors if set to 0 (but defaults to GOMAXPROCS).
	if encoderConcurrency == 0 {
		encoderConcurrency = runtime.GOMAXPROCS(0)
	}
	c1z, err := zstd.NewWriter(outFile,
		zstd.WithEncoderConcurrency(encoderConcurrency),
	)
	if err != nil {
		return err
	}

	_, err = io.Copy(c1z, dbFile)
	if err != nil {
		return err
	}

	err = c1z.Flush()
	if err != nil {
		return fmt.Errorf("failed to flush c1z: %w", err)
	}
	err = c1z.Close()
	if err != nil {
		return fmt.Errorf("failed to close c1z: %w", err)
	}

	err = outFile.Sync()
	if err != nil {
		return fmt.Errorf("failed to sync temp file: %w", err)
	}

	err = outFile.Close()
	if err != nil {
		return fmt.Errorf("failed to close temp file: %w", err)
	}
	outFile = nil // Prevent double-close in defer

	err = dbFile.Close()
	if err != nil {
		return fmt.Errorf("failed to close db file: %w", err)
	}
	dbFile = nil // Prevent double-close in defer

	// Atomic rename: outputFilePath now has complete, valid data
	// This is the only point where outputFilePath is modified
	if err = os.Rename(tmpPath, outputFilePath); err != nil {
		return fmt.Errorf("failed to rename temp file to output: %w", err)
	}

	return nil
}
