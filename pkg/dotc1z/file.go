package dotc1z

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"syscall"

	"github.com/klauspost/compress/zstd"
)

const (
	maxDecodedSizeEnvVar    = "BATON_DECODER_MAX_DECODED_SIZE_MB"
	maxDecoderMemorySizeEnv = "BATON_DECODER_MAX_MEMORY_MB"
)

func loadC1z(filePath string) (string, error) {
	workingDir, err := os.MkdirTemp("", "c1z")
	if err != nil {
		return "", err
	}
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

		var opts []DecoderOption

		maxDecodedSizeVar := os.Getenv(maxDecodedSizeEnvVar)
		if maxDecodedSizeVar != "" {
			maxDecodedSize, err := strconv.ParseUint(maxDecodedSizeVar, 10, 64)
			if err == nil {
				opts = append(opts, WithDecoderMaxDecodedSize(maxDecodedSize*1024*1024))
			}
		}

		maxDecoderMemorySizeVar := os.Getenv(maxDecoderMemorySizeEnv)
		if maxDecoderMemorySizeVar != "" {
			maxDecoderMemorySize, err := strconv.ParseUint(maxDecoderMemorySizeVar, 10, 64)
			if err == nil {
				opts = append(opts, WithDecoderMaxMemory(maxDecoderMemorySize*1024*1024))
			}
		}

		r, err := NewDecoder(c1zFile, opts...)
		if err != nil {
			return "", err
		}
		_, err = io.Copy(dbFile, r)
		if err != nil {
			return "", err
		}
	}

	return dbFilePath, nil
}

func saveC1z(dbFilePath string, outputFilePath string) error {
	if outputFilePath == "" {
		return errors.New("c1z: output file path not configured")
	}

	dbFile, err := os.Open(dbFilePath)
	if err != nil {
		return err
	}
	defer dbFile.Close()

	outFile, err := os.OpenFile(outputFilePath, os.O_RDWR|os.O_CREATE|syscall.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer outFile.Close()

	// Write the magic file header
	_, err = outFile.Write(C1ZFileHeader)
	if err != nil {
		return err
	}

	c1z, err := zstd.NewWriter(outFile)
	if err != nil {
		return err
	}

	_, err = io.Copy(c1z, dbFile)
	if err != nil {
		c1z.Close()
		return err
	}

	err = c1z.Flush()
	if err != nil {
		return err
	}
	err = c1z.Close()
	if err != nil {
		return err
	}

	// Cleanup the databaase filepath. This shoould always be a file within a temp directory, so we remove the entire dir.
	err = os.RemoveAll(filepath.Dir(dbFilePath))
	if err != nil {
		return err
	}

	return nil
}
