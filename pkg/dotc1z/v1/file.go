package v1

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"github.com/klauspost/compress/zstd"
	"go.uber.org/zap"
)

// loadC1z loads the c1z file at filePath into a temp directory (optionally rooted at tmpDir).
// If the filePath exists, it will be extracted to the working dir.
// The working dir is returned.
func loadC1z(filePath string, tmpDir string) (string, error) {
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

		r, err := NewDecoder(c1zFile)
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

	return workingDir, nil
}

func saveC1z(workingDir string, outputFilePath string) error {
	if outputFilePath == "" {
		return errors.New("c1z: sqlite: output file path not configured")
	}

	dbFilePath := filepath.Join(workingDir, "db")
	dbFile, err := os.Open(dbFilePath)
	if err != nil {
		return err
	}
	defer func() {
		err = dbFile.Close()
		if err != nil {
			zap.L().Error("failed to close db file", zap.Error(err))
		}

		// Cleanup the database filepath. This should always be a file within a temp directory, so we remove the entire dir.
		err = os.RemoveAll(filepath.Dir(dbFilePath))
		if err != nil {
			zap.L().Error("failed to remove db dir", zap.Error(err))
		}
	}()

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

	return nil
}
