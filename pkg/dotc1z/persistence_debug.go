package dotc1z

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

func FileSHA256Hex(path string) (string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return "", 0, err
	}

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", 0, err
	}

	return hex.EncodeToString(h.Sum(nil)), stat.Size(), nil
}

func SyncParentDir(path string) error {
	dir := filepath.Dir(path)
	f, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open parent dir %s: %w", dir, err)
	}
	defer f.Close()

	if err := f.Sync(); err != nil {
		return fmt.Errorf("sync parent dir %s: %w", dir, err)
	}

	return nil
}
