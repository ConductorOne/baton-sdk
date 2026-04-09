//go:build !windows

package dotc1z

import (
	"fmt"
	"os"
	"path/filepath"
)

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
