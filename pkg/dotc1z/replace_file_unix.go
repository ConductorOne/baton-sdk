//go:build !windows

package dotc1z

import (
	"fmt"
	"os"
)

func ReplaceFileAtomically(fromPath string, toPath string) error {
	// On Unix, syncing the file contents before rename is not enough. The parent
	// directory also needs a sync so the directory entry update survives a crash.
	if err := os.Rename(fromPath, toPath); err != nil {
		return fmt.Errorf("rename temp file to destination: %w", err)
	}
	if err := SyncParentDir(toPath); err != nil {
		return fmt.Errorf("sync parent dir after rename: %w", err)
	}

	return nil
}
