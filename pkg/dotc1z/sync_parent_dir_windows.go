//go:build windows

package dotc1z

import (
	"fmt"
	"path/filepath"

	"golang.org/x/sys/windows"
)

func SyncParentDir(path string) error {
	dir := filepath.Dir(path)
	dirPath, err := windows.UTF16PtrFromString(dir)
	if err != nil {
		return fmt.Errorf("utf16 parent dir %s: %w", dir, err)
	}

	handle, err := windows.CreateFile(
		dirPath,
		0,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS,
		0,
	)
	if err != nil {
		return fmt.Errorf("open parent dir %s: %w", dir, err)
	}
	defer windows.CloseHandle(handle)

	if err := windows.FlushFileBuffers(handle); err != nil {
		return fmt.Errorf("flush parent dir %s: %w", dir, err)
	}

	return nil
}
