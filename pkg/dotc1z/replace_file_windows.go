//go:build windows

package dotc1z

import (
	"fmt"

	"golang.org/x/sys/windows"
)

func ReplaceFileAtomically(fromPath string, toPath string) error {
	from, err := windows.UTF16PtrFromString(fromPath)
	if err != nil {
		return fmt.Errorf("utf16 source path %s: %w", fromPath, err)
	}
	to, err := windows.UTF16PtrFromString(toPath)
	if err != nil {
		return fmt.Errorf("utf16 destination path %s: %w", toPath, err)
	}

	// Windows does not support the Unix "rename then fsync the parent
	// directory" pattern. Use a write-through replace so the publish step itself
	// completes synchronously.
	if err := windows.MoveFileEx(
		from,
		to,
		windows.MOVEFILE_REPLACE_EXISTING|windows.MOVEFILE_WRITE_THROUGH,
	); err != nil {
		return fmt.Errorf("replace destination with write-through move: %w", err)
	}

	return nil
}
