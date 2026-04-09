//go:build windows

package dotc1z

// Windows does not expose the same directory fsync pattern used on Unix after
// rename. The parent-dir sync is a Unix durability hardening, not a content
// correctness requirement for the saved c1z itself, so it is a no-op here.
func SyncParentDir(string) error {
	return nil
}
