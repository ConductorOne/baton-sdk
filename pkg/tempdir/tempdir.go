package tempdir

import "os"

const DefaultDatastorePath = "/c1-tenant-datastore"

// Resolve returns the best temporary directory to use. If the provided dir
// is non-empty it is returned as-is. Otherwise, it checks for the
// well-known /c1-tenant-datastore volume mount and falls back to the OS
// temp directory.
func Resolve(dir string) string {
	if dir != "" {
		return dir
	}
	if fi, err := os.Stat(DefaultDatastorePath); err == nil && fi.IsDir() {
		return DefaultDatastorePath
	}
	return os.TempDir()
}
