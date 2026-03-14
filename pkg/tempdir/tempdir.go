package tempdir

import "os"

const DefaultDatastorePath = "/c1-tenant-datastore"

// EnvVar is the environment variable that overrides the temp directory.
// When set, it takes precedence over the default datastore path detection.
const EnvVar = "BATON_TMPDIR"

// Resolve returns the best temporary directory to use.
// Priority order:
//  1. The provided dir argument (from --c1z-temp-dir flag)
//  2. BATON_TMPDIR environment variable
//  3. /c1-tenant-datastore if it exists as a directory
//  4. os.TempDir()
func Resolve(dir string) string {
	if dir != "" {
		return dir
	}
	if envDir := os.Getenv(EnvVar); envDir != "" {
		return envDir
	}
	if fi, err := os.Stat(DefaultDatastorePath); err == nil && fi.IsDir() {
		return DefaultDatastorePath
	}
	return os.TempDir()
}
