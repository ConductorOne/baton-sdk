package tempdir

import "os"

// EnvVar is the environment variable that overrides the temp directory.
const EnvVar = "BATON_TMPDIR"

// Resolve returns the best temporary directory to use.
// Priority order:
//  1. The provided dir argument (from --c1z-temp-dir flag)
//  2. BATON_TMPDIR environment variable
//  3. os.TempDir()
func Resolve(dir string) string {
	if dir != "" {
		return dir
	}
	if envDir := os.Getenv(EnvVar); envDir != "" {
		return envDir
	}
	return os.TempDir()
}
