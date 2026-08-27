package testtier

import (
	"os"
	"testing"
)

const (
	ExtraEnv   = "BATON_TEST_EXTRA"
	NightlyEnv = "BATON_TEST_NIGHTLY"
)

// RequireExtra skips a deterministic long-running test unless an extra or
// nightly confidence suite explicitly enabled it.
func RequireExtra(t testing.TB) {
	t.Helper()
	if !enabled(ExtraEnv) && !enabled(NightlyEnv) {
		t.Skipf("set %s=1 or run the corresponding Make target", ExtraEnv)
	}
}

// RequireNightly skips a randomized, repeated, or full-corpus test unless the
// nightly confidence suite explicitly enabled it.
func RequireNightly(t testing.TB) {
	t.Helper()
	if !enabled(NightlyEnv) {
		t.Skipf("set %s=1 or run make test-nightly", NightlyEnv)
	}
}

func enabled(name string) bool {
	return os.Getenv(name) == "1"
}
