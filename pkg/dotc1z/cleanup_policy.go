package dotc1z

import "github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"

// The sync retention policy lives in pkg/dotc1z/c1zstore so storage engines
// can apply it without importing this package. These wrappers preserve the
// historical dotc1z names.

// SelectSyncsToDelete applies the SDK retention policy to a snapshot of sync
// runs and returns the IDs whose data should be deleted. See
// c1zstore.SelectSyncsToDelete for the policy details.
func SelectSyncsToDelete(candidates []c1zstore.SyncRun, currentSyncID string, syncLimit int) []string {
	return c1zstore.SelectSyncsToDelete(candidates, currentSyncID, syncLimit)
}

// ResolveCleanupSyncLimit resolves the effective retention limit. See
// c1zstore.ResolveCleanupSyncLimit.
func ResolveCleanupSyncLimit(callerLimit int, currentSyncOpen bool) int {
	return c1zstore.ResolveCleanupSyncLimit(callerLimit, currentSyncOpen)
}

// CleanupSkippedByEnv reports whether BATON_SKIP_CLEANUP is set to a truthy
// value. See c1zstore.CleanupSkippedByEnv.
func CleanupSkippedByEnv() bool {
	return c1zstore.CleanupSkippedByEnv()
}
