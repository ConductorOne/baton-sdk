package dotc1z

import "database/sql"

// Needed for sync_runs_test.go to prevent circular imports and allow reuse of code in c1ztest/helpers.go.
func (f *C1File) RawDB() *sql.DB {
	return f.rawDb
}

// SetDeleteSyncRunBatchSize shrinks the DeleteSyncRun batch size for tests so
// the multi-batch loop is exercised without a 50k-row fixture. Returns a
// restore func.
func SetDeleteSyncRunBatchSize(n int) func() {
	orig := deleteSyncRunBatchSize
	deleteSyncRunBatchSize = n
	return func() { deleteSyncRunBatchSize = orig }
}
