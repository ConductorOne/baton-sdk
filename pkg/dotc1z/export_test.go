package dotc1z

import "database/sql"

// Needed for sync_runs_test.go to prevent circular imports and allow reuse of code in c1ztest/helpers.go.
func (f *C1File) RawDB() *sql.DB {
	return f.rawDb
}
