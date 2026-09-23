package dotc1z

import (
	"database/sql"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
)

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

type wrappedPageTestStore struct {
	*pebbleStore
	wrap func(c1zstore.PageWriter) c1zstore.PageWriter
}

func (s *wrappedPageTestStore) BeginPage() c1zstore.PageWriter {
	return s.wrap(s.pebbleStore.BeginPage())
}
func PebbleStoreForTesting(e *pebble.Engine, wrap func(c1zstore.PageWriter) c1zstore.PageWriter) c1zstore.Store {
	return &wrappedPageTestStore{pebbleStore: &pebbleStore{Engine: e}, wrap: wrap}
}
