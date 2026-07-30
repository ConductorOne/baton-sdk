package pebble

// Mutation adequacy for the ride-along resource-leak oracle (§4 rung 3,
// ride-along variant): every engine handle class that requires explicit
// release — iterators and Get closers (tracked by pebble itself through
// version refcounts) and family batches (tracked by rawdb's
// batchAccounting) — must turn Engine.Close red when leaked. The shared
// test fixtures (newTestEngine, newAdapter) assert a clean Close, so
// once these planted violations prove the oracle can see each class,
// every test in the package is a leak detector for free.
//
// The subtests open engines directly instead of through the fixtures:
// the fixtures' cleanup requires a clean Close, which the planted
// violations exist to violate.

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
)

func openLeakProbeEngine(t *testing.T) *Engine {
	t.Helper()
	e, err := Open(context.Background(), filepath.Join(t.TempDir(), "db"))
	require.NoError(t, err, "Open")
	return e
}

func TestResourceLeakRideAlongAdequacy(t *testing.T) {
	t.Run("clean-lifecycle-close-is-clean", func(t *testing.T) {
		// Non-vacuity: the ordinary mint→commit→close and read→close
		// lifecycles must not trip the oracle.
		e := openLeakProbeEngine(t)
		rb := e.db.NewRecordBatch()
		require.NoError(t, rb.Commit(pebble.NoSync), "empty commit")
		require.NoError(t, rb.Close(), "close after commit")
		it, err := e.NewIter(&pebble.IterOptions{})
		require.NoError(t, err)
		require.NoError(t, it.Close())
		require.NoError(t, e.Close())
	})

	t.Run("double-close-batch-is-nil-safe-and-balanced", func(t *testing.T) {
		// The pooled-object ownership rule (§5.2): the first Close
		// transfers the batch back to pebble's pool; a second Close
		// must be a no-op, not a second release, and must not
		// underflow the ledger into masking a real leak elsewhere.
		e := openLeakProbeEngine(t)
		rb := e.db.NewRecordBatch()
		require.NoError(t, rb.Close())
		require.NoError(t, rb.Close(), "second Close must be a nil-safe no-op")
		sb := e.db.NewSessionBatch()
		require.NoError(t, sb.Close())
		require.NoError(t, sb.Close())
		db := e.db.NewDigestBatch()
		require.NoError(t, db.Close())
		require.NoError(t, db.Close())
		fb := e.db.NewFoldBatch()
		require.NoError(t, fb.Close())
		require.NoError(t, fb.Close())
		require.NoError(t, e.Close(), "balanced double closes must leave the ledger at zero")
	})

	t.Run("planted-leaked-record-batch-fails-close", func(t *testing.T) {
		e := openLeakProbeEngine(t)
		_ = e.db.NewRecordBatch() // deliberately leaked
		err := e.Close()
		require.ErrorContains(t, err, "unreleased family batches")
		require.ErrorContains(t, err, "record=1")
	})

	t.Run("planted-leaked-session-batch-fails-close", func(t *testing.T) {
		e := openLeakProbeEngine(t)
		_ = e.db.NewSessionBatch() // deliberately leaked
		err := e.Close()
		require.ErrorContains(t, err, "unreleased family batches")
		require.ErrorContains(t, err, "session=1")
	})

	t.Run("planted-leaked-digest-batch-fails-close", func(t *testing.T) {
		e := openLeakProbeEngine(t)
		_ = e.db.NewDigestBatch() // deliberately leaked
		err := e.Close()
		require.ErrorContains(t, err, "unreleased family batches")
		require.ErrorContains(t, err, "digest=1")
	})

	t.Run("planted-leaked-fold-batch-fails-close", func(t *testing.T) {
		e := openLeakProbeEngine(t)
		_ = e.db.NewFoldBatch() // deliberately leaked
		err := e.Close()
		require.ErrorContains(t, err, "unreleased family batches")
		require.ErrorContains(t, err, "fold=1")
	})

	t.Run("planted-leaked-iterator-fails-close", func(t *testing.T) {
		e := openLeakProbeEngine(t)
		it, err := e.NewIter(&pebble.IterOptions{})
		require.NoError(t, err)
		_ = it // deliberately leaked
		require.ErrorContains(t, e.Close(), "leaked",
			"pebble must report the leaked iterator at Close")
	})

	t.Run("planted-leaked-get-closer-fails-close", func(t *testing.T) {
		e := openLeakProbeEngine(t)
		key := []byte("leak-probe-key")
		require.NoError(t, e.db.MetaSet(key, []byte("v"), pebble.Sync))
		_, closer, err := e.db.Get(key)
		require.NoError(t, err)
		require.NotNil(t, closer) // deliberately never closed
		require.ErrorContains(t, e.Close(), "leaked",
			"pebble must report the leaked Get closer at Close")
	})
}
