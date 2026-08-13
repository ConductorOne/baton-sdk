package pebble

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// stampSyncRunInsideWrite is the mistake the barrier check exists to
// catch, written the way someone would actually write it: a wrapper that
// wants a record write and a sync-run stamp to go together, so it calls
// the exported stamp from inside the write's body. Nothing at this call
// site says the barrier is already held.
func (e *Engine) stampSyncRunInsideWrite(ctx context.Context, syncID string) error {
	return e.withWrite(func() error {
		rec, err := e.GetSyncRunRecord(ctx, syncID)
		if err != nil {
			return err
		}
		return e.PutSyncRunRecord(ctx, rec)
	})
}

// closeInsideWrite is the same mistake one step earlier in the teardown:
// Close waits for in-flight writes, and this goroutine's write is one of
// them, so it hangs on the wait rather than on the mutex.
func (e *Engine) closeInsideWrite() error {
	return e.withWrite(func() error {
		return e.Close()
	})
}

// TestWriteBarrierPanicsOnReentry pins the check that turns a
// single-goroutine deadlock into a diagnosable failure. Without it the
// call below hangs forever with no output: writeMu is not reentrant, and
// there is no concurrency involved to make the hang look like a race.
func TestWriteBarrierPanicsOnReentry(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	syncID, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	require.PanicsWithValue(t, reentrantWriteBarrierPanic, func() {
		_ = e.stampSyncRunInsideWrite(ctx, syncID)
	})

	// The panic unwinds through the outer write's release, so the barrier
	// is free and the engine still works. A check that left the barrier
	// held would trade one hang for another.
	require.NoError(t, e.CheckpointSync(ctx, "after-panic"))
	step, err := e.CurrentSyncStep(ctx)
	require.NoError(t, err)
	require.Equal(t, "after-panic", step)
}

// TestCloseFromInsideWritePanics covers the wait-side variant: Close and
// CheckpointTo drain writeWG before they reach the barrier, so the
// ownership comparison has to happen there too or these hang one step
// short of the mutex the re-entrancy check watches.
func TestCloseFromInsideWritePanics(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	_, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	require.PanicsWithValue(t, writeBarrierWaitFromWritePanic, func() {
		_ = e.closeInsideWrite()
	})
	require.NoError(t, e.CheckpointSync(ctx, "still-open"))
}

// TestWriteBarrierAdmitsSequentialAndConcurrentWrites is the negative
// control: the check must fire on one goroutine holding the barrier
// twice, and never on the ordinary cases. Two writers contending for the
// barrier hand ownership back and forth, which is exactly where a
// bookkeeping bug (clearing after the unlock, say) would report the next
// holder as a re-entrant one.
func TestWriteBarrierAdmitsSequentialAndConcurrentWrites(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	_, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	for i := 0; i < 4; i++ {
		require.NoError(t, e.CheckpointSync(ctx, "sequential"))
	}

	const writers = 8
	errs := make(chan error, writers)
	for i := 0; i < writers; i++ {
		go func() { errs <- e.CheckpointSync(ctx, "concurrent") }()
	}
	for i := 0; i < writers; i++ {
		require.NoError(t, <-errs)
	}
}
