// These tests assert the panics the deadlock-shape checks raise, so they
// only exist in builds where the checks are compiled in. Unarmed builds
// would hang exactly where these expect a panic.
//go:build baton_lockchecks || race

package pebble

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
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

// setCurrentSyncInsideWrite is the third shape of the same mistake, and
// the one that gets no help from the barrier: rebinding the current sync
// from inside a write body takes lifecycleMu while holding writeMu, the
// reverse of the order EndSync uses. Sequentially it works, which is what
// makes it dangerous — it needs a concurrent EndSync to deadlock, so it
// can pass review, pass tests, and hang in production.
func (e *Engine) setCurrentSyncInsideWrite(ctx context.Context, syncID string) error {
	return e.withWrite(func() error {
		return e.SetCurrentSync(ctx, syncID)
	})
}

// TestLifecycleTransitionFromInsideWritePanics pins the guard on the two
// lifecycle transitions that never take the barrier. ResumeSync and
// SetCurrentSync read a record and rebind, touching currentSyncMu and
// sealMu but never writeMu, so neither the re-entrancy check nor the
// writeWG drain sees them: without the explicit assertion this call
// returns cleanly and leaves the lock-order violation in the tree.
func TestLifecycleTransitionFromInsideWritePanics(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	syncID, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	require.PanicsWithValue(t, lifecycleFromWriteBarrierPanic, func() {
		_ = e.setCurrentSyncInsideWrite(ctx, syncID)
	})
	// Called normally, from outside a write, the same transition is fine.
	require.NoError(t, e.SetCurrentSync(ctx, syncID))
	require.NoError(t, e.CheckpointSync(ctx, "after-panic"))
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
// twice, and never on the ordinary cases. Writers contending for the
// barrier hand ownership back and forth, which is exactly where a
// bookkeeping bug (clearing after the unlock, say) would report the next
// holder as a re-entrant one.
//
// The concurrent phase writes grants rather than checkpoints, and that is
// the difference between contending and looking like it: CheckpointSync
// holds lifecycleMu across its whole body and only reaches the barrier
// inside PutSyncRunRecord, so concurrent checkpoints queue on the
// lifecycle mutex and reach writeMu one at a time. PutGrants takes the
// barrier with no lifecycle lock above it.
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
		go func(i int) {
			errs <- e.PutGrants(ctx, mkV2Grant(
				fmt.Sprintf("grant-%d", i),
				fmt.Sprintf("entitlement-%d", i),
				"user",
				fmt.Sprintf("principal-%d", i),
			))
		}(i)
	}
	for i := 0; i < writers; i++ {
		require.NoError(t, <-errs)
	}
}

// closeInsidePinnedRead is the read-side shape of the same mistake, and
// the one the engine invites: Iterate* holds the pin across the yield
// callback, so a caller that decides mid-scan it is done and closes the
// engine is waiting for the read it is still inside.
func (e *Engine) closeInsidePinnedRead(ctx context.Context) error {
	return e.IterateGrants(ctx, func(*v3.GrantRecord) bool {
		_ = e.Close()
		return false
	})
}

// TestCloseFromInsidePinnedReadPanics covers the read half of the drain.
// Close waits on readWG as well as writeWG, so the wait-side check has to
// see pinned readers too — a scan callback is caller-supplied code
// running with the pin held, which makes this the easiest way to reach
// the hang from outside the package.
func TestCloseFromInsidePinnedReadPanics(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	_, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, e.PutGrants(ctx, mkV2Grant("grant-1", "entitlement-1", "user", "principal-1")))

	require.PanicsWithValue(t, readPinWaitFromReadPanic, func() {
		_ = e.closeInsidePinnedRead(ctx)
	})
	// The panic unwinds through the scan's release, and nothing was
	// marked closing, so the engine is still usable.
	require.NoError(t, e.CheckpointSync(ctx, "after-panic"))
}

// TestCloseFromInsideWriteRacingAnotherClosePanics pins where the
// assertion sits relative to closeMu. One Close already holds the lock
// and is parked draining writes; the goroutine holding the write it waits
// for is the one calling here. Behind the lock this caller would block on
// closeMu and never reach the check — the same silent hang the check
// exists to report, moved one lock earlier.
func TestCloseFromInsideWriteRacingAnotherClosePanics(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	_, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	e.enterWriteWG()
	closed := make(chan error, 1)
	go func() { closed <- e.Close() }()
	// The flag flips under closeMu on the way to the drain, so observing
	// it means the other Close holds the lock this caller would queue on.
	require.Eventually(t, func() bool { return e.closing.Load() }, 10*time.Second, time.Millisecond,
		"the concurrent Close never reached its drain")

	require.PanicsWithValue(t, writeBarrierWaitFromWritePanic, func() {
		_ = e.Close()
	})
	e.exitWriteWG()
	require.NoError(t, <-closed)
}

// TestCloseFromBarrierFreeWriteWGHolderPanics covers the writeWG holders
// that never take the barrier. CompactAllRanges and Flush join the group
// for the length of a compaction or flush deliberately without writeMu,
// and cleanup.go documents that a Close during either one hangs on the
// drain — so a check keyed on barrier ownership would see nothing wrong
// with the very hang it is there to report. Neither has an injection
// point mid-operation, so this joins the group the same way they do.
func TestCloseFromBarrierFreeWriteWGHolderPanics(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	_, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	e.enterWriteWG()
	defer e.exitWriteWG()
	require.Zero(t, e.writeBarrierOwner.Load(), "a writeWG holder that skipped the barrier must not own it")
	require.PanicsWithValue(t, writeBarrierWaitFromWritePanic, func() {
		_ = e.Close()
	})
}
