package pebble

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// lifecycleTestTimeout is how long these tests wait before calling a
// deadlock a deadlock. Both sides do a few key reads, so any real run
// finishes in milliseconds; the budget is loose so a slow CI box does not
// have to be fast to be correct.
const lifecycleTestTimeout = 30 * time.Second

// readStepInsideWrite is the shape that closed the lock cycle: a write
// that consults the current step while it holds the write barrier. It
// parks between the two so the test can line up the interleaving instead
// of hoping for it.
func (e *Engine) readStepInsideWrite(ctx context.Context, held, proceed chan struct{}) (string, error) {
	var step string
	err := e.withWrite(func() error {
		close(held)
		<-proceed
		var err error
		step, err = e.CurrentSyncStep(ctx)
		return err
	})
	return step, err
}

// TestCurrentSyncStepDoesNotDeadlockWithEndSync is the regression test
// for the ABBA deadlock between the write barrier and the lifecycle
// mutex.
//
// The interleaving: a writer holds writeMu and then wants the current
// step, while EndSync holds lifecycleMu and then wants writeMu for its
// finalize. When CurrentSyncStep took lifecycleMu, both sides held what
// the other needed and the process hung — including any goroutine that
// later touched either lock. Neither side is doing anything exotic: one
// is a write that reads its own progress, the other is the ordinary end
// of a sync.
func TestCurrentSyncStepDoesNotDeadlockWithEndSync(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	_, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, e.CheckpointSync(ctx, "mid-sync"))

	held := make(chan struct{})
	proceed := make(chan struct{})
	type writeResult struct {
		step string
		err  error
	}
	writerDone := make(chan writeResult, 1)
	go func() {
		step, err := e.readStepInsideWrite(ctx, held, proceed)
		writerDone <- writeResult{step: step, err: err}
	}()

	// Wait until the writer owns the barrier, so EndSync is guaranteed to
	// block on it rather than racing ahead and finishing first — a pass
	// that skipped the interleaving would prove nothing.
	select {
	case <-held:
	case <-time.After(lifecycleTestTimeout):
		t.Fatal("the writer never took the write barrier")
	}

	endDone := make(chan error, 1)
	go func() { endDone <- e.EndSync(ctx) }()
	requireLifecycleMuHeld(t, e)

	// Both sides are now holding one lock and about to want the other.
	close(proceed)

	select {
	case got := <-writerDone:
		require.NoError(t, got.err)
		require.Equal(t, "mid-sync", got.step, "the step read inside the write must be the bound sync's")
	case <-time.After(lifecycleTestTimeout):
		t.Fatal("the write blocked reading the current step while EndSync held the lifecycle mutex")
	}
	select {
	case err := <-endDone:
		require.NoError(t, err)
	case <-time.After(lifecycleTestTimeout):
		t.Fatal("EndSync never completed after the write released the barrier")
	}

	// EndSync detached the sync, so there is no step to report.
	step, err := e.CurrentSyncStep(ctx)
	require.NoError(t, err)
	require.Empty(t, step)
}

// TestCurrentSyncStepRetriesAcrossRebind covers the consistency the
// removed lock used to provide. The generation is sampled on both sides
// of the record read, so a binding that moves in between is caught and
// the read is retried against the new one — the returned token always
// belongs to a sync that was actually bound.
func TestCurrentSyncStepRetriesAcrossRebind(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	first, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, e.CheckpointSync(ctx, "first-token"))
	require.NoError(t, e.EndSync(ctx))

	// Rebinding the finished sync is what SetCurrentSync is for; the step
	// must come from that sync's record and not from anything cached.
	require.NoError(t, e.SetCurrentSync(ctx, first))
	step, err := e.CurrentSyncStep(ctx)
	require.NoError(t, err)
	require.Equal(t, "first-token", step)

	// A generation that never moved would make the retry loop vacuous, so
	// check the counter actually advances with the transitions above.
	_, gen := e.currentSyncBinding()
	require.Greater(t, gen, uint64(0), "binding transitions must bump the generation")
	e.clearCurrentSync()
	_, cleared := e.currentSyncBinding()
	require.Greater(t, cleared, gen, "clearing the binding must bump the generation")
}

// requireLifecycleMuHeld waits until some other goroutine owns
// lifecycleMu. TryLock is the only way to ask, and this test needs the
// answer: without it, EndSync might not have reached its lock yet when
// the writer is released, and the ordering the test exists to check
// would not have happened.
func requireLifecycleMuHeld(t *testing.T, e *Engine) {
	t.Helper()
	deadline := time.Now().Add(lifecycleTestTimeout)
	for time.Now().Before(deadline) {
		if !e.lifecycleMu.TryLock() {
			return
		}
		e.lifecycleMu.Unlock()
		time.Sleep(time.Millisecond)
	}
	t.Fatal("EndSync never took the lifecycle mutex")
}
