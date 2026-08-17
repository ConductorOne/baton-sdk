package pebble

import (
	"context"
	"testing"
	"time"

	"github.com/segmentio/ksuid"
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

// TestCurrentSyncStepReadsRebindFromTheRecord covers the durable read:
// after a rebind the step comes from that sync's record, with no
// in-memory step cache to go stale, and the generation the retry loop
// samples actually moves on a transition. It is deliberately sequential,
// so every call sees an unchanged generation and returns on the first
// pass — the retry branch is exercised by
// TestCurrentSyncStepRetriesWhenBindingMovesMidRead below.
func TestCurrentSyncStepReadsRebindFromTheRecord(t *testing.T) {
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

// TestCurrentSyncStepRetriesWhenBindingMovesMidRead executes the retry
// branch. The window it protects is two statements wide — sample the
// generation, read the record — so a transition has to land inside
// another goroutine's read to reach it, which no amount of concurrent
// hammering can be made to guarantee. The seam puts the transition
// there.
//
// The oracle is the answer, not the retry count: a read that returned
// after the first pass would report the step of the sync that was bound
// when it started, which is the inconsistency dropping lifecycleMu had to
// pay for somewhere.
func TestCurrentSyncStepRetriesWhenBindingMovesMidRead(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	_, other := boundSyncAndSpareID(t, e)

	// Rebind on the first pass only. The hook runs on every iteration, so
	// a hook that rebinds every time is an infinite retry.
	passes := 0
	e.test.currentSyncStepPreReadHook = func() {
		passes++
		if passes > 1 {
			return
		}
		require.NoError(t, e.SetCurrentSync(ctx, other))
	}

	step, err := e.CurrentSyncStep(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, passes, "the binding moved inside the read window, so the read must have been retried")
	require.Empty(t, step,
		`the retry must answer for the binding in force when it finished; "token-a" is the sync that was bound when it started`)
}

// TestCurrentSyncStepRetriesWhenBindingMovesMidReadOnMiss is the same
// window, reached through the not-found branch instead of the hit.
//
// A miss is an answer too, and it has to clear the same bar: the record
// lookup keys on the id sampled at the top, so once the binding has
// moved, "no record for that id" says nothing about the sync now bound.
// Returning "" there reports no step for a sync that never unbound and
// has one — the inconsistency the lock used to rule out, arrived at by
// the one path that skipped the generation re-check.
func TestCurrentSyncStepRetriesWhenBindingMovesMidReadOnMiss(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	bound, spare := boundSyncAndSpareID(t, e)

	// Start from the binding with no record, so the first pass misses.
	require.NoError(t, e.SetCurrentSync(ctx, spare))

	passes := 0
	e.test.currentSyncStepPreReadHook = func() {
		passes++
		if passes > 1 {
			return
		}
		require.NoError(t, e.SetCurrentSync(ctx, bound))
	}

	step, err := e.CurrentSyncStep(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, passes, "the binding moved inside the read window, so the miss must have been retried")
	require.Equal(t, "token-a", step,
		`the retry must answer for the binding in force when it finished; "" is the spare that was bound when it started`)
}

// TestCurrentSyncStepRetryHonorsCancellation drives the retry branch
// forever and requires the call to come back anyway. A hook that rebinds
// on every pass is the pathological case the loop's termination argument
// sets aside as unreachable — transitions are supposed to run out — and
// the argument is about the engine's own behavior, so nothing in it
// protects a caller from a bug or a hostile workload that keeps them
// coming. Without the cancellation check this hangs until the test
// binary's timeout rather than failing.
func TestCurrentSyncStepRetryHonorsCancellation(t *testing.T) {
	e, _ := newTestEngine(t)
	bound, spare := boundSyncAndSpareID(t, e)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Rebind to a different id on every pass, so the generation re-check
	// never agrees and the read can only end by giving up. Cancel from
	// inside the loop rather than up front: the first pass is deliberately
	// unguarded, so a context that was already dead would prove nothing
	// about the retry.
	//
	// The hook runs on the goroutine below, where require's FailNow is
	// not legal, so failures are carried back and asserted here.
	setup := context.Background()
	ids := []string{bound, spare}
	passes := 0
	var rebindErr error
	e.test.currentSyncStepPreReadHook = func() {
		passes++
		if err := e.SetCurrentSync(setup, ids[passes%len(ids)]); err != nil && rebindErr == nil {
			rebindErr = err
		}
		if passes == 2 {
			cancel()
		}
	}

	done := make(chan error, 1)
	go func() {
		_, err := e.CurrentSyncStep(ctx)
		done <- err
	}()

	// Reading passes and rebindErr is only safe on this side of the
	// channel, which is also why the timeout branch does not.
	select {
	case err := <-done:
		require.NoError(t, rebindErr, "rebinding the sync inside the read window failed")
		require.ErrorIs(t, err, context.Canceled,
			"a retry loop that outlives its caller's context has no way to stop")
		require.Greater(t, passes, 1, "the read must have reached the retry branch, not just the first pass")
	case <-time.After(lifecycleTestTimeout):
		t.Fatal("CurrentSyncStep kept retrying after its context was cancelled")
	}
}

// boundSyncAndSpareID starts a sync with the step token "token-a" and
// returns its id plus a second id that is bindable but has no record of
// its own.
//
// The spare has no record because it cannot: a v3 Pebble c1z holds
// exactly one sync and its record lives at a single fixed key, so a
// second StartNewSync wipes the first (ResetForNewSync) and a second
// PutSyncRunRecord overwrites it. SetCurrentSync deliberately allows a
// binding whose record is absent — GetSyncRunRecord's miss is not an
// error to it — and CurrentSyncStep answers "" for that state, which is
// what makes the spare a usable second binding.
func boundSyncAndSpareID(t *testing.T, e *Engine) (string, string) {
	t.Helper()
	ctx := context.Background()
	bound, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, e.CheckpointSync(ctx, "token-a"))

	step, err := e.CurrentSyncStep(ctx)
	require.NoError(t, err)
	require.Equal(t, "token-a", step, "the bound sync must report its own token before anything moves")

	// A KSUID, because that is what the key codec accepts.
	return bound, ksuid.New().String()
}

// TestCurrentSyncStepUnderConcurrentTransitions is the -race soak on the
// lock-free read. The seam test above pins the retry deterministically
// but says nothing about the unsynchronized field access underneath it,
// which is what the race detector is for. The oracle is weak on purpose:
// with transitions landing at arbitrary points, the only invariant left
// is that every answer belongs to a sync that was bound at some point
// during the call — the bound sync's own token, or "" for a binding with
// no record — never a torn or invented one.
func TestCurrentSyncStepUnderConcurrentTransitions(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)

	bound, spare := boundSyncAndSpareID(t, e)
	ids := []string{bound, spare}

	const transitions = 300
	done := make(chan error, 1)
	go func() {
		for i := 0; i < transitions; i++ {
			if err := e.SetCurrentSync(ctx, ids[i%len(ids)]); err != nil {
				done <- err
				return
			}
			if i%3 == 0 {
				e.clearCurrentSync()
			}
		}
		done <- nil
	}()

	// Read on this goroutine: require's FailNow is only legal here.
	for {
		select {
		case err := <-done:
			require.NoError(t, err)
			return
		default:
		}
		step, err := e.CurrentSyncStep(ctx)
		require.NoError(t, err)
		if step == "" {
			continue // cleared, or bound to the spare
		}
		require.Equal(t, "token-a", step,
			"CurrentSyncStep returned a token belonging to no bound sync")
	}
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
