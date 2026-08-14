package pebble

import (
	"bytes"
	"runtime"
	"strconv"
)

// Panic messages for the write barrier's ownership checks. Constants so
// the regression tests assert the exact value rather than a substring
// that could drift. The drain-side panics (waiting on your own write or
// pinned read) live with the admission gate in admission.go.
const (
	reentrantWriteBarrierPanic = "pebble engine: re-entrant write barrier — this goroutine already holds it, " +
		"so an exported write was called from inside another write's body. Restructure the caller, or give " +
		"the inner write an unexported sibling that runs under the barrier already held."
	lifecycleFromWriteBarrierPanic = "pebble engine: sync-lifecycle transition called from inside a write body — " +
		"the lock order is lifecycleMu then writeMu everywhere else, so taking lifecycleMu while holding the " +
		"write barrier deadlocks against a concurrent EndSync. Hoist the transition out of the write."
)

// writeBarrierOwnerChecks (lock_checks_enabled.go / _disabled.go) gates
// every deadlock-shape check at compile time: the barrier ownership
// checks below and the admission gate's self-wait checks (admission.go).
// A goroutine that waits on itself does so deterministically, on the
// first call, with no data dependence and no concurrency required — it
// cannot reach production without hanging the first armed run that
// exercises it, so the armed builds are `make test`, CI, and anything
// built with -race or -tags=baton_lockchecks.

// lockWriteBarrier takes the engine's write barrier; unlockWriteBarrier
// releases it.
//
// writeMu is a plain, non-reentrant mutex, so calling an exported write
// from inside another write's body wedges that goroutine permanently:
// one goroutine, no output, no stack unless someone sends SIGQUIT. The
// mistake is easy to make because the composed call reads like every
// other write, and the engine offers no way to spell "these two writes
// go together" — so a contributor reaching for atomicity reaches for
// the exported method. This turns the hang into a panic that names what
// happened.
//
// A paired unlock rather than a returned release, which would read
// better: returning one is a heap allocation on every write (the method
// value in the unchecked branch escapes just as the closure does), and
// this check is supposed to cost production nothing.
func (e *Engine) lockWriteBarrier() {
	if !writeBarrierOwnerChecks {
		e.writeMu.Lock()
		return
	}
	self := goroutineID()
	// A self of 0 means the id was unreadable; 0 is also "unheld", so
	// skip the comparison rather than report a barrier that is free as
	// re-entered.
	if self != 0 && e.writeBarrierOwner.Load() == self {
		panic(reentrantWriteBarrierPanic)
	}
	e.writeMu.Lock()
	e.writeBarrierOwner.Store(self)
}

// unlockWriteBarrier releases the barrier taken by lockWriteBarrier.
func (e *Engine) unlockWriteBarrier() {
	if writeBarrierOwnerChecks {
		// Clear before unlocking, or the next holder inherits our id.
		e.writeBarrierOwner.Store(0)
	}
	e.writeMu.Unlock()
}

// trackedGoroutineID returns the calling goroutine's id, or 0 when the
// bookkeeping is off or the id was unreadable. Callers skip their
// bookkeeping on 0: it is also the "unheld" value, so recording it would
// make one unreadable id look like every other goroutine.
func trackedGoroutineID() uint64 {
	if !writeBarrierOwnerChecks {
		return 0
	}
	return goroutineID()
}

// assertNotTakingLifecycleFromWrite panics when the calling goroutine
// holds the write barrier. The sync-lifecycle transitions take
// lifecycleMu, and EndSync holds it across a finalize whose steps take
// the barrier — so a transition called from inside a write body supplies
// the writeMu → lifecycleMu order that deadlocks against it.
//
// Three of the five transitions cannot get there: they take the barrier
// themselves, so lockWriteBarrier's re-entrancy check fires first.
// ResumeSync and SetCurrentSync do not — a record read and a rebind,
// neither of which touches writeMu — which leaves them with nothing but
// this to turn the same mistake into a panic instead of a hang.
func (e *Engine) assertNotTakingLifecycleFromWrite() {
	if !writeBarrierOwnerChecks {
		return
	}
	if self := goroutineID(); self != 0 && e.writeBarrierOwner.Load() == self {
		panic(lifecycleFromWriteBarrierPanic)
	}
}

// goroutineID returns the calling goroutine's runtime id, or 0 if the
// runtime's format changes under us. Only reachable from the test-gated
// checks above: it formats a stack frame, which no production write path
// should pay for.
func goroutineID() uint64 {
	var buf [64]byte
	// "goroutine 123 [running]:" — the id is the second field.
	line := buf[:runtime.Stack(buf[:], false)]
	line, ok := bytes.CutPrefix(line, []byte("goroutine "))
	if !ok {
		return 0
	}
	i := bytes.IndexByte(line, ' ')
	if i < 0 {
		return 0
	}
	id, err := strconv.ParseUint(string(line[:i]), 10, 64)
	if err != nil {
		return 0
	}
	return id
}
