package pebble

import (
	"bytes"
	"runtime"
	"strconv"
	"sync"
	"testing"
)

// Panic messages for the two ways a goroutine can wait on itself here.
// Exported as constants so the regression tests assert the exact value
// rather than a substring that could drift.
const (
	reentrantWriteBarrierPanic = "pebble engine: re-entrant write barrier — this goroutine already holds it, " +
		"so an exported write was called from inside another write's body. Restructure the caller, or give " +
		"the inner write an unexported sibling that runs under the barrier already held."
	writeBarrierWaitFromWritePanic = "pebble engine: waited for in-flight writes from inside a write body — " +
		"this goroutine's own write is one of them, so the wait can never finish."
	readPinWaitFromReadPanic = "pebble engine: waited for in-flight reads from inside a pinned read — this " +
		"goroutine's own read is one of them, so the wait can never finish. Iterate* and ForEach* hold the pin " +
		"across the yield callback, so closing the engine from inside one waits on itself."
	lifecycleFromWriteBarrierPanic = "pebble engine: sync-lifecycle transition called from inside a write body — " +
		"the lock order is lifecycleMu then writeMu everywhere else, so taking lifecycleMu while holding the " +
		"write barrier deadlocks against a concurrent EndSync. Hoist the transition out of the write."
)

// writeBarrierOwnerChecks turns on the ownership bookkeeping below. It
// is on under `go test` and off in production, the same runtime gate
// rawdb uses for its escape hatch.
//
// Knowing which goroutine holds the barrier means asking the runtime to
// format a stack, and paying that on every write to catch this class of
// mistake is the wrong trade: a goroutine that waits on itself does so
// deterministically, on the first call, with no data dependence and no
// concurrency required. It cannot reach production without hanging in
// the test that first exercises it — which is the run this gate covers.
var writeBarrierOwnerChecks = testing.Testing()

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

// goroutineSet records which goroutines are currently counted in one of
// the WaitGroups Close drains.
//
// Counted rather than a plain set: nothing forbids a goroutine from
// joining one twice — a compaction inside a write body, a second pinned
// read taken inside a scan's yield callback — and a plain delete on the
// inner exit would hide the outer one.
type goroutineSet struct {
	mu sync.Mutex
	m  map[uint64]int
}

func (s *goroutineSet) enter(self uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.m == nil {
		s.m = make(map[uint64]int, 1)
	}
	s.m[self]++
}

func (s *goroutineSet) exit(self uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if n := s.m[self]; n > 1 {
		s.m[self] = n - 1
		return
	}
	delete(s.m, self)
}

func (s *goroutineSet) holds(self uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.m[self] > 0
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

// enterWriteWG counts the calling goroutine in writeWG; exitWriteWG
// releases it. Every writeWG.Add and Done goes through the pair (pinned
// by TestWriteBarrierWaitersCheckOwnership) so the participant set stays
// in step with the counter Close and CheckpointTo drain.
//
// Barrier ownership is not a usable substitute: CompactAllRanges and
// Flush hold writeWG for a long operation and deliberately never take
// writeMu (pebble's compactions and flushes are concurrency-safe with
// foreground writes), yet a Close called from inside either one blocks
// on the wait forever — the hang cleanup.go documents. Ownership of the
// barrier is 0 for those goroutines, so a check that consulted it saw
// nothing wrong.
//
// Two methods rather than one returning its own release, which would read
// better at the call sites: the returned func is a heap allocation on
// every write, and writes are the hot path this engine exists for.
func (e *Engine) enterWriteWG() {
	e.writeWG.Add(1)
	if self := trackedGoroutineID(); self != 0 {
		e.writeWGParticipants.enter(self)
	}
}

// exitWriteWG is enterWriteWG's release. Always deferred immediately
// after the enter, so an early return between them cannot leave the
// counter — or the set — holding a write that finished.
func (e *Engine) exitWriteWG() {
	if self := trackedGoroutineID(); self != 0 {
		e.writeWGParticipants.exit(self)
	}
	e.writeWG.Done()
}

// enterReadWG and exitReadWG are the read-side pair, for the group
// pinRead joins and Close drains alongside writeWG.
func (e *Engine) enterReadWG() {
	e.readWG.Add(1)
	if self := trackedGoroutineID(); self != 0 {
		e.readWGParticipants.enter(self)
	}
}

func (e *Engine) exitReadWG() {
	if self := trackedGoroutineID(); self != 0 {
		e.readWGParticipants.exit(self)
	}
	e.readWG.Done()
}

// assertNotWaitingOnOwnWrite panics when the calling goroutine is
// counted in writeWG. Close and CheckpointTo wait on writeWG before they
// take the barrier, so calling either from a write body — or from a
// compaction or flush, which join the group without the barrier — hangs
// on the wait, one step before the mutex that lockWriteBarrier watches.
func (e *Engine) assertNotWaitingOnOwnWrite() {
	if self := trackedGoroutineID(); self != 0 && e.writeWGParticipants.holds(self) {
		panic(writeBarrierWaitFromWritePanic)
	}
}

// assertNotWaitingOnOwnRead is the same check for readWG, which Close
// drains too. The read side has a shape the write side does not: a
// pinned read runs caller-supplied code — the yield and visit callbacks
// of the Iterate* and ForEach* families — for the whole of the pin, so
// the engine hands a stranger the one context in which closing it hangs.
func (e *Engine) assertNotWaitingOnOwnRead() {
	if self := trackedGoroutineID(); self != 0 && e.readWGParticipants.holds(self) {
		panic(readPinWaitFromReadPanic)
	}
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
