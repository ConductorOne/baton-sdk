package pebble

import (
	"bytes"
	"runtime"
	"strconv"
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

// lockWriteBarrier takes the engine's write barrier and returns its
// release.
//
// writeMu is a plain, non-reentrant mutex, so calling an exported write
// from inside another write's body wedges that goroutine permanently:
// one goroutine, no output, no stack unless someone sends SIGQUIT. The
// mistake is easy to make because the composed call reads like every
// other write, and the engine offers no way to spell "these two writes
// go together" — so a contributor reaching for atomicity reaches for
// the exported method. This turns the hang into a panic that names what
// happened.
func (e *Engine) lockWriteBarrier() func() {
	if !writeBarrierOwnerChecks {
		e.writeMu.Lock()
		return e.writeMu.Unlock
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
	return func() {
		// Clear before unlocking, or the next holder inherits our id.
		e.writeBarrierOwner.Store(0)
		e.writeMu.Unlock()
	}
}

// assertNotWaitingOnOwnWrite panics when the calling goroutine is inside
// its own write body. Close and CheckpointTo wait on writeWG before they
// take the barrier, so calling either from a write body hangs on the
// wait, one step before the mutex that lockWriteBarrier watches.
func (e *Engine) assertNotWaitingOnOwnWrite() {
	if !writeBarrierOwnerChecks {
		return
	}
	if self := goroutineID(); self != 0 && e.writeBarrierOwner.Load() == self {
		panic(writeBarrierWaitFromWritePanic)
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
