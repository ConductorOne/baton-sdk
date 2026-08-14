package pebble

import (
	"sync"
	"sync/atomic"
)

// Panic messages for waiting on yourself through the drains below.
// Constants so the regression tests assert the exact value rather than
// a substring that could drift.
const (
	writeBarrierWaitFromWritePanic = "pebble engine: waited for in-flight writes from inside a write body — " +
		"this goroutine's own write is one of them, so the wait can never finish."
	readPinWaitFromReadPanic = "pebble engine: waited for in-flight reads from inside a pinned read — this " +
		"goroutine's own read is one of them, so the wait can never finish. Iterate* and ForEach* hold the pin " +
		"across the yield callback, so closing the engine from inside one waits on itself."
)

// admission is the engine's open/close gate. Operations enter as reads
// or writes and exit when done; Close flips the gate shut and waits for
// every operation already inside; anything arriving after the flip gets
// ErrEngineClosing. It answers exactly one question — "may this
// operation borrow the handle Close tears down?" — and owns everything
// needed to answer it, so the invariants live behind this type's five
// methods instead of on the Engine struct.
//
// The load-bearing detail is that entering is atomic against the flip.
// A closing check and a WaitGroup.Add are two steps, and re-checking
// after the Add does not make them one: a joiner that read
// closing==false can be descheduled and run its Add after close has
// parked in Wait with the counter at zero, which sync.WaitGroup answers
// with the "Add called concurrently with Wait" fatal rather than the
// ErrEngineClosing the joiner is owed. So joiners hold mu.RLock across
// the check and the Add, and closeAndDrain holds mu.Lock across the
// flip: the Add either precedes the flip (close waits for that
// operation) or never happens.
//
// The drains also refuse to wait on their own caller. Both waits are
// forever when the calling goroutine is a member of the group being
// drained — the read side is the invited shape, since a pinned read runs
// caller-supplied code (an Iterate*/ForEach* yield callback) for the
// whole of the pin — so closeAndDrain and drainWrites check membership
// first and panic with the constants above instead of hanging silently.
// Membership tracking costs a runtime-stack format per operation and is
// compiled in only when writeBarrierOwnerChecks is set
// (lock_checks_enabled.go).
type admission struct {
	// mu makes entering atomic against closeAndDrain's flip; see the
	// type comment. Joiners RLock, the flip Locks.
	mu      sync.RWMutex
	closing atomic.Bool
	// closeMu serializes closeAndDrain bodies; closed (guarded by it)
	// makes the teardown run exactly once, with later calls returning
	// nil.
	closeMu sync.Mutex
	closed  bool
	writers sync.WaitGroup
	readers sync.WaitGroup
	// Membership of the two groups by goroutine id, for the self-wait
	// panics. Empty (and never written) in unchecked builds.
	writerIDs goroutineSet
	readerIDs goroutineSet
}

// enterWrite admits the caller as a write, or refuses with
// ErrEngineClosing once the gate is shut. On success the caller must
// exitWrite when done, normally with an immediate defer.
func (a *admission) enterWrite() error {
	a.mu.RLock()
	closing := a.closing.Load()
	if !closing {
		a.writers.Add(1)
		if self := trackedGoroutineID(); self != 0 {
			a.writerIDs.enter(self)
		}
	}
	a.mu.RUnlock()
	if closing {
		return ErrEngineClosing
	}
	return nil
}

// exitWrite releases an enterWrite.
func (a *admission) exitWrite() {
	if self := trackedGoroutineID(); self != 0 {
		a.writerIDs.exit(self)
	}
	a.writers.Done()
}

// enterRead admits the caller as a read; same contract as enterWrite.
func (a *admission) enterRead() error {
	a.mu.RLock()
	closing := a.closing.Load()
	if !closing {
		a.readers.Add(1)
		if self := trackedGoroutineID(); self != 0 {
			a.readerIDs.enter(self)
		}
	}
	a.mu.RUnlock()
	if closing {
		return ErrEngineClosing
	}
	return nil
}

// exitRead releases an enterRead.
func (a *admission) exitRead() {
	if self := trackedGoroutineID(); self != 0 {
		a.readerIDs.exit(self)
	}
	a.readers.Done()
}

// isClosing reports whether the gate has been flipped shut. Advisory
// for fail-fast checks: the answer can change immediately after, and
// only enterRead/enterWrite decide admission.
func (a *admission) isClosing() bool {
	return a.closing.Load()
}

// drainWrites waits for every admitted write to exit, without shutting
// the gate — new writes may enter as soon as it returns. CheckpointTo's
// quiesce. Panics rather than deadlocking when called from inside a
// write.
func (a *admission) drainWrites() {
	if self := trackedGoroutineID(); self != 0 && a.writerIDs.holds(self) {
		panic(writeBarrierWaitFromWritePanic)
	}
	a.writers.Wait()
}

// closeAndDrain shuts the gate, waits for every admitted operation to
// exit, then runs teardown exactly once, returning its error. Later
// calls return nil after the first completes. Panics rather than
// deadlocking when called from inside an admitted operation — checked
// before closeMu, not merely before the flip, because a concurrent
// closeAndDrain can already hold the lock and be parked in a wait this
// caller's own operation is keeping parked: behind the lock this caller
// would block silently one lock earlier than the check was looking.
// Asking first also leaves a usable engine behind — nothing is marked
// closing when it panics.
func (a *admission) closeAndDrain(teardown func() error) error {
	if self := trackedGoroutineID(); self != 0 {
		if a.writerIDs.holds(self) {
			panic(writeBarrierWaitFromWritePanic)
		}
		if a.readerIDs.holds(self) {
			panic(readPinWaitFromReadPanic)
		}
	}
	a.closeMu.Lock()
	defer a.closeMu.Unlock()
	if a.closed {
		return nil
	}
	a.mu.Lock()
	a.closing.Store(true)
	a.mu.Unlock()
	a.writers.Wait()
	a.readers.Wait()
	err := teardown()
	a.closed = true
	return err
}

// goroutineSet records which goroutines are currently admitted through
// one side of the gate.
//
// Counted rather than a plain set: nothing forbids a goroutine from
// entering twice — a compaction inside a write body, a second pinned
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
