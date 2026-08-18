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
	admissionUnderflowPanic = "pebble engine: admission exit without a matching enter — an operation released " +
		"the gate twice, so the drain accounting is corrupt."
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
// A closing check and a counter increment are two steps, and re-checking
// after the increment does not make them one: a joiner that read
// closing==false can be descheduled and land its increment after close
// has started draining at zero, becoming a member the drain never
// counted. So joiners hold mu.RLock across the check and the increment,
// and closeAndDrain holds mu.Lock across the flip: the increment either
// precedes the flip (close waits for that operation) or never happens.
//
// The members are counted with plain integers and a condition variable,
// NOT sync.WaitGroup. WaitGroup's contract forbids an Add concurrent
// with a Wait that started at counter zero — a rule closeAndDrain can
// honor (the flip stops new Adds before the Wait), but drainWrites
// cannot: it drains while the gate stays OPEN, so a new writer entering
// in the instant the counter touches zero is legal here and answered by
// WaitGroup with the "Add called concurrently with Wait" runtime fatal.
// A cond-based wait-until-zero admits joiners while parked and simply
// keeps waiting, which is the semantic both drains actually want.
//
// The drains also refuse to wait on their own caller. Both waits are
// forever when the calling goroutine is a member of the group being
// drained — the read side is the invited shape, since a pinned read runs
// caller-supplied code (an Iterate*/ForEach* yield callback) for the
// whole of the pin — so closeAndDrain and drainWrites check membership
// first and panic with the constants above instead of hanging silently.
// Membership tracking costs a runtime-stack format per operation and is
// compiled in only when writeBarrierOwnerChecks is set
// (lock_checks_enabled.go); unchecked builds skip the check and would
// hang, which is why the armed builds are the ones CI runs.
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
	// countMu guards writers/readers; zero (lazily minted on countMu)
	// is broadcast whenever either counter returns to zero, waking the
	// drains to re-check their condition.
	countMu sync.Mutex
	zero    *sync.Cond
	writers int
	readers int
	// Membership of the two groups by goroutine id, for the self-wait
	// panics. Empty (and never written) in unchecked builds.
	writerIDs goroutineSet
	readerIDs goroutineSet
}

// zeroCond returns the drain wake-up cond, minting it on first use so
// the zero value of admission stays usable. Callers hold countMu.
func (a *admission) zeroCond() *sync.Cond {
	if a.zero == nil {
		a.zero = sync.NewCond(&a.countMu)
	}
	return a.zero
}

// enterWrite admits the caller as a write, or refuses with
// ErrEngineClosing once the gate is shut. On success the caller must
// exitWrite when done, normally with an immediate defer. Entering is
// counted, not owned: a goroutine already admitted may enter again (a
// nested withWrite inside an admitted lifecycle transition), and the
// drains wait for every entry to exit.
func (a *admission) enterWrite() error {
	a.mu.RLock()
	closing := a.closing.Load()
	if !closing {
		a.countMu.Lock()
		a.writers++
		a.countMu.Unlock()
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
	a.countMu.Lock()
	a.writers--
	if a.writers < 0 {
		a.countMu.Unlock()
		panic(admissionUnderflowPanic)
	}
	if a.writers == 0 {
		a.zeroCond().Broadcast()
	}
	a.countMu.Unlock()
}

// enterRead admits the caller as a read; same contract as enterWrite.
func (a *admission) enterRead() error {
	a.mu.RLock()
	closing := a.closing.Load()
	if !closing {
		a.countMu.Lock()
		a.readers++
		a.countMu.Unlock()
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
	a.countMu.Lock()
	a.readers--
	if a.readers < 0 {
		a.countMu.Unlock()
		panic(admissionUnderflowPanic)
	}
	if a.readers == 0 {
		a.zeroCond().Broadcast()
	}
	a.countMu.Unlock()
}

// isClosing reports whether the gate has been flipped shut. Advisory
// for fail-fast checks: the answer can change immediately after, and
// only enterRead/enterWrite decide admission.
func (a *admission) isClosing() bool {
	return a.closing.Load()
}

// drainWrites waits for every admitted write to exit, without shutting
// the gate — new writes may enter while it waits and after it returns,
// each extending the wait (see the type comment for why that rules out
// sync.WaitGroup). CheckpointTo's quiesce. Panics rather than
// deadlocking when called from inside a write, in armed builds; an
// unchecked build hangs.
func (a *admission) drainWrites() {
	if self := trackedGoroutineID(); self != 0 && a.writerIDs.holds(self) {
		panic(writeBarrierWaitFromWritePanic)
	}
	a.countMu.Lock()
	for a.writers > 0 {
		a.zeroCond().Wait()
	}
	a.countMu.Unlock()
}

// closeAndDrain shuts the gate, waits for every admitted operation to
// exit, then runs teardown exactly once, returning its error. Later
// calls return nil after the first completes. Panics rather than
// deadlocking when called from inside an admitted operation (armed
// builds only) — checked before closeMu, not merely before the flip,
// because a concurrent closeAndDrain can already hold the lock and be
// parked in a wait this caller's own operation is keeping parked:
// behind the lock this caller would block silently one lock earlier
// than the check was looking. Asking first also leaves a usable engine
// behind — nothing is marked closing when it panics.
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
	a.countMu.Lock()
	for a.writers > 0 || a.readers > 0 {
		a.zeroCond().Wait()
	}
	a.countMu.Unlock()
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
