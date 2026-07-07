package pebble

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
)

// fakeCompactionDB is a minimal DBForCompaction that lets the tests control
// exactly when the scheduler sees waiting compactions and how Schedule
// behaves (including blocking mid-grant to freeze the grant loop at a known
// point).
type fakeCompactionDB struct {
	allowed  atomic.Int64
	waiting  atomic.Bool
	calls    atomic.Int64
	schedule func(pebble.CompactionGrantHandle) bool
}

func (f *fakeCompactionDB) GetAllowedWithoutPermission() int { return int(f.allowed.Load()) }

func (f *fakeCompactionDB) GetWaitingCompaction() (bool, pebble.WaitingCompaction) {
	return f.waiting.Load(), pebble.WaitingCompaction{}
}

func (f *fakeCompactionDB) Schedule(h pebble.CompactionGrantHandle) bool {
	f.calls.Add(1)
	if f.schedule != nil {
		return f.schedule(h)
	}
	return true
}

// TestPausableSchedulerPauseBlocksGrantingResumePokes pins the basic
// contract: while paused, neither TrySchedule nor the periodic granter
// grants anything even with a waiting backlog; resume() pokes the granter so
// the backlog is picked up promptly (not only on the next 100ms tick).
func TestPausableSchedulerPauseBlocksGrantingResumePokes(t *testing.T) {
	s := newPausableCompactionScheduler()
	db := &fakeCompactionDB{}
	db.allowed.Store(2)
	s.Register(1, db)
	defer s.Unregister()

	s.pause()
	ok, handle := s.TrySchedule()
	require.False(t, ok, "TrySchedule while paused")
	require.Nil(t, handle, "TrySchedule while paused returns no handle")

	// A waiting backlog while paused: several granter ticks must pass with
	// zero Schedule calls.
	db.waiting.Store(true)
	time.Sleep(300 * time.Millisecond)
	require.Zero(t, db.calls.Load(), "paused scheduler granted a compaction")

	// Resume picks the backlog up promptly (allowed=2 → both grants land).
	s.resume()
	require.Eventually(t, func() bool { return db.calls.Load() == 2 },
		2*time.Second, 5*time.Millisecond, "resume did not grant the waiting backlog")

	// Fully granted: nothing further to grant even though waiting stays true
	// (runningCompactions == allowed).
	time.Sleep(250 * time.Millisecond)
	require.Equal(t, int64(2), db.calls.Load(), "granted beyond allowed concurrency")

	// Done frees a slot; the granter re-grants.
	s.Done()
	require.Eventually(t, func() bool { return db.calls.Load() == 3 },
		2*time.Second, 5*time.Millisecond, "Done did not re-grant the freed slot")
	db.waiting.Store(false)
}

// TestPausableSchedulerPauseDuringInFlightGrantLoop freezes the grant loop
// inside db.Schedule, pauses, then releases: the loop must honor the pause
// on its next iteration and stop granting, even though the backlog and the
// concurrency budget would allow more.
func TestPausableSchedulerPauseDuringInFlightGrantLoop(t *testing.T) {
	s := newPausableCompactionScheduler()
	db := &fakeCompactionDB{}
	db.allowed.Store(4)
	entered := make(chan struct{}, 8)
	release := make(chan struct{})
	db.schedule = func(pebble.CompactionGrantHandle) bool {
		entered <- struct{}{}
		<-release
		return true
	}
	db.waiting.Store(true)
	s.Register(1, db)
	defer func() {
		db.waiting.Store(false)
		s.resume()
		s.Unregister()
	}()

	// The periodic granter enters the grant loop and blocks in Schedule.
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("grant loop never called Schedule")
	}

	// Pause while the grant is in flight, then let Schedule return.
	s.pause()
	close(release)

	// The loop's next-iteration pause check must stop it: exactly the one
	// in-flight grant, despite waiting=true and 3 more allowed slots.
	time.Sleep(300 * time.Millisecond)
	require.Equal(t, int64(1), db.calls.Load(), "grant loop kept granting after pause")
}

// TestPausableSchedulerUnregisterWaitsOutGrantLoop pins Unregister's
// ordering contract: it must not return while a grant loop is mid-flight
// (isGranting), and once it returns no further granting can happen.
func TestPausableSchedulerUnregisterWaitsOutGrantLoop(t *testing.T) {
	s := newPausableCompactionScheduler()
	db := &fakeCompactionDB{}
	db.allowed.Store(1)
	s.Register(1, db)

	// Prime one running compaction through TrySchedule so Done() below
	// triggers the grant loop on THIS test's goroutine tree, leaving the
	// periodic granter free at its select (Unregister's stop send needs it
	// there).
	ok, handle := s.TrySchedule()
	require.True(t, ok, "TrySchedule with a free slot")

	// TrySchedule at the concurrency limit refuses.
	ok, _ = s.TrySchedule()
	require.False(t, ok, "TrySchedule beyond allowed concurrency")

	db.allowed.Store(2)
	db.waiting.Store(true)
	entered := make(chan struct{}, 8)
	release := make(chan struct{})
	db.schedule = func(pebble.CompactionGrantHandle) bool {
		entered <- struct{}{}
		<-release
		return true
	}

	// Done frees the slot and runs the grant loop synchronously on its own
	// goroutine; it blocks inside Schedule with isGranting=true.
	go handle.Done()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("Done's grant loop never called Schedule")
	}

	unregDone := make(chan struct{})
	go func() {
		s.Unregister()
		close(unregDone)
	}()

	// Unregister must be parked until the in-flight grant loop exits.
	select {
	case <-unregDone:
		t.Fatal("Unregister returned while a grant loop was mid-flight")
	case <-time.After(200 * time.Millisecond):
	}

	// Let the loop finish its only grant (drop the backlog so it exits
	// rather than granting again).
	db.waiting.Store(false)
	close(release)
	select {
	case <-unregDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Unregister never returned after the grant loop exited")
	}

	// Unregistered: no further granting, from either entry point.
	calls := db.calls.Load()
	db.waiting.Store(true)
	ok, _ = s.TrySchedule()
	require.False(t, ok, "TrySchedule after Unregister")
	s.Done() // returns the loop's grant; must not re-grant
	time.Sleep(250 * time.Millisecond)
	require.Equal(t, calls, db.calls.Load(), "scheduler granted after Unregister")
}

// TestPausableSchedulerUnregisterWithoutRegister pins the failed-Open guard:
// Unregister with no granter goroutine must return instead of blocking
// forever on the stop channel.
func TestPausableSchedulerUnregisterWithoutRegister(t *testing.T) {
	s := newPausableCompactionScheduler()
	done := make(chan struct{})
	go func() {
		s.Unregister()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Unregister without Register blocked")
	}
}
