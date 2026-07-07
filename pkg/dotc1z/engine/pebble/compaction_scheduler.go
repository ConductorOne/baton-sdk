package pebble

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/v2"
)

// pausableCompactionScheduler is pebble's ConcurrencyLimitScheduler with a
// pause switch. It exists because a fresh-sync store is saved (checkpoint +
// envelope) and closed moments after EndSync: any automatic compaction still
// running or scheduled in that window is discarded work that competes with
// the deferred index build and the envelope encode for CPU and IO. On the
// whale benchmark the post-expansion window ran 200+ compactions totaling
// ~2 minutes of compaction time whose output never survived to the saved
// artifact.
//
// Pause only stops NEW compactions from being granted; in-flight ones run to
// completion. Flushes are unaffected (they do not go through the
// CompactionScheduler), so memtables still drain to L0. A paused engine can
// accumulate L0 files toward L0StopWritesThreshold, which is why pausing is
// reserved for the EndSync-to-close window where writes are near zero, and
// why binding a new sync resumes the scheduler.
type pausableCompactionScheduler struct {
	// db is set in Register, strictly before any other method is called.
	db         pebble.DBForCompaction
	paused     atomic.Bool
	registered atomic.Bool

	mu struct {
		sync.Mutex
		runningCompactions int
		// unregistered transitions once from false => true.
		unregistered bool
		// isGranting serializes granting from Done and the periodic granter,
		// and lets Unregister wait out an in-flight grant loop.
		isGranting                   bool
		isGrantingCond               *sync.Cond
		lastAllowedWithoutPermission int
	}
	stopPeriodicGranterCh chan struct{}
	pokePeriodicGranterCh chan struct{}
}

var _ pebble.CompactionScheduler = (*pausableCompactionScheduler)(nil)
var _ pebble.CompactionGrantHandle = (*pausableCompactionScheduler)(nil)

func newPausableCompactionScheduler() *pausableCompactionScheduler {
	s := &pausableCompactionScheduler{
		stopPeriodicGranterCh: make(chan struct{}),
		pokePeriodicGranterCh: make(chan struct{}, 1),
	}
	s.mu.isGrantingCond = sync.NewCond(&s.mu.Mutex)
	return s
}

// pause stops granting new compactions. In-flight compactions finish
// normally.
func (s *pausableCompactionScheduler) pause() {
	s.paused.Store(true)
}

// resume re-enables compaction granting and pokes the granter so a backlog
// is picked up promptly.
func (s *pausableCompactionScheduler) resume() {
	if !s.paused.Swap(false) {
		return
	}
	select {
	case s.pokePeriodicGranterCh <- struct{}{}:
	default:
	}
}

func (s *pausableCompactionScheduler) Register(numGoroutinesPerCompaction int, db pebble.DBForCompaction) {
	s.db = db
	s.registered.Store(true)
	go s.periodicGranter()
}

func (s *pausableCompactionScheduler) Unregister() {
	// Guard against an Unregister without a successful Register (e.g. a
	// failed Open): with no granter goroutine, the stop send would block
	// forever.
	if !s.registered.Swap(false) {
		return
	}
	s.stopPeriodicGranterCh <- struct{}{}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.unregistered = true
	// Wait until isGranting becomes false. Since unregistered is now true,
	// once isGranting is false no more granting can happen.
	for s.mu.isGranting {
		s.mu.isGrantingCond.Wait()
	}
}

func (s *pausableCompactionScheduler) TrySchedule() (bool, pebble.CompactionGrantHandle) {
	if s.paused.Load() {
		return false, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mu.unregistered {
		return false, nil
	}
	s.mu.lastAllowedWithoutPermission = s.db.GetAllowedWithoutPermission()
	if s.mu.lastAllowedWithoutPermission > s.mu.runningCompactions {
		s.mu.runningCompactions++
		return true, s
	}
	return false, nil
}

func (s *pausableCompactionScheduler) Started()                                          {}
func (s *pausableCompactionScheduler) MeasureCPU(pebble.CompactionGoroutineKind)         {}
func (s *pausableCompactionScheduler) CumulativeStats(pebble.CompactionGrantHandleStats) {}

func (s *pausableCompactionScheduler) Done() {
	s.mu.Lock()
	s.mu.runningCompactions--
	s.tryGrantLockedAndUnlock()
}

func (s *pausableCompactionScheduler) UpdateGetAllowedWithoutPermission() {
	s.mu.Lock()
	allowedWithoutPermission := s.db.GetAllowedWithoutPermission()
	tryGrant := allowedWithoutPermission > s.mu.lastAllowedWithoutPermission
	s.mu.lastAllowedWithoutPermission = allowedWithoutPermission
	s.mu.Unlock()
	if tryGrant {
		select {
		case s.pokePeriodicGranterCh <- struct{}{}:
		default:
		}
	}
}

// tryGrantLockedAndUnlock mirrors ConcurrencyLimitScheduler: grant as many
// waiting compactions as the DB's allowed concurrency permits. Callers hold
// s.mu; it is unlocked on return.
func (s *pausableCompactionScheduler) tryGrantLockedAndUnlock() {
	defer s.mu.Unlock()
	if s.mu.unregistered || s.paused.Load() {
		return
	}
	// Wait for turn to grant.
	for s.mu.isGranting {
		s.mu.isGrantingCond.Wait()
	}
	if s.mu.unregistered || s.paused.Load() {
		return
	}
	s.mu.lastAllowedWithoutPermission = s.db.GetAllowedWithoutPermission()
	toGrant := s.mu.lastAllowedWithoutPermission - s.mu.runningCompactions
	if toGrant <= 0 {
		return
	}
	s.mu.isGranting = true
	s.mu.Unlock()
	// INVARIANT: loop exits with s.mu unlocked.
	for toGrant > 0 && !s.paused.Load() {
		waiting, _ := s.db.GetWaitingCompaction()
		if !waiting {
			break
		}
		if !s.db.Schedule(s) {
			break
		}
		s.mu.Lock()
		s.mu.runningCompactions++
		toGrant--
		s.mu.Unlock()
	}
	// Unlocked by the defer.
	s.mu.Lock()
	s.mu.isGranting = false
	s.mu.isGrantingCond.Broadcast()
}

func (s *pausableCompactionScheduler) periodicGranter() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.mu.Lock()
			s.tryGrantLockedAndUnlock()
		case <-s.pokePeriodicGranterCh:
			s.mu.Lock()
			s.tryGrantLockedAndUnlock()
		case <-s.stopPeriodicGranterCh:
			return
		}
	}
}
