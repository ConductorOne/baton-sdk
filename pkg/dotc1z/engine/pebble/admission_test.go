package pebble

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// These test the admission type directly, with no Engine and no DB
// behind it: the gate's contract is self-contained, and testing it here
// is what lets TestAdmissionUsedOnlyThroughItsMethods confine the
// mechanism to admission.go instead of enumerating WaitGroup call sites
// across the package.

func TestAdmissionRefusesAfterClose(t *testing.T) {
	var a admission
	require.NoError(t, a.enterWrite())
	a.exitWrite()
	require.NoError(t, a.enterRead())
	a.exitRead()

	require.NoError(t, a.closeAndDrain(func() error { return nil }))
	require.True(t, a.isClosing())
	require.ErrorIs(t, a.enterWrite(), ErrEngineClosing)
	require.ErrorIs(t, a.enterRead(), ErrEngineClosing)
}

func TestAdmissionCloseWaitsForAdmittedOperations(t *testing.T) {
	var a admission
	require.NoError(t, a.enterWrite())
	require.NoError(t, a.enterRead())

	var toreDown atomic.Bool
	closed := make(chan error, 1)
	go func() {
		closed <- a.closeAndDrain(func() error {
			toreDown.Store(true)
			return nil
		})
	}()

	// The gate must shut (new arrivals refused) while it drains, but the
	// teardown must not run until both members exit.
	require.Eventually(t, a.isClosing, 10*time.Second, time.Millisecond)
	require.ErrorIs(t, a.enterWrite(), ErrEngineClosing)
	require.Never(t, toreDown.Load, 100*time.Millisecond, 5*time.Millisecond,
		"teardown ran while operations were still admitted")

	a.exitWrite()
	require.Never(t, toreDown.Load, 100*time.Millisecond, 5*time.Millisecond,
		"teardown ran while a read was still admitted")
	a.exitRead()
	require.NoError(t, <-closed)
	require.True(t, toreDown.Load())
}

func TestAdmissionTeardownRunsExactlyOnce(t *testing.T) {
	var a admission
	calls := 0
	sentinel := errors.New("teardown result")
	require.ErrorIs(t, a.closeAndDrain(func() error { calls++; return sentinel }), sentinel)
	// Later closes are nil, matching Engine.Close's idempotency: the
	// engine is closed, there is nothing left to report.
	require.NoError(t, a.closeAndDrain(func() error { calls++; return nil }))
	require.Equal(t, 1, calls)
}

func TestAdmissionDrainWritesQuiescesWithoutShutting(t *testing.T) {
	var a admission
	require.NoError(t, a.enterWrite())

	drained := make(chan struct{})
	go func() {
		a.drainWrites()
		close(drained)
	}()
	select {
	case <-drained:
		t.Fatal("drainWrites returned while a write was admitted")
	case <-time.After(100 * time.Millisecond):
	}
	a.exitWrite()
	<-drained

	// Not a close: both sides stay open.
	require.False(t, a.isClosing())
	require.NoError(t, a.enterWrite())
	a.exitWrite()
}

// TestAdmissionEnterNeverTripsDrainingWaitGroup hammers the exact
// interleaving the gate's mu exists for: enters racing a close whose
// drain is parked at zero. Without admission atomicity a joiner that
// read closing==false can land its increment after the drain sampled
// zero — a member the drain never counted (and, in the WaitGroup
// implementation this replaced, the "Add called concurrently with Wait"
// runtime fatal — a crash, not a test failure). Every refused enter
// must also be refused with the error, never admitted after the flip.
func TestAdmissionEnterNeverTripsDrainingWaitGroup(t *testing.T) {
	for round := 0; round < 200; round++ {
		var a admission
		var admitted sync.WaitGroup
		start := make(chan struct{})
		const workers = 8
		results := make(chan error, workers)
		for i := 0; i < workers; i++ {
			admitted.Add(1)
			go func(i int) {
				defer admitted.Done()
				<-start
				var err error
				if i%2 == 0 {
					err = a.enterWrite()
					if err == nil {
						a.exitWrite()
					}
				} else {
					err = a.enterRead()
					if err == nil {
						a.exitRead()
					}
				}
				results <- err
			}(i)
		}
		closed := make(chan error, 1)
		go func() {
			<-start
			closed <- a.closeAndDrain(func() error { return nil })
		}()
		close(start)
		admitted.Wait()
		require.NoError(t, <-closed)
		for i := 0; i < workers; i++ {
			if err := <-results; err != nil {
				require.ErrorIs(t, err, ErrEngineClosing)
			}
		}
		// After the dust settles the gate must be shut for good.
		require.ErrorIs(t, a.enterWrite(), ErrEngineClosing)
	}
}

// TestAdmissionDrainWritesToleratesConcurrentEnters hammers drainWrites
// against a stream of entering writers. This is the drain sync.WaitGroup
// could NOT express: the gate stays open, so a writer legally enters in
// the same instant the counter touches zero — WaitGroup answers that
// with the "Add called concurrently with Wait" runtime fatal, while the
// cond-based drain just keeps waiting. The drain must return only at a
// real zero and the gate must stay open throughout.
func TestAdmissionDrainWritesToleratesConcurrentEnters(t *testing.T) {
	for round := 0; round < 200; round++ {
		var a admission
		const workers = 8
		var churn sync.WaitGroup
		start := make(chan struct{})
		for i := 0; i < workers; i++ {
			churn.Add(1)
			go func() {
				defer churn.Done()
				<-start
				for j := 0; j < 4; j++ {
					if err := a.enterWrite(); err == nil {
						a.exitWrite()
					}
				}
			}()
		}
		drained := make(chan struct{})
		go func() {
			<-start
			a.drainWrites()
			close(drained)
		}()
		close(start)
		churn.Wait()
		<-drained
		require.False(t, a.isClosing(), "drainWrites shut the gate; it must only quiesce")
		require.NoError(t, a.enterWrite())
		a.exitWrite()
	}
}
