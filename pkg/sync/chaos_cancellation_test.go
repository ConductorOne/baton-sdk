package sync //nolint:revive,nolintlint // backwards-compatible package name

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	stdsync "sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/conductorone/baton-sdk/internal/chaosconnector"
	chaosoracle "github.com/conductorone/baton-sdk/internal/chaosconnector/oracle"
)

func TestChaosConnectorCancellationTerminatesAndColdResumes(t *testing.T) {
	skipChaosInShort(t)
	cases := []struct {
		name    string
		effect  chaosconnector.Effect
		timeout time.Duration
		wantErr error
	}{
		{
			name: "deadline-releases-blocked-call",
			effect: chaosconnector.Effect{
				Kind: chaosconnector.EffectBlock, Barrier: "never-released",
			},
			// Leave enough setup budget for -race and expire only after the
			// connector call has reached the deterministic block.
			timeout: 3 * time.Second,
			wantErr: ErrSyncNotComplete,
		},
		{
			name:    "connector-cancellation",
			effect:  chaosconnector.Effect{Kind: chaosconnector.EffectCancel},
			timeout: 2 * time.Second,
			wantErr: context.Canceled,
		},
	}
	for _, testCase := range cases {
		for _, transport := range []chaosTransport{chaosTransportDirect, chaosTransportGRPC} {
			t.Run(testCase.name+"/"+transport.String(), func(t *testing.T) {
				ctx := t.Context()
				tmpDir := t.TempDir()
				path := filepath.Join(tmpDir, "cancel-resume.c1z")
				scenario, err := chaosconnector.NewFullScenario()
				require.NoError(t, err)
				manifest, err := scenario.Manifest(scenario.InitialEpoch)
				require.NoError(t, err)
				expected := chaosoracle.ExpectedIdentities(manifest)

				baselinePath := filepath.Join(tmpDir, "baseline.c1z")
				baselineRun, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
				require.NoError(t, err)
				newChaosHarness(t, ctx, baselineRun, baselinePath, tmpDir, transport, WithWorkerCount(1)).
					SyncAndClose(t, ctx)
				baseline := readChaosLogicalContent(t, ctx, baselinePath, tmpDir)

				run, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule(chaosconnector.Rule{
					ID: "cancel-entitlement-call",
					Match: chaosconnector.Matcher{
						Service: chaosconnector.ExactString("EntitlementsService"),
						Method:  chaosconnector.ExactString("ListEntitlements"),
						Attempt: 1,
						Phase:   chaosconnector.PhaseBeforeCall,
					},
					Effects: []chaosconnector.Effect{testCase.effect}, MinFires: 1, MaxFires: 1,
				}))
				require.NoError(t, err)
				firstCtx, cancel := context.WithTimeout(ctx, testCase.timeout)
				defer cancel()
				harness := newChaosHarness(t, firstCtx, run, path, tmpDir, transport, WithWorkerCount(1))
				started := time.Now()
				var firstErr error
				if testCase.name == "deadline-releases-blocked-call" {
					done := make(chan error, 1)
					go func() { done <- harness.Syncer.Sync(firstCtx) }()
					require.Eventually(t, func() bool {
						return run.Runtime().ActiveOperations() > 0
					}, 2*time.Second, 10*time.Millisecond,
						"deadline premise requires an actively blocked connector call")
					firstErr = <-done
				} else {
					firstErr = harness.Syncer.Sync(firstCtx)
				}
				require.Less(t, time.Since(started), 6*time.Second, "cancellation must terminate boundedly")
				require.True(t, errors.Is(firstErr, testCase.wantErr),
					"expected %v, got %v", testCase.wantErr, firstErr)
				if testCase.name == "deadline-releases-blocked-call" {
					require.ErrorIs(t, firstCtx.Err(), context.DeadlineExceeded)
				}
				require.Zero(t, run.Runtime().ActiveOperations(),
					"cancellation must release the blocked connector call")
				require.NoError(t, harness.Close(ctx))
				require.NoError(t, run.Runtime().VerifyRequired())

				interruptedRuns := readChaosSyncRuns(t, ctx, path, tmpDir)
				require.Len(t, interruptedRuns, 1)
				require.Nil(t, interruptedRuns[0].EndedAt)
				syncID := interruptedRuns[0].ID

				resumeRun, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
				require.NoError(t, err)
				resumeHarness := newChaosHarness(
					t, ctx, resumeRun, path, tmpDir, transport, WithWorkerCount(1),
				)
				resumeHarness.SyncAndClose(t, ctx)
				finalRuns := readChaosSyncRuns(t, ctx, path, tmpDir)
				require.Len(t, finalRuns, 1)
				require.Equal(t, syncID, finalRuns[0].ID)
				require.NotNil(t, finalRuns[0].EndedAt)
				actual := readChaosLogicalContent(t, ctx, path, tmpDir)
				require.NoError(t, chaosoracle.CompareLogicalContent(baseline, actual))
				assertChaosStoreMatches(t, path, tmpDir, expected)
			})
		}
	}
}

// TestChaosExternalCancelStopsQuietlyAndCheckpoints covers the shutdown
// shape the RFC 0009 §4.2 mechanical fixes target: the CALLER cancels the
// sync context (activity teardown, deploy, operator stop) while a connector
// call is in flight. Pins, per §4.2 and the §4.4 frozen error surface:
//   - Sync surfaces the cancel as context.Canceled (loop-top/cause paths) or
//     as a codes.Canceled status passing through the batch path unaltered —
//     the two shapes the hosted runner sees during the SDK-first window;
//   - no "cancelling context due to error in action" error log fires:
//     workers observing shutdown are not discovering failures;
//   - a best-effort checkpoint runs after the cancel (the detached stop
//     checkpoint), so Close packs the freshest resumable state;
//   - the blocked call is released and Sync returns boundedly: siblings that
//     exhausted the queue while one call is blocked are parked in
//     queue.next(), so this fails if the suppressed error path stops
//     releasing them;
//   - the artifact reopens and cold-resumes to the baseline content.
//
// One cell, deliberately. The mechanism is transport-independent (the chaos
// effects apply client-side on both transports, so the cancel never crosses
// the wire), and TestChaosConnectorCancellationTerminatesAndColdResumes above
// already runs cancellation on both. The multi-worker count is the dimension
// that buys something here: it is what parks siblings in queue.next().
func TestChaosExternalCancelStopsQuietlyAndCheckpoints(t *testing.T) {
	skipChaosInShort(t)
	const workers = 4
	transport := chaosTransportDirect
	ctx := t.Context()
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "external-cancel.c1z")
	scenario, err := chaosconnector.NewFullScenario()
	require.NoError(t, err)
	manifest, err := scenario.Manifest(scenario.InitialEpoch)
	require.NoError(t, err)
	expected := chaosoracle.ExpectedIdentities(manifest)

	baselinePath := filepath.Join(tmpDir, "baseline.c1z")
	baselineRun, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
	require.NoError(t, err)
	newChaosHarness(t, ctx, baselineRun, baselinePath, tmpDir, transport, WithWorkerCount(1)).
		SyncAndClose(t, ctx)
	baseline := readChaosLogicalContent(t, ctx, baselinePath, tmpDir)

	run, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule(chaosconnector.Rule{
		ID: "block-until-external-cancel",
		Match: chaosconnector.Matcher{
			Service: chaosconnector.ExactString("EntitlementsService"),
			Method:  chaosconnector.ExactString("ListEntitlements"),
			Attempt: 1,
			Phase:   chaosconnector.PhaseBeforeCall,
		},
		Effects:  []chaosconnector.Effect{{Kind: chaosconnector.EffectBlock, Barrier: "held-until-external-cancel"}},
		MinFires: 1, MaxFires: 1,
	}))
	require.NoError(t, err)

	core, capturedEntries := newCaptureCore()
	firstCtx, cancelFirst := context.WithCancel(ctx)
	defer cancelFirst()
	firstCtx = ctxzap.ToContext(firstCtx, zap.New(core))

	harness := newChaosHarness(t, firstCtx, run, path, tmpDir, transport, WithWorkerCount(workers))
	sc, ok := harness.Syncer.(*syncer)
	require.True(t, ok)
	var cancelIssued atomic.Bool
	var checkpointAfterCancel atomic.Bool
	sc.testCheckpointHook = func(string) {
		if cancelIssued.Load() {
			checkpointAfterCancel.Store(true)
		}
	}

	done := make(chan error, 1)
	go func() { done <- harness.Syncer.Sync(firstCtx) }()
	// The premise gates on the blocking rule having FIRED, not just on
	// an active operation: ActiveOperations counts every call inside
	// the fault wrapper, so any healthy in-flight RPC satisfies it
	// long before the sync reaches the blocked entitlements call —
	// and canceling that early tests startup teardown, not a
	// mid-collection stop.
	require.Eventually(t, func() bool {
		return run.Runtime().Fires("block-until-external-cancel") > 0 &&
			run.Runtime().ActiveOperations() > 0
	}, 5*time.Second, 10*time.Millisecond,
		"external-cancel premise requires the blocked connector call to be in flight")
	cancelIssued.Store(true)
	cancelFirst()

	var firstErr error
	select {
	case firstErr = <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("external cancel must terminate the sync boundedly; the worker pool is likely hung")
	}
	// The frozen surface (RFC 0009 §4.4) admits two cancel shapes,
	// racing on which exit path observes the cancel first:
	// context.Canceled from the loop-top/cause paths, or the
	// connector's codes.Canceled status passing through the batch
	// path unaltered (grpc-go status errors do not satisfy
	// errors.Is against context.Canceled). Both must stay
	// recognizable; anything else is a reshaped cancel.
	require.True(t,
		errors.Is(firstErr, context.Canceled) || status.Code(firstErr) == codes.Canceled,
		"frozen surface (RFC 0009 §4.4): an external cancel must surface as context.Canceled "+
			"or a codes.Canceled status, got: %v", firstErr)
	require.Zero(t, run.Runtime().ActiveOperations(),
		"external cancel must release the blocked connector call")

	require.Nil(t, findEntry(capturedEntries(), zapcore.ErrorLevel, "cancelling context due to error in action"),
		"workers observing shutdown must not log action-failure errors (RFC 0009 §4.2)")
	require.True(t, checkpointAfterCancel.Load(),
		"the detached stop checkpoint must run after an external cancel (RFC 0009 §4.2)")

	require.NoError(t, harness.Close(ctx))
	require.NoError(t, run.Runtime().VerifyRequired())

	interruptedRuns := readChaosSyncRuns(t, ctx, path, tmpDir)
	require.Len(t, interruptedRuns, 1)
	require.Nil(t, interruptedRuns[0].EndedAt)
	syncID := interruptedRuns[0].ID

	resumeRun, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
	require.NoError(t, err)
	resumeHarness := newChaosHarness(t, ctx, resumeRun, path, tmpDir, transport, WithWorkerCount(1))
	resumeHarness.SyncAndClose(t, ctx)
	finalRuns := readChaosSyncRuns(t, ctx, path, tmpDir)
	require.Len(t, finalRuns, 1)
	require.Equal(t, syncID, finalRuns[0].ID)
	require.NotNil(t, finalRuns[0].EndedAt)
	actual := readChaosLogicalContent(t, ctx, path, tmpDir)
	require.NoError(t, chaosoracle.CompareLogicalContent(baseline, actual))
	assertChaosStoreMatches(t, path, tmpDir, expected)
}

// TestChaosActionErrorStillLogsCancelCause is the positive control for the
// suppression assertion above (the instrument must fail on a planted
// violation, REVIEW_CHECKLIST §2): a genuine action failure while the run
// context is still live must keep producing the "cancelling context due to
// error in action" error log — suppression applies only to shutdown-shaped
// exits.
func TestChaosActionErrorStillLogsCancelCause(t *testing.T) {
	skipChaosInShort(t)
	ctx := t.Context()
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "action-error.c1z")
	scenario, err := chaosconnector.NewFullScenario()
	require.NoError(t, err)
	run, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule(chaosconnector.Rule{
		ID: "fatal-entitlement-call",
		Match: chaosconnector.Matcher{
			Service: chaosconnector.ExactString("EntitlementsService"),
			Method:  chaosconnector.ExactString("ListEntitlements"),
			Attempt: 1,
			Phase:   chaosconnector.PhaseBeforeCall,
		},
		Effects:  []chaosconnector.Effect{{Kind: chaosconnector.EffectError, Code: codes.InvalidArgument, Message: "injected fatal"}},
		MinFires: 1, MaxFires: 1,
	}))
	require.NoError(t, err)

	core, capturedEntries := newCaptureCore()
	syncCtx := ctxzap.ToContext(ctx, zap.New(core))
	harness := newChaosHarness(t, syncCtx, run, path, tmpDir, chaosTransportDirect, WithWorkerCount(1))

	require.Error(t, harness.Syncer.Sync(syncCtx))
	require.NotNil(t, findEntry(capturedEntries(), zapcore.ErrorLevel, "cancelling context due to error in action"),
		"a genuine action failure must still log the cancel cause")
	require.NoError(t, harness.Close(ctx))
}

// TestParallelBatchSecondGenuineFailureStillLogs pins the suppression's scope
// at worker granularity: the guard reads the caller's context, not the
// batch's cancel-cause context, so a second worker's independent genuine
// failure — which necessarily arrives after the first failure has already
// canceled the batch context — still logs "cancelling context due to error
// in action". Both workers rendezvous inside their actions before either
// returns its error, so both failures are deterministically genuine
// discoveries, not shutdown observations. Under a batch-context guard this
// fails with exactly one logged line.
func TestParallelBatchSecondGenuineFailureStillLogs(t *testing.T) {
	ctx := t.Context()
	core, capturedEntries := newCaptureCore()
	ctx = ctxzap.ToContext(ctx, zap.New(core))

	st := newEmptySchedulerState(t)
	first := st.pushAction(ctx, Action{Op: SyncGrantsOp, ResourceID: "group-1"})
	second := st.pushAction(ctx, Action{Op: SyncGrantsOp, ResourceID: "group-2"})
	s := &syncer{state: st, cfg: syncConfig{workerCount: 2}}

	// Bounded rendezvous, not a WaitGroup: if the pool ever stops dispatching
	// these two actions concurrently, the premise is broken and this must fail
	// loudly rather than block a worker forever and hang the package.
	var mu stdsync.Mutex
	arrived := 0
	bothArrived := make(chan struct{})
	var premiseFailed atomic.Bool
	f := func(ctx context.Context, action *Action) error {
		mu.Lock()
		arrived++
		if arrived == 2 {
			close(bothArrived)
		}
		mu.Unlock()
		select {
		case <-bothArrived:
		case <-time.After(30 * time.Second):
			premiseFailed.Store(true)
		}
		return fmt.Errorf("injected genuine failure for %s", action.ResourceID)
	}

	_, err := s.syncParallel(ctx, newTestRetryer(ctx), []*Action{first, second}, f)
	require.Error(t, err)
	require.False(t, premiseFailed.Load(),
		"premise: both actions must be in flight before either fails, so the second failure is a genuine discovery")

	logged := 0
	for _, entry := range capturedEntries() {
		if entry.level == zapcore.ErrorLevel && contains(entry.message, "cancelling context due to error in action") {
			logged++
		}
	}
	require.Equal(t, 2, logged,
		"within a live run every genuine action failure must log the cancel cause")
}

// TestParallelBatchFailureDuringStopStaysQuiet pins the other side of the
// suppression rule, the cell the two tests above leave open: a genuine action
// failure that races a stop is suppressed too. Production reaches this on
// run-duration expiry, where parallelSync's AfterFunc cancels workerCtx — the
// context syncParallel receives — with a DeadlineExceeded cause, so the shape
// is reproduced by cancelling the pre-batch context with that cause from
// inside the action. The suppression is deliberate: the sync is on its way out
// and about to checkpoint, and the failure still reaches the caller in the
// returned error, which is what this asserts alongside the missing log.
func TestParallelBatchFailureDuringStopStaysQuiet(t *testing.T) {
	core, capturedEntries := newCaptureCore()
	ctx := ctxzap.ToContext(t.Context(), zap.New(core))
	preBatchCtx, stop := context.WithCancelCause(ctx)
	defer stop(nil)

	st := newEmptySchedulerState(t)
	action := st.pushAction(ctx, Action{Op: SyncGrantsOp, ResourceID: "group-1"})
	s := &syncer{state: st, cfg: syncConfig{workerCount: 1}}

	f := func(ctx context.Context, action *Action) error {
		stop(context.DeadlineExceeded)
		return fmt.Errorf("genuine failure racing the run-duration deadline")
	}

	_, err := s.syncParallel(preBatchCtx, newTestRetryer(ctx), []*Action{action}, f)
	require.Error(t, err, "the failure must still reach the caller")

	for _, entry := range capturedEntries() {
		require.False(t,
			entry.level == zapcore.ErrorLevel && contains(entry.message, "cancelling context due to error in action"),
			"a failure observed while the run is stopping must not log as a discovery")
	}
}
