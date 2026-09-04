package sync //nolint:revive,nolintlint // backwards-compatible package name

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// errInjectedCheckpointWrite is a fixed sentinel so the guard test can pin
// the returned error's identity, not just its presence.
var errInjectedCheckpointWrite = errors.New("injected checkpoint write failure")

// checkpointOutcomeStore stubs the single store call the loop-top periodic
// checkpoint reaches (CheckpointSync) and records the outcome of each call.
// Every other method except SyncMeta panics through the embedded nil
// interface, which is part of the assertion — this exit must touch nothing
// else in the store.
type checkpointOutcomeStore struct {
	c1zstore.Store
	failWhenCallerDone bool
	failAlways         bool
	outcomes           []error
}

// SyncMeta answers the capability resolution setStore performs at attach; see
// legacyPaginatedCheckpointStore.SyncMeta. It returns nil rather than panicking
// because attach happens before the exit under test, not on it.
func (s *checkpointOutcomeStore) SyncMeta() c1zstore.SyncMeta { return nil }

func (s *checkpointOutcomeStore) CheckpointSync(ctx context.Context, _ string) error {
	var err error
	switch {
	case s.failAlways:
		err = errInjectedCheckpointWrite
	case s.failWhenCallerDone && ctx.Err() != nil:
		err = ctx.Err()
	}
	s.outcomes = append(s.outcomes, err)
	return err
}

// TestPeriodicCheckpointStopExitTakesDetachedRescue pins the third stop exit
// (RFC 0009 §4.2 change order) deterministically: the loop-top periodic
// checkpoint runs on the caller's context, so an external cancel landing
// between batches surfaces as a checkpoint failure before the runCtx.Done()
// select can observe it. That exit must take the same best-effort detached
// checkpoint as the other two stop exits. The store's CheckpointSync fails
// exactly when the caller's context is done, so the periodic write fails
// (canceled caller) and the rescue write succeeds only if it really is
// detached — the recorded outcomes pin both facts at once.
func TestPeriodicCheckpointStopExitTakesDetachedRescue(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	st := newEmptySchedulerState(t)
	st.pushAction(ctx, Action{Op: SyncGrantsOp, ResourceID: "group-1"})
	store := &checkpointOutcomeStore{failWhenCallerDone: true}
	s := &syncer{state: st, cfg: syncConfig{workerCount: 1}}
	s.setStore(store)

	// The stop lands "between batches": the loop's first periodic checkpoint
	// is the first code to observe it.
	cancel()

	_, err := s.parallelSync(ctx, ctx, nil)
	// Identity, not just presence: the stop reason must reach the caller
	// unchanged (RFC 0009 §4.2, side-effect-only). A regression that joined
	// the rescue's outcome or rewrapped the cancel would still be an error;
	// it would not still be context.Canceled.
	require.ErrorIs(t, err, context.Canceled,
		"the stop reason must reach the caller byte-for-byte, not rewrapped")

	require.Len(t, store.outcomes, 2, "the periodic write plus exactly one detached rescue write")
	require.Error(t, store.outcomes[0], "premise: the periodic checkpoint on the canceled caller context fails")
	require.NoError(t, store.outcomes[1], "the detached stop checkpoint must run and succeed")
}

// TestPeriodicCheckpointFailureWithLiveCallerIsNotRetried pins the guard
// direction on the same branch: a genuine store failure while the caller is
// live is a real error, not a stop, and retrying it on a detached context
// would launder a store fault into a second caller-uninterruptible write.
// Exactly one CheckpointSync call may happen.
func TestPeriodicCheckpointFailureWithLiveCallerIsNotRetried(t *testing.T) {
	ctx := t.Context()
	st := newEmptySchedulerState(t)
	st.pushAction(ctx, Action{Op: SyncGrantsOp, ResourceID: "group-1"})
	store := &checkpointOutcomeStore{failAlways: true}
	s := &syncer{state: st, cfg: syncConfig{workerCount: 1}}
	s.setStore(store)

	_, err := s.parallelSync(ctx, ctx, nil)
	require.ErrorIs(t, err, errInjectedCheckpointWrite,
		"the store failure must reach the caller unchanged")
	require.Len(t, store.outcomes, 1,
		"a genuine store failure with a live caller must not be retried on a detached context")
}
