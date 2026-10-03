package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/conductorone/baton-sdk/internal/chaosconnector"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
)

// retentionTaxonomy is the error taxonomy the retention property test runs
// over (RFC 0009 §5): every shape the production paths produce at the
// SDK ↔ runner seam. The oracle for ShouldDiscardSyncArtifact is "discard ==
// presence of ErrArtifactUnusable, never a function of the connector error".
// Wrap-shape rows double as the conformance suite: a sanitization or
// errors.Join refactor that breaks errors.Is traversal must fail here before
// it ships (negative control for the #1048 incident shape).
var retentionTaxonomy = []struct {
	name string
	err  error
	// discard is the ShouldDiscardSyncArtifact verdict.
	discard bool
}{
	// Storage verdicts: the only discard rows.
	{name: "sentinel bare", err: ErrArtifactUnusable, discard: true},
	{name: "sentinel %w-wrapped", err: fmt.Errorf("error closing store: %w", ErrArtifactUnusable), discard: true},
	{name: "sentinel joined with sync error (runner shape)", err: errors.Join(context.Canceled, fmt.Errorf("error closing store: %w", ErrArtifactUnusable)), discard: true},
	{name: "sentinel double-wrapped", err: fmt.Errorf("task failed: %w", errors.Join(errors.New("connector broke"), fmt.Errorf("close: %w", ErrArtifactUnusable))), discard: true},
	{
		// The verdict is orthogonal to the cause it wraps: C1File.finalize
		// runs under FinalizeTimeout, so a WAL checkpoint outliving it
		// yields a verdict wrapping context.DeadlineExceeded — a cause the
		// old policy calls preservable. The verdict still discards; see
		// TestRetentionPreservableCoherence for why that is coherent.
		name:    "sentinel wrapping a finalize deadline (old-policy-preservable cause)",
		err:     errors.Join(fmt.Errorf("c1z: WAL checkpoint failed: %w", context.DeadlineExceeded), ErrArtifactUnusable),
		discard: true,
	},

	// Everything else preserves — including every shape the old allowlist
	// discarded.
	{name: "nil", err: nil, discard: false},
	{name: "plain error", err: errors.New("plain"), discard: false},
	{name: "bare context.Canceled", err: context.Canceled, discard: false},
	{name: "wrapped context.Canceled", err: fmt.Errorf("sync-grants-for-resource: %w", context.Canceled), discard: false},
	{name: "status Canceled", err: status.Error(codes.Canceled, "context canceled"), discard: false},
	{name: "bare context.DeadlineExceeded", err: context.DeadlineExceeded, discard: false},
	{name: "status DeadlineExceeded", err: status.Error(codes.DeadlineExceeded, "lambda_transport: function timed out"), discard: false},
	{name: "status Internal", err: status.Error(codes.Internal, "internal error"), discard: false},
	{name: "status Unknown", err: status.Error(codes.Unknown, "unknown"), discard: false},
	{name: "status Unavailable", err: status.Error(codes.Unavailable, "server unavailable"), discard: false},
	{
		name:    "lambda transport text without status",
		err:     errors.New("lambda_transport: failed to invoke lambda function: operation error Lambda: Invoke, https response error StatusCode: 0, RequestID: , canceled, context canceled"),
		discard: false,
	},
	{name: "ErrSyncNotComplete", err: ErrSyncNotComplete, discard: false},
	{name: "ErrSyncNotComplete joined with checkpoint error (production shape)", err: errors.Join(errors.New("checkpoint failed"), ErrSyncNotComplete), discard: false},
	{name: "ErrTooManyWarnings production shape", err: fmt.Errorf("%w: warnings: %v completed actions: %d", ErrTooManyWarnings, []error{errors.New("w")}, 5), discard: false},
	{name: "ErrIngestInvariantViolated", err: ErrIngestInvariantViolated, discard: false},
	{name: "connector error joined with cancel", err: errors.Join(status.Error(codes.PermissionDenied, "denied"), context.Canceled), discard: false},
}

func TestShouldDiscardSyncArtifact(t *testing.T) {
	for _, tt := range retentionTaxonomy {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.discard, ShouldDiscardSyncArtifact(tt.err))
		})
	}
}

// TestRetentionPreservableCoherence pins the stated relation between the two
// retention classifiers over one domain (BUG_CATCHING §5.3 coherence), in
// two halves.
//
// Over VERDICT-FREE errors, preserve-by-default strictly widens
// preservation: anything the frozen IsSyncPreservable would keep,
// ShouldDiscardSyncArtifact must also keep. (The converse is intentionally
// false — e.g. Canceled: old policy discards, new policy preserves.)
//
// Verdict-carrying errors are the stated exception, not a violation: the
// verdict classifies the ARTIFACT (mutations existed but the c1z was not
// rewritten — stale by construction), while IsSyncPreservable classifies the
// error's CAUSE, and a verdict can wrap any cause, including old-policy-
// preservable ones (C1File.finalize runs under FinalizeTimeout, so a WAL
// checkpoint outliving it wraps context.DeadlineExceeded). Discard still
// wins and still loses nothing: a stale artifact holds no progress from this
// run, so there is nothing for "preservable" to preserve.
func TestRetentionPreservableCoherence(t *testing.T) {
	for _, tt := range retentionTaxonomy {
		t.Run(tt.name, func(t *testing.T) {
			if errors.Is(tt.err, ErrArtifactUnusable) {
				require.True(t, ShouldDiscardSyncArtifact(tt.err),
					"a carried verdict discards regardless of the wrapped cause's class")
				return
			}
			if IsSyncPreservable(tt.err) {
				require.False(t, ShouldDiscardSyncArtifact(tt.err),
					"verdict-free preservable ⊆ preserved: IsSyncPreservable keeps this error, so the new policy must not discard it")
			}
		})
	}

	// The exception is real, not hypothetical: pin the production shape where
	// both classifiers fire and the verdict wins.
	finalizeDeadlineVerdict := errors.Join(
		fmt.Errorf("c1z: WAL checkpoint failed: %w", context.DeadlineExceeded),
		ErrArtifactUnusable,
	)
	require.True(t, IsSyncPreservable(finalizeDeadlineVerdict),
		"premise: the old policy calls this cause preservable")
	require.True(t, ShouldDiscardSyncArtifact(finalizeDeadlineVerdict),
		"the verdict discards even an old-policy-preservable cause")
}

// TestArtifactSentinelSeamIdentity pins the re-export: the pkg/sync sentinel
// and the pkg/dotc1z sentinel are one value, so a verdict produced in the
// storage layer is found by errors.Is against either name.
func TestArtifactSentinelSeamIdentity(t *testing.T) {
	require.ErrorIs(t, ErrArtifactUnusable, dotc1z.ErrArtifactUnusable)
	require.ErrorIs(t, dotc1z.ErrArtifactUnusable, ErrArtifactUnusable)
}

// TestArtifactVerdictSurvivesSyncerClose is the cross-layer half of the
// storage-verdict injection test (RFC 0009 §5): a real store save failure
// must carry ErrArtifactUnusable through the engine's Close, syncer.Close's
// "error closing store: %w" wrap, and the runner-style errors.Join with the
// Sync error — the exact chain the hosted runner branches on.
func TestArtifactVerdictSurvivesSyncerClose(t *testing.T) {
	skipChaosInShort(t)
	ctx := t.Context()
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "verdict.c1z")

	scenario, err := chaosconnector.NewFullScenario()
	require.NoError(t, err)
	run, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
	require.NoError(t, err)
	harness := newChaosHarness(t, ctx, run, path, tmpDir, chaosTransportDirect, WithWorkerCount(1))

	syncErr := harness.Syncer.Sync(ctx)
	require.NoError(t, syncErr)

	// Sabotage the atomic save: the engine writes to path+".tmp" before
	// renaming over the c1z; a directory there fails the save while the
	// (here: nonexistent) previous artifact stays untouched.
	tmpTarget := path + ".tmp"
	require.NoError(t, os.Mkdir(tmpTarget, 0o755))

	closeErr := harness.Syncer.Close(ctx)
	require.Error(t, closeErr)
	require.ErrorIs(t, closeErr, ErrArtifactUnusable)

	// The runner-side branch shape: join of Sync and Close errors.
	require.True(t, ShouldDiscardSyncArtifact(errors.Join(syncErr, closeErr)))

	// RFC 0009 §4.4 obligation 1, against the production wrapper rather than a
	// hand-built %w chain: attaching the sentinel must not move how the frozen
	// classifier reads the error, or an unmigrated runner's retention shifts on
	// an SDK bump. Reshaping Unwrap() []error breaks this before it ships.
	require.False(t, IsSyncPreservable(closeErr))

	// Recovery path advertised by the pebble store: fix the condition and
	// Close again; the retried commit succeeds and carries no verdict.
	require.NoError(t, os.Remove(tmpTarget))
	require.NoError(t, harness.Close(ctx))
	require.FileExists(t, path)
}
