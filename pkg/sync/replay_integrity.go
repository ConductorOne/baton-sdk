package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"errors"
	"fmt"

	"github.com/conductorone/baton-sdk/pkg/sourcecache"
)

// ErrReplayIntegrity is the errors.Is target for every source-cache replay
// failure the sync orchestration raises (plan B7 in
// docs/verification/sync-replay-6b/plan.md). Use errors.As with
// *ReplayIntegrityError to read the verdict, row kind, and scope key.
var ErrReplayIntegrity = errors.New("source-cache replay integrity")

// ReplayVerdict tells a retrying runner what a failed replay says about the
// previous artifact.
type ReplayVerdict int

const (
	// ReplayVerdictCold: the previous artifact or the connector's replay
	// decision cannot be trusted — discard the replay source and retry
	// cold. This is the fail-closed default: a wrong cold wastes one
	// fetch, a wrong warm can loop on a poisoned artifact.
	ReplayVerdictCold ReplayVerdict = iota

	// ReplayVerdictWarm: the replay decision was sound and the failure was
	// destination-side (copy commit, overlay write, manifest publish) or
	// an interruption — a retry may succeed with replay still armed.
	ReplayVerdictWarm
)

func (v ReplayVerdict) String() string {
	if v == ReplayVerdictWarm {
		return "warm"
	}
	return "cold"
}

// ReplayIntegrityError wraps a source-cache replay failure with its retry
// verdict and the failing (row kind, scope key). It matches
// ErrReplayIntegrity via errors.Is and unwraps to the underlying cause.
type ReplayIntegrityError struct {
	Verdict  ReplayVerdict
	RowKind  sourcecache.RowKind
	ScopeKey string
	Err      error
}

func (e *ReplayIntegrityError) Error() string {
	return fmt.Sprintf(
		"source-cache replay integrity (%s verdict) for %s scope %q: %v",
		e.Verdict, e.RowKind, e.ScopeKey, e.Err,
	)
}

func (e *ReplayIntegrityError) Unwrap() error { return e.Err }

func (e *ReplayIntegrityError) Is(target error) bool { return target == ErrReplayIntegrity }

func newReplayIntegrityError(verdict ReplayVerdict, rowKind sourcecache.RowKind, scopeKey string, err error) *ReplayIntegrityError {
	return &ReplayIntegrityError{Verdict: verdict, RowKind: rowKind, ScopeKey: scopeKey, Err: err}
}
