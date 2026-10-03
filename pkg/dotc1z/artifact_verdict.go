package dotc1z

import "errors"

// ErrArtifactUnusable classifies STORAGE VERDICTS — the output c1z does not
// reflect a clean commit of this run's progress — and is the only signal
// that permits a runner to discard a partial sync artifact (RFC 0009:
// preserve by default, discard on typed verdict). Producers are the
// close/finalize paths that had mutations but failed before the c1z was
// rewritten; both engines save atomically, so a carried verdict means the
// artifact is STALE, never torn. Failures after a successful save (cleanup,
// engine teardown) deliberately do not carry it — the artifact is a
// faithful commit at that point. Consumed via
// sync.ShouldDiscardSyncArtifact; test with errors.Is.
var ErrArtifactUnusable = errors.New("sync artifact unusable")

// artifactVerdictError carries the sentinel WITHOUT altering the verdict's
// message (operator-facing text and any error-string assertions stay
// byte-identical — the same pattern as sync.ErrIngestInvariantViolated).
type artifactVerdictError struct{ err error }

func (e *artifactVerdictError) Error() string { return e.err.Error() }

func (e *artifactVerdictError) Unwrap() []error {
	return []error{e.err, ErrArtifactUnusable}
}

// artifactUnusable marks err as a storage verdict: the output c1z was not
// rewritten and does not reflect the store's final state.
func artifactUnusable(err error) error {
	if err == nil {
		return nil
	}
	return &artifactVerdictError{err: err}
}
