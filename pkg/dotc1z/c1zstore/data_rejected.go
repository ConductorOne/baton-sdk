package c1zstore

import "errors"

// ErrDataRejected classifies deterministic, immutable-DATA defects in a c1z
// input as distinct from IO/cancellation failures (which stay retryable) and
// from ErrArtifactUnusable (which authorizes runners to discard an
// output-storage-commit verdict, RFC 0009). Input rejection means the bytes
// the caller handed us are hostile or unsupported and no file-authored SQL
// executed and no attacker-derived state was trusted; retrying re-fails
// identically, so runners map it to their non-retryable failure class.
// Test with errors.Is.
var ErrDataRejected = errors.New("c1z data rejected")

// DataRejectedError carries the sentinel without altering the
// operator-facing message; Unwrap returns both the cause and ErrDataRejected.
type DataRejectedError struct{ err error }

func (e *DataRejectedError) Error() string { return e.err.Error() }

func (e *DataRejectedError) Unwrap() []error { return []error{e.err, ErrDataRejected} }

// RejectData marks err as a deterministic data verdict. Returns nil for nil.
func RejectData(err error) error {
	if err == nil {
		return nil
	}
	return &DataRejectedError{err: err}
}
