package pebble

import (
	"database/sql"
	"errors"

	"github.com/cockroachdb/pebble/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// notFoundError is the adapter-boundary "record missing" error. It
// carries a gRPC NotFound status (status.Code(err) == codes.NotFound)
// while remaining compatible with both pre-existing not-found
// sentinels:
//
//   - errors.Is(err, sql.ErrNoRows): pkg/sync, pkg/sync/expand, and
//     other clients of dotc1z.C1ZStore were originally written against
//     the SQLite implementation and rely on sql.ErrNoRows as the
//     cross-engine "row missing" signal.
//   - errors.Is(err, pebble.ErrNotFound): preserved via Unwrap for
//     callers that know they're talking to the Pebble engine.
type notFoundError struct {
	err error
}

func (e *notFoundError) Error() string { return e.err.Error() }

func (e *notFoundError) Unwrap() error { return e.err }

func (e *notFoundError) Is(target error) bool { return target == sql.ErrNoRows }

// GRPCStatus implements the interface status.FromError looks for, so
// gRPC consumers see codes.NotFound instead of codes.Unknown.
func (e *notFoundError) GRPCStatus() *status.Status {
	return status.New(codes.NotFound, e.err.Error())
}

// adaptNotFound rewrites a pebble.ErrNotFound (the engine-internal
// not-found sentinel) into a gRPC NotFound status error that also
// matches sql.ErrNoRows under errors.Is (see notFoundError).
// Translating at the adapter boundary keeps engine-agnostic call
// sites untouched and lets the Pebble engine plug into the existing
// pkg/sync.NewSyncer path via WithConnectorStore.
//
// Non-not-found errors pass through unchanged, so it is safe to apply
// to any engine error at the adapter boundary.
//
// We unwrap by errors.Is so the translation survives any wrapping
// the engine layer might add (today none does; future-proof anyway).
func adaptNotFound(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, pebble.ErrNotFound) {
		return &notFoundError{err: err}
	}
	return err
}
