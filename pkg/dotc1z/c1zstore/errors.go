package c1zstore

import (
	"database/sql"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// notFoundError is the store-boundary "record missing" error. It carries a
// gRPC NotFound status while preserving the underlying sentinel via Unwrap.
type notFoundError struct {
	err error
}

func (e *notFoundError) Error() string { return e.err.Error() }

func (e *notFoundError) Unwrap() error { return e.err }

// GRPCStatus implements the interface status.FromError looks for, so
// gRPC consumers see codes.NotFound instead of codes.Unknown.
func (e *notFoundError) GRPCStatus() *status.Status {
	return status.New(codes.NotFound, e.err.Error())
}

// AdaptNotFound converts record-missing sentinels into gRPC NotFound.
// sql.ErrNoRows is always recognized; pass additional engine sentinels
// (for example pebble.ErrNotFound) as needed.
func AdaptNotFound(err error, extra ...error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return &notFoundError{err: err}
	}
	for _, sentinel := range extra {
		if errors.Is(err, sentinel) {
			return &notFoundError{err: err}
		}
	}
	return err
}
