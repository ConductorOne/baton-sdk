// Package pebble is the v3 storage engine for baton-sdk. It is the
// implementation behind dotc1z.EnginePebble and the v3 envelope.
package pebble

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The Engine struct itself lives in engine.go. This file retains only
// the centralized sentinel-error declarations.

// sentinelError is a sentinel error that carries a gRPC status code,
// so callers using status.FromError / status.Code see a meaningful
// code instead of codes.Unknown. errors.Is comparisons against the
// sentinels below keep working: each is a unique instance and
// errors.Is falls back to identity comparison.
type sentinelError struct {
	code codes.Code
	msg  string
}

func (e *sentinelError) Error() string { return e.msg }

// GRPCStatus implements the interface status.FromError looks for.
func (e *sentinelError) GRPCStatus() *status.Status { return status.New(e.code, e.msg) }

func sentinel(code codes.Code, msg string) error { return &sentinelError{code: code, msg: msg} }

var (
	ErrEngineClosing                 = sentinel(codes.Unavailable, "pebble engine: closing")
	ErrEngineNotAvailable            = sentinel(codes.Unavailable, "pebble engine: not available")
	ErrEngineMismatch                = sentinel(codes.InvalidArgument, "pebble engine: source/dest engine mismatch")
	ErrManifestInvalid               = sentinel(codes.DataLoss, "pebble engine: manifest unmarshal failed")
	ErrManifestIncompleteDescriptors = sentinel(codes.DataLoss, "pebble engine: manifest descriptor closure incomplete")
	ErrPebbleFormatNewer             = sentinel(codes.FailedPrecondition, "pebble engine: pebble file format newer than this binary supports")
	ErrPebbleFormatOlder             = sentinel(codes.FailedPrecondition, "pebble engine: pebble file format older than this binary can read")
	ErrUnknownEngine                 = sentinel(codes.FailedPrecondition, "pebble engine: unknown engine name in manifest")
	ErrUnknownRecordType             = sentinel(codes.FailedPrecondition, "pebble engine: unknown record type in manifest")
	ErrEnvelopeTruncated             = sentinel(codes.DataLoss, "pebble engine: v3 envelope truncated")
	ErrDiskFull                      = sentinel(codes.ResourceExhausted, "pebble engine: disk full (ENOSPC)")
	ErrNoCurrentSync                 = sentinel(codes.FailedPrecondition, "pebble engine: no current sync")
	ErrSaveDestExists                = sentinel(codes.AlreadyExists, "pebble engine: save destination already exists")
	ErrCrossFilesystem               = sentinel(codes.InvalidArgument, "pebble engine: save tmpDir and dest must be on the same filesystem")
	ErrInvalidPageToken              = sentinel(codes.InvalidArgument, "pebble engine: invalid page token")
)
