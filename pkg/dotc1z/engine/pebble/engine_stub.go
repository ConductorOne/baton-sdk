// Package pebble is the v3 storage engine for baton-sdk. It is the
// implementation behind dotc1z.EnginePebble and the v3 envelope.
package pebble

import (
	"errors"
)

// The Engine struct itself lives in engine.go. This file retains only
// the centralized sentinel-error declarations.

// Sentinel errors from Appendix E. Centralized here so the codec
// package + engine package + envelope package all reference one
// source of truth.
var (
	ErrEngineClosing                 = errors.New("pebble engine: closing")
	ErrEngineQuiesced                = errors.New("pebble engine: quiesced; writes refused")
	ErrEngineNotAvailable            = errors.New("pebble engine: not available")
	ErrEngineMismatch                = errors.New("pebble engine: source/dest engine mismatch")
	ErrManifestInvalid               = errors.New("pebble engine: manifest unmarshal failed")
	ErrManifestIncompleteDescriptors = errors.New("pebble engine: manifest descriptor closure incomplete")
	ErrPebbleFormatNewer             = errors.New("pebble engine: pebble file format newer than this binary supports")
	ErrPebbleFormatOlder             = errors.New("pebble engine: pebble file format older than this binary can read")
	ErrUnknownEngine                 = errors.New("pebble engine: unknown engine name in manifest")
	ErrUnknownRecordType             = errors.New("pebble engine: unknown record type in manifest")
	ErrEnvelopeTruncated             = errors.New("pebble engine: v3 envelope truncated")
	ErrDiskFull                      = errors.New("pebble engine: disk full (ENOSPC)")
	ErrNoCurrentSync                 = errors.New("pebble engine: no current sync")
	ErrSaveDestExists                = errors.New("pebble engine: save destination already exists")
	ErrCrossFilesystem               = errors.New("pebble engine: save tmpDir and dest must be on the same filesystem")
	ErrInvalidPageToken              = errors.New("pebble engine: invalid page token")
)
