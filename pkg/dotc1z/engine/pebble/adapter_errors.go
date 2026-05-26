package pebble

import (
	"database/sql"
	"errors"

	"github.com/cockroachdb/pebble/v2"
)

// adaptNotFound rewrites a pebble.ErrNotFound (the engine-internal
// not-found sentinel) as sql.ErrNoRows so engine-agnostic consumers
// can keep using `errors.Is(err, sql.ErrNoRows)` uniformly across
// the SQLite and Pebble backends.
//
// pkg/sync, pkg/sync/expand, and other clients of dotc1z.C1ZStore
// were originally written against the SQLite implementation and
// rely on sql.ErrNoRows as the cross-engine "row missing" signal.
// Translating at the adapter boundary keeps those call sites
// untouched and lets the Pebble engine plug into the existing
// pkg/sync.NewSyncer path via WithConnectorStore.
//
// We unwrap by errors.Is so the translation survives any wrapping
// the engine layer might add (today none does; future-proof anyway).
func adaptNotFound(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, pebble.ErrNotFound) {
		return sql.ErrNoRows
	}
	return err
}
