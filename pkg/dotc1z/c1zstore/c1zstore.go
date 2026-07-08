// Package c1zstore defines the engine-neutral storage contract for .c1z
// files: the Store interface and its sub-stores (GrantStore, SyncMeta,
// FileOps), the row shapes they exchange, and the SDK-wide sync retention
// policy.
//
// This package is a dependency leaf: it imports only protos and
// pkg/connectorstore. Storage engines (pkg/dotc1z's SQLite C1File,
// pkg/dotc1z/engine/pebble's Adapter) implement these interfaces without
// importing pkg/dotc1z, which lets dotc1z import the engines and register
// them statically. Callers reference these types directly through this
// package (c1zstore.Store, c1zstore.Engine, etc.).
package c1zstore

import (
	"context"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/types/sessions"
)

// Store is the internal contract used by the sync pipeline, compactor, and
// related infrastructure to read and write a .c1z file. It is NOT the
// connector-facing contract — connectors write through connectorstore.Writer,
// which is embedded here but stays the stable API surface for external code.
//
// Store adds three kinds of operations that the sync pipeline needs but
// connectors don't:
//
//   - Grant operations with expansion-aware semantics, accessed via Grants().
//   - Sync-run metadata operations, accessed via SyncMeta().
//   - File-level operations (clone, diff), accessed via FileOps().
//
// Implementations:
//
//   - *dotc1z.C1File — the original SQLite-backed implementation
//     (pkg/dotc1z/c1file.go).
//   - The Pebble v3 engine implementation opened via
//     dotc1z.NewStore(WithEngine(EnginePebble)) (pkg/dotc1z/pebble_store.go,
//     wrapping pkg/dotc1z/engine/pebble.Adapter).
//
// New implementations are expected; treat the interface methods as
// load-bearing and the implementation list above as informational.
//
// The Close override replaces connectorstore.Reader.Close with an
// explicit context parameter.
type Store interface {
	connectorstore.Writer

	// Grants returns the grant-specific sub-store.
	Grants() GrantStore

	// SyncMeta returns the sync-run metadata sub-store.
	SyncMeta() SyncMeta

	// FileOps returns the file-level operations sub-store.
	FileOps() FileOps

	// Close releases resources, flushing any pending writes.
	// Overrides the embedded Reader.Close to document this signature.
	Close(ctx context.Context) error

	SessionStore() sessions.SessionStore
}
