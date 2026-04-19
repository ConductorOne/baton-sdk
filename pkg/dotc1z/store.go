package dotc1z

import (
	"context"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// C1ZStore is the internal contract used by the sync pipeline, compactor, and
// related infrastructure to read and write a .c1z file. It is NOT the
// connector-facing contract — connectors write through connectorstore.Writer,
// which is embedded here but stays the stable API surface for external code.
//
// C1ZStore adds three kinds of operations that the sync pipeline needs but
// connectors don't:
//
//   - Grant operations with expansion-aware semantics, accessed via Grants().
//   - Sync-run metadata operations, accessed via SyncMeta().
//   - File-level operations (clone, diff), accessed via FileOps().
//
// *C1File is the sole implementation.
//
// The Close override replaces connectorstore.Reader.Close with an
// explicit context parameter (*C1File.Close already takes context on main,
// so this is exact match, no behavior change).
type C1ZStore interface {
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
}
