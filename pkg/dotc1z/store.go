package dotc1z

import (
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// C1ZStore is the internal contract used by the sync pipeline, compactor, and
// related infrastructure to read and write a .c1z file. The interface lives
// in pkg/dotc1z/c1zstore (as c1zstore.Store) so storage engines can
// implement it without importing this package; this alias preserves the
// historical dotc1z name.
//
// Implementations:
//
//   - *C1File — the original SQLite-backed implementation
//     (pkg/dotc1z/c1file.go).
//   - *pebbleStore — the Pebble v3 engine implementation opened via
//     NewStore(WithEngine(EnginePebble)) (pkg/dotc1z/pebble_store.go).
//
// Both engines are registered statically; no extra imports are needed to
// open either format.
type C1ZStore = c1zstore.Store

// AsSQLiteStore type-asserts a C1ZStore to the concrete *C1File. It is an
// escape hatch for callers that legitimately need SQLite-specific primitives
// (today: the attached compactor in pkg/synccompactor/attached, which uses
// SQL ATTACH for cross-file merge). Returns (nil, false) when the store is
// not backed by *C1File OR when the underlying *C1File is nil.
//
// Avoid using this outside pkg/synccompactor. If you find yourself reaching
// for it, prefer adding a named method to C1ZStore that expresses what you
// need; sqlite-specific leak-through is a smell. See RFC 0002 §4.4.
func AsSQLiteStore(s C1ZStore) (*C1File, bool) {
	cf, ok := s.(*C1File)
	if !ok || cf == nil {
		return nil, false
	}
	return cf, true
}
