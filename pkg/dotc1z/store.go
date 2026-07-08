package dotc1z

import (
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// AsSQLiteStore type-asserts a c1zstore.Store to the concrete *C1File. It is an
// escape hatch for callers that legitimately need SQLite-specific primitives
// (today: the attached compactor in pkg/synccompactor/attached, which uses
// SQL ATTACH for cross-file merge). Returns (nil, false) when the store is
// not backed by *C1File OR when the underlying *C1File is nil.
//
// Avoid using this outside pkg/synccompactor. If you find yourself reaching
// for it, prefer adding a named method to c1zstore.Store that expresses what you
// need; sqlite-specific leak-through is a smell. See RFC 0002 §4.4.
func AsSQLiteStore(s c1zstore.Store) (*C1File, bool) {
	cf, ok := s.(*C1File)
	if !ok || cf == nil {
		return nil, false
	}
	return cf, true
}
