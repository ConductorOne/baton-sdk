package pebble

import (
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// AsEngine recovers the underlying *Engine from a connectorstore.Writer
// produced by dotc1z.NewStore for the Pebble engine. NewStore returns a
// *registeredStore wrapper (which embeds *Adapter); a bare *Adapter is
// also accepted for callers that construct one directly. Returns
// (nil, false) for any non-Pebble store, so a caller can branch on the
// engine without importing internal types.
func AsEngine(w connectorstore.Writer) (*Engine, bool) {
	switch s := w.(type) {
	case *registeredStore:
		if s == nil || s.engine == nil {
			return nil, false
		}
		return s.engine, true
	case *Adapter:
		if s == nil || s.engine == nil {
			return nil, false
		}
		return s.engine, true
	default:
		return nil, false
	}
}
