package pebble

import (
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// engineAccessor is implemented by *Adapter and by pkg/dotc1z's Pebble
// store wrapper (which embeds *Adapter and overrides the method with a
// nil-safe version).
type engineAccessor interface {
	PebbleEngine() *Engine
}

// PebbleEngine returns the underlying *Engine. Nil-safe so AsEngine can
// probe arbitrary writers.
func (a *Adapter) PebbleEngine() *Engine {
	if a == nil {
		return nil
	}
	return a.engine
}

// AsEngine recovers the underlying *Engine from a connectorstore.Writer
// produced by dotc1z.NewStore for the Pebble engine. NewStore returns a
// wrapper that embeds *Adapter; a bare *Adapter is also accepted for
// callers that construct one directly. Returns (nil, false) for any
// non-Pebble store, so a caller can branch on the engine without
// importing internal types.
func AsEngine(w connectorstore.Writer) (*Engine, bool) {
	if a, ok := w.(engineAccessor); ok {
		if e := a.PebbleEngine(); e != nil {
			return e, true
		}
	}
	return nil, false
}
