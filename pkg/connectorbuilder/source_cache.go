package connectorbuilder

import (
	"context"

	"github.com/conductorone/baton-sdk/pkg/sourcecache"
)

// The builder receives the sync runner's source-cache lookup
// (ADVANCED FUNCTIONALITY — see pkg/sourcecache) and hands it to resource
// syncers through SyncOpAttrs.Lookup. Delivery mirrors the session-store
// pattern: the syncer calls SetSourceCache at sync start and
// SetSourceCache(nil) at sync end so a late RPC cannot read stale state.
var _ sourcecache.SetLookup = (*builder)(nil)

// SetSourceCache installs (or, with nil, clears) the source-cache lookup
// used to populate SyncOpAttrs.Lookup for list calls.
func (b *builder) SetSourceCache(_ context.Context, lookup sourcecache.Lookup) {
	b.sourceCacheMu.Lock()
	defer b.sourceCacheMu.Unlock()
	b.sourceCacheLookup = lookup
}

// sourceCache returns the currently installed lookup, or NoopLookup when
// none is installed: connectors never observe a nil SyncOpAttrs.Lookup.
func (b *builder) sourceCache() sourcecache.Lookup {
	b.sourceCacheMu.RLock()
	defer b.sourceCacheMu.RUnlock()
	if b.sourceCacheLookup == nil {
		return sourcecache.NoopLookup{}
	}
	return b.sourceCacheLookup
}
