package sourcecache

import (
	"context"
	"fmt"

	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
)

// GRPCLookup is the connector-side Lookup implementation that talks directly
// to BatonSourceCacheService on the parent SDK.
//
// This replaces the previous SessionLookup adapter, which tunneled lookups
// through BatonSessionService using reserved key prefixes and was therefore
// wrapped by the connector's local MemorySessionCache (otter). That wrapping
// burned generic-session-cache budget on etag state and added otter TTL /
// eviction semantics to data that should live and die with the sync. The
// dedicated service avoids both problems.
//
// The parent SDK has exactly one active Lookup registered at a time (set per
// sync via SetSourceCache on the BatonSourceCacheService server), so the wire
// format does not need a sync_id; routing is implicit. Per-resource-type
// scoping that the old session adapter baked into the cache key is now
// metadata on the RPC (scope_hash + row_kind).
type GRPCLookup struct {
	client v1.BatonSourceCacheServiceClient
}

// NewGRPCLookup returns a Lookup backed by the given BatonSourceCacheService
// client. A nil client yields NoopLookup, which the connector treats the
// same as "no previous sync available" (so callers can configure the client
// optionally without sprinkling nil checks at every call site).
func NewGRPCLookup(client v1.BatonSourceCacheServiceClient) Lookup {
	if client == nil {
		return NoopLookup{}
	}
	return &GRPCLookup{client: client}
}

func (g *GRPCLookup) LookupPreviousSourceCache(ctx context.Context, rowKind RowKind, scopeHashHex string) (Entry, bool, error) {
	if err := ValidateRowKind(rowKind); err != nil {
		return Entry{}, false, err
	}
	if err := ValidateScopeHash(scopeHashHex); err != nil {
		return Entry{}, false, err
	}
	resp, err := g.client.Lookup(ctx, v1.LookupRequest_builder{
		RowKind:   string(rowKind),
		ScopeHash: scopeHashHex,
	}.Build())
	if err != nil {
		return Entry{}, false, fmt.Errorf("source cache rpc lookup: %w", err)
	}
	if !resp.GetFound() {
		return Entry{}, false, nil
	}
	return Entry{Key: resp.GetSourceCacheKey(), ETag: resp.GetSourceEtag()}, true, nil
}
