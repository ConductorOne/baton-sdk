package sourcecache

import (
	"context"
	"fmt"
	"sync/atomic"

	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
)

// GRPCServer is the parent-side BatonSourceCacheService implementation.
//
// The parent SDK holds a single GRPCServer for the lifetime of the connector
// subprocess and swaps the active Lookup via SetLookup as syncs come and go.
// The Lookup is set by the syncer once configureSourceCache has resolved the
// previous-sync run id and (optionally) opened an external source-cache c1z;
// it is cleared when the sync ends so a late RPC against a stale source store
// can't accidentally serve from a c1z the syncer no longer owns.
//
// Until the first SetLookup call the server answers all lookups with
// `found = false`, which the connector treats the same as "no previous sync"
// and falls back to an unconditional fetch. That matches the behavior of the
// previous session-store hijack when sourceCacheLookup was nil.
type GRPCServer struct {
	v1.UnimplementedBatonSourceCacheServiceServer
	lookup atomic.Pointer[Lookup]
}

var _ v1.BatonSourceCacheServiceServer = (*GRPCServer)(nil)
var _ SetLookup = (*GRPCServer)(nil)

// NewGRPCServer returns a GRPCServer with no active Lookup registered. The
// server safely answers Lookup RPCs with `found = false` until the syncer
// installs a real lookup via SetSourceCache.
func NewGRPCServer() *GRPCServer {
	return &GRPCServer{}
}

// SetSourceCache replaces the active lookup. Safe to call concurrently with
// in-flight RPCs (existing RPCs will continue against whatever value they
// read at entry; new RPCs see the swapped value).
func (s *GRPCServer) SetSourceCache(ctx context.Context, lookup Lookup) {
	if lookup == nil {
		s.lookup.Store(nil)
		return
	}
	s.lookup.Store(&lookup)
}

func (s *GRPCServer) Lookup(ctx context.Context, req *v1.LookupRequest) (*v1.LookupResponse, error) {
	rowKind := RowKind(req.GetRowKind())
	if err := ValidateRowKind(rowKind); err != nil {
		return nil, err
	}
	scopeHash := req.GetScopeHash()
	if err := ValidateScopeHash(scopeHash); err != nil {
		return nil, err
	}

	lookupPtr := s.lookup.Load()
	if lookupPtr == nil {
		return v1.LookupResponse_builder{Found: false}.Build(), nil
	}
	entry, found, err := (*lookupPtr).LookupPreviousSourceCache(ctx, rowKind, scopeHash)
	if err != nil {
		return nil, fmt.Errorf("source cache lookup: %w", err)
	}
	if !found {
		return v1.LookupResponse_builder{Found: false}.Build(), nil
	}
	return v1.LookupResponse_builder{
		Found:          true,
		SourceCacheKey: entry.Key,
		SourceEtag:     entry.ETag,
	}.Build(), nil
}
