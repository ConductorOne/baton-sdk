package sourcecache

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"

	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
)

// GRPCServer is the parent-side BatonSourceCacheService implementation.
//
// The parent SDK holds a single GRPCServer for the lifetime of the connector
// subprocess and swaps the active Lookup via SetSourceCache as syncs come
// and go. The syncer installs a real lookup once it has resolved a usable
// previous sync, and clears it when the sync ends so a late RPC can't serve
// from a store the syncer no longer owns.
//
// Until the first SetSourceCache call the server answers every lookup with
// found=false, which the connector treats as "no previous sync" and falls
// back to an unconditional fetch.
type GRPCServer struct {
	v1.UnimplementedBatonSourceCacheServiceServer
	lookup atomic.Pointer[Lookup]
}

var _ v1.BatonSourceCacheServiceServer = (*GRPCServer)(nil)
var _ SetLookup = (*GRPCServer)(nil)

// NewGRPCServer returns a GRPCServer with no active Lookup registered.
func NewGRPCServer() *GRPCServer {
	return &GRPCServer{}
}

// SetSourceCache replaces the active lookup. Safe to call concurrently with
// in-flight RPCs: existing RPCs continue against the value they read at
// entry; new RPCs see the swapped value.
//
// The slot is still SINGLE — the wire carries no sync id — so two live
// syncs installing lookups cross-wire each other (one sync's connector
// resolves scopes against the other's previous store) and the first to
// finish blinds the survivor when it clears. That topology isn't
// supported; an install landing on an occupied slot warns loudly so it is
// visible instead of silently wrong.
func (s *GRPCServer) SetSourceCache(ctx context.Context, lookup Lookup) {
	if lookup == nil {
		s.lookup.Store(nil)
		return
	}
	if prev := s.lookup.Swap(&lookup); prev != nil {
		ctxzap.Extract(ctx).Warn("source cache: installing a lookup over an existing one; " +
			"concurrent syncs against one connector share a single lookup slot and will cross-wire or blind each other")
	}
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
		Found: true,
		Etag:  entry.ETag,
	}.Build(), nil
}
