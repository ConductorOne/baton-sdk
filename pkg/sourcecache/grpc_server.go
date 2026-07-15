package sourcecache

import (
	"context"
	"fmt"
	"sync"
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

	// installMu guards the install accounting below; the lookup pointer
	// stays atomic so the RPC hot path never takes the mutex.
	installMu sync.Mutex
	// installs counts live installs (non-nil SetSourceCache calls not yet
	// cleared). The wire carries no sync id, so an RPC cannot be
	// attributed to one of several concurrent syncs; the slot is only
	// servable while exactly one owner holds it.
	installs int
	// contended latches when a second concurrent install lands. From that
	// point every lookup misses (found=false — always safe: the connector
	// fetches cold) until the slot fully drains, because the server keeps
	// no per-owner state and cannot know which lookup a surviving sync
	// owns after the others clear.
	contended bool
}

var _ v1.BatonSourceCacheServiceServer = (*GRPCServer)(nil)
var _ SourceCacheSetter = (*GRPCServer)(nil)

// NewGRPCServer returns a GRPCServer with no active Lookup registered.
func NewGRPCServer() *GRPCServer {
	return &GRPCServer{}
}

// SetSourceCache installs (non-nil) or clears (nil) a sync's lookup. Safe
// to call concurrently with in-flight RPCs: existing RPCs continue against
// the value they read at entry; new RPCs see the swapped value.
//
// The slot is SINGLE — the wire carries no sync id — so with two live
// syncs the server cannot attribute a Lookup RPC to either one. Serving
// the last-installed lookup would cross-wire them: one sync's connector
// would resolve validators against the other's previous store, and its
// syncer would then replay rows from its OWN store under a validator the
// other artifact vouched for — stale rows sealed as current. A miss, by
// contrast, is always safe (the connector fetches cold). So on contention
// the server blinds every caller until all concurrent owners have
// cleared, and warns loudly so the degraded overlap is visible.
func (s *GRPCServer) SetSourceCache(ctx context.Context, lookup Lookup) {
	s.installMu.Lock()
	defer s.installMu.Unlock()
	if lookup == nil {
		if s.installs > 0 {
			s.installs--
		}
		if s.installs == 0 {
			// Slot fully drained: the next sync starts clean.
			s.contended = false
		}
		s.lookup.Store(nil)
		return
	}
	s.installs++
	if s.installs > 1 || s.contended {
		s.contended = true
		s.lookup.Store(nil)
		ctxzap.Extract(ctx).Warn("source cache: concurrent syncs share this connector's single lookup slot; " +
			"serving misses to every sync (cold fetches) until the overlap drains, to avoid cross-wiring validators between previous-sync artifacts")
		return
	}
	s.lookup.Store(&lookup)
}

func (s *GRPCServer) Lookup(ctx context.Context, req *v1.LookupRequest) (*v1.LookupResponse, error) {
	rowKind := RowKind(req.GetRowKind())
	if err := ValidateRowKind(rowKind); err != nil {
		return nil, err
	}
	scopeKey := req.GetScopeKey()
	if err := ValidateScopeKey(scopeKey); err != nil {
		return nil, err
	}

	lookupPtr := s.lookup.Load()
	if lookupPtr == nil {
		return v1.LookupResponse_builder{Found: false}.Build(), nil
	}
	entry, found, err := (*lookupPtr).Lookup(ctx, rowKind, scopeKey)
	if err != nil {
		return nil, fmt.Errorf("source cache lookup: %w", err)
	}
	if !found {
		return v1.LookupResponse_builder{Found: false}.Build(), nil
	}
	return v1.LookupResponse_builder{
		Found:          true,
		CacheValidator: entry.CacheValidator,
	}.Build(), nil
}
