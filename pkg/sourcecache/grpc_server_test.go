package sourcecache

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
)

// staticLookup answers every scope with a fixed validator, tagging which
// sync's store it stands in for.
type staticLookup struct{ validator string }

func (l staticLookup) Lookup(context.Context, RowKind, string) (Entry, bool, error) {
	return Entry{CacheValidator: l.validator}, true, nil
}

func lookupReq(t *testing.T) *v1.LookupRequest {
	t.Helper()
	return v1.LookupRequest_builder{
		RowKind:  string(RowKindGrants),
		ScopeKey: HashScope("scope"),
	}.Build()
}

func requireServes(t *testing.T, s *GRPCServer, wantValidator string) {
	t.Helper()
	resp, err := s.Lookup(context.Background(), lookupReq(t))
	require.NoError(t, err)
	require.True(t, resp.GetFound())
	require.Equal(t, wantValidator, resp.GetCacheValidator())
}

func requireMisses(t *testing.T, s *GRPCServer) {
	t.Helper()
	resp, err := s.Lookup(context.Background(), lookupReq(t))
	require.NoError(t, err)
	require.False(t, resp.GetFound())
}

// TestGRPCServerSingleOwnerLifecycle pins the uncontended path: install
// serves, clear blinds, a fresh install serves again.
func TestGRPCServerSingleOwnerLifecycle(t *testing.T) {
	ctx := context.Background()
	s := NewGRPCServer()

	requireMisses(t, s)
	s.SetSourceCache(ctx, staticLookup{validator: "etag-a"})
	requireServes(t, s, "etag-a")
	s.SetSourceCache(ctx, nil)
	requireMisses(t, s)
	s.SetSourceCache(ctx, staticLookup{validator: "etag-b"})
	requireServes(t, s, "etag-b")
}

// TestGRPCServerContentionBlindsAllOwners pins the cross-wiring defense:
// the wire carries no sync id, so with two live syncs the server cannot
// attribute an RPC to either — serving the last-installed lookup would
// hand sync A validators from sync B's previous artifact while A replays
// rows from its own. On contention every lookup must MISS (cold fetch,
// always safe) until the slot fully drains.
func TestGRPCServerContentionBlindsAllOwners(t *testing.T) {
	ctx := context.Background()
	s := NewGRPCServer()

	// Sync A installs; sync B overlaps.
	s.SetSourceCache(ctx, staticLookup{validator: "etag-a"})
	requireServes(t, s, "etag-a")
	s.SetSourceCache(ctx, staticLookup{validator: "etag-b"})
	requireMisses(t, s)

	// A finishes and clears. B is the sole survivor, but the server kept
	// no per-owner state — it must stay blind rather than guess.
	s.SetSourceCache(ctx, nil)
	requireMisses(t, s)

	// A third sync overlapping the still-live B must not be served either
	// (this is the exact validator-from-S1/rows-from-S0 shape).
	s.SetSourceCache(ctx, staticLookup{validator: "etag-c"})
	requireMisses(t, s)
	s.SetSourceCache(ctx, nil)
	requireMisses(t, s)

	// Only when the overlap fully drains does the slot reset.
	s.SetSourceCache(ctx, nil)
	requireMisses(t, s)
	s.SetSourceCache(ctx, staticLookup{validator: "etag-d"})
	requireServes(t, s, "etag-d")
}

// TestGRPCServerUnbalancedClearIsClamped pins the defensive clamp: a
// spurious extra clear (a sync that never installed) must not wedge the
// counter below zero and break the next legitimate install.
func TestGRPCServerUnbalancedClearIsClamped(t *testing.T) {
	ctx := context.Background()
	s := NewGRPCServer()

	s.SetSourceCache(ctx, nil)
	s.SetSourceCache(ctx, nil)
	s.SetSourceCache(ctx, staticLookup{validator: "etag-a"})
	requireServes(t, s, "etag-a")
	s.SetSourceCache(ctx, nil)
	requireMisses(t, s)
}
