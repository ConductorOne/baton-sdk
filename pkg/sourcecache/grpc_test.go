package sourcecache

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"testing"

	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// loopbackServer wires up a TCP-loopback gRPC server hosting `srv` and
// returns a connected client and a teardown.
func loopbackServer(t *testing.T, srv v1.BatonSourceCacheServiceServer) (v1.BatonSourceCacheServiceClient, func()) {
	t.Helper()
	lc := net.ListenConfig{}
	lis, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	gs := grpc.NewServer()
	v1.RegisterBatonSourceCacheServiceServer(gs, srv)
	go func() { _ = gs.Serve(lis) }()

	conn, err := grpc.NewClient(
		lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	return v1.NewBatonSourceCacheServiceClient(conn), func() {
		_ = conn.Close()
		gs.Stop()
	}
}

// fakeLookup is a Lookup that records calls and returns a canned response.
type fakeLookup struct {
	entry  Entry
	found  bool
	err    error
	calls  atomic.Int64
	lastRK RowKind
	lastSH string
}

func (f *fakeLookup) LookupPreviousSourceCache(_ context.Context, rk RowKind, sh string) (Entry, bool, error) {
	f.calls.Add(1)
	f.lastRK = rk
	f.lastSH = sh
	return f.entry, f.found, f.err
}

func TestGRPCServerLookupNoLookupRegistered(t *testing.T) {
	srv := NewGRPCServer()
	client, teardown := loopbackServer(t, srv)
	defer teardown()

	resp, err := client.Lookup(context.Background(), v1.LookupRequest_builder{
		RowKind:   string(RowKindResources),
		ScopeHash: strings.Repeat("a", 64),
	}.Build())
	require.NoError(t, err)
	require.False(t, resp.GetFound())
}

func TestGRPCServerLookupValidatesInputs(t *testing.T) {
	srv := NewGRPCServer()
	client, teardown := loopbackServer(t, srv)
	defer teardown()

	// Validation errors run BEFORE the nil-lookup short-circuit so the test
	// catches them regardless of whether SetSourceCache has been called.
	_, err := client.Lookup(context.Background(), v1.LookupRequest_builder{
		RowKind:   "bogus-kind",
		ScopeHash: strings.Repeat("a", 64),
	}.Build())
	require.Error(t, err)

	_, err = client.Lookup(context.Background(), v1.LookupRequest_builder{
		RowKind:   string(RowKindResources),
		ScopeHash: "not-a-hash",
	}.Build())
	require.Error(t, err)
}

func TestGRPCServerLookupRoundTripHitAndMiss(t *testing.T) {
	srv := NewGRPCServer()
	fake := &fakeLookup{}
	srv.SetSourceCache(context.Background(), fake)
	client, teardown := loopbackServer(t, srv)
	defer teardown()

	scope := strings.Repeat("b", 64)
	key, err := BuildKey(scope, `"etag-1"`)
	require.NoError(t, err)
	fake.entry = Entry{Key: key, ETag: `"etag-1"`}
	fake.found = true

	resp, err := client.Lookup(context.Background(), v1.LookupRequest_builder{
		RowKind:   string(RowKindGrants),
		ScopeHash: scope,
	}.Build())
	require.NoError(t, err)
	require.True(t, resp.GetFound())
	require.Equal(t, key, resp.GetSourceCacheKey())
	require.Equal(t, `"etag-1"`, resp.GetSourceEtag())
	require.Equal(t, RowKindGrants, fake.lastRK)
	require.Equal(t, scope, fake.lastSH)

	fake.found = false
	fake.entry = Entry{}
	resp, err = client.Lookup(context.Background(), v1.LookupRequest_builder{
		RowKind:   string(RowKindGrants),
		ScopeHash: scope,
	}.Build())
	require.NoError(t, err)
	require.False(t, resp.GetFound())
	require.Empty(t, resp.GetSourceCacheKey())
	require.Empty(t, resp.GetSourceEtag())
}

func TestGRPCServerLookupPropagatesError(t *testing.T) {
	srv := NewGRPCServer()
	fake := &fakeLookup{err: errors.New("boom")}
	srv.SetSourceCache(context.Background(), fake)
	client, teardown := loopbackServer(t, srv)
	defer teardown()

	_, err := client.Lookup(context.Background(), v1.LookupRequest_builder{
		RowKind:   string(RowKindResources),
		ScopeHash: strings.Repeat("c", 64),
	}.Build())
	require.Error(t, err)
}

func TestGRPCServerSetSourceCacheSwapAndClear(t *testing.T) {
	srv := NewGRPCServer()
	first := &fakeLookup{found: true, entry: Entry{}}
	srv.SetSourceCache(context.Background(), first)
	client, teardown := loopbackServer(t, srv)
	defer teardown()

	scope := strings.Repeat("d", 64)
	_, err := client.Lookup(context.Background(), v1.LookupRequest_builder{
		RowKind:   string(RowKindResources),
		ScopeHash: scope,
	}.Build())
	require.NoError(t, err)
	require.Equal(t, int64(1), first.calls.Load())

	second := &fakeLookup{}
	srv.SetSourceCache(context.Background(), second)
	_, err = client.Lookup(context.Background(), v1.LookupRequest_builder{
		RowKind:   string(RowKindResources),
		ScopeHash: scope,
	}.Build())
	require.NoError(t, err)
	require.Equal(t, int64(1), first.calls.Load(), "old lookup must not be hit after swap")
	require.Equal(t, int64(1), second.calls.Load())

	srv.SetSourceCache(context.Background(), nil)
	resp, err := client.Lookup(context.Background(), v1.LookupRequest_builder{
		RowKind:   string(RowKindResources),
		ScopeHash: scope,
	}.Build())
	require.NoError(t, err)
	require.False(t, resp.GetFound(), "cleared server must short-circuit to found=false")
	require.Equal(t, int64(1), second.calls.Load(), "old lookup must not be hit after clear")
}

func TestGRPCLookupReturnsNoopForNilClient(t *testing.T) {
	lookup := NewGRPCLookup(nil)
	require.IsType(t, NoopLookup{}, lookup)

	entry, ok, err := lookup.LookupPreviousSourceCache(context.Background(), RowKindResources, strings.Repeat("e", 64))
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, Entry{}, entry)
}

func TestGRPCLookupRoundTrip(t *testing.T) {
	srv := NewGRPCServer()
	scope := strings.Repeat("f", 64)
	key, err := BuildKey(scope, "etag-7")
	require.NoError(t, err)
	srv.SetSourceCache(context.Background(), &fakeLookup{
		found: true,
		entry: Entry{Key: key, ETag: "etag-7"},
	})
	client, teardown := loopbackServer(t, srv)
	defer teardown()

	lookup := NewGRPCLookup(client)
	entry, ok, err := lookup.LookupPreviousSourceCache(context.Background(), RowKindEntitlements, scope)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, key, entry.Key)
	require.Equal(t, "etag-7", entry.ETag)
}

func TestGRPCLookupValidatesInputs(t *testing.T) {
	srv := NewGRPCServer()
	client, teardown := loopbackServer(t, srv)
	defer teardown()

	lookup := NewGRPCLookup(client)

	_, _, err := lookup.LookupPreviousSourceCache(context.Background(), RowKind("not-a-kind"), strings.Repeat("a", 64))
	require.Error(t, err)

	_, _, err = lookup.LookupPreviousSourceCache(context.Background(), RowKindResources, "not-hex")
	require.Error(t, err)
}

func TestNoopLookup(t *testing.T) {
	entry, ok, err := NoopLookup{}.LookupPreviousSourceCache(context.Background(), RowKindResources, strings.Repeat("a", 64))
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, Entry{}, entry)
}
