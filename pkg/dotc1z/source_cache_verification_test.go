package dotc1z

import (
	"path/filepath"
	"sync"
	"testing"

	cockroachpebble "github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/bid"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	enginepebble "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	"github.com/conductorone/baton-sdk/pkg/sourcecache"
)

type sourceCacheVerificationStore struct {
	store  c1zstore.Store
	cache  SourceCacheStore
	engine *enginepebble.Engine
}

func newSourceCacheVerificationStore(t *testing.T) sourceCacheVerificationStore {
	t.Helper()
	ctx := t.Context()
	store, err := NewStore(ctx, t.TempDir()+"/source-cache-verification.c1z", WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close(ctx)) })
	_, err = store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	cache, ok := store.(SourceCacheStore)
	require.True(t, ok, "Pebble store must expose SourceCacheStore")
	engine, ok := enginepebble.AsEngine(store)
	require.True(t, ok, "Pebble store must expose its engine")
	return sourceCacheVerificationStore{store: store, cache: cache, engine: engine}
}

func putVerificationGrant(t *testing.T, s sourceCacheVerificationStore, scope, entitlement, principal string) *v2.Grant {
	t.Helper()
	grant := mkV2Grant("", entitlement, "user", principal)
	require.NoError(t, s.store.PutGrants(sourcecache.WithScope(t.Context(), scope), grant))
	return grant
}

// C07/C32: a scope is replayable only when the previous artifact owns a
// matching manifest entry. Stamped rows alone are not authority to replay.
func TestVerificationReplayRejectsStampedRowsWithoutManifest(t *testing.T) {
	prev := newSourceCacheVerificationStore(t)
	grant := putVerificationGrant(t, prev, "scope-a", "member", "alice")
	cur := newSourceCacheVerificationStore(t)

	_, err := cur.cache.ReplaySourceCache(t.Context(), prev.store, sourcecache.RowKindGrants, "scope-a")
	require.Error(t, err, "replay without a matching manifest must fail closed")
	_, getErr := cur.engine.GetGrantRecord(t.Context(), grant.GetId())
	require.ErrorIs(t, getErr, cockroachpebble.ErrNotFound, "failed replay must leave no destination row")
}

// C25: empty validators are transitional; they cannot replace or create a
// completed manifest entry that a future lookup treats as replayable.
func TestVerificationEmptyValidatorDoesNotPublishManifest(t *testing.T) {
	s := newSourceCacheVerificationStore(t)

	err := s.cache.PutSourceCacheEntry(t.Context(), sourcecache.RowKindGrants, "scope-a", "")
	if err != nil {
		return // loud rejection is a valid residue-free disposition.
	}
	_, found, lookupErr := s.cache.LookupSourceCacheEntry(t.Context(), sourcecache.RowKindGrants, "scope-a")
	require.NoError(t, lookupErr)
	require.False(t, found, "an empty validator must not become a completed manifest hit")
}

// C31: retry-partial and otherwise occupied destinations must converge to
// replacement, not union, semantics for the selected scope.
func TestVerificationPureReplayReplacesOccupiedScope(t *testing.T) {
	prev := newSourceCacheVerificationStore(t)
	copied := putVerificationGrant(t, prev, "scope-a", "member", "alice")
	require.NoError(t, prev.cache.PutSourceCacheEntry(t.Context(), sourcecache.RowKindGrants, "scope-a", "validator-a"))

	cur := newSourceCacheVerificationStore(t)
	obsolete := putVerificationGrant(t, cur, "scope-a", "owner", "bob")
	decoy := putVerificationGrant(t, cur, "scope-b", "member", "carol")

	_, err := cur.cache.ReplaySourceCache(t.Context(), prev.store, sourcecache.RowKindGrants, "scope-a")
	require.NoError(t, err)
	_, err = cur.engine.GetGrantRecord(t.Context(), copied.GetId())
	require.NoError(t, err, "source row must be present")
	_, err = cur.engine.GetGrantRecord(t.Context(), obsolete.GetId())
	require.ErrorIs(t, err, cockroachpebble.ErrNotFound, "destination-only target-scope row must be removed")
	_, err = cur.engine.GetGrantRecord(t.Context(), decoy.GetId())
	require.NoError(t, err, "neighbor scope must remain unchanged")
}

// C39: scope identity includes row kind. A manifest hit for resources does
// not authorize a grants replay using the same scope bytes.
func TestVerificationWrongKindScopeDoesNotReplayAsEmptySuccess(t *testing.T) {
	prev := newSourceCacheVerificationStore(t)
	resource := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(),
	}.Build()
	require.NoError(t, prev.store.PutResources(sourcecache.WithScope(t.Context(), "scope-a"), resource))
	require.NoError(t, prev.cache.PutSourceCacheEntry(t.Context(), sourcecache.RowKindResources, "scope-a", "validator-a"))
	cur := newSourceCacheVerificationStore(t)

	_, err := cur.cache.ReplaySourceCache(t.Context(), prev.store, sourcecache.RowKindGrants, "scope-a")
	require.Error(t, err, "wrong-kind scope miss must fail closed at the direct replay primitive")
}

// C40: lookup of an already-invalidated manifest entry is a miss. This
// criterion consumes invalidation state; it does not test the deferred policy
// that decides when to set it.
func TestVerificationInvalidatedManifestLookupMisses(t *testing.T) {
	s := newSourceCacheVerificationStore(t)
	require.NoError(t, s.cache.PutSourceCacheEntry(t.Context(), sourcecache.RowKindGrants, "scope-a", "validator-a"))
	rec, err := s.engine.GetSourceCacheEntry(t.Context(), string(sourcecache.RowKindGrants), "scope-a")
	require.NoError(t, err)
	rec.SetInvalidated(true)
	value, err := proto.Marshal(rec)
	require.NoError(t, err)

	iter, err := s.engine.NewIter(&cockroachpebble.IterOptions{
		LowerBound: enginepebble.SourceCacheEntryLowerBound(),
		UpperBound: enginepebble.SourceCacheEntryUpperBound(),
	})
	require.NoError(t, err)
	require.True(t, iter.First(), "manifest key must exist")
	key := append([]byte(nil), iter.Key()...)
	require.NoError(t, iter.Close())
	require.NoError(t, s.engine.UnsafeForTesting().Set(key, value, nil))

	entry, found, err := s.cache.LookupSourceCacheEntry(t.Context(), sourcecache.RowKindGrants, "scope-a")
	require.NoError(t, err)
	require.False(t, found, "invalidated manifest must be exposed as a lookup miss; got %+v", entry)
}

// C41: prefix-neighbor scopes must not alias in either indexes or replay.
func TestVerificationPrefixNeighborScopeIsolation(t *testing.T) {
	prev := newSourceCacheVerificationStore(t)
	target := putVerificationGrant(t, prev, "foo", "member", "alice")
	neighbor := putVerificationGrant(t, prev, "foobar", "member", "bob")
	require.NoError(t, prev.cache.PutSourceCacheEntry(t.Context(), sourcecache.RowKindGrants, "foo", "validator-foo"))
	require.NoError(t, prev.cache.PutSourceCacheEntry(t.Context(), sourcecache.RowKindGrants, "foobar", "validator-foobar"))
	cur := newSourceCacheVerificationStore(t)

	res, err := cur.cache.ReplaySourceCache(t.Context(), prev.store, sourcecache.RowKindGrants, "foo")
	require.NoError(t, err)
	require.Equal(t, int64(1), res.Rows)
	_, err = cur.engine.GetGrantRecord(t.Context(), target.GetId())
	require.NoError(t, err)
	_, err = cur.engine.GetGrantRecord(t.Context(), neighbor.GetId())
	require.ErrorIs(t, err, cockroachpebble.ErrNotFound)
}

// C28: the replay capability owns scope validation just like lookup and
// manifest writes. Invalid input must fail before source iteration or mutation.
func TestVerificationReplayRejectsInvalidScope(t *testing.T) {
	prev := newSourceCacheVerificationStore(t)
	cur := newSourceCacheVerificationStore(t)

	_, err := cur.cache.ReplaySourceCache(t.Context(), prev.store, sourcecache.RowKindGrants, "")
	require.Error(t, err, "empty replay scope must be rejected")
}

// C42/O17: validate a whole tombstone request before applying any element.
// Otherwise a malformed trailing resource BID turns a nominally failed page
// into a durable partial deletion.
func TestVerificationCanonicalTombstonesRejectAtomically(t *testing.T) {
	s := newSourceCacheVerificationStore(t)
	resource := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "user", Resource: "alice"}.Build(),
	}.Build()
	require.NoError(t, s.store.PutResources(sourcecache.WithScope(t.Context(), "scope-a"), resource))
	resourceBID, err := bid.MakeResourceBid(resource)
	require.NoError(t, err)

	err = s.cache.DeleteSourceCacheRows(
		t.Context(),
		sourcecache.RowKindResources,
		[]string{resourceBID, "not-a-resource-bid"},
	)
	require.Error(t, err)
	_, getErr := s.engine.GetResourceRecord(t.Context(), "user", "alice")
	require.NoError(t, getErr, "mixed valid+invalid tombstones must apply nothing")
}

// C43: source and destination aliasing is an invalid state. The capability
// must reject it before iterating or writing, rather than relying on Pebble's
// incidental same-DB iterator/write behavior.
func TestVerificationReplayFromSelfRejected(t *testing.T) {
	s := newSourceCacheVerificationStore(t)
	putVerificationGrant(t, s, "scope-a", "member", "alice")
	require.NoError(t, s.cache.PutSourceCacheEntry(t.Context(), sourcecache.RowKindGrants, "scope-a", "validator-a"))

	_, err := s.cache.ReplaySourceCache(t.Context(), s.store, sourcecache.RowKindGrants, "scope-a")
	require.Error(t, err, "replay from the destination store itself must fail before mutation")
}

// C23/C26: a successful replay is a read-only operation on its source and
// preserves row metadata exactly.
func TestVerificationReplayPreservesSourceAndTimestamps(t *testing.T) {
	prev := newSourceCacheVerificationStore(t)
	grant := putVerificationGrant(t, prev, "scope-a", "member", "alice")
	require.NoError(t, prev.cache.PutSourceCacheEntry(t.Context(), sourcecache.RowKindGrants, "scope-a", "validator-a"))
	beforeRow, err := prev.engine.GetGrantRecord(t.Context(), grant.GetId())
	require.NoError(t, err)
	beforeEntry, err := prev.engine.GetSourceCacheEntry(t.Context(), string(sourcecache.RowKindGrants), "scope-a")
	require.NoError(t, err)

	cur := newSourceCacheVerificationStore(t)
	_, err = cur.cache.ReplaySourceCache(t.Context(), prev.store, sourcecache.RowKindGrants, "scope-a")
	require.NoError(t, err)

	afterSourceRow, err := prev.engine.GetGrantRecord(t.Context(), grant.GetId())
	require.NoError(t, err)
	afterSourceEntry, err := prev.engine.GetSourceCacheEntry(t.Context(), string(sourcecache.RowKindGrants), "scope-a")
	require.NoError(t, err)
	replayedRow, err := cur.engine.GetGrantRecord(t.Context(), grant.GetId())
	require.NoError(t, err)
	require.True(t, proto.Equal(beforeRow, afterSourceRow), "replay mutated the source row")
	require.True(t, proto.Equal(beforeEntry, afterSourceEntry), "replay mutated the source manifest")
	require.True(t, proto.Equal(beforeRow.GetDiscoveredAt(), replayedRow.GetDiscoveredAt()),
		"base replay must preserve the source row timestamp")
}

// C24/C33: when the caller publishes the completed manifest after replay, the
// result must be independently usable as the sole source for a second hop.
func TestVerificationReplayResultIsForwardCacheable(t *testing.T) {
	first := newSourceCacheVerificationStore(t)
	grant := putVerificationGrant(t, first, "scope-a", "member", "alice")
	require.NoError(t, first.cache.PutSourceCacheEntry(t.Context(), sourcecache.RowKindGrants, "scope-a", "validator-a"))

	second := newSourceCacheVerificationStore(t)
	res, err := second.cache.ReplaySourceCache(t.Context(), first.store, sourcecache.RowKindGrants, "scope-a")
	require.NoError(t, err)
	require.Equal(t, int64(1), res.Rows)
	require.NoError(t, second.cache.PutSourceCacheEntry(t.Context(), sourcecache.RowKindGrants, "scope-a", "validator-a"))
	entry, found, err := second.cache.LookupSourceCacheEntry(t.Context(), sourcecache.RowKindGrants, "scope-a")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "validator-a", entry.CacheValidator)

	third := newSourceCacheVerificationStore(t)
	res, err = third.cache.ReplaySourceCache(t.Context(), second.store, sourcecache.RowKindGrants, "scope-a")
	require.NoError(t, err)
	require.Equal(t, int64(1), res.Rows)
	secondRow, err := second.engine.GetGrantRecord(t.Context(), grant.GetId())
	require.NoError(t, err)
	thirdRow, err := third.engine.GetGrantRecord(t.Context(), grant.GetId())
	require.NoError(t, err)
	require.True(t, proto.Equal(secondRow, thirdRow), "second replay hop changed the materialized row")
}

// C21: duplicate scheduling is measured rather than exhaustively closed, but
// two calls released together must converge to one semantic materialization.
func TestVerificationConcurrentDuplicateReplay(t *testing.T) {
	prev := newSourceCacheVerificationStore(t)
	grant := putVerificationGrant(t, prev, "scope-a", "member", "alice")
	require.NoError(t, prev.cache.PutSourceCacheEntry(t.Context(), sourcecache.RowKindGrants, "scope-a", "validator-a"))
	cur := newSourceCacheVerificationStore(t)

	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := cur.cache.ReplaySourceCache(t.Context(), prev.store, sourcecache.RowKindGrants, "scope-a")
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	var rows int
	require.NoError(t, cur.engine.IterateGrants(t.Context(), func(*v3.GrantRecord) bool {
		rows++
		return true
	}))
	require.Equal(t, 1, rows)
	_, err := cur.engine.GetGrantRecord(t.Context(), grant.GetId())
	require.NoError(t, err)
}

// C11/C35: seal, envelope close, and a read-only reopen preserve the complete
// replay source, not merely the primary rows.
func TestVerificationReplaySourceSurvivesReadOnlyReopen(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "previous.c1z")
	first, err := NewStore(ctx, path, WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	_, err = first.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	cache, ok := first.(SourceCacheStore)
	require.True(t, ok)
	grant := mkV2Grant("", "member", "user", "alice")
	require.NoError(t, first.PutGrants(sourcecache.WithScope(ctx, "scope-a"), grant))
	require.NoError(t, cache.PutSourceCacheEntry(ctx, sourcecache.RowKindGrants, "scope-a", "validator-a"))
	require.NoError(t, first.EndSync(ctx))
	require.NoError(t, first.Close(ctx))

	reopened, err := NewStore(ctx, path, WithReadOnly(true))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close(ctx)) })
	reopenedCache, ok := reopened.(SourceCacheStore)
	require.True(t, ok)
	entry, found, err := reopenedCache.LookupSourceCacheEntry(ctx, sourcecache.RowKindGrants, "scope-a")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "validator-a", entry.CacheValidator)

	cur := newSourceCacheVerificationStore(t)
	res, err := cur.cache.ReplaySourceCache(ctx, reopened, sourcecache.RowKindGrants, "scope-a")
	require.NoError(t, err)
	require.Equal(t, int64(1), res.Rows)
	_, err = cur.engine.GetGrantRecord(ctx, grant.GetId())
	require.NoError(t, err)
}

// C20/C35: SQLite exposes no partial source-cache capability, and passing it
// directly to Pebble replay fails loudly rather than becoming an empty hit.
func TestVerificationUnsupportedSourceFailsClosed(t *testing.T) {
	ctx := t.Context()
	previous, err := NewC1ZFile(ctx, filepath.Join(t.TempDir(), "previous.c1z"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, previous.Close(ctx)) })
	_, err = previous.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	_, ok := any(previous).(SourceCacheStore)
	require.False(t, ok, "SQLite must expose capability absence, not a partial implementation")

	cur := newSourceCacheVerificationStore(t)
	_, err = cur.cache.ReplaySourceCache(ctx, previous, sourcecache.RowKindGrants, "scope-a")
	require.ErrorContains(t, err, "not a pebble store")
}
