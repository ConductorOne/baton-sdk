package dotc1z

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"unicode/utf8"

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

type sourceCacheVerificationSemanticState struct {
	rows          []string
	indexKeys     []string
	manifestFound bool
	validator     string
}

func sourceCacheVerificationEngineDigest(t *testing.T, engine *enginepebble.Engine) [sha256.Size]byte {
	t.Helper()
	iter, err := engine.NewIter(nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, iter.Close()) }()

	var snapshot []byte
	for iter.First(); iter.Valid(); iter.Next() {
		snapshot = binary.BigEndian.AppendUint64(snapshot, uint64(len(iter.Key())))
		snapshot = append(snapshot, iter.Key()...)
		snapshot = binary.BigEndian.AppendUint64(snapshot, uint64(len(iter.Value())))
		snapshot = append(snapshot, iter.Value()...)
	}
	require.NoError(t, iter.Error())
	return sha256.Sum256(snapshot)
}

func validateSourceCacheDigest(before, after [sha256.Size]byte) error {
	if before != after {
		return errors.New("source-cache artifact digest changed")
	}
	return nil
}

func validateSourceCacheScopeCounts(target, neighbor, wantTarget, wantNeighbor int) error {
	if target != wantTarget || neighbor != wantNeighbor {
		return fmt.Errorf("scope counts target=%d neighbor=%d, want target=%d neighbor=%d",
			target, neighbor, wantTarget, wantNeighbor)
	}
	return nil
}

func countSourceCacheVerificationKeys(t *testing.T, engine *enginepebble.Engine, lower, upper []byte) int {
	t.Helper()
	iter, err := engine.NewIter(&cockroachpebble.IterOptions{LowerBound: lower, UpperBound: upper})
	require.NoError(t, err)
	defer func() { require.NoError(t, iter.Close()) }()
	count := 0
	for iter.First(); iter.Valid(); iter.Next() {
		count++
	}
	require.NoError(t, iter.Error())
	return count
}

func sourceCacheVerificationIndexKeys(
	t *testing.T,
	engine *enginepebble.Engine,
	bounds ...[2][]byte,
) []string {
	t.Helper()
	var keys []string
	for _, bound := range bounds {
		iter, err := engine.NewIter(&cockroachpebble.IterOptions{LowerBound: bound[0], UpperBound: bound[1]})
		require.NoError(t, err)
		for iter.First(); iter.Valid(); iter.Next() {
			keys = append(keys, string(append([]byte(nil), iter.Key()...)))
		}
		require.NoError(t, iter.Error())
		require.NoError(t, iter.Close())
	}
	sort.Strings(keys)
	return keys
}

func sourceCacheVerificationSemanticSnapshot(
	t *testing.T,
	s sourceCacheVerificationStore,
	kind sourcecache.RowKind,
	scope string,
) sourceCacheVerificationSemanticState {
	t.Helper()
	var rows []string
	var bounds [][2][]byte
	switch kind {
	case sourcecache.RowKindResources:
		require.NoError(t, s.engine.IterateResources(t.Context(), func(rec *v3.ResourceRecord) bool {
			if rec.GetSourceScopeKey() != scope {
				return true
			}
			cloned := proto.Clone(rec).(*v3.ResourceRecord)
			cloned.SetDiscoveredAt(nil)
			value, err := proto.MarshalOptions{Deterministic: true}.Marshal(cloned)
			require.NoError(t, err)
			rows = append(rows, string(value))
			return true
		}))
		bounds = [][2][]byte{
			{enginepebble.ResourceBySourceScopeLowerBound(), enginepebble.ResourceBySourceScopeUpperBound()},
			{enginepebble.ResourceByParentLowerBound(), enginepebble.ResourceByParentUpperBound()},
		}
	case sourcecache.RowKindEntitlements:
		require.NoError(t, s.engine.IterateEntitlements(t.Context(), func(rec *v3.EntitlementRecord) bool {
			if rec.GetSourceScopeKey() != scope {
				return true
			}
			cloned := proto.Clone(rec).(*v3.EntitlementRecord)
			cloned.SetDiscoveredAt(nil)
			value, err := proto.MarshalOptions{Deterministic: true}.Marshal(cloned)
			require.NoError(t, err)
			rows = append(rows, string(value))
			return true
		}))
		bounds = [][2][]byte{
			{enginepebble.EntitlementBySourceScopeLowerBound(), enginepebble.EntitlementBySourceScopeUpperBound()},
		}
	case sourcecache.RowKindGrants:
		require.NoError(t, s.engine.IterateGrants(t.Context(), func(rec *v3.GrantRecord) bool {
			if rec.GetSourceScopeKey() != scope {
				return true
			}
			cloned := proto.Clone(rec).(*v3.GrantRecord)
			cloned.SetDiscoveredAt(nil)
			value, err := proto.MarshalOptions{Deterministic: true}.Marshal(cloned)
			require.NoError(t, err)
			rows = append(rows, string(value))
			return true
		}))
		bounds = [][2][]byte{
			{enginepebble.GrantBySourceScopeLowerBound(), enginepebble.GrantBySourceScopeUpperBound()},
			{enginepebble.GrantByPrincipalLowerBound(), enginepebble.GrantByPrincipalUpperBound()},
		}
	default:
		t.Fatalf("unsupported row kind %q", kind)
	}
	sort.Strings(rows)
	entry, found, err := s.cache.LookupSourceCacheEntry(t.Context(), kind, scope)
	require.NoError(t, err)
	return sourceCacheVerificationSemanticState{
		rows:          rows,
		indexKeys:     sourceCacheVerificationIndexKeys(t, s.engine, bounds...),
		manifestFound: found,
		validator:     entry.CacheValidator,
	}
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

func sealSourceCacheVerificationStore(t *testing.T, store sourceCacheVerificationStore) {
	t.Helper()
	require.NoError(t, store.store.EndSync(t.Context()))
}

func putVerificationGrant(t *testing.T, s sourceCacheVerificationStore, scope, entitlement, principal string) *v2.Grant {
	t.Helper()
	grant := mkV2Grant("", entitlement, "user", principal)
	require.NoError(t, s.store.PutGrants(sourcecache.WithScope(t.Context(), scope), grant))
	return grant
}

func putSourceCacheVerificationRows(
	t *testing.T,
	s sourceCacheVerificationStore,
	kind sourcecache.RowKind,
	scope string,
	count int,
	prefix string,
) {
	t.Helper()
	ctx := sourcecache.WithScope(t.Context(), scope)
	switch kind {
	case sourcecache.RowKindResources:
		rows := make([]*v2.Resource, 0, count)
		for i := range count {
			rows = append(rows, v2.Resource_builder{
				Id: v2.ResourceId_builder{
					ResourceType: "user",
					Resource:     fmt.Sprintf("%s-%d", prefix, i),
				}.Build(),
			}.Build())
		}
		require.NoError(t, s.store.PutResources(ctx, rows...))
	case sourcecache.RowKindEntitlements:
		rows := make([]*v2.Entitlement, 0, count)
		for i := range count {
			rows = append(rows, v2.Entitlement_builder{
				Id: fmt.Sprintf("%s-%d", prefix, i),
				Resource: v2.Resource_builder{
					Id: v2.ResourceId_builder{
						ResourceType: "group",
						Resource:     fmt.Sprintf("%s-group-%d", prefix, i),
					}.Build(),
				}.Build(),
			}.Build())
		}
		require.NoError(t, s.store.PutEntitlements(ctx, rows...))
	case sourcecache.RowKindGrants:
		rows := make([]*v2.Grant, 0, count)
		for i := range count {
			rows = append(rows, mkV2Grant(
				"",
				fmt.Sprintf("%s-%d", prefix, i),
				"user",
				fmt.Sprintf("%s-principal-%d", prefix, i),
			))
		}
		require.NoError(t, s.store.PutGrants(ctx, rows...))
	default:
		t.Fatalf("unsupported row kind %q", kind)
	}
}

func countSourceCacheVerificationRowsInScope(
	t *testing.T,
	engine *enginepebble.Engine,
	kind sourcecache.RowKind,
	scope string,
) int {
	t.Helper()
	count := 0
	switch kind {
	case sourcecache.RowKindResources:
		require.NoError(t, engine.IterateResources(t.Context(), func(rec *v3.ResourceRecord) bool {
			if rec.GetSourceScopeKey() == scope {
				count++
			}
			return true
		}))
	case sourcecache.RowKindEntitlements:
		require.NoError(t, engine.IterateEntitlements(t.Context(), func(rec *v3.EntitlementRecord) bool {
			if rec.GetSourceScopeKey() == scope {
				count++
			}
			return true
		}))
	case sourcecache.RowKindGrants:
		require.NoError(t, engine.IterateGrants(t.Context(), func(rec *v3.GrantRecord) bool {
			if rec.GetSourceScopeKey() == scope {
				count++
			}
			return true
		}))
	default:
		t.Fatalf("unsupported row kind %q", kind)
	}
	return count
}

func invalidateSourceCacheVerificationManifest(
	t *testing.T,
	s sourceCacheVerificationStore,
	kind sourcecache.RowKind,
	scope string,
) {
	t.Helper()
	rec, err := s.engine.GetSourceCacheEntry(t.Context(), string(kind), scope)
	require.NoError(t, err)
	rec.SetInvalidated(true)
	value, err := proto.Marshal(rec)
	require.NoError(t, err)

	iter, err := s.engine.NewIter(&cockroachpebble.IterOptions{
		LowerBound: enginepebble.SourceCacheEntryLowerBound(),
		UpperBound: enginepebble.SourceCacheEntryUpperBound(),
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, iter.Close()) }()
	require.True(t, iter.First(), "manifest key must exist")
	key := append([]byte(nil), iter.Key()...)
	require.NoError(t, s.engine.UnsafeForTesting().Set(key, value, nil))
}

// C07/C32: a scope is replayable only when the previous artifact owns a
// matching manifest entry. Stamped rows alone are not authority to replay.
func TestVerificationReplayRejectsStampedRowsWithoutManifest(t *testing.T) {
	for _, kind := range []sourcecache.RowKind{
		sourcecache.RowKindResources,
		sourcecache.RowKindEntitlements,
		sourcecache.RowKindGrants,
	} {
		t.Run(string(kind), func(t *testing.T) {
			prev := newSourceCacheVerificationStore(t)
			putSourceCacheVerificationRows(t, prev, kind, "scope-a", 1, "source")
			cur := newSourceCacheVerificationStore(t)
			putSourceCacheVerificationRows(t, cur, kind, "scope-a", 1, "destination")
			prevBefore := sourceCacheVerificationEngineDigest(t, prev.engine)
			curBefore := sourceCacheVerificationEngineDigest(t, cur.engine)

			_, err := cur.cache.ReplaySourceCache(t.Context(), prev.store, kind, "scope-a")
			require.Error(t, err, "replay without a matching manifest must fail closed")
			require.Equal(t, 1, countSourceCacheVerificationRowsInScope(t, cur.engine, kind, "scope-a"),
				"rejection must not clear an occupied destination scope")
			require.Equal(t, prevBefore, sourceCacheVerificationEngineDigest(t, prev.engine), "rejection mutated source")
			require.Equal(t, curBefore, sourceCacheVerificationEngineDigest(t, cur.engine), "rejection mutated destination")
		})
	}
}

// C25: empty validators are transitional; they cannot replace or create a
// completed manifest entry that a future lookup treats as replayable.
func TestVerificationEmptyValidatorDoesNotPublishManifest(t *testing.T) {
	for _, kind := range []sourcecache.RowKind{
		sourcecache.RowKindResources,
		sourcecache.RowKindEntitlements,
		sourcecache.RowKindGrants,
	} {
		t.Run(string(kind), func(t *testing.T) {
			s := newSourceCacheVerificationStore(t)

			err := s.cache.PutSourceCacheEntry(t.Context(), kind, "scope-a", "")
			require.Error(t, err, "the Phase 6a engine rejects empty validators")
			_, rawErr := s.engine.GetSourceCacheEntry(t.Context(), string(kind), "scope-a")
			require.ErrorIs(t, rawErr, cockroachpebble.ErrNotFound,
				"empty-validator rejection must leave no raw manifest residue")
			_, found, lookupErr := s.cache.LookupSourceCacheEntry(t.Context(), kind, "scope-a")
			require.NoError(t, lookupErr)
			require.False(t, found, "an empty validator must not become a completed manifest hit")

			require.NoError(t, s.cache.PutSourceCacheEntry(t.Context(), kind, "scope-a", "validator-old"))
			before := sourceCacheVerificationEngineDigest(t, s.engine)
			err = s.cache.PutSourceCacheEntry(t.Context(), kind, "scope-a", "")
			require.Error(t, err)
			require.Equal(t, before, sourceCacheVerificationEngineDigest(t, s.engine),
				"empty validator replaced an existing completed manifest")
			entry, found, lookupErr := s.cache.LookupSourceCacheEntry(t.Context(), kind, "scope-a")
			require.NoError(t, lookupErr)
			require.True(t, found)
			require.Equal(t, "validator-old", entry.CacheValidator)
		})
	}

	t.Run("rejection-does-not-dirty-clean-or-closed-store", func(t *testing.T) {
		ctx := t.Context()
		path := filepath.Join(t.TempDir(), "empty-validator.c1z")
		store, err := NewStore(ctx, path, WithEngine(c1zstore.EnginePebble))
		require.NoError(t, err)
		_, err = store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)
		require.NoError(t, store.Close(ctx))

		store, err = NewStore(ctx, path, WithEngine(c1zstore.EnginePebble))
		require.NoError(t, err)
		_, startedNew, err := store.StartOrResumeSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)
		require.False(t, startedNew)
		ps := store.(*pebbleStore)
		require.False(t, ps.dirty)
		err = ps.PutSourceCacheEntry(ctx, sourcecache.RowKindResources, "scope-a", "")
		require.ErrorContains(t, err, "cache validator is required")
		require.False(t, ps.dirty, "input rejection must precede dirty transition")
		require.NoError(t, ps.Close(ctx))

		err = ps.PutSourceCacheEntry(ctx, sourcecache.RowKindResources, "scope-a", "")
		require.ErrorContains(t, err, "cache validator is required",
			"input validation must retain precedence after Close")
	})
}

// C31: retry-partial and otherwise occupied destinations must converge to
// replacement, not union, semantics for the selected scope.
func TestVerificationPureReplayReplacesOccupiedScope(t *testing.T) {
	prev := newSourceCacheVerificationStore(t)
	copied := putVerificationGrant(t, prev, "scope-a", "member", "alice")
	require.NoError(t, prev.cache.PutSourceCacheEntry(t.Context(), sourcecache.RowKindGrants, "scope-a", "validator-a"))
	sealSourceCacheVerificationStore(t, prev)

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
	require.Equal(t, 2, countSourceCacheVerificationKeys(
		t,
		cur.engine,
		enginepebble.GrantBySourceScopeLowerBound(),
		enginepebble.GrantBySourceScopeUpperBound(),
	), "replacement left a stale grant source-scope index")
	require.Equal(t, 2, countSourceCacheVerificationKeys(
		t,
		cur.engine,
		enginepebble.GrantByPrincipalLowerBound(),
		enginepebble.GrantByPrincipalUpperBound(),
	), "replacement left a stale grant principal index")
}

func TestVerificationPureReplayReplacesOccupiedScopeResourcesAndEntitlements(t *testing.T) {
	t.Run("resources", func(t *testing.T) {
		prev := newSourceCacheVerificationStore(t)
		source := v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "user", Resource: "source"}.Build(),
		}.Build()
		require.NoError(t, prev.store.PutResources(sourcecache.WithScope(t.Context(), "scope-a"), source))
		require.NoError(t, prev.cache.PutSourceCacheEntry(t.Context(), sourcecache.RowKindResources, "scope-a", "validator-a"))
		sealSourceCacheVerificationStore(t, prev)

		cur := newSourceCacheVerificationStore(t)
		obsolete := v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "user", Resource: "obsolete"}.Build(),
			ParentResourceId: v2.ResourceId_builder{
				ResourceType: "team",
				Resource:     "obsolete-parent",
			}.Build(),
		}.Build()
		decoy := v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "user", Resource: "decoy"}.Build(),
		}.Build()
		require.NoError(t, cur.store.PutResources(sourcecache.WithScope(t.Context(), "scope-a"), obsolete))
		require.NoError(t, cur.store.PutResources(sourcecache.WithScope(t.Context(), "scope-b"), decoy))

		_, err := cur.cache.ReplaySourceCache(t.Context(), prev.store, sourcecache.RowKindResources, "scope-a")
		require.NoError(t, err)
		_, err = cur.engine.GetResourceRecord(t.Context(), "user", "source")
		require.NoError(t, err)
		_, err = cur.engine.GetResourceRecord(t.Context(), "user", "obsolete")
		require.ErrorIs(t, err, cockroachpebble.ErrNotFound)
		_, err = cur.engine.GetResourceRecord(t.Context(), "user", "decoy")
		require.NoError(t, err)
		require.Equal(t, 2, countSourceCacheVerificationKeys(
			t,
			cur.engine,
			enginepebble.ResourceBySourceScopeLowerBound(),
			enginepebble.ResourceBySourceScopeUpperBound(),
		), "replacement left a stale resource source-scope index")
		require.Zero(t, countSourceCacheVerificationKeys(
			t,
			cur.engine,
			enginepebble.ResourceByParentLowerBound(),
			enginepebble.ResourceByParentUpperBound(),
		), "replacement left the obsolete resource parent index")
	})

	t.Run("entitlements", func(t *testing.T) {
		resource := v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(),
		}.Build()
		prev := newSourceCacheVerificationStore(t)
		source := v2.Entitlement_builder{Id: "source", Resource: resource}.Build()
		require.NoError(t, prev.store.PutEntitlements(sourcecache.WithScope(t.Context(), "scope-a"), source))
		require.NoError(t, prev.cache.PutSourceCacheEntry(t.Context(), sourcecache.RowKindEntitlements, "scope-a", "validator-a"))
		sealSourceCacheVerificationStore(t, prev)

		cur := newSourceCacheVerificationStore(t)
		obsolete := v2.Entitlement_builder{Id: "obsolete", Resource: resource}.Build()
		decoy := v2.Entitlement_builder{Id: "decoy", Resource: resource}.Build()
		require.NoError(t, cur.store.PutEntitlements(sourcecache.WithScope(t.Context(), "scope-a"), obsolete))
		require.NoError(t, cur.store.PutEntitlements(sourcecache.WithScope(t.Context(), "scope-b"), decoy))

		_, err := cur.cache.ReplaySourceCache(t.Context(), prev.store, sourcecache.RowKindEntitlements, "scope-a")
		require.NoError(t, err)
		_, err = cur.engine.GetEntitlementRecord(t.Context(), "source")
		require.NoError(t, err)
		_, err = cur.engine.GetEntitlementRecord(t.Context(), "obsolete")
		require.Error(t, err)
		_, err = cur.engine.GetEntitlementRecord(t.Context(), "decoy")
		require.NoError(t, err)
		require.Equal(t, 2, countSourceCacheVerificationKeys(
			t,
			cur.engine,
			enginepebble.EntitlementBySourceScopeLowerBound(),
			enginepebble.EntitlementBySourceScopeUpperBound(),
		), "replacement left a stale entitlement source-scope index")
	})
}

// C10/C31: zero copied rows can still be a destructive replacement. On a
// resumed store, replay must mark the public wrapper dirty so Close persists
// the cleared destination scope.
func TestVerificationZeroRowReplayReplacementPersistsAfterResume(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.c1z")
	currentPath := filepath.Join(dir, "current.c1z")

	source, err := NewStore(ctx, sourcePath, WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	_, err = source.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	sourceCache := source.(SourceCacheStore)
	require.NoError(t, sourceCache.PutSourceCacheEntry(ctx, sourcecache.RowKindGrants, "scope-a", "validator-a"))
	require.NoError(t, source.EndSync(ctx))
	require.NoError(t, source.Close(ctx))

	current, err := NewStore(ctx, currentPath, WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	syncID, err := current.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	grant := mkV2Grant("", "member", "user", "alice")
	require.NoError(t, current.PutGrants(sourcecache.WithScope(ctx, "scope-a"), grant))
	require.NoError(t, current.Close(ctx), "persist interrupted destination fixture")

	source, err = NewStore(ctx, sourcePath, WithReadOnly(true))
	require.NoError(t, err)
	defer func() { require.NoError(t, source.Close(ctx)) }()
	current, err = NewStore(ctx, currentPath, WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	resumedID, startedNew, err := current.StartOrResumeSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.False(t, startedNew)
	require.Equal(t, syncID, resumedID)
	currentPebble := current.(*pebbleStore)
	require.False(t, currentPebble.dirty, "resume premise requires a clean wrapper")

	currentCache := current.(SourceCacheStore)
	res, err := currentCache.ReplaySourceCache(ctx, source, sourcecache.RowKindGrants, "scope-a")
	require.NoError(t, err)
	require.Zero(t, res.Rows)
	require.True(t, currentPebble.dirty, "destructive zero-row replacement must mark the wrapper dirty")
	require.NoError(t, current.Close(ctx))

	reopened, err := NewStore(ctx, currentPath, WithReadOnly(true))
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Close(ctx)) }()
	reopenedEngine, ok := enginepebble.AsEngine(reopened)
	require.True(t, ok)
	_, err = reopenedEngine.GetGrantRecord(ctx, grant.GetId())
	require.ErrorIs(t, err, cockroachpebble.ErrNotFound, "cleared destination row returned after close/reopen")
}

// C10/CO-005: the public wrapper may return an error after the engine has
// committed replay work. It must remain dirty so Close preserves that prefix.
func TestVerificationReplayErrorAfterCommittedClearPersistsAfterResume(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.c1z")
	currentPath := filepath.Join(dir, "current.c1z")

	source, err := NewStore(ctx, sourcePath, WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	_, err = source.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	sourceGrant := mkV2Grant("", "source", "user", "alice")
	require.NoError(t, source.PutGrants(sourcecache.WithScope(ctx, "scope-a"), sourceGrant))
	require.NoError(t, source.(SourceCacheStore).PutSourceCacheEntry(
		ctx,
		sourcecache.RowKindGrants,
		"scope-a",
		"validator-a",
	))
	require.NoError(t, source.EndSync(ctx))
	require.NoError(t, source.Close(ctx))

	current, err := NewStore(ctx, currentPath, WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	syncID, err := current.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	obsolete := mkV2Grant("", "obsolete", "user", "bob")
	require.NoError(t, current.PutGrants(sourcecache.WithScope(ctx, "scope-a"), obsolete))
	require.NoError(t, current.Close(ctx))

	source, err = NewStore(ctx, sourcePath, WithReadOnly(true))
	require.NoError(t, err)
	defer func() { require.NoError(t, source.Close(ctx)) }()
	current, err = NewStore(ctx, currentPath, WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	resumedID, startedNew, err := current.StartOrResumeSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.False(t, startedNew)
	require.Equal(t, syncID, resumedID)
	currentPebble := current.(*pebbleStore)
	require.False(t, currentPebble.dirty)
	injected := errors.New("verification replay post-commit failure")
	currentPebble.sourceCacheTest.afterEngineReplay = func() error { return injected }

	_, err = current.(SourceCacheStore).ReplaySourceCache(
		ctx,
		source,
		sourcecache.RowKindGrants,
		"scope-a",
	)
	require.ErrorIs(t, err, injected)
	require.True(t, currentPebble.dirty, "committed replay followed by wrapper error must mark dirty")
	currentPebble.sourceCacheTest.afterEngineReplay = nil
	require.NoError(t, current.Close(ctx))

	reopened, err := NewStore(ctx, currentPath, WithReadOnly(true))
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Close(ctx)) }()
	reopenedEngine, ok := enginepebble.AsEngine(reopened)
	require.True(t, ok)
	_, err = reopenedEngine.GetGrantRecord(ctx, obsolete.GetId())
	require.ErrorIs(t, err, cockroachpebble.ErrNotFound, "committed destination clear was not persisted")
	_, err = reopenedEngine.GetGrantRecord(ctx, sourceGrant.GetId())
	require.NoError(t, err, "committed replay row was not persisted after wrapper error")
}

// C39: scope identity includes row kind. A manifest hit for resources does
// not authorize a grants replay using the same scope bytes.
func TestVerificationWrongKindScopeDoesNotReplayAsEmptySuccess(t *testing.T) {
	kinds := []sourcecache.RowKind{
		sourcecache.RowKindResources,
		sourcecache.RowKindEntitlements,
		sourcecache.RowKindGrants,
	}
	for _, presentKind := range kinds {
		for _, requestedKind := range kinds {
			if requestedKind == presentKind {
				continue
			}
			t.Run(fmt.Sprintf("%s-present/%s-requested", presentKind, requestedKind), func(t *testing.T) {
				prev := newSourceCacheVerificationStore(t)
				putSourceCacheVerificationRows(t, prev, presentKind, "scope-a", 1, "source")
				require.NoError(t, prev.cache.PutSourceCacheEntry(
					t.Context(),
					presentKind,
					"scope-a",
					"validator-a",
				))
				cur := newSourceCacheVerificationStore(t)
				putSourceCacheVerificationRows(t, cur, requestedKind, "scope-a", 1, "destination")
				prevBefore := sourceCacheVerificationEngineDigest(t, prev.engine)
				curBefore := sourceCacheVerificationEngineDigest(t, cur.engine)

				_, err := cur.cache.ReplaySourceCache(t.Context(), prev.store, requestedKind, "scope-a")
				require.Error(t, err, "wrong-kind scope miss must fail closed at the direct replay primitive")
				require.Equal(t, prevBefore, sourceCacheVerificationEngineDigest(t, prev.engine),
					"wrong-kind rejection mutated source")
				require.Equal(t, curBefore, sourceCacheVerificationEngineDigest(t, cur.engine),
					"wrong-kind rejection mutated destination")
			})
		}
	}
}

// C40: lookup of an already-invalidated manifest entry is a miss. This
// criterion consumes invalidation state; it does not test the deferred policy
// that decides when to set it.
func TestVerificationInvalidatedManifestLookupMisses(t *testing.T) {
	for _, kind := range []sourcecache.RowKind{
		sourcecache.RowKindResources,
		sourcecache.RowKindEntitlements,
		sourcecache.RowKindGrants,
	} {
		for _, count := range []int{0, 1, 3} {
			t.Run(fmt.Sprintf("%s/%d-rows", kind, count), func(t *testing.T) {
				s := newSourceCacheVerificationStore(t)
				putSourceCacheVerificationRows(t, s, kind, "scope-a", count, "source")
				require.NoError(t, s.cache.PutSourceCacheEntry(t.Context(), kind, "scope-a", "validator-a"))
				invalidateSourceCacheVerificationManifest(t, s, kind, "scope-a")

				entry, found, err := s.cache.LookupSourceCacheEntry(t.Context(), kind, "scope-a")
				require.NoError(t, err)
				require.False(t, found, "invalidated manifest must be exposed as a lookup miss; got %+v", entry)
				sealSourceCacheVerificationStore(t, s)

				cur := newSourceCacheVerificationStore(t)
				putSourceCacheVerificationRows(t, cur, kind, "scope-a", 1, "destination")
				sourceBefore := sourceCacheVerificationEngineDigest(t, s.engine)
				curBefore := sourceCacheVerificationEngineDigest(t, cur.engine)
				_, err = cur.cache.ReplaySourceCache(t.Context(), s.store, kind, "scope-a")
				require.ErrorContains(t, err, "invalidated")
				require.Equal(t, sourceBefore, sourceCacheVerificationEngineDigest(t, s.engine),
					"invalidated rejection mutated source")
				require.Equal(t, curBefore, sourceCacheVerificationEngineDigest(t, cur.engine),
					"invalidated rejection mutated destination")
			})
		}
	}
}

// C41: prefix-neighbor scopes must not alias in either indexes or replay.
func TestVerificationPrefixNeighborScopeIsolation(t *testing.T) {
	scopePairs := [][2]string{
		{"foo", "foobar"},
		{"foobar", "foo"},
		{"a\x00b", "a\x00bc"},
		{"a\x00bc", "a\x00b"},
	}
	for _, kind := range []sourcecache.RowKind{
		sourcecache.RowKindResources,
		sourcecache.RowKindEntitlements,
		sourcecache.RowKindGrants,
	} {
		for _, pair := range scopePairs {
			targetScope, neighborScope := pair[0], pair[1]
			t.Run(fmt.Sprintf("%s/%q-to-%q", kind, targetScope, neighborScope), func(t *testing.T) {
				prev := newSourceCacheVerificationStore(t)
				putSourceCacheVerificationRows(t, prev, kind, targetScope, 1, "source")
				putSourceCacheVerificationRows(t, prev, kind, neighborScope, 1, "source-neighbor")
				require.NoError(t, prev.cache.PutSourceCacheEntry(t.Context(), kind, targetScope, "validator-target"))
				require.NoError(t, prev.cache.PutSourceCacheEntry(t.Context(), kind, neighborScope, "validator-neighbor"))

				targetEntry, found, err := prev.cache.LookupSourceCacheEntry(t.Context(), kind, targetScope)
				require.NoError(t, err)
				require.True(t, found)
				require.Equal(t, "validator-target", targetEntry.CacheValidator)
				neighborEntry, found, err := prev.cache.LookupSourceCacheEntry(t.Context(), kind, neighborScope)
				require.NoError(t, err)
				require.True(t, found)
				require.Equal(t, "validator-neighbor", neighborEntry.CacheValidator)
				sealSourceCacheVerificationStore(t, prev)

				cur := newSourceCacheVerificationStore(t)
				putSourceCacheVerificationRows(t, cur, kind, targetScope, 1, "obsolete")
				putSourceCacheVerificationRows(t, cur, kind, neighborScope, 1, "decoy")
				res, err := cur.cache.ReplaySourceCache(t.Context(), prev.store, kind, targetScope)
				require.NoError(t, err)
				require.Equal(t, int64(1), res.Rows)
				require.NoError(t, validateSourceCacheScopeCounts(
					countSourceCacheVerificationRowsInScope(t, cur.engine, kind, targetScope),
					countSourceCacheVerificationRowsInScope(t, cur.engine, kind, neighborScope),
					1,
					1,
				), "replay/replacement crossed the scope boundary")

				switch kind {
				case sourcecache.RowKindResources:
					_, err = cur.cache.DeleteSourceCacheRowsInScope(
						t.Context(),
						kind,
						targetScope,
						[]string{"source-0"},
					)
				case sourcecache.RowKindEntitlements:
					err = cur.cache.DeleteSourceCacheRows(t.Context(), kind, []string{"source-0"})
				case sourcecache.RowKindGrants:
					_, err = cur.cache.DeleteSourceCacheRowsInScope(
						t.Context(),
						kind,
						targetScope,
						[]string{"source-principal-0"},
					)
				}
				require.NoError(t, err)
				require.NoError(t, validateSourceCacheScopeCounts(
					countSourceCacheVerificationRowsInScope(t, cur.engine, kind, targetScope),
					countSourceCacheVerificationRowsInScope(t, cur.engine, kind, neighborScope),
					0,
					1,
				), "tombstone crossed the scope boundary")
			})
		}
	}
}

// C28: the replay capability owns scope validation just like lookup and
// manifest writes. Invalid input must fail before source iteration or mutation.
func TestVerificationReplayRejectsInvalidScope(t *testing.T) {
	for _, kind := range []sourcecache.RowKind{
		sourcecache.RowKindResources,
		sourcecache.RowKindEntitlements,
		sourcecache.RowKindGrants,
	} {
		for _, invalidScope := range []string{"", strings.Repeat("x", 257)} {
			t.Run(fmt.Sprintf("%s/%d-bytes", kind, len(invalidScope)), func(t *testing.T) {
				prev := newSourceCacheVerificationStore(t)
				cur := newSourceCacheVerificationStore(t)
				putSourceCacheVerificationRows(t, cur, kind, "scope-a", 1, "destination")
				prevBefore := sourceCacheVerificationEngineDigest(t, prev.engine)
				curBefore := sourceCacheVerificationEngineDigest(t, cur.engine)

				_, err := cur.cache.ReplaySourceCache(t.Context(), prev.store, kind, invalidScope)
				require.Error(t, err)
				require.NoError(t, validateSourceCacheDigest(prevBefore, sourceCacheVerificationEngineDigest(t, prev.engine)))
				require.NoError(t, validateSourceCacheDigest(curBefore, sourceCacheVerificationEngineDigest(t, cur.engine)))
				require.Error(t, cur.cache.PutSourceCacheEntry(t.Context(), kind, invalidScope, "validator"))
				_, _, err = cur.cache.LookupSourceCacheEntry(t.Context(), kind, invalidScope)
				require.Error(t, err)
				require.NoError(t, validateSourceCacheDigest(curBefore, sourceCacheVerificationEngineDigest(t, cur.engine)))
			})
		}
	}

	s := newSourceCacheVerificationStore(t)
	before := sourceCacheVerificationEngineDigest(t, s.engine)
	invalidKind := sourcecache.RowKind("invalid")
	require.Error(t, s.cache.PutSourceCacheEntry(t.Context(), invalidKind, "scope-a", "validator"))
	_, _, err := s.cache.LookupSourceCacheEntry(t.Context(), invalidKind, "scope-a")
	require.Error(t, err)
	require.Error(t, s.cache.DeleteSourceCacheRows(t.Context(), invalidKind, nil))
	_, err = s.cache.DeleteSourceCacheRowsInScope(t.Context(), invalidKind, "scope-a", nil)
	require.Error(t, err)
	require.NoError(t, validateSourceCacheDigest(before, sourceCacheVerificationEngineDigest(t, s.engine)))
}

// C42/O17: validate a whole tombstone request before applying any element.
// Otherwise a malformed trailing resource BID turns a nominally failed page
// into a durable partial deletion.
func TestVerificationCanonicalTombstonesRejectAtomically(t *testing.T) {
	t.Run("resources", func(t *testing.T) {
		s := newSourceCacheVerificationStore(t)
		resource := v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "user", Resource: "alice"}.Build(),
		}.Build()
		require.NoError(t, s.store.PutResources(sourcecache.WithScope(t.Context(), "scope-a"), resource))
		resourceBID, err := bid.MakeResourceBid(resource)
		require.NoError(t, err)
		before := sourceCacheVerificationEngineDigest(t, s.engine)

		err = s.cache.DeleteSourceCacheRows(
			t.Context(),
			sourcecache.RowKindResources,
			[]string{resourceBID, "not-a-resource-bid"},
		)
		require.Error(t, err)
		_, getErr := s.engine.GetResourceRecord(t.Context(), "user", "alice")
		require.NoError(t, getErr, "mixed valid+invalid tombstones must apply nothing")
		require.Equal(t, before, sourceCacheVerificationEngineDigest(t, s.engine))
	})

	t.Run("entitlements", func(t *testing.T) {
		s := newSourceCacheVerificationStore(t)
		valid := v3.EntitlementRecord_builder{
			ExternalId: "valid",
			Resource:   v3.ResourceRef_builder{ResourceTypeId: "team", ResourceId: "green"}.Build(),
		}.Build()
		ambiguousA := v3.EntitlementRecord_builder{
			ExternalId: "ambiguous",
			Resource:   v3.ResourceRef_builder{ResourceTypeId: "team", ResourceId: "red"}.Build(),
		}.Build()
		ambiguousB := v3.EntitlementRecord_builder{
			ExternalId: "ambiguous",
			Resource:   v3.ResourceRef_builder{ResourceTypeId: "team", ResourceId: "blue"}.Build(),
		}.Build()
		require.NoError(t, s.engine.PutEntitlementRecords(t.Context(), valid, ambiguousA, ambiguousB))
		before := sourceCacheVerificationEngineDigest(t, s.engine)

		err := s.cache.DeleteSourceCacheRows(
			t.Context(),
			sourcecache.RowKindEntitlements,
			[]string{"valid", "ambiguous"},
		)
		require.Error(t, err)
		_, getErr := s.engine.GetEntitlementRecord(t.Context(), "valid")
		require.NoError(t, getErr, "mixed valid+ambiguous tombstones must apply nothing")
		require.Equal(t, before, sourceCacheVerificationEngineDigest(t, s.engine))
	})

	t.Run("grants", func(t *testing.T) {
		s := newSourceCacheVerificationStore(t)
		valid := putVerificationGrant(t, s, "scope-a", "valid", "alice")
		const ambiguousID = "ambiguous:user:erin"
		require.NoError(t, s.engine.PutEntitlementRecords(t.Context(),
			v3.EntitlementRecord_builder{
				ExternalId: "ambiguous",
				Resource:   v3.ResourceRef_builder{ResourceTypeId: "team", ResourceId: "red"}.Build(),
			}.Build(),
			v3.EntitlementRecord_builder{
				ExternalId: "ambiguous",
				Resource:   v3.ResourceRef_builder{ResourceTypeId: "team", ResourceId: "blue"}.Build(),
			}.Build(),
		))
		require.NoError(t, s.engine.PutGrantRecords(t.Context(),
			v3.GrantRecord_builder{
				ExternalId: ambiguousID,
				Entitlement: v3.EntitlementRef_builder{
					ResourceTypeId: "team",
					ResourceId:     "red",
					EntitlementId:  "ambiguous",
				}.Build(),
				Principal: v3.PrincipalRef_builder{ResourceTypeId: "user", ResourceId: "erin"}.Build(),
			}.Build(),
			v3.GrantRecord_builder{
				ExternalId: ambiguousID,
				Entitlement: v3.EntitlementRef_builder{
					ResourceTypeId: "team",
					ResourceId:     "blue",
					EntitlementId:  "ambiguous",
				}.Build(),
				Principal: v3.PrincipalRef_builder{ResourceTypeId: "user", ResourceId: "erin"}.Build(),
			}.Build(),
		))
		before := sourceCacheVerificationEngineDigest(t, s.engine)

		err := s.cache.DeleteSourceCacheRows(
			t.Context(),
			sourcecache.RowKindGrants,
			[]string{valid.GetId(), ambiguousID},
		)
		require.Error(t, err)
		_, getErr := s.engine.GetGrantRecord(t.Context(), valid.GetId())
		require.NoError(t, getErr, "mixed valid+ambiguous tombstones must apply nothing")
		require.Equal(t, before, sourceCacheVerificationEngineDigest(t, s.engine))
	})

	t.Run("principal selector on entitlements", func(t *testing.T) {
		s := newSourceCacheVerificationStore(t)
		putSourceCacheVerificationRows(t, s, sourcecache.RowKindEntitlements, "scope-a", 1, "destination")
		before := sourceCacheVerificationEngineDigest(t, s.engine)
		_, err := s.cache.DeleteSourceCacheRowsInScope(
			t.Context(),
			sourcecache.RowKindEntitlements,
			"scope-a",
			[]string{"alice"},
		)
		require.Error(t, err)
		require.Equal(t, before, sourceCacheVerificationEngineDigest(t, s.engine))
	})
}

// C43: source and destination aliasing is an invalid state. The capability
// must reject it before iterating or writing, rather than relying on Pebble's
// incidental same-DB iterator/write behavior.
func TestVerificationReplayFromSelfRejected(t *testing.T) {
	s := newSourceCacheVerificationStore(t)
	putVerificationGrant(t, s, "scope-a", "member", "alice")
	require.NoError(t, s.cache.PutSourceCacheEntry(t.Context(), sourcecache.RowKindGrants, "scope-a", "validator-a"))
	before := sourceCacheVerificationEngineDigest(t, s.engine)

	_, err := s.cache.ReplaySourceCache(t.Context(), s.store, sourcecache.RowKindGrants, "scope-a")
	require.Error(t, err, "replay from the destination store itself must fail before mutation")
	require.Equal(t, before, sourceCacheVerificationEngineDigest(t, s.engine), "self-replay rejection mutated the artifact")
}

func writeVerificationReplayArtifact(t *testing.T, path, principal string) {
	t.Helper()
	ctx := t.Context()
	store, err := NewStore(ctx, path, WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	_, err = store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	cache, ok := store.(SourceCacheStore)
	require.True(t, ok)
	grant := mkV2Grant("", "member", "user", principal)
	require.NoError(t, store.PutGrants(sourcecache.WithScope(ctx, "scope-a"), grant))
	require.NoError(t, cache.PutSourceCacheEntry(ctx, sourcecache.RowKindGrants, "scope-a", "validator-a"))
	require.NoError(t, store.EndSync(ctx))
	require.NoError(t, store.Close(ctx))
}

// C43: two independently opened stores can still refer to the same c1z file.
// Reject both the exact path and a filesystem alias before replay, then prove
// the rejected call did not poison a subsequent replay from a distinct source.
func TestVerificationReplayFromSameArtifactPathRejected(t *testing.T) {
	for _, aliased := range []bool{false, true} {
		name := "exact-path"
		if aliased {
			name = "symlink"
		}
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			dir := t.TempDir()
			path := filepath.Join(dir, "shared.c1z")
			writeVerificationReplayArtifact(t, path, "alice")

			previousPath := path
			if aliased {
				previousPath = filepath.Join(dir, "shared-alias.c1z")
				require.NoError(t, os.Symlink(path, previousPath))
			}

			current, err := NewStore(ctx, path, WithEngine(c1zstore.EnginePebble))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, current.Close(ctx)) })
			_, err = current.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
			require.NoError(t, err)
			currentCache, ok := current.(SourceCacheStore)
			require.True(t, ok)
			destinationGrant := mkV2Grant("", "destination", "user", "destination")
			require.NoError(t, current.PutGrants(
				sourcecache.WithScope(ctx, "scope-a"),
				destinationGrant,
			))
			currentEngine, ok := enginepebble.AsEngine(current)
			require.True(t, ok)

			previous, err := NewStore(ctx, previousPath, WithReadOnly(true))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, previous.Close(ctx)) })
			previousEngine, ok := enginepebble.AsEngine(previous)
			require.True(t, ok)
			currentBefore := sourceCacheVerificationEngineDigest(t, currentEngine)
			previousBefore := sourceCacheVerificationEngineDigest(t, previousEngine)
			_, err = currentCache.ReplaySourceCache(ctx, previous, sourcecache.RowKindGrants, "scope-a")
			require.ErrorContains(t, err, "same artifact")
			require.Equal(t, currentBefore, sourceCacheVerificationEngineDigest(t, currentEngine),
				"same-artifact rejection mutated destination")
			require.Equal(t, previousBefore, sourceCacheVerificationEngineDigest(t, previousEngine),
				"same-artifact rejection mutated source")

			distinctPath := filepath.Join(dir, "distinct.c1z")
			writeVerificationReplayArtifact(t, distinctPath, "bob")
			distinct, err := NewStore(ctx, distinctPath, WithReadOnly(true))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, distinct.Close(ctx)) })
			res, err := currentCache.ReplaySourceCache(ctx, distinct, sourcecache.RowKindGrants, "scope-a")
			require.NoError(t, err)
			require.Equal(t, int64(1), res.Rows)
		})
	}
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
	sealSourceCacheVerificationStore(t, prev)

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
	for _, kind := range []sourcecache.RowKind{
		sourcecache.RowKindResources,
		sourcecache.RowKindEntitlements,
		sourcecache.RowKindGrants,
	} {
		for _, scenario := range []struct {
			name             string
			sourceRows       int
			overlayRows      int
			tombstoneBase    bool
			expectedHop2Rows int
		}{
			{name: "zero-row", expectedHop2Rows: 0},
			{name: "populated", sourceRows: 1, expectedHop2Rows: 1},
			{name: "overlay", sourceRows: 1, overlayRows: 1, expectedHop2Rows: 2},
			{name: "tombstoned", sourceRows: 1, tombstoneBase: true, expectedHop2Rows: 0},
		} {
			t.Run(fmt.Sprintf("%s/%s", kind, scenario.name), func(t *testing.T) {
				first := newSourceCacheVerificationStore(t)
				putSourceCacheVerificationRows(t, first, kind, "scope-a", scenario.sourceRows, "base")
				require.NoError(t, first.cache.PutSourceCacheEntry(t.Context(), kind, "scope-a", "validator-a"))
				sealSourceCacheVerificationStore(t, first)

				second := newSourceCacheVerificationStore(t)
				res, err := second.cache.ReplaySourceCache(t.Context(), first.store, kind, "scope-a")
				require.NoError(t, err)
				require.Equal(t, int64(scenario.sourceRows), res.Rows)
				putSourceCacheVerificationRows(t, second, kind, "scope-a", scenario.overlayRows, "overlay")
				if scenario.tombstoneBase {
					var id string
					switch kind {
					case sourcecache.RowKindResources:
						resource := v2.Resource_builder{
							Id: v2.ResourceId_builder{ResourceType: "user", Resource: "base-0"}.Build(),
						}.Build()
						id, err = bid.MakeResourceBid(resource)
						require.NoError(t, err)
					case sourcecache.RowKindEntitlements:
						id = "base-0"
					case sourcecache.RowKindGrants:
						id = mkV2Grant("", "base-0", "user", "base-principal-0").GetId()
					}
					require.NoError(t, second.cache.DeleteSourceCacheRows(t.Context(), kind, []string{id}))
				}
				require.NoError(t, second.cache.PutSourceCacheEntry(t.Context(), kind, "scope-a", "validator-a"))
				entry, found, err := second.cache.LookupSourceCacheEntry(t.Context(), kind, "scope-a")
				require.NoError(t, err)
				require.True(t, found)
				require.Equal(t, "validator-a", entry.CacheValidator)
				sealSourceCacheVerificationStore(t, second)

				direct := newSourceCacheVerificationStore(t)
				if scenario.sourceRows > 0 && !scenario.tombstoneBase {
					putSourceCacheVerificationRows(t, direct, kind, "scope-a", scenario.sourceRows, "base")
				}
				putSourceCacheVerificationRows(t, direct, kind, "scope-a", scenario.overlayRows, "overlay")
				require.NoError(t, direct.cache.PutSourceCacheEntry(t.Context(), kind, "scope-a", "validator-a"))
				secondBefore := sourceCacheVerificationEngineDigest(t, second.engine)
				third := newSourceCacheVerificationStore(t)
				res, err = third.cache.ReplaySourceCache(t.Context(), second.store, kind, "scope-a")
				require.NoError(t, err)
				require.Equal(t, int64(scenario.expectedHop2Rows), res.Rows)
				require.NoError(t, third.cache.PutSourceCacheEntry(t.Context(), kind, "scope-a", "validator-a"))
				require.Equal(t, secondBefore, sourceCacheVerificationEngineDigest(t, second.engine),
					"second-hop replay mutated its source")
				require.Equal(t, scenario.expectedHop2Rows,
					countSourceCacheVerificationRowsInScope(t, second.engine, kind, "scope-a"))
				require.Equal(t, scenario.expectedHop2Rows,
					countSourceCacheVerificationRowsInScope(t, third.engine, kind, "scope-a"))
				secondSnapshot := sourceCacheVerificationSemanticSnapshot(t, second, kind, "scope-a")
				require.Equal(t, secondSnapshot, sourceCacheVerificationSemanticSnapshot(t, third, kind, "scope-a"))
				require.Equal(t, secondSnapshot, sourceCacheVerificationSemanticSnapshot(t, direct, kind, "scope-a"))
			})
		}
	}
}

// C21: duplicate scheduling is measured rather than exhaustively closed, but
// two calls released together must converge to one semantic materialization.
func TestVerificationConcurrentDuplicateReplay(t *testing.T) {
	prev := newSourceCacheVerificationStore(t)
	grant := putVerificationGrant(t, prev, "scope-a", "member", "alice")
	require.NoError(t, prev.cache.PutSourceCacheEntry(t.Context(), sourcecache.RowKindGrants, "scope-a", "validator-a"))
	sealSourceCacheVerificationStore(t, prev)
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
	require.NoError(t, cur.cache.PutSourceCacheEntry(
		t.Context(),
		sourcecache.RowKindGrants,
		"scope-a",
		"validator-a",
	))

	var rows int
	require.NoError(t, cur.engine.IterateGrants(t.Context(), func(*v3.GrantRecord) bool {
		rows++
		return true
	}))
	require.Equal(t, 1, rows)
	_, err := cur.engine.GetGrantRecord(t.Context(), grant.GetId())
	require.NoError(t, err)
	direct := newSourceCacheVerificationStore(t)
	putVerificationGrant(t, direct, "scope-a", "member", "alice")
	require.NoError(t, direct.cache.PutSourceCacheEntry(
		t.Context(),
		sourcecache.RowKindGrants,
		"scope-a",
		"validator-a",
	))
	require.Equal(t,
		sourceCacheVerificationSemanticSnapshot(t, direct, sourcecache.RowKindGrants, "scope-a"),
		sourceCacheVerificationSemanticSnapshot(t, cur, sourcecache.RowKindGrants, "scope-a"),
	)
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
	beforeBytes, err := os.ReadFile(path)
	require.NoError(t, err)
	beforeDigest := sha256.Sum256(beforeBytes)

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
	afterBytes, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, beforeDigest, sha256.Sum256(afterBytes), "replay changed the source c1z artifact bytes")
}

// C23: sealed source artifact bytes are immutable for every row kind under
// success, injected destination failure, cancellation, and retry.
func TestVerificationSourceArtifactDigestAllKindOutcomes(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "previous.c1z")
	source, err := NewStore(ctx, path, WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	_, err = source.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	resource := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "user", Resource: "alice"}.Build(),
	}.Build()
	entitlement := v2.Entitlement_builder{
		Id: "group:g1:member",
		Resource: v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(),
		}.Build(),
	}.Build()
	grant := mkV2Grant("", "member", "user", "alice")
	require.NoError(t, source.PutResources(sourcecache.WithScope(ctx, "scope-a"), resource))
	require.NoError(t, source.PutEntitlements(sourcecache.WithScope(ctx, "scope-a"), entitlement))
	require.NoError(t, source.PutGrants(sourcecache.WithScope(ctx, "scope-a"), grant))
	sourceCache := source.(SourceCacheStore)
	for _, kind := range []sourcecache.RowKind{
		sourcecache.RowKindResources,
		sourcecache.RowKindEntitlements,
		sourcecache.RowKindGrants,
	} {
		require.NoError(t, sourceCache.PutSourceCacheEntry(ctx, kind, "scope-a", "validator-a"))
	}
	require.NoError(t, source.EndSync(ctx))
	require.NoError(t, source.Close(ctx))
	sourceBytes, err := os.ReadFile(path)
	require.NoError(t, err)
	sourceDigest := sha256.Sum256(sourceBytes)

	source, err = NewStore(ctx, path, WithReadOnly(true))
	require.NoError(t, err)
	defer func() { require.NoError(t, source.Close(ctx)) }()
	assertSourceDigest := func(label string) {
		t.Helper()
		after, err := os.ReadFile(path)
		require.NoError(t, err)
		require.NoError(t, validateSourceCacheDigest(sourceDigest, sha256.Sum256(after)), label)
	}

	for _, kind := range []sourcecache.RowKind{
		sourcecache.RowKindResources,
		sourcecache.RowKindEntitlements,
		sourcecache.RowKindGrants,
	} {
		t.Run(string(kind), func(t *testing.T) {
			success := newSourceCacheVerificationStore(t)
			_, err := success.cache.ReplaySourceCache(ctx, source, kind, "scope-a")
			require.NoError(t, err)
			assertSourceDigest("successful replay changed source artifact")

			failed := newSourceCacheVerificationStore(t)
			injected := errors.New("verification destination commit failure")
			failedStore := failed.store.(*pebbleStore)
			failedStore.sourceCacheTest.afterEngineReplay = func() error { return injected }
			_, err = failed.cache.ReplaySourceCache(ctx, source, kind, "scope-a")
			require.ErrorIs(t, err, injected)
			assertSourceDigest("failed replay changed source artifact")
			failedStore.sourceCacheTest.afterEngineReplay = nil
			_, err = failed.cache.ReplaySourceCache(ctx, source, kind, "scope-a")
			require.NoError(t, err)
			assertSourceDigest("retry changed source artifact")

			cancelled := newSourceCacheVerificationStore(t)
			cancelCtx, cancel := context.WithCancel(ctx)
			cancel()
			_, err = cancelled.cache.ReplaySourceCache(cancelCtx, source, kind, "scope-a")
			require.ErrorIs(t, err, context.Canceled)
			assertSourceDigest("cancelled replay changed source artifact")
		})
	}
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
	putVerificationGrant(t, cur, "scope-a", "destination", "alice")
	before := sourceCacheVerificationEngineDigest(t, cur.engine)
	_, err = cur.cache.ReplaySourceCache(ctx, previous, sourcecache.RowKindGrants, "scope-a")
	require.ErrorContains(t, err, "not a pebble store")
	require.Equal(t, before, sourceCacheVerificationEngineDigest(t, cur.engine),
		"unsupported-source rejection mutated occupied destination")
}

func TestVerificationReplayRejectsUnfinishedSourceAllKinds(t *testing.T) {
	for _, kind := range []sourcecache.RowKind{
		sourcecache.RowKindResources,
		sourcecache.RowKindEntitlements,
		sourcecache.RowKindGrants,
	} {
		t.Run(string(kind), func(t *testing.T) {
			previous := newSourceCacheVerificationStore(t)
			putSourceCacheVerificationRows(t, previous, kind, "scope-a", 2, "source")
			require.NoError(t, previous.cache.PutSourceCacheEntry(t.Context(), kind, "scope-a", "validator"))

			current := newSourceCacheVerificationStore(t)
			putSourceCacheVerificationRows(t, current, kind, "scope-a", 1, "destination")
			previousBefore := sourceCacheVerificationEngineDigest(t, previous.engine)
			currentBefore := sourceCacheVerificationEngineDigest(t, current.engine)
			enteredMutation := false
			current.store.(*pebbleStore).sourceCacheTest.beforeEngineMutation = func() {
				enteredMutation = true
			}

			_, err := current.cache.ReplaySourceCache(t.Context(), previous.store, kind, "scope-a")
			require.ErrorContains(t, err, "not finished")
			require.False(t, enteredMutation, "unfinished-source rejection entered destination mutation")
			require.Equal(t, previousBefore, sourceCacheVerificationEngineDigest(t, previous.engine))
			require.Equal(t, currentBefore, sourceCacheVerificationEngineDigest(t, current.engine))
		})
	}
}

func TestVerificationReplayRejectsIneligibleFinishedSourceAllKinds(t *testing.T) {
	for _, policy := range []struct {
		name      string
		configure func(*v3.SyncRunRecord)
		wantError string
	}{
		{
			name: "compacted",
			configure: func(run *v3.SyncRunRecord) {
				run.SetCompacted(true)
			},
			wantError: "compacted artifacts are not replay-eligible",
		},
		{
			name: "partial",
			configure: func(run *v3.SyncRunRecord) {
				run.SetType(v3.SyncType_SYNC_TYPE_PARTIAL)
			},
			wantError: "not replay-eligible",
		},
	} {
		for _, kind := range []sourcecache.RowKind{
			sourcecache.RowKindResources,
			sourcecache.RowKindEntitlements,
			sourcecache.RowKindGrants,
		} {
			t.Run(policy.name+"/"+string(kind), func(t *testing.T) {
				previous := newSourceCacheVerificationStore(t)
				putSourceCacheVerificationRows(t, previous, kind, "scope-a", 2, "source")
				require.NoError(t, previous.cache.PutSourceCacheEntry(t.Context(), kind, "scope-a", "validator"))
				sealSourceCacheVerificationStore(t, previous)
				run, err := previous.engine.LatestFinishedSyncRecord(t.Context(), nil)
				require.NoError(t, err)
				require.NotNil(t, run)
				policy.configure(run)
				require.NoError(t, previous.engine.PutSyncRunRecord(t.Context(), run))

				current := newSourceCacheVerificationStore(t)
				putSourceCacheVerificationRows(t, current, kind, "scope-a", 1, "destination")
				previousBefore := sourceCacheVerificationEngineDigest(t, previous.engine)
				currentBefore := sourceCacheVerificationEngineDigest(t, current.engine)
				enteredMutation := false
				current.store.(*pebbleStore).sourceCacheTest.beforeEngineMutation = func() {
					enteredMutation = true
				}

				_, err = current.cache.ReplaySourceCache(t.Context(), previous.store, kind, "scope-a")
				require.ErrorContains(t, err, policy.wantError)
				require.False(t, enteredMutation, "ineligible-source rejection entered destination mutation")
				require.Equal(t, previousBefore, sourceCacheVerificationEngineDigest(t, previous.engine))
				require.Equal(t, currentBefore, sourceCacheVerificationEngineDigest(t, current.engine))
			})
		}
	}
}

func TestVerificationReplayRejectsReopenedUnfinishedSourceAllKinds(t *testing.T) {
	for _, kind := range []sourcecache.RowKind{
		sourcecache.RowKindResources,
		sourcecache.RowKindEntitlements,
		sourcecache.RowKindGrants,
	} {
		t.Run(string(kind), func(t *testing.T) {
			previous := newSourceCacheVerificationStore(t)
			previousPath := previous.store.(*pebbleStore).outputFilePath
			putSourceCacheVerificationRows(t, previous, kind, "scope-a", 2, "source")
			require.NoError(t, previous.cache.PutSourceCacheEntry(t.Context(), kind, "scope-a", "validator"))
			require.NoError(t, previous.store.Close(t.Context()), "close without EndSync")

			reopened, err := NewStore(t.Context(), previousPath, WithReadOnly(true))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, reopened.Close(t.Context())) })
			reopenedEngine, ok := enginepebble.AsEngine(reopened)
			require.True(t, ok)

			current := newSourceCacheVerificationStore(t)
			putSourceCacheVerificationRows(t, current, kind, "scope-a", 1, "destination")
			previousBefore := sourceCacheVerificationEngineDigest(t, reopenedEngine)
			currentBefore := sourceCacheVerificationEngineDigest(t, current.engine)
			enteredMutation := false
			current.store.(*pebbleStore).sourceCacheTest.beforeEngineMutation = func() {
				enteredMutation = true
			}

			_, err = current.cache.ReplaySourceCache(t.Context(), reopened, kind, "scope-a")
			require.ErrorContains(t, err, "not finished")
			require.False(t, enteredMutation, "reopened unfinished-source rejection entered destination mutation")
			require.Equal(t, previousBefore, sourceCacheVerificationEngineDigest(t, reopenedEngine))
			require.Equal(t, currentBefore, sourceCacheVerificationEngineDigest(t, current.engine))
		})
	}
}

// C35: a corrupt envelope is rejected while opening the previous artifact, so
// it can never degrade into an empty or partially replayable source.
func TestVerificationCorruptSourceEnvelopeFailsClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "corrupt.c1z")
	require.NoError(t, os.WriteFile(path, []byte("not a c1z envelope"), 0o600))
	previous, err := NewStore(t.Context(), path, WithReadOnly(true))
	require.Error(t, err)
	require.Nil(t, previous)
}

// C05: manifests are exact (kind, scope) cells. Overwriting one cell must not
// disturb neighboring scopes or kinds, and zero-row scopes remain durable.
func TestVerificationManifestPartitionAndOverwriteMatrix(t *testing.T) {
	s := newSourceCacheVerificationStore(t)
	kinds := []sourcecache.RowKind{
		sourcecache.RowKindResources,
		sourcecache.RowKindEntitlements,
		sourcecache.RowKindGrants,
	}
	for _, kind := range kinds {
		require.NoError(t, s.cache.PutSourceCacheEntry(t.Context(), kind, "scope-a", "validator-a1"))
		require.NoError(t, s.cache.PutSourceCacheEntry(t.Context(), kind, "scope-b", "validator-b"))
	}
	for _, kind := range kinds {
		require.NoError(t, s.cache.PutSourceCacheEntry(t.Context(), kind, "scope-a", "validator-a2"))
		for _, cell := range []struct {
			scope, validator string
		}{
			{"scope-a", "validator-a2"},
			{"scope-b", "validator-b"},
		} {
			entry, found, err := s.cache.LookupSourceCacheEntry(t.Context(), kind, cell.scope)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, cell.validator, entry.CacheValidator)
		}
	}
}

// C28: tuple-hostile and byte-distinct scopes remain independently
// addressable through stamping, manifest lookup, and replay.
func TestVerificationHostileScopeEncodingCorpus(t *testing.T) {
	scopes := []string{
		"a\x00b",
		"é",
		"e\u0301",
		strings.Repeat("x", 256),
	}
	prev := newSourceCacheVerificationStore(t)
	grants := make([]*v2.Grant, len(scopes))
	for i, scope := range scopes {
		grants[i] = putVerificationGrant(t, prev, scope, fmt.Sprintf("member-%d", i), fmt.Sprintf("user-%d", i))
		require.NoError(t, prev.cache.PutSourceCacheEntry(t.Context(), sourcecache.RowKindGrants, scope, fmt.Sprintf("validator-%d", i)))
	}
	sealSourceCacheVerificationStore(t, prev)
	for i, scope := range scopes {
		cur := newSourceCacheVerificationStore(t)
		res, err := cur.cache.ReplaySourceCache(t.Context(), prev.store, sourcecache.RowKindGrants, scope)
		require.NoError(t, err)
		require.Equal(t, int64(1), res.Rows)
		_, err = cur.engine.GetGrantRecord(t.Context(), grants[i].GetId())
		require.NoError(t, err)
		for j, decoy := range grants {
			if i == j {
				continue
			}
			_, err = cur.engine.GetGrantRecord(t.Context(), decoy.GetId())
			require.ErrorIs(t, err, cockroachpebble.ErrNotFound)
		}
	}
}

// C28: valid opaque IDs retain byte identity through all-kind materialization,
// replay, canonical tombstones, and neighboring-row survival.
func TestVerificationHostileIDEncodingCorpus(t *testing.T) {
	hostileIDs := []string{
		"a\x00b",
		"é",
		"e\u0301",
		strings.Repeat("x", 512),
	}
	for _, kind := range []sourcecache.RowKind{
		sourcecache.RowKindResources,
		sourcecache.RowKindEntitlements,
		sourcecache.RowKindGrants,
	} {
		t.Run(string(kind), func(t *testing.T) {
			prev := newSourceCacheVerificationStore(t)
			ids := append([]string{""}, hostileIDs...)
			canonicalIDs := make([]string, 0, len(ids))
			canonicalRowIDs := make([]string, 0, len(ids))
			switch kind {
			case sourcecache.RowKindResources:
				var rows []*v2.Resource
				for _, id := range ids {
					row := v2.Resource_builder{
						Id: v2.ResourceId_builder{ResourceType: "hostile", Resource: id}.Build(),
					}.Build()
					rows = append(rows, row)
					if id == "" {
						continue
					}
					canonicalID, err := bid.MakeResourceBid(row)
					require.NoError(t, err)
					canonicalIDs = append(canonicalIDs, canonicalID)
					canonicalRowIDs = append(canonicalRowIDs, id)
				}
				require.NoError(t, prev.store.PutResources(sourcecache.WithScope(t.Context(), "scope-a"), rows...))
			case sourcecache.RowKindEntitlements:
				var rows []*v2.Entitlement
				for i, id := range ids {
					rows = append(rows, v2.Entitlement_builder{
						Id: id,
						Resource: v2.Resource_builder{
							Id: v2.ResourceId_builder{
								ResourceType: "group",
								Resource:     fmt.Sprintf("group-%d", i),
							}.Build(),
						}.Build(),
					}.Build())
					canonicalIDs = append(canonicalIDs, id)
					canonicalRowIDs = append(canonicalRowIDs, id)
				}
				require.NoError(t, prev.store.PutEntitlements(sourcecache.WithScope(t.Context(), "scope-a"), rows...))
			case sourcecache.RowKindGrants:
				var rows []*v2.Grant
				for i, id := range ids {
					row := mkV2Grant("", id, "user", fmt.Sprintf("principal-%d", i))
					rows = append(rows, row)
					canonicalIDs = append(canonicalIDs, row.GetId())
					canonicalRowIDs = append(canonicalRowIDs, row.GetId())
				}
				require.NoError(t, prev.store.PutGrants(sourcecache.WithScope(t.Context(), "scope-a"), rows...))
			}
			require.NoError(t, prev.cache.PutSourceCacheEntry(t.Context(), kind, "scope-a", "validator-a"))
			sealSourceCacheVerificationStore(t, prev)

			cur := newSourceCacheVerificationStore(t)
			res, err := cur.cache.ReplaySourceCache(t.Context(), prev.store, kind, "scope-a")
			require.NoError(t, err)
			require.Equal(t, int64(len(ids)), res.Rows)
			require.Equal(t, len(ids), countSourceCacheVerificationRowsInScope(t, cur.engine, kind, "scope-a"))

			for i, canonicalID := range canonicalIDs {
				require.NoError(t, cur.cache.DeleteSourceCacheRows(t.Context(), kind, []string{canonicalID}))
				require.Equal(t, len(ids)-i-1,
					countSourceCacheVerificationRowsInScope(t, cur.engine, kind, "scope-a"))
				for j := i + 1; j < len(canonicalIDs); j++ {
					switch kind {
					case sourcecache.RowKindResources:
						_, err = cur.engine.GetResourceRecord(t.Context(), "hostile", canonicalRowIDs[j])
					case sourcecache.RowKindEntitlements:
						_, err = cur.engine.GetEntitlementRecord(t.Context(), canonicalRowIDs[j])
					case sourcecache.RowKindGrants:
						_, err = cur.engine.GetGrantRecord(t.Context(), canonicalIDs[j])
					}
					require.NoError(t, err, "tombstone for hostile ID %q removed neighbor %q",
						canonicalRowIDs[i], canonicalRowIDs[j])
				}
			}
			if kind == sourcecache.RowKindResources {
				_, err = cur.engine.GetResourceRecord(t.Context(), "hostile", "")
				require.NoError(t, err, "resource with an empty opaque ID should survive unavailable BID tombstones")
			}
		})
	}
}

// C37: a whole-sync Pebble clone preserves the complete replay source and can
// be reopened read-only for a future hop.
func TestVerificationClonePreservesReplaySource(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.c1z")
	source, err := NewStore(ctx, sourcePath, WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	syncID, err := source.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	cache, ok := source.(SourceCacheStore)
	require.True(t, ok)
	resource := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "user", Resource: "alice"}.Build(),
	}.Build()
	entitlement := v2.Entitlement_builder{
		Id: "group:g1:member",
		Resource: v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(),
		}.Build(),
	}.Build()
	grant := mkV2Grant("", "member", "user", "alice")
	require.NoError(t, source.PutResources(sourcecache.WithScope(ctx, "scope-a"), resource))
	require.NoError(t, source.PutEntitlements(sourcecache.WithScope(ctx, "scope-a"), entitlement))
	require.NoError(t, source.PutGrants(sourcecache.WithScope(ctx, "scope-a"), grant))
	for _, kind := range []sourcecache.RowKind{
		sourcecache.RowKindResources,
		sourcecache.RowKindEntitlements,
		sourcecache.RowKindGrants,
	} {
		require.NoError(t, cache.PutSourceCacheEntry(ctx, kind, "scope-a", "validator-a"))
	}
	require.NoError(t, source.EndSync(ctx))
	require.NoError(t, source.Close(ctx))

	source, err = NewStore(ctx, sourcePath, WithReadOnly(true))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, source.Close(ctx)) })
	sourceEngine, ok := enginepebble.AsEngine(source)
	require.True(t, ok)
	sourceView := sourceCacheVerificationStore{
		store:  source,
		cache:  source.(SourceCacheStore),
		engine: sourceEngine,
	}
	sourceBytes, err := os.ReadFile(sourcePath)
	require.NoError(t, err)
	sourceDigest := sha256.Sum256(sourceBytes)
	for _, operation := range []struct {
		name string
		run  func(string) error
	}{
		{name: "clone", run: func(path string) error {
			return source.FileOps().CloneSync(ctx, path, syncID)
		}},
		{name: "copy-isolate", run: func(path string) error {
			return source.FileOps().CopyIsolateSync(ctx, path, syncID)
		}},
	} {
		t.Run(operation.name, func(t *testing.T) {
			clonePath := filepath.Join(dir, operation.name+".c1z")
			require.NoError(t, operation.run(clonePath))
			sourceBytes, err = os.ReadFile(sourcePath)
			require.NoError(t, err)
			require.Equal(t, sourceDigest, sha256.Sum256(sourceBytes), operation.name+" mutated its source artifact")

			clone, err := NewStore(ctx, clonePath, WithReadOnly(true))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, clone.Close(ctx)) })
			cloneCache, ok := clone.(SourceCacheStore)
			require.True(t, ok)
			cloneEngine, ok := enginepebble.AsEngine(clone)
			require.True(t, ok)
			cloneView := sourceCacheVerificationStore{store: clone, cache: cloneCache, engine: cloneEngine}
			require.Equal(t, sourceCacheVerificationEngineDigest(t, sourceEngine),
				sourceCacheVerificationEngineDigest(t, cloneEngine))
			cur := newSourceCacheVerificationStore(t)
			for _, kind := range []sourcecache.RowKind{
				sourcecache.RowKindResources,
				sourcecache.RowKindEntitlements,
				sourcecache.RowKindGrants,
			} {
				entry, found, err := cloneCache.LookupSourceCacheEntry(ctx, kind, "scope-a")
				require.NoError(t, err)
				require.True(t, found)
				require.Equal(t, "validator-a", entry.CacheValidator)
				require.Equal(t,
					sourceCacheVerificationSemanticSnapshot(t, sourceView, kind, "scope-a"),
					sourceCacheVerificationSemanticSnapshot(t, cloneView, kind, "scope-a"),
				)
				res, err := cur.cache.ReplaySourceCache(ctx, clone, kind, "scope-a")
				require.NoError(t, err)
				require.Equal(t, int64(1), res.Rows)
				require.Equal(t, 1, countSourceCacheVerificationRowsInScope(t, cur.engine, kind, "scope-a"))
			}
		})
	}
}

// C15/C17: canonical tombstones delete the selected row and remain harmless
// when duplicated, repeated, or aimed at an absent canonical identity.
func TestVerificationCanonicalTombstoneIdempotencyMatrix(t *testing.T) {
	t.Run("resources", func(t *testing.T) {
		s := newSourceCacheVerificationStore(t)
		target := v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "user", Resource: "target"}.Build(),
		}.Build()
		survivor := v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "user", Resource: "survivor"}.Build(),
		}.Build()
		absent := v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "user", Resource: "absent"}.Build(),
		}.Build()
		require.NoError(t, s.store.PutResources(sourcecache.WithScope(t.Context(), "scope-a"), target, survivor))
		targetID, err := bid.MakeResourceBid(target)
		require.NoError(t, err)
		absentID, err := bid.MakeResourceBid(absent)
		require.NoError(t, err)
		ids := []string{targetID, targetID, absentID}
		require.NoError(t, s.cache.DeleteSourceCacheRows(t.Context(), sourcecache.RowKindResources, ids))
		require.NoError(t, s.cache.DeleteSourceCacheRows(t.Context(), sourcecache.RowKindResources, ids))
		_, err = s.engine.GetResourceRecord(t.Context(), "user", "target")
		require.ErrorIs(t, err, cockroachpebble.ErrNotFound)
		_, err = s.engine.GetResourceRecord(t.Context(), "user", "survivor")
		require.NoError(t, err)
	})

	t.Run("entitlements", func(t *testing.T) {
		s := newSourceCacheVerificationStore(t)
		resource := v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(),
		}.Build()
		target := v2.Entitlement_builder{Id: "target", Resource: resource}.Build()
		survivor := v2.Entitlement_builder{Id: "survivor", Resource: resource}.Build()
		require.NoError(t, s.store.PutEntitlements(sourcecache.WithScope(t.Context(), "scope-a"), target, survivor))
		ids := []string{"target", "target", "absent"}
		require.NoError(t, s.cache.DeleteSourceCacheRows(t.Context(), sourcecache.RowKindEntitlements, ids))
		require.NoError(t, s.cache.DeleteSourceCacheRows(t.Context(), sourcecache.RowKindEntitlements, ids))
		_, err := s.engine.GetEntitlementRecord(t.Context(), "target")
		require.Error(t, err)
		_, err = s.engine.GetEntitlementRecord(t.Context(), "survivor")
		require.NoError(t, err)
	})

	t.Run("grants", func(t *testing.T) {
		s := newSourceCacheVerificationStore(t)
		target := putVerificationGrant(t, s, "scope-a", "target", "alice")
		survivor := putVerificationGrant(t, s, "scope-a", "survivor", "bob")
		absent := mkV2Grant("", "absent", "user", "ghost")
		ids := []string{target.GetId(), target.GetId(), absent.GetId()}
		require.NoError(t, s.cache.DeleteSourceCacheRows(t.Context(), sourcecache.RowKindGrants, ids))
		require.NoError(t, s.cache.DeleteSourceCacheRows(t.Context(), sourcecache.RowKindGrants, ids))
		_, err := s.engine.GetGrantRecord(t.Context(), target.GetId())
		require.ErrorIs(t, err, cockroachpebble.ErrNotFound)
		_, err = s.engine.GetGrantRecord(t.Context(), survivor.GetId())
		require.NoError(t, err)
	})
}

// C36 storage-composition evidence only: when the caller applies canonical
// then principal tombstones, overlapping selectors delete one identity once
// and principal-only matches still delete. Page ordering itself belongs to the
// deferred orchestration owner; this test does not claim C29 closure.
func TestVerificationGrantOverlayTombstoneOrderingModel(t *testing.T) {
	s := newSourceCacheVerificationStore(t)
	canonicalTarget := putVerificationGrant(t, s, "scope-a", "canonical", "bob")
	principalTarget := putVerificationGrant(t, s, "scope-a", "principal", "bob")
	survivor := putVerificationGrant(t, s, "scope-a", "survivor", "carol")
	scopeDecoy := putVerificationGrant(t, s, "scope-b", "scope-decoy", "bob")

	// Same-page model: overlay first, then canonical + principal tombstones.
	require.NoError(t, s.store.PutGrants(sourcecache.WithScope(t.Context(), "scope-a"), canonicalTarget))
	require.NoError(t, s.cache.DeleteSourceCacheRows(
		t.Context(),
		sourcecache.RowKindGrants,
		[]string{canonicalTarget.GetId()},
	))
	deleted, err := s.cache.DeleteSourceCacheRowsInScope(
		t.Context(),
		sourcecache.RowKindGrants,
		"scope-a",
		[]string{"bob"},
	)
	require.NoError(t, err)
	require.Equal(t, int64(1), deleted,
		"the overlapping canonical target was already absent; only the principal-only row remains to delete")
	_, err = s.engine.GetGrantRecord(t.Context(), canonicalTarget.GetId())
	require.ErrorIs(t, err, cockroachpebble.ErrNotFound)
	_, err = s.engine.GetGrantRecord(t.Context(), principalTarget.GetId())
	require.ErrorIs(t, err, cockroachpebble.ErrNotFound)
	_, err = s.engine.GetGrantRecord(t.Context(), survivor.GetId())
	require.NoError(t, err)
	_, err = s.engine.GetGrantRecord(t.Context(), scopeDecoy.GetId())
	require.NoError(t, err)

	// A later page may re-add a tombstoned identity.
	require.NoError(t, s.store.PutGrants(sourcecache.WithScope(t.Context(), "scope-a"), canonicalTarget))
	_, err = s.engine.GetGrantRecord(t.Context(), canonicalTarget.GetId())
	require.NoError(t, err)

	// A still-later tombstone wins again.
	require.NoError(t, s.cache.DeleteSourceCacheRows(
		t.Context(),
		sourcecache.RowKindGrants,
		[]string{canonicalTarget.GetId()},
	))
	_, err = s.engine.GetGrantRecord(t.Context(), canonicalTarget.GetId())
	require.ErrorIs(t, err, cockroachpebble.ErrNotFound)
	_, err = s.engine.GetGrantRecord(t.Context(), survivor.GetId())
	require.NoError(t, err)
	_, err = s.engine.GetGrantRecord(t.Context(), scopeDecoy.GetId())
	require.NoError(t, err)
}

// C36 resource-selector symmetry: a canonical resource tombstone and scoped
// resource-ID selector may overlap without double counting or touching decoys.
func TestVerificationResourceCombinedTombstoneComposition(t *testing.T) {
	s := newSourceCacheVerificationStore(t)
	build := func(id string) *v2.Resource {
		return v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "user", Resource: id}.Build(),
		}.Build()
	}
	overlap := build("alice")
	selectorOnly := build("bob")
	survivor := build("carol")
	scopeDecoy := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "service", Resource: "bob"}.Build(),
	}.Build()
	require.NoError(t, s.store.PutResources(
		sourcecache.WithScope(t.Context(), "scope-a"),
		overlap,
		selectorOnly,
		survivor,
	))
	require.NoError(t, s.store.PutResources(sourcecache.WithScope(t.Context(), "scope-b"), scopeDecoy))
	overlapBID, err := bid.MakeResourceBid(overlap)
	require.NoError(t, err)
	require.NoError(t, s.cache.DeleteSourceCacheRows(
		t.Context(),
		sourcecache.RowKindResources,
		[]string{overlapBID},
	))
	deleted, err := s.cache.DeleteSourceCacheRowsInScope(
		t.Context(),
		sourcecache.RowKindResources,
		"scope-a",
		[]string{"alice", "bob"},
	)
	require.NoError(t, err)
	require.Equal(t, int64(1), deleted,
		"the overlapping canonical target was already absent; only the selector-only row remains")
	_, err = s.engine.GetResourceRecord(t.Context(), "user", "alice")
	require.ErrorIs(t, err, cockroachpebble.ErrNotFound)
	_, err = s.engine.GetResourceRecord(t.Context(), "user", "bob")
	require.ErrorIs(t, err, cockroachpebble.ErrNotFound)
	_, err = s.engine.GetResourceRecord(t.Context(), "user", "carol")
	require.NoError(t, err)
	_, err = s.engine.GetResourceRecord(t.Context(), "service", "bob")
	require.NoError(t, err)
	require.Equal(t, 2, countSourceCacheVerificationKeys(
		t,
		s.engine,
		enginepebble.ResourceBySourceScopeLowerBound(),
		enginepebble.ResourceBySourceScopeUpperBound(),
	))
}

func TestVerificationOrphanScopeIndexHealingPersistsAfterReopen(t *testing.T) {
	type testCase struct {
		name      string
		kind      sourcecache.RowKind
		primaryLo []byte
		indexLo   []byte
		indexHi   []byte
		put       func(t *testing.T, s sourceCacheVerificationStore) []string
		heal      func(ctx context.Context, cache SourceCacheStore, ids []string) (int64, error)
	}
	cases := []testCase{
		{
			name:      "resources-by-id",
			kind:      sourcecache.RowKindResources,
			primaryLo: enginepebble.ResourceLowerBound(),
			indexLo:   enginepebble.ResourceBySourceScopeLowerBound(),
			indexHi:   enginepebble.ResourceBySourceScopeUpperBound(),
			put: func(t *testing.T, s sourceCacheVerificationStore) []string {
				putSourceCacheVerificationRows(t, s, sourcecache.RowKindResources, "scope-a", 1, "orphan-resource")
				return []string{"orphan-resource-0"}
			},
			heal: func(ctx context.Context, cache SourceCacheStore, ids []string) (int64, error) {
				return cache.DeleteSourceCacheRowsInScope(ctx, sourcecache.RowKindResources, "scope-a", ids)
			},
		},
		{
			name:      "grants-by-principal",
			kind:      sourcecache.RowKindGrants,
			primaryLo: enginepebble.GrantLowerBound(),
			indexLo:   enginepebble.GrantBySourceScopeLowerBound(),
			indexHi:   enginepebble.GrantBySourceScopeUpperBound(),
			put: func(t *testing.T, s sourceCacheVerificationStore) []string {
				putVerificationGrant(t, s, "scope-a", "orphan-entitlement", "orphan-principal")
				return []string{"orphan-principal"}
			},
			heal: func(ctx context.Context, cache SourceCacheStore, ids []string) (int64, error) {
				return cache.DeleteSourceCacheRowsInScope(ctx, sourcecache.RowKindGrants, "scope-a", ids)
			},
		},
		{
			name:      "grants-by-external-id",
			kind:      sourcecache.RowKindGrants,
			primaryLo: enginepebble.GrantLowerBound(),
			indexLo:   enginepebble.GrantBySourceScopeLowerBound(),
			indexHi:   enginepebble.GrantBySourceScopeUpperBound(),
			put: func(t *testing.T, s sourceCacheVerificationStore) []string {
				grant := putVerificationGrant(t, s, "scope-a", "orphan-entitlement", "orphan-principal")
				return []string{grant.GetId()}
			},
			heal: func(ctx context.Context, cache SourceCacheStore, ids []string) (int64, error) {
				return cache.DeleteSourceCacheGrantsByIDInScope(ctx, "scope-a", ids)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			path := filepath.Join(t.TempDir(), "orphan-healing.c1z")
			store, err := NewStore(ctx, path, WithEngine(c1zstore.EnginePebble))
			require.NoError(t, err)
			_, err = store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
			require.NoError(t, err)
			ps := store.(*pebbleStore)
			engine, ok := enginepebble.AsEngine(store)
			require.True(t, ok)
			ids := tc.put(t, sourceCacheVerificationStore{
				store:  store,
				cache:  ps,
				engine: engine,
			})
			require.NoError(t, store.Close(ctx))

			resume := func() (*pebbleStore, *enginepebble.Engine) {
				t.Helper()
				resumed, err := NewStore(ctx, path, WithEngine(c1zstore.EnginePebble))
				require.NoError(t, err)
				_, startedNew, err := resumed.StartOrResumeSync(ctx, connectorstore.SyncTypeFull, "")
				require.NoError(t, err)
				require.False(t, startedNew)
				resumedEngine, ok := enginepebble.AsEngine(resumed)
				require.True(t, ok)
				return resumed.(*pebbleStore), resumedEngine
			}

			// Persist a corruption shape production typed writers cannot create:
			// the source-scope index remains but its primary row is absent.
			ps, engine = resume()
			iter, err := engine.NewIter(&cockroachpebble.IterOptions{LowerBound: tc.primaryLo})
			require.NoError(t, err)
			require.True(t, iter.First())
			primaryKey := append([]byte(nil), iter.Key()...)
			require.True(t, bytes.HasPrefix(primaryKey, tc.primaryLo))
			require.NoError(t, iter.Close())
			require.NoError(t, engine.UnsafeForTesting().Delete(primaryKey, cockroachpebble.Sync))
			ps.MarkDirty()
			require.NoError(t, ps.Close(ctx))

			// The public scoped delete heals the orphan while reporting zero
			// primary-row deletions. That mutation must still survive Close.
			ps, engine = resume()
			require.Equal(t, 1, countSourceCacheVerificationKeys(t, engine, tc.indexLo, tc.indexHi))
			deleted, err := tc.heal(ctx, ps, ids)
			require.NoError(t, err)
			require.Zero(t, deleted)
			require.Zero(t, countSourceCacheVerificationKeys(t, engine, tc.indexLo, tc.indexHi),
				"healing premise: orphan index was removed from live state")
			require.NoError(t, ps.Close(ctx))

			ps, engine = resume()
			require.Zero(t, countSourceCacheVerificationKeys(t, engine, tc.indexLo, tc.indexHi),
				"orphan-only healing must survive public Close/reopen")
			require.NoError(t, ps.Close(ctx))
		})
	}
}

func TestVerificationSourceCacheMutationHandoffToConcurrentClose(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "mutation-close-handoff.c1z")
	store, err := NewStore(ctx, path, WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	_, err = store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	ps := store.(*pebbleStore)
	engine, ok := enginepebble.AsEngine(store)
	require.True(t, ok)
	putSourceCacheVerificationRows(t, sourceCacheVerificationStore{
		store:  store,
		cache:  ps,
		engine: engine,
	}, sourcecache.RowKindResources, "scope-a", 1, "handoff")
	require.NoError(t, store.Close(ctx))

	store, err = NewStore(ctx, path, WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	_, startedNew, err := store.StartOrResumeSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.False(t, startedNew)
	ps = store.(*pebbleStore)
	mutationEntered := make(chan struct{})
	releaseMutation := make(chan struct{})
	closeAttempted := make(chan struct{})
	ps.sourceCacheTest.beforeEngineMutation = func() {
		close(mutationEntered)
		<-releaseMutation
	}
	ps.sourceCacheTest.beforeCloseLock = func() {
		close(closeAttempted)
	}

	deleteDone := make(chan error, 1)
	go func() {
		_, deleteErr := ps.DeleteSourceCacheRowsInScope(
			context.Background(),
			sourcecache.RowKindResources,
			"scope-a",
			[]string{"handoff-0"},
		)
		deleteDone <- deleteErr
	}()
	<-mutationEntered
	if ps.closeMu.TryLock() {
		ps.closeMu.Unlock()
		close(releaseMutation)
		t.Fatal("source-cache mutation did not own closeMu before entering the engine")
	}

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- ps.Close(context.Background())
	}()
	<-closeAttempted
	close(releaseMutation)
	require.NoError(t, <-deleteDone)
	require.NoError(t, <-closeDone)

	reopened, err := NewStore(ctx, path, WithReadOnly(true))
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Close(ctx)) }()
	reopenedEngine, ok := enginepebble.AsEngine(reopened)
	require.True(t, ok)
	require.Zero(t, countSourceCacheVerificationRowsInScope(
		t,
		reopenedEngine,
		sourcecache.RowKindResources,
		"scope-a",
	), "Close checkpoint omitted a successful source-cache mutation")
}

// C28 measured supplement: arbitrary valid, byte-distinct scope strings must
// remain isolated through manifest lookup and replay.
func FuzzVerificationScopeEncodingIsolation(f *testing.F) {
	f.Add("foo", "foobar")
	f.Add("a\x00b", "a\x00bc")
	f.Add("tenant/α", "tenant/β")
	f.Fuzz(func(t *testing.T, targetScope, neighborScope string) {
		if targetScope == neighborScope ||
			!utf8.ValidString(targetScope) ||
			!utf8.ValidString(neighborScope) ||
			len(targetScope) > 128 ||
			len(neighborScope) > 128 ||
			sourcecache.ValidateScopeKey(targetScope) != nil ||
			sourcecache.ValidateScopeKey(neighborScope) != nil {
			t.Skip()
		}

		prev := newSourceCacheVerificationStore(t)
		putSourceCacheVerificationRows(t, prev, sourcecache.RowKindGrants, targetScope, 1, "target")
		putSourceCacheVerificationRows(t, prev, sourcecache.RowKindGrants, neighborScope, 1, "neighbor")
		require.NoError(t, prev.cache.PutSourceCacheEntry(
			t.Context(),
			sourcecache.RowKindGrants,
			targetScope,
			"validator-target",
		))
		require.NoError(t, prev.cache.PutSourceCacheEntry(
			t.Context(),
			sourcecache.RowKindGrants,
			neighborScope,
			"validator-neighbor",
		))
		sealSourceCacheVerificationStore(t, prev)

		cur := newSourceCacheVerificationStore(t)
		res, err := cur.cache.ReplaySourceCache(
			t.Context(),
			prev.store,
			sourcecache.RowKindGrants,
			targetScope,
		)
		require.NoError(t, err)
		require.Equal(t, int64(1), res.Rows)
		require.Equal(t, 1, countSourceCacheVerificationRowsInScope(
			t,
			cur.engine,
			sourcecache.RowKindGrants,
			targetScope,
		))
		require.Zero(t, countSourceCacheVerificationRowsInScope(
			t,
			cur.engine,
			sourcecache.RowKindGrants,
			neighborScope,
		))
	})
}

func TestVerificationPublicOracleMutationAdequacy(t *testing.T) {
	t.Run("source artifact mutation", func(t *testing.T) {
		before := sha256.Sum256([]byte("source"))
		after := sha256.Sum256([]byte("source-mutated"))
		require.Error(t, validateSourceCacheDigest(before, after))
	})
	t.Run("prefix neighbor deletion", func(t *testing.T) {
		require.Error(t, validateSourceCacheScopeCounts(1, 0, 1, 1))
	})
}

// Coverage-triage finding F1 (HIGH): the replay copy loop's
// same-identity overwrite arm — a destination row already exists at the
// replayed identity under a DIFFERENT scope, and the prior value must
// be threaded into the typed stage op so the old scope's
// by_source_scope index entry is cleaned. No prior instrument reached
// this branch in any kind; a regression leaves a stale foreign-scope
// index entry that a later scoped delete or replay-replacement would
// trust into a silent durable over-delete.
func TestVerificationReplayOverwriteCleansForeignScopeIndex(t *testing.T) {
	bySourceScopeBounds := map[sourcecache.RowKind][2][]byte{
		sourcecache.RowKindResources:    {enginepebble.ResourceBySourceScopeLowerBound(), enginepebble.ResourceBySourceScopeUpperBound()},
		sourcecache.RowKindEntitlements: {enginepebble.EntitlementBySourceScopeLowerBound(), enginepebble.EntitlementBySourceScopeUpperBound()},
		sourcecache.RowKindGrants:       {enginepebble.GrantBySourceScopeLowerBound(), enginepebble.GrantBySourceScopeUpperBound()},
	}
	for _, kind := range []sourcecache.RowKind{
		sourcecache.RowKindResources,
		sourcecache.RowKindEntitlements,
		sourcecache.RowKindGrants,
	} {
		t.Run(string(kind), func(t *testing.T) {
			prev := newSourceCacheVerificationStore(t)
			putSourceCacheVerificationRows(t, prev, kind, "scope-a", 1, "clash")
			require.NoError(t, prev.cache.PutSourceCacheEntry(t.Context(), kind, "scope-a", "validator-a"))
			sealSourceCacheVerificationStore(t, prev)

			// The same identity (same prefix) already lives in the
			// destination under scope-b.
			cur := newSourceCacheVerificationStore(t)
			putSourceCacheVerificationRows(t, cur, kind, "scope-b", 1, "clash")

			res, err := cur.cache.ReplaySourceCache(t.Context(), prev.store, kind, "scope-a")
			require.NoError(t, err)
			require.Equal(t, int64(1), res.Rows)

			require.Equal(t, 1, countSourceCacheVerificationRowsInScope(t, cur.engine, kind, "scope-a"),
				"the replayed row must be stamped scope-a")
			require.Zero(t, countSourceCacheVerificationRowsInScope(t, cur.engine, kind, "scope-b"),
				"no row may keep the old scope-b stamp")

			indexKeys := sourceCacheVerificationIndexKeys(t, cur.engine, bySourceScopeBounds[kind])
			sawScopeA := false
			for _, key := range indexKeys {
				require.NotContains(t, key, "scope-b",
					"the overwrite must clean the prior row's scope-b index entry")
				if strings.Contains(key, "scope-a") {
					sawScopeA = true
				}
			}
			require.True(t, sawScopeA, "non-vacuity: the replayed row must own a scope-a index entry")

			// Behavioral oracle: a scoped delete addressed at the stale
			// scope must be a no-op — if the foreign-scope index entry
			// survived, this is exactly the call that would over-delete.
			switch kind {
			case sourcecache.RowKindResources:
				deleted, err := cur.cache.DeleteSourceCacheRowsInScope(t.Context(), kind, "scope-b", []string{"clash-0"})
				require.NoError(t, err)
				require.Zero(t, deleted, "scope-b delete must not reach the row now owned by scope-a")
			case sourcecache.RowKindGrants:
				deleted, err := cur.cache.DeleteSourceCacheRowsInScope(t.Context(), kind, "scope-b", []string{"clash-principal-0"})
				require.NoError(t, err)
				require.Zero(t, deleted, "scope-b delete must not reach the row now owned by scope-a")
			case sourcecache.RowKindEntitlements:
				// No scoped delete for entitlements; the index assertions
				// above carry the obligation.
			}
			require.Equal(t, 1, countSourceCacheVerificationRowsInScope(t, cur.engine, kind, "scope-a"),
				"the replayed row must survive a delete addressed at its stale scope")
		})
	}
}

// Coverage-triage finding F2 (HIGH): every prior scoped external-id
// grant delete tombstoned the whole scope, so a regression that ignored
// the id set and swept the scope would pass the suite. This pins the
// survival half: rows whose stored id is NOT in the tombstone set must
// remain, with the returned count reporting only real deletions.
func TestVerificationScopedGrantIDDeletePreservesNonTombstonedRows(t *testing.T) {
	s := newSourceCacheVerificationStore(t)
	doomed := putVerificationGrant(t, s, "scope-a", "ent-doomed", "alice")
	survivor := putVerificationGrant(t, s, "scope-a", "ent-survivor", "bob")
	bystander := putVerificationGrant(t, s, "scope-a", "ent-bystander", "carol")

	deleted, err := s.cache.DeleteSourceCacheGrantsByIDInScope(t.Context(), "scope-a", []string{doomed.GetId()})
	require.NoError(t, err)
	require.Equal(t, int64(1), deleted)

	_, err = s.engine.GetGrantRecord(t.Context(), doomed.GetId())
	require.ErrorIs(t, err, cockroachpebble.ErrNotFound)
	_, err = s.engine.GetGrantRecord(t.Context(), survivor.GetId())
	require.NoError(t, err, "a non-tombstoned row must survive the scoped delete")
	_, err = s.engine.GetGrantRecord(t.Context(), bystander.GetId())
	require.NoError(t, err, "a non-tombstoned row must survive the scoped delete")
	require.Equal(t, 2, countSourceCacheVerificationRowsInScope(t, s.engine, sourcecache.RowKindGrants, "scope-a"))

	// Idempotence: repeating the tombstone finds nothing.
	deleted, err = s.cache.DeleteSourceCacheGrantsByIDInScope(t.Context(), "scope-a", []string{doomed.GetId()})
	require.NoError(t, err)
	require.Zero(t, deleted)
}

// Coverage-triage finding F3 (MEDIUM): a mutation arriving AFTER Close
// must be refused before it marks dirty and enters the engine — the
// closed-store half of the dirty/checkpoint contract. Without it, a
// caller could see a write succeed that the final checkpoint never
// captured.
func TestVerificationClosedStoreRejectsSourceCacheMutations(t *testing.T) {
	ctx := t.Context()
	prev := newSourceCacheVerificationStore(t)
	require.NoError(t, prev.cache.PutSourceCacheEntry(ctx, sourcecache.RowKindGrants, "scope-a", "validator-a"))
	sealSourceCacheVerificationStore(t, prev)

	store, err := NewStore(ctx, t.TempDir()+"/closed.c1z", WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	_, err = store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	cache, ok := store.(SourceCacheStore)
	require.True(t, ok)
	require.NoError(t, store.Close(ctx))

	err = cache.PutSourceCacheEntry(ctx, sourcecache.RowKindGrants, "scope-a", "validator-a")
	require.ErrorIs(t, err, enginepebble.ErrEngineClosing, "PutSourceCacheEntry after Close")

	_, err = cache.ReplaySourceCache(ctx, prev.store, sourcecache.RowKindGrants, "scope-a")
	require.ErrorIs(t, err, enginepebble.ErrEngineClosing, "ReplaySourceCache after Close")

	err = cache.DeleteSourceCacheRows(ctx, sourcecache.RowKindEntitlements, []string{"x"})
	require.ErrorIs(t, err, enginepebble.ErrEngineClosing, "DeleteSourceCacheRows after Close")

	_, err = cache.DeleteSourceCacheRowsInScope(ctx, sourcecache.RowKindResources, "scope-a", []string{"x"})
	require.ErrorIs(t, err, enginepebble.ErrEngineClosing, "DeleteSourceCacheRowsInScope after Close")

	_, err = cache.DeleteSourceCacheGrantsByIDInScope(ctx, "scope-a", []string{"x"})
	require.ErrorIs(t, err, enginepebble.ErrEngineClosing, "DeleteSourceCacheGrantsByIDInScope after Close")
}
