package pebble

// Deterministic crash test for the dangling-reference drops' manifest
// invalidation. The drops delete rows in chunked batches; a crash (or
// mid-loop error) between a committed chunk and the drop's completion
// must NEVER leave a scope with durably deleted rows and an intact
// manifest validator — the resumed invariant pass would find nothing
// dangling to re-trigger the invalidation, and every later sync would
// 304-replay the sanitized subset (the exact permanent-divergence bug
// manifest invalidation exists to prevent). The contract
// (stageSourceCacheScopeInvalidationLocked): the invalidation rides the
// SAME batch as the scope's first staged delete, so batch atomicity plus
// WAL prefix ordering make "committed delete ⇒ committed invalidation"
// hold through any crash point.

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/sourcecache"
)

// TestDanglingDropCrashBetweenChunksKeepsInvalidation crashes
// DeleteGrantsForEntitlement after its FIRST committed chunk (via the
// engine's test hook — the in-process analog of a process kill at that
// seam) and asserts the half-dropped scope's manifest entry is already
// invalidated. The re-run (the resumed sync's idempotent invariant pass)
// then converges: remaining rows drop, the entry stays invalidated.
func TestDanglingDropCrashBetweenChunksKeepsInvalidation(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	e := a.PebbleEngine()

	// More than one drop chunk of grants under ONE dangling entitlement
	// (scGrant's group:g1:member), all stamped with one scope that holds
	// a valid manifest entry.
	const total = dropBatchRows + 500
	scopedCtx := sourcecache.WithScope(ctx, scopeA)
	const putChunk = 1000
	for start := 0; start < total; start += putChunk {
		end := min(start+putChunk, total)
		page := make([]*v2.Grant, 0, end-start)
		for i := start; i < end; i++ {
			page = append(page, scGrant("member", fmt.Sprintf("u%05d", i), false))
		}
		require.NoError(t, a.PutGrants(scopedCtx, page...))
	}
	require.NoError(t, e.PutSourceCacheEntry(ctx, "grants", scopeA, "etag-v1"))

	scopePrefix := encodeGrantBySourceScopePrefix(scopeA)
	before, err := e.countSourceScopeIndexRange(ctx, scopePrefix)
	require.NoError(t, err)
	require.Equal(t, int64(total), before)

	// Crash after the first committed chunk.
	injected := errors.New("injected crash between drop chunks")
	e.testDropChunkHook = func() error { return injected }
	_, _, err = e.DeleteGrantsForEntitlement(ctx, "group:g1:member", "group", "g1")
	require.ErrorIs(t, err, injected)
	e.testDropChunkHook = nil

	// The crash landed between a committed chunk and completion: some
	// rows are durably gone, some remain.
	remaining, err := e.countSourceScopeIndexRange(ctx, scopePrefix)
	require.NoError(t, err)
	require.Equal(t, int64(total-dropBatchRows), remaining,
		"exactly the first chunk must be committed at the crash point")

	// THE CONTRACT: the scope's manifest entry must already be
	// invalidated — it rode the first chunk's batch. An intact validator
	// here is the permanent-sanitized-replay bug.
	rec, err := e.GetSourceCacheEntry(ctx, "grants", scopeA)
	require.NoError(t, err)
	require.True(t, rec.GetInvalidated(),
		"a committed drop chunk without a committed invalidation would 304-replay the sanitized scope forever")
	require.Equal(t, "etag-v1", rec.GetCacheValidator(),
		"the stale validator is kept (marked, not erased) for the audit surface")

	// Resume: the idempotent re-run converges — remaining rows drop and
	// the entry stays invalidated.
	deleted, skipped, err := e.DeleteGrantsForEntitlement(ctx, "group:g1:member", "group", "g1")
	require.NoError(t, err)
	require.Zero(t, skipped)
	require.Equal(t, int64(total-dropBatchRows), deleted)
	after, err := e.countSourceScopeIndexRange(ctx, scopePrefix)
	require.NoError(t, err)
	require.Zero(t, after)
	rec, err = e.GetSourceCacheEntry(ctx, "grants", scopeA)
	require.NoError(t, err)
	require.True(t, rec.GetInvalidated())
}

// TestManifestSnapshotInvalidationIsTyped pins the snapshot's typed
// invalidation verdict: validators are OPAQUE connector strings, so a
// legitimate validator that happens to end with any in-band sentinel
// must never be misclassified as invalidated (the audit once encoded
// invalidation as a "\x00invalidated" validator suffix — this is its
// regression test), and an invalidated entry must report its verdict in
// the typed field with the original validator intact.
func TestManifestSnapshotInvalidationIsTyped(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	e := a.PebbleEngine()

	// A hostile-but-legal validator: byte-identical to the old in-band
	// sentinel encoding.
	trapValidator := "W/\"etag\"\x00invalidated"
	require.NoError(t, e.PutSourceCacheEntry(ctx, "grants", scopeA, trapValidator))

	// A genuinely invalidated scope, via the drop path.
	require.NoError(t, a.PutGrants(sourcecache.WithScope(ctx, scopeB),
		scGrant("member", "alice", false)))
	require.NoError(t, e.PutSourceCacheEntry(ctx, "grants", scopeB, "etag-b"))
	_, _, err = e.DeleteGrantsForEntitlement(ctx, "group:g1:member", "group", "g1")
	require.NoError(t, err)

	snap, err := e.SourceCacheManifestSnapshot(ctx)
	require.NoError(t, err)

	trap := snap["grants\x00"+scopeA]
	require.False(t, trap.Invalidated,
		"a legitimate validator matching the old sentinel encoding must not be misclassified")
	require.Equal(t, trapValidator, trap.CacheValidator,
		"the opaque validator must round-trip byte-exact")

	dropped := snap["grants\x00"+scopeB]
	require.True(t, dropped.Invalidated, "the dropped-from scope must report its typed verdict")
	require.Equal(t, "etag-b", dropped.CacheValidator,
		"invalidation must not mutate the stored validator")
}

// TestDanglingDropCleanCompletionInvalidates pins the non-crash contract
// for all three drop arms: a completed drop leaves every touched scope's
// manifest entry invalidated (and only touched scopes — a skipped
// exempt-carrier's scope keeps its validator).
func TestDanglingDropCleanCompletionInvalidates(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	e := a.PebbleEngine()

	// Scope A: dangling grants (dropped). Scope B: untouched grants under
	// a different entitlement (validator must survive).
	require.NoError(t, a.PutGrants(sourcecache.WithScope(ctx, scopeA),
		scGrant("member", "alice", false), scGrant("member", "bob", false)))
	require.NoError(t, a.PutGrants(sourcecache.WithScope(ctx, scopeB),
		scGrant("owner", "carol", false)))
	require.NoError(t, e.PutSourceCacheEntry(ctx, "grants", scopeA, "etag-a"))
	require.NoError(t, e.PutSourceCacheEntry(ctx, "grants", scopeB, "etag-b"))

	deleted, skipped, err := e.DeleteGrantsForEntitlement(ctx, "group:g1:member", "group", "g1")
	require.NoError(t, err)
	require.Zero(t, skipped)
	require.Equal(t, int64(2), deleted)

	recA, err := e.GetSourceCacheEntry(ctx, "grants", scopeA)
	require.NoError(t, err)
	require.True(t, recA.GetInvalidated(), "the dropped-from scope must be invalidated")
	recB, err := e.GetSourceCacheEntry(ctx, "grants", scopeB)
	require.NoError(t, err)
	require.False(t, recB.GetInvalidated(), "an untouched scope's validator must survive")
}
