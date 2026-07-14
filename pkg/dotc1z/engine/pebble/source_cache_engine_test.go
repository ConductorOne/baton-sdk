package pebble

import (
	"context"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/sourcecache"
	batonGrant "github.com/conductorone/baton-sdk/pkg/types/grant"
)

var (
	scopeA = sourcecache.HashScope("https://example.test/teams/1/members?page=1")
	scopeB = sourcecache.HashScope("https://example.test/teams/2/members?page=1")
)

func scGrant(entName, principalID string, expandable bool) *v2.Grant {
	// Public ids are the raw ":"-join (an external contract, never parsed
	// for identity — see identity.go).
	canonicalEntID := "group:g1:" + entName
	ent := v2.Entitlement_builder{
		Id: canonicalEntID,
		Resource: v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(),
		}.Build(),
	}.Build()
	principal := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "user", Resource: principalID}.Build(),
	}.Build()
	var annos annotations.Annotations
	if expandable {
		annos.Update(v2.GrantExpandable_builder{EntitlementIds: []string{canonicalEntID}}.Build())
	}
	return v2.Grant_builder{
		Id:          batonGrant.NewGrantID(principal, ent),
		Entitlement: ent,
		Principal:   principal,
		Annotations: annos,
	}.Build()
}

func TestSourceCacheEntryCRUD(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	e := a.PebbleEngine()
	_, err = e.GetSourceCacheEntry(ctx, "grants", scopeA)
	require.ErrorIs(t, err, pebble.ErrNotFound)

	require.NoError(t, e.PutSourceCacheEntry(ctx, "grants", scopeA, `W/"etag-1"`))
	rec, err := e.GetSourceCacheEntry(ctx, "grants", scopeA)
	require.NoError(t, err)
	require.Equal(t, `W/"etag-1"`, rec.GetEtag())
	require.NotNil(t, rec.GetDiscoveredAt())

	// Row-kind partition: the same scope hash under a different kind misses.
	_, err = e.GetSourceCacheEntry(ctx, "resources", scopeA)
	require.ErrorIs(t, err, pebble.ErrNotFound)

	// Rotation overwrites in place (delta tokens rotate every round).
	require.NoError(t, e.PutSourceCacheEntry(ctx, "grants", scopeA, "delta-token-2"))
	rec, err = e.GetSourceCacheEntry(ctx, "grants", scopeA)
	require.NoError(t, err)
	require.Equal(t, "delta-token-2", rec.GetEtag())
}

// TestSourceCacheReplayGrantsAcrossEngines is the core replay contract:
// grants stamped with a scope in file A are copied — rows, indexes, and
// needs_expansion reporting — into file B, and unstamped/other-scope rows
// stay behind.
func TestSourceCacheReplayGrantsAcrossEngines(t *testing.T) {
	ctx := context.Background()

	prev := newAdapter(t)
	_, err := prev.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	gExpandable := scGrant("member", "alice", true)
	gPlain := scGrant("member", "bob", false)
	gOtherScope := scGrant("member", "carol", false)
	gUnstamped := scGrant("member", "dave", false)

	scopedCtx := sourcecache.WithScope(ctx, scopeA)
	require.NoError(t, prev.PutGrants(scopedCtx, gExpandable, gPlain))
	require.NoError(t, prev.PutGrants(sourcecache.WithScope(ctx, scopeB), gOtherScope))
	require.NoError(t, prev.PutGrants(ctx, gUnstamped))
	require.NoError(t, prev.PebbleEngine().PutSourceCacheEntry(ctx, "grants", scopeA, "etag-a"))

	cur := newAdapter(t)
	_, err = cur.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	res, err := cur.PebbleEngine().ReplaySourceCacheGrants(ctx, prev.PebbleEngine(), scopeA)
	require.NoError(t, err)
	require.Equal(t, int64(2), res.Rows)
	require.True(t, res.NeedsExpansion, "replay must report the expandable grant so the syncer can arm expansion")

	// Replayed rows are readable by ID and carry their expansion flag.
	got, err := cur.PebbleEngine().GetGrantRecord(ctx, gExpandable.GetId())
	require.NoError(t, err)
	require.True(t, got.GetNeedsExpansion())
	require.Equal(t, scopeA, got.GetSourceScopeHash())
	_, err = cur.PebbleEngine().GetGrantRecord(ctx, gPlain.GetId())
	require.NoError(t, err)

	// Rows outside the scope are not copied.
	_, err = cur.PebbleEngine().GetGrantRecord(ctx, gOtherScope.GetId())
	require.ErrorIs(t, err, pebble.ErrNotFound)
	_, err = cur.PebbleEngine().GetGrantRecord(ctx, gUnstamped.GetId())
	require.ErrorIs(t, err, pebble.ErrNotFound)

	// The by_source_scope index came across too: a second replay FROM the
	// current store (as next sync's previous) finds the same rows.
	next := newAdapter(t)
	_, err = next.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	res2, err := next.PebbleEngine().ReplaySourceCacheGrants(ctx, cur.PebbleEngine(), scopeA)
	require.NoError(t, err)
	require.Equal(t, int64(2), res2.Rows)
}

// TestSourceCacheReplayIsIdempotent pins double-execution safety: a
// resumed sync can re-run a replay page whose first execution already
// committed (the checkpoint races the crash), so replaying the same scope
// twice into the same store must converge to the same state — same rows,
// same count reported, no error.
func TestSourceCacheReplayIsIdempotent(t *testing.T) {
	ctx := context.Background()

	prev := newAdapter(t)
	_, err := prev.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	g1 := scGrant("member", "alice", true)
	g2 := scGrant("member", "bob", false)
	require.NoError(t, prev.PutGrants(sourcecache.WithScope(ctx, scopeA), g1, g2))

	cur := newAdapter(t)
	_, err = cur.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	res1, err := cur.PebbleEngine().ReplaySourceCacheGrants(ctx, prev.PebbleEngine(), scopeA)
	require.NoError(t, err)
	require.Equal(t, int64(2), res1.Rows)

	res2, err := cur.PebbleEngine().ReplaySourceCacheGrants(ctx, prev.PebbleEngine(), scopeA)
	require.NoError(t, err)
	require.Equal(t, int64(2), res2.Rows, "second replay converges, not errors")
	require.Equal(t, res1.NeedsExpansion, res2.NeedsExpansion)

	// State after double replay is exactly the two rows, once each.
	var seen int
	require.NoError(t, cur.PebbleEngine().IterateGrants(ctx, func(*v3.GrantRecord) bool {
		seen++
		return true
	}))
	require.Equal(t, 2, seen)
}

// TestSourceCacheReplayIgnoresStaleIndexEntries pins the stale-index
// defense: an index entry whose target row is stamped with a DIFFERENT
// scope (left behind by a path that replaced rows without cleaning the
// index — e.g. a fold compaction predating the source-cache bucket plans)
// must not be copied. Replaying it would inject rows upstream never
// returned for the queried scope.
func TestSourceCacheReplayIgnoresStaleIndexEntries(t *testing.T) {
	ctx := context.Background()

	prev := newAdapter(t)
	_, err := prev.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	// Row genuinely stamped with scopeB...
	g := scGrant("member", "alice", false)
	require.NoError(t, prev.PutGrants(sourcecache.WithScope(ctx, scopeB), g))

	// ...plus a stale index entry claiming it belongs to scopeA.
	rec, err := prev.PebbleEngine().GetGrantRecord(ctx, g.GetId())
	require.NoError(t, err)
	staleIdx := replayTestGrantScopeIndexKey(t, scopeA, rec)
	require.NoError(t, prev.PebbleEngine().DB().Set(staleIdx, nil, nil))

	cur := newAdapter(t)
	_, err = cur.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	res, err := cur.PebbleEngine().ReplaySourceCacheGrants(ctx, prev.PebbleEngine(), scopeA)
	require.NoError(t, err)
	require.Zero(t, res.Rows, "a row stamped with a different scope must not be replayed via a stale index entry")
	require.Equal(t, int64(1), res.StaleSkipped,
		"the skipped stale entry must be reported — it is the syncer's only way to tell 'legitimately empty scope' from 'scope contents clobbered without index cleanup'")
	_, err = cur.PebbleEngine().GetGrantRecord(ctx, g.GetId())
	require.ErrorIs(t, err, pebble.ErrNotFound)

	// The genuine scope still replays, with no staleness reported.
	res, err = cur.PebbleEngine().ReplaySourceCacheGrants(ctx, prev.PebbleEngine(), scopeB)
	require.NoError(t, err)
	require.Equal(t, int64(1), res.Rows)
	require.Zero(t, res.StaleSkipped)
}

// TestEntitlementRestampCleansOldScopeIndex pins the read-before-write
// cleanup on the entitlement put path: rewriting the SAME entitlement
// identity under a NEW scope must delete the old scope's by_source_scope
// entry. Without the cleanup, the old scope's index still claims the row
// while its value stamp names the new scope — the next sync's replay of
// the old scope then copies zero rows against a manifest entry that
// promised content (Rows == 0, StaleSkipped > 0), which the syncer treats
// as data loss and fails.
func TestEntitlementRestampCleansOldScopeIndex(t *testing.T) {
	ctx := context.Background()

	prev := newAdapter(t)
	_, err := prev.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	res1 := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(),
	}.Build()
	entID := "group:g1:member"
	ent1 := v2.Entitlement_builder{Id: entID, Resource: res1, DisplayName: "member"}.Build()

	// First put stamps scopeA (and consumes the fresh-empty fast path);
	// the rewrite stamps scopeB and must clean scopeA's index entry.
	require.NoError(t, prev.PutEntitlements(sourcecache.WithScope(ctx, scopeA), ent1))
	ent1b := v2.Entitlement_builder{Id: entID, Resource: res1, DisplayName: "member (renamed)"}.Build()
	require.NoError(t, prev.PutEntitlements(sourcecache.WithScope(ctx, scopeB), ent1b))

	cur := newAdapter(t)
	_, err = cur.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	// Old scope: legitimately empty now — no rows AND no stale entries.
	resA, err := cur.PebbleEngine().ReplaySourceCacheEntitlements(ctx, prev.PebbleEngine(), scopeA)
	require.NoError(t, err)
	require.Zero(t, resA.Rows)
	require.Zero(t, resA.StaleSkipped, "re-stamp must delete the old scope's index entry, not leave it stale")

	// New scope: exactly the rewritten row.
	resB, err := cur.PebbleEngine().ReplaySourceCacheEntitlements(ctx, prev.PebbleEngine(), scopeB)
	require.NoError(t, err)
	require.Equal(t, int64(1), resB.Rows)
	require.Zero(t, resB.StaleSkipped)
	got, err := cur.PebbleEngine().GetEntitlementRecord(ctx, entID)
	require.NoError(t, err)
	require.Equal(t, scopeB, got.GetSourceScopeHash())
	require.Equal(t, "member (renamed)", got.GetDisplayName())
}

// replayTestGrantScopeIndexKey builds a by_source_scope index key for the
// given scope pointing at rec's identity, bypassing the write path — the
// test plants it as a stale entry.
func replayTestGrantScopeIndexKey(t *testing.T, scopeHash string, rec *v3.GrantRecord) []byte {
	t.Helper()
	id, err := grantIdentityFromRecord(rec)
	require.NoError(t, err)
	return encodeGrantBySourceScopeIndexKey(scopeHash, id)
}

// TestDeleteGrantsByPrincipalsInScope pins the principal-scoped tombstone
// path: bare principal ids kill exactly their rows within the scope —
// other scopes' rows for the same principal survive, unknown principals
// no-op, and the deleted rows' index entries (including the scope index)
// go with them.
func TestDeleteGrantsByPrincipalsInScope(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	gAlice := scGrant("member", "alice", false)
	gBob := scGrant("member", "bob", true)
	require.NoError(t, a.PutGrants(sourcecache.WithScope(ctx, scopeA), gAlice, gBob))
	// Same principal, different scope — must survive an A-scoped delete.
	gAliceOther := scGrant("owner", "alice", false)
	require.NoError(t, a.PutGrants(sourcecache.WithScope(ctx, scopeB), gAliceOther))

	deleted, err := a.PebbleEngine().DeleteGrantsByPrincipalsInScope(ctx, scopeA, map[string]struct{}{
		"alice":   {},
		"bob":     {},
		"unknown": {}, // tombstone for a principal never synced — no-op
	})
	require.NoError(t, err)
	require.Equal(t, int64(2), deleted)

	_, err = a.PebbleEngine().GetGrantRecord(ctx, gAlice.GetId())
	require.ErrorIs(t, err, pebble.ErrNotFound)
	_, err = a.PebbleEngine().GetGrantRecord(ctx, gBob.GetId())
	require.ErrorIs(t, err, pebble.ErrNotFound)
	_, err = a.PebbleEngine().GetGrantRecord(ctx, gAliceOther.GetId())
	require.NoError(t, err, "same principal in a different scope must survive")

	// Scope index entries went with the rows: a replay of scopeA from this
	// store copies nothing.
	cur := newAdapter(t)
	_, err = cur.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	res, err := cur.PebbleEngine().ReplaySourceCacheGrants(ctx, a.PebbleEngine(), scopeA)
	require.NoError(t, err)
	require.Zero(t, res.Rows)
}

// TestDeleteResourcesByIDsInScope pins the resources analog: bare object
// ids, any resource type, scope-relative.
func TestDeleteResourcesByIDsInScope(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(), DisplayName: "U1"}.Build()
	u2 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u2"}.Build(), DisplayName: "U2"}.Build()
	require.NoError(t, a.PutResources(sourcecache.WithScope(ctx, scopeA), u1, u2))
	u3 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u3"}.Build(), DisplayName: "U3"}.Build()
	require.NoError(t, a.PutResources(sourcecache.WithScope(ctx, scopeB), u3))

	deleted, err := a.PebbleEngine().DeleteResourcesByIDsInScope(ctx, scopeA, map[string]struct{}{
		"u1": {}, "u3": {}, // u3 is in scope B — must not die from an A-scoped tombstone
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), deleted)

	_, err = a.PebbleEngine().GetResourceRecord(ctx, "user", "u1")
	require.ErrorIs(t, err, pebble.ErrNotFound)
	_, err = a.PebbleEngine().GetResourceRecord(ctx, "user", "u2")
	require.NoError(t, err)
	_, err = a.PebbleEngine().GetResourceRecord(ctx, "user", "u3")
	require.NoError(t, err, "tombstone must be scope-relative")
}

// TestDeleteGrantRecordBounded pins the no-scan contract: SDK-shaped ids
// resolve and delete; absent ids no-op without error (and without the
// O(all grants) fallback scan, by construction).
func TestDeleteGrantRecordBounded(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	g := scGrant("member", "alice", false)
	require.NoError(t, a.PutGrants(ctx, g))

	// Absent id (well-formed, no row): no-op, no error.
	require.NoError(t, a.PebbleEngine().DeleteGrantRecordBounded(ctx, "group:g1:custom:member:user:ghost"))
	_, err = a.PebbleEngine().GetGrantRecord(ctx, g.GetId())
	require.NoError(t, err)

	// Present id: deleted.
	require.NoError(t, a.PebbleEngine().DeleteGrantRecordBounded(ctx, g.GetId()))
	_, err = a.PebbleEngine().GetGrantRecord(ctx, g.GetId())
	require.ErrorIs(t, err, pebble.ErrNotFound)
}

func TestSourceCacheReplayResourcesAndEntitlements(t *testing.T) {
	ctx := context.Background()

	prev := newAdapter(t)
	_, err := prev.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	res1 := v2.Resource_builder{
		Id:          v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(),
		DisplayName: "Group One",
	}.Build()
	entID := "group:g1:member"
	ent1 := v2.Entitlement_builder{Id: entID, Resource: res1, DisplayName: "member"}.Build()

	scoped := sourcecache.WithScope(ctx, scopeA)
	require.NoError(t, prev.PutResources(scoped, res1))
	require.NoError(t, prev.PutEntitlements(scoped, ent1))

	cur := newAdapter(t)
	_, err = cur.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	rres, err := cur.PebbleEngine().ReplaySourceCacheResources(ctx, prev.PebbleEngine(), scopeA)
	require.NoError(t, err)
	require.Equal(t, int64(1), rres.Rows)
	eres, err := cur.PebbleEngine().ReplaySourceCacheEntitlements(ctx, prev.PebbleEngine(), scopeA)
	require.NoError(t, err)
	require.Equal(t, int64(1), eres.Rows)

	gotRes, err := cur.PebbleEngine().GetResourceRecord(ctx, "group", "g1")
	require.NoError(t, err)
	require.Equal(t, "Group One", gotRes.GetDisplayName())
	require.Equal(t, scopeA, gotRes.GetSourceScopeHash())

	gotEnt, err := cur.PebbleEngine().GetEntitlementRecord(ctx, entID)
	require.NoError(t, err)
	require.Equal(t, scopeA, gotEnt.GetSourceScopeHash())
}

// TestReplayInvalidatesEntitlementIDLookup pins the keyspace-generation
// contract for the replay writer: replay mutates the entitlement primary
// keyspace, so a bare-id lookup map built BEFORE the replay must be
// invalidated by it. If replay skips noteEntitlementKeyspaceWrite, the
// stale map hides replayed rows from resolveEntitlementIdentityByExternalID
// and a delta tombstone (DeleteEntitlementRecord) silently no-ops — a row
// upstream reported deleted survives the sync.
func TestReplayInvalidatesEntitlementIDLookup(t *testing.T) {
	ctx := context.Background()

	prev := newAdapter(t)
	_, err := prev.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	res1 := v2.Resource_builder{
		Id:          v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(),
		DisplayName: "Group One",
	}.Build()
	entID := "group:g1:member"
	ent1 := v2.Entitlement_builder{Id: entID, Resource: res1, DisplayName: "member"}.Build()
	require.NoError(t, prev.PutEntitlements(sourcecache.WithScope(ctx, scopeA), ent1))

	cur := newAdapter(t)
	_, err = cur.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	// Force the lazy bare-id map to build BEFORE the replay (empty
	// keyspace → entID resolves to nothing).
	_, err = cur.PebbleEngine().GetEntitlementRecord(ctx, entID)
	require.Error(t, err, "sanity: entitlement must not resolve before replay")

	rres, err := cur.PebbleEngine().ReplaySourceCacheEntitlements(ctx, prev.PebbleEngine(), scopeA)
	require.NoError(t, err)
	require.Equal(t, int64(1), rres.Rows)

	// The replayed row must be visible through the bare-id lookup (the
	// map built above is stale and must have been invalidated) ...
	gotEnt, err := cur.PebbleEngine().GetEntitlementRecord(ctx, entID)
	require.NoError(t, err, "replayed entitlement must resolve by external id after a pre-replay map build")
	require.Equal(t, scopeA, gotEnt.GetSourceScopeHash())

	// ... and, the actual production stake: a delta tombstone against the
	// replayed row must DELETE it, not silently no-op.
	require.NoError(t, cur.PebbleEngine().DeleteEntitlementRecord(ctx, entID))
	_, err = cur.PebbleEngine().GetEntitlementRecord(ctx, entID)
	require.Error(t, err, "tombstoned replayed entitlement must be gone")
}

// TestStoreExpandedGrantsPreservesSourceScope pins the write-path contract
// that keeps replay correct across expansion: when the expander rewrites an
// existing direct grant (to bake in Sources), the record's source scope
// stamp — and therefore its membership in the next sync's replay of that
// scope — must survive, exactly like expansion/needs_expansion do.
func TestStoreExpandedGrantsPreservesSourceScope(t *testing.T) {
	ctx := context.Background()

	a := newAdapter(t)
	_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	g := scGrant("member", "alice", true)
	require.NoError(t, a.PutGrants(sourcecache.WithScope(ctx, scopeA), g))

	// Expander-style rewrite: same grant, Sources baked in, no scope ctx.
	rewrite := scGrant("member", "alice", false)
	rewrite.SetSources(v2.GrantSources_builder{
		Sources: map[string]*v2.GrantSources_GrantSource{
			g.GetEntitlement().GetId(): {},
		},
	}.Build())
	require.NoError(t, a.Grants().StoreExpandedGrants(ctx, rewrite))

	got, err := a.PebbleEngine().GetGrantRecord(ctx, g.GetId())
	require.NoError(t, err)
	require.Equal(t, scopeA, got.GetSourceScopeHash(), "expander rewrite clobbered the source scope stamp")
	require.True(t, got.GetNeedsExpansion(), "expansion side-state must be preserved too")

	// And the index survives: a replay from this store still finds the row.
	cur := newAdapter(t)
	_, err = cur.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	res, err := cur.PebbleEngine().ReplaySourceCacheGrants(ctx, a.PebbleEngine(), scopeA)
	require.NoError(t, err)
	require.Equal(t, int64(1), res.Rows)
	replayed, err := cur.PebbleEngine().GetGrantRecord(ctx, g.GetId())
	require.NoError(t, err)
	// Replay-equivalence: expander-written Sources (self-source present)
	// are stripped so the current sync's expansion recomputes them from
	// true state — a full resync would produce Sources reflecting current
	// membership, so a cached sync must too. needs_expansion survived the
	// copy (asserted via the replay result above), so expansion re-adds
	// them.
	require.Empty(t, replayed.GetSources(), "expander-written Sources must be stripped on replay for full-resync equivalence")
	require.True(t, replayed.GetNeedsExpansion())
}

// TestSourceCacheReplayPreservesConnectorSetSources pins the other half of
// the sources-strip classification: a Sources map WITHOUT a self-source
// entry is connector-set public data (the connector emitted it on the
// ordinary path) and must survive replay byte-for-byte — never stripped.
func TestSourceCacheReplayPreservesConnectorSetSources(t *testing.T) {
	ctx := context.Background()

	prev := newAdapter(t)
	_, err := prev.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	g := scGrant("member", "alice", false)
	// Connector-set provenance: keyed by a FOREIGN entitlement, no
	// self-source. This is public v2.Grant data, not expander output.
	g.SetSources(v2.GrantSources_builder{
		Sources: map[string]*v2.GrantSources_GrantSource{
			"group:other:member": {},
		},
	}.Build())
	require.NoError(t, prev.PutGrants(sourcecache.WithScope(ctx, scopeA), g))

	cur := newAdapter(t)
	_, err = cur.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	res, err := cur.PebbleEngine().ReplaySourceCacheGrants(ctx, prev.PebbleEngine(), scopeA)
	require.NoError(t, err)
	require.Equal(t, int64(1), res.Rows)

	replayed, err := cur.PebbleEngine().GetGrantRecord(ctx, g.GetId())
	require.NoError(t, err)
	require.Len(t, replayed.GetSources(), 1, "connector-set Sources must survive replay")
	_, ok := replayed.GetSources()["group:other:member"]
	require.True(t, ok)
}
