package expand

import (
	"context"
	"encoding/json"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/stretchr/testify/require"
)

// buildExpandedChain constructs an already-expanded linear chain
// e0 -> e1 -> ... -> eN in both the graph and the store: every edge is
// marked expanded, and every downstream entitlement already carries the
// root's member. This mirrors the post-sync state an incremental run starts
// from.
func buildExpandedChain(t *testing.T, ctx context.Context, store *MockExpanderStore, rootMember string, ents ...string) *EntitlementGraph {
	t.Helper()
	g := NewEntitlementGraph(ctx)
	for _, e := range ents {
		g.AddEntitlementID(e)
		store.AddEntitlement(makeEntitlement(e, makeResource("group", e)))
	}
	// Root's direct grant.
	store.AddGrant(directGrant(ents[0], makeResource("user", rootMember)))
	for i := 0; i+1 < len(ents); i++ {
		require.NoError(t, g.AddEdge(ctx, ents[i], ents[i+1], false, nil))
	}
	// Run the real expander so the base carries realistic expanded grants and
	// provenance (sources maps), not hand-seeded approximations.
	require.NoError(t, NewExpander(store, g).Run(ctx))
	return g
}

func directGrant(entitlementID string, principal *v2.Resource) *v2.Grant {
	ent := makeEntitlement(entitlementID, makeResource("group", entitlementID))
	return makeGrant(
		entitlementID+":"+principal.GetId().GetResourceType()+":"+principal.GetId().GetResource(),
		ent, principal,
	)
}

func expandedGrantWithSource(entitlementID string, principal *v2.Resource, sourceEntitlementID string) *v2.Grant {
	g := directGrant(entitlementID, principal)
	g.SetSources(v2.GrantSources_builder{
		Sources: map[string]*v2.GrantSources_GrantSource{sourceEntitlementID: {}},
	}.Build())
	return g
}

func principalsOn(t *testing.T, ctx context.Context, store *MockExpanderStore, entitlementID string) map[string]struct{} {
	t.Helper()
	resp, err := store.ListGrantsForEntitlement(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
		Entitlement: v2.Entitlement_builder{Id: entitlementID}.Build(),
	}.Build())
	require.NoError(t, err)
	out := make(map[string]struct{})
	for _, g := range resp.GetList() {
		out[g.GetPrincipal().GetId().GetResource()] = struct{}{}
	}
	return out
}

// TestIncremental_LeafAddition: a brand-new destination hung off an existing,
// already-expanded source. Only the new leaf is walked.
func TestIncremental_LeafAddition(t *testing.T) {
	ctx := context.Background()
	store := NewMockExpanderStore()
	g := buildExpandedChain(t, ctx, store, "alice", "eng:member")
	store.AddEntitlement(makeEntitlement("github:access", makeResource("app", "github")))

	ie := NewIncrementalExpander(store, g)
	res, err := ie.ExpandChanges(ctx, []NewEdge{{SourceEntitlementID: "eng:member", DestEntitlementID: "github:access"}}, nil)
	require.NoError(t, err)

	require.Equal(t, []string{"github:access"}, res.EntitlementsWalked)
	require.Equal(t, 1, res.GrantsWritten)
	require.Contains(t, principalsOn(t, ctx, store, "github:access"), "alice")
}

// TestIncremental_UpstreamCascade: insert a new source ABOVE an existing
// expanded chain (the "director" case). Every downstream entitlement gains
// the new principal; only the affected chain is walked.
func TestIncremental_UpstreamCascade(t *testing.T) {
	ctx := context.Background()
	store := NewMockExpanderStore()
	g := buildExpandedChain(t, ctx, store, "alice",
		"eng:senior_manager", "eng:manager", "eng:member", "employee:general")

	// A brand-new director entitlement with its own direct member carol.
	store.AddEntitlement(makeEntitlement("eng:director", makeResource("group", "eng:director")))
	store.AddGrant(directGrant("eng:director", makeResource("user", "carol")))

	ie := NewIncrementalExpander(store, g)
	res, err := ie.ExpandChanges(ctx, []NewEdge{{SourceEntitlementID: "eng:director", DestEntitlementID: "eng:senior_manager"}}, nil)
	require.NoError(t, err)

	// carol cascades to every node from senior_manager down.
	for _, e := range []string{"eng:senior_manager", "eng:manager", "eng:member", "employee:general"} {
		require.Contains(t, principalsOn(t, ctx, store, e), "carol", "carol should reach %s", e)
		require.Contains(t, principalsOn(t, ctx, store, e), "alice", "alice should still be on %s", e)
	}
	// eng:director itself is NOT walked (it has no incoming edges / isn't a
	// destination); only the impacted chain of 4 is.
	require.ElementsMatch(t, []string{"eng:senior_manager", "eng:manager", "eng:member", "employee:general"}, res.EntitlementsWalked)
	require.Equal(t, 4, res.GrantsWritten)
}

// TestIncremental_BoundedWalk: adding a leaf to one node of a wide graph must
// touch only that leaf, not the many unrelated sibling nodes. This is the
// core optimization claim: work scales with the impacted subgraph, not the
// whole graph.
func TestIncremental_BoundedWalk(t *testing.T) {
	ctx := context.Background()
	store := NewMockExpanderStore()

	g := NewEntitlementGraph(ctx)
	// 50 unrelated, already-expanded source->dest pairs.
	for i := 0; i < 50; i++ {
		src := "src" + itoa(i)
		dst := "dst" + itoa(i)
		g.AddEntitlementID(src)
		g.AddEntitlementID(dst)
		store.AddEntitlement(makeEntitlement(src, makeResource("group", src)))
		store.AddEntitlement(makeEntitlement(dst, makeResource("group", dst)))
		store.AddGrant(directGrant(src, makeResource("user", "u"+itoa(i))))
		require.NoError(t, g.AddEdge(ctx, src, dst, false, nil))
		g.MarkEdgeExpanded(src, dst)
		store.AddGrant(expandedGrantWithSource(dst, makeResource("user", "u"+itoa(i)), src))
	}
	store.AddEntitlement(makeEntitlement("new:leaf", makeResource("app", "new")))

	ie := NewIncrementalExpander(store, g)
	res, err := ie.ExpandChanges(ctx, []NewEdge{{SourceEntitlementID: "src7", DestEntitlementID: "new:leaf"}}, nil)
	require.NoError(t, err)

	// Only new:leaf is walked — 1 of 101 entitlements — proving the walk is
	// bounded to the impacted subgraph.
	require.Equal(t, []string{"new:leaf"}, res.EntitlementsWalked)
	require.Contains(t, principalsOn(t, ctx, store, "new:leaf"), "u7")
}

// TestIncremental_InsertBetween: a new node spliced between two existing
// nodes (A -> newMid -> C added on top of an already-expanded A -> C). The
// mid node is populated from A, and C gains only what the mid contributes
// that it didn't already have via the pre-existing A -> C edge.
func TestIncremental_InsertBetween(t *testing.T) {
	ctx := context.Background()
	store := NewMockExpanderStore()
	// Existing expanded A -> C: alice flows A to C.
	g := buildExpandedChain(t, ctx, store, "alice", "a:member", "c:member")

	// New middle entitlement with its own direct member bob.
	store.AddEntitlement(makeEntitlement("mid:member", makeResource("group", "mid:member")))
	store.AddGrant(directGrant("mid:member", makeResource("user", "bob")))

	ie := NewIncrementalExpander(store, g)
	res, err := ie.ExpandChanges(ctx, []NewEdge{
		{SourceEntitlementID: "a:member", DestEntitlementID: "mid:member"},
		{SourceEntitlementID: "mid:member", DestEntitlementID: "c:member"},
	}, nil)
	require.NoError(t, err)

	// mid gains alice (from A). C gains bob (via mid); alice stays.
	require.Contains(t, principalsOn(t, ctx, store, "mid:member"), "alice")
	require.Contains(t, principalsOn(t, ctx, store, "mid:member"), "bob")
	require.Contains(t, principalsOn(t, ctx, store, "c:member"), "alice")
	require.Contains(t, principalsOn(t, ctx, store, "c:member"), "bob")
	require.ElementsMatch(t, []string{"mid:member", "c:member"}, res.EntitlementsWalked)
}

// TestIncremental_NewRootNoEdges: a new root entitlement that isn't part of
// any expandable relationship produces no edges, so there is nothing to
// expand — an empty edge list is a clean no-op.
func TestIncremental_NewRootNoEdges(t *testing.T) {
	ctx := context.Background()
	store := NewMockExpanderStore()
	g := buildExpandedChain(t, ctx, store, "alice", "eng:member")

	ie := NewIncrementalExpander(store, g)
	res, err := ie.ExpandChanges(ctx, nil, nil)
	require.NoError(t, err)
	require.Equal(t, 0, res.GrantsWritten)
	require.Empty(t, res.EntitlementsWalked)
}

// TestIncremental_NewMemberNoNewEdge is the blocker regression: a new member
// added to an EXISTING expandable entitlement produces NO new edge, only a
// changed membership. Seeding via changedEntitlementIDs must still propagate
// the new member downstream. Before the fix, this silently expanded nothing.
func TestIncremental_NewMemberNoNewEdge(t *testing.T) {
	ctx := context.Background()
	store := NewMockExpanderStore()
	// Existing, already-expanded chain: alice flows manager -> member.
	g := buildExpandedChain(t, ctx, store, "alice", "eng:manager", "eng:member")

	// A new member (bob) is added to the existing manager entitlement — no new
	// edge, just a new direct grant on an entitlement the graph already knows.
	store.AddGrant(directGrant("eng:manager", makeResource("user", "bob")))

	ie := NewIncrementalExpander(store, g)
	// No new edges; only the changed entitlement seeds the walk.
	res, err := ie.ExpandChanges(ctx, nil, []string{"eng:manager"})
	require.NoError(t, err)

	// bob must reach eng:member (downstream of the changed entitlement).
	require.Contains(t, principalsOn(t, ctx, store, "eng:member"), "bob",
		"new member on an existing group must propagate downstream")
	require.Contains(t, principalsOn(t, ctx, store, "eng:member"), "alice")
	require.Equal(t, 1, res.GrantsWritten) // only bob→member is new
}

// TestIncremental_ChangedEntitlementNotInGraph: a changed entitlement with no
// expandable edges (not a graph node) has nothing downstream — clean no-op,
// no panic.
func TestIncremental_ChangedEntitlementNotInGraph(t *testing.T) {
	ctx := context.Background()
	store := NewMockExpanderStore()
	g := buildExpandedChain(t, ctx, store, "alice", "eng:member")

	ie := NewIncrementalExpander(store, g)
	res, err := ie.ExpandChanges(ctx, nil, []string{"some:unrelated:entitlement"})
	require.NoError(t, err)
	require.Equal(t, 0, res.GrantsWritten)
	require.Empty(t, res.EntitlementsWalked)
}

// TestIncremental_ChunkedFlush exercises the multi-flush path: with the flush
// chunk lowered below the number of new grants, the destination's grants must
// be flushed in several batches yet still all land, with none dropped or
// duplicated.
func TestIncremental_ChunkedFlush(t *testing.T) {
	ctx := context.Background()
	store := NewMockExpanderStore()

	// group -> app, both entitlements exist; group has 25 members, none yet on app.
	g := NewEntitlementGraph(ctx)
	g.AddEntitlementID("group:member")
	g.AddEntitlementID("app:access")
	store.AddEntitlement(makeEntitlement("group:member", makeResource("group", "g")))
	store.AddEntitlement(makeEntitlement("app:access", makeResource("app", "a")))
	const n = 25
	for i := 0; i < n; i++ {
		store.AddGrant(directGrant("group:member", makeResource("user", "u"+itoa(i))))
	}

	// Force multiple flushes: chunk of 10 over 25 grants → 3 flushes.
	orig := incrementalFlushChunk
	incrementalFlushChunk = 10
	defer func() { incrementalFlushChunk = orig }()

	ie := NewIncrementalExpander(store, g)
	res, err := ie.ExpandChanges(ctx, []NewEdge{{SourceEntitlementID: "group:member", DestEntitlementID: "app:access"}}, nil)
	require.NoError(t, err)

	require.Equal(t, n, res.GrantsWritten, "all members written across flushes")
	got := principalsOn(t, ctx, store, "app:access")
	require.Len(t, got, n, "no member dropped or duplicated across flushes")
	for i := 0; i < n; i++ {
		require.Contains(t, got, "u"+itoa(i))
	}
}

// TestIncremental_ShallowEdgeDirectness (C1): a principal who is direct on the
// source but whose grant also carries a sources map must still count as direct
// on a shallow edge. The naive "no sources == direct" check would drop them;
// the correct predicate (sources empty OR the source entitlement is recorded)
// keeps them.
func TestIncremental_ShallowEdgeDirectness(t *testing.T) {
	ctx := context.Background()
	store := NewMockExpanderStore()

	g := NewEntitlementGraph(ctx)
	g.AddEntitlementID("group-b:member")
	g.AddEntitlementID("y:access")
	store.AddEntitlement(makeEntitlement("group-b:member", makeResource("group", "b")))
	store.AddEntitlement(makeEntitlement("y:access", makeResource("app", "y")))
	// alice is a direct member of B, but her grant records B in its sources map
	// (IsDirect) — so len(sources) != 0 even though she IS direct on B.
	store.AddGrant(expandedGrantWithSource("group-b:member", makeResource("user", "alice"), "group-b:member"))

	ie := NewIncrementalExpander(store, g)
	// New SHALLOW edge B -> Y: only direct members of B should propagate.
	res, err := ie.ExpandChanges(ctx, []NewEdge{
		{SourceEntitlementID: "group-b:member", DestEntitlementID: "y:access", Shallow: true},
	}, nil)
	require.NoError(t, err)

	// alice is direct on B, so she must reach Y over the shallow edge.
	require.Contains(t, principalsOn(t, ctx, store, "y:access"), "alice",
		"a direct member with a non-empty sources map must propagate over a shallow edge")
	require.Equal(t, 1, res.GrantsWritten)
}

// TestIncremental_PreservedGraphTokenSize (#12) measures the serialized size of
// a large nested-groups graph and confirms ClearTransientState shrinks it. It's
// a measurement (logged), plus a guard that stripping never grows the token.
func TestIncremental_PreservedGraphTokenSize(t *testing.T) {
	ctx := context.Background()
	g := NewEntitlementGraph(ctx)

	// A wide, shallow nested-groups graph: 5000 groups each feeding a common
	// downstream entitlement (fan-in), plus a chain spine (depth).
	const groups = 5000
	g.AddEntitlementID("downstream:access")
	for i := 0; i < groups; i++ {
		src := "group:" + itoa(i) + ":member"
		g.AddEntitlementID(src)
		require.NoError(t, g.AddEdge(ctx, src, "downstream:access", false, nil))
		g.MarkEdgeExpanded(src, "downstream:access")
	}
	// Simulate leftover transient state a real run would carry.
	g.Actions = []*EntitlementGraphAction{{SourceEntitlementID: "group:0:member"}}
	g.ExpansionMetrics = &EntitlementGraphMetrics{Algorithm: "topological_projection"}

	full, err := json.Marshal(g)
	require.NoError(t, err)

	g.ClearTransientState()
	stripped, err := json.Marshal(g)
	require.NoError(t, err)

	t.Logf("preserved-graph token: %d nodes, full=%d bytes, stripped=%d bytes",
		groups+1, len(full), len(stripped))
	require.LessOrEqual(t, len(stripped), len(full), "stripping transient state must not grow the token")
	require.Nil(t, g.Actions)
	require.Nil(t, g.ExpansionPlan)
	require.Nil(t, g.ExpansionMetrics)

	// The stripped graph still round-trips and is usable.
	loaded, err := g.Clone()
	require.NoError(t, err)
	require.NotNil(t, loaded.GetNode("downstream:access"))
	require.Len(t, loaded.Edges, groups)
}

// TestIncremental_CycleFallback: a new edge that closes a cycle must return
// ErrIncrementalFallback (and leave the edge in the graph for a full
// expansion) rather than silently produce wrong grants.
func TestIncremental_CycleFallback(t *testing.T) {
	ctx := context.Background()
	store := NewMockExpanderStore()
	g := buildExpandedChain(t, ctx, store, "alice", "a:member", "b:member", "c:member")

	ie := NewIncrementalExpander(store, g)
	// c -> a closes the cycle a -> b -> c -> a.
	_, err := ie.ExpandChanges(ctx, []NewEdge{{SourceEntitlementID: "c:member", DestEntitlementID: "a:member"}}, nil)
	require.ErrorIs(t, err, ErrIncrementalFallback)

	// The edge was still added, so a subsequent full expansion sees it.
	require.NotNil(t, g.GetNode("a:member"))
	require.True(t, g.HasCycles(ctx))
}

// TestIncremental_NoOp: re-adding an edge that grants nothing new writes
// nothing.
func TestIncremental_NoOp(t *testing.T) {
	ctx := context.Background()
	store := NewMockExpanderStore()
	g := buildExpandedChain(t, ctx, store, "alice", "eng:member", "github:access")

	ie := NewIncrementalExpander(store, g)
	// github:access already has alice (expanded from eng:member).
	res, err := ie.ExpandChanges(ctx, []NewEdge{{SourceEntitlementID: "eng:member", DestEntitlementID: "github:access"}}, nil)
	require.NoError(t, err)
	require.Equal(t, 0, res.GrantsWritten)
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}
