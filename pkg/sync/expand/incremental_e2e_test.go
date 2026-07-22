package expand

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/stretchr/testify/require"
)

// roundTripGraph serializes and reloads a graph, mirroring what c1 does across
// an event: persist the graph in the sync token, then deserialize it later.
func roundTripGraph(t *testing.T, g *EntitlementGraph) *EntitlementGraph {
	t.Helper()
	data, err := json.Marshal(g)
	require.NoError(t, err)
	out := &EntitlementGraph{}
	require.NoError(t, json.Unmarshal(data, out))
	// json leaves absent maps nil; the token-load path re-inits them.
	if out.Nodes == nil {
		out.Nodes = map[int]Node{}
	}
	if out.EntitlementsToNodes == nil {
		out.EntitlementsToNodes = map[string]int{}
	}
	if out.SourcesToDestinations == nil {
		out.SourcesToDestinations = map[int]map[int]int{}
	}
	if out.DestinationsToSources == nil {
		out.DestinationsToSources = map[int]map[int]int{}
	}
	if out.Edges == nil {
		out.Edges = map[int]Edge{}
	}
	return out
}

// grantSet lists every grant across the given entitlements and returns the set
// of full-row keys "entitlement|principalType|principalResource|sources=..."
// — INCLUDING the sources/provenance map, so incremental is held to producing
// byte-identical provenance to a full expansion, not just the same access.
func grantSet(t *testing.T, ctx context.Context, store *MockExpanderStore, entIDs ...string) map[string]struct{} {
	t.Helper()
	set := make(map[string]struct{})
	for _, entID := range entIDs {
		resp, err := store.ListGrantsForEntitlement(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
			Entitlement: v2.Entitlement_builder{Id: entID}.Build(),
		}.Build())
		require.NoError(t, err)
		for _, g := range resp.GetList() {
			pid := g.GetPrincipal().GetId()
			set[entID+"|"+pid.GetResourceType()+"|"+pid.GetResource()+"|"+sourcesString(g)] = struct{}{}
		}
	}
	return set
}

// sourcesString renders a grant's sources map as a stable "id:direct,..." string.
func sourcesString(g *v2.Grant) string {
	srcs := g.GetSources().GetSources()
	parts := make([]string, 0, len(srcs))
	for id, s := range srcs {
		parts = append(parts, fmt.Sprintf("%s:%t", id, s.GetIsDirect()))
	}
	sort.Strings(parts)
	return "sources=" + strings.Join(parts, ",")
}

func keys(set map[string]struct{}) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// seedChainStore registers entitlements A,B,C and their direct members, and
// returns a graph with the given edges (unexpanded). ents[i] gets member
// members[i] if non-empty.
func seedChainStore(t *testing.T, ctx context.Context, store *MockExpanderStore, ents []string, members []string, edges [][2]string) *EntitlementGraph {
	t.Helper()
	g := NewEntitlementGraph(ctx)
	for i, e := range ents {
		g.AddEntitlementID(e)
		store.AddEntitlement(makeEntitlement(e, makeResource("group", e)))
		if members[i] != "" {
			store.AddGrant(directGrant(e, makeResource("user", members[i])))
		}
	}
	for _, e := range edges {
		require.NoError(t, g.AddEdge(ctx, e[0], e[1], false, nil))
	}
	return g
}

// TestE2E_IncrementalMatchesFullRebuild is the end-to-end correctness bar:
// after loading a persisted graph and folding in one new edge incrementally,
// the store's grants must exactly equal what a full expansion produces when
// that edge was present from the start.
//
// Scenario: base graph has eng:manager -> eng:member (mandy is a manager).
// Event: eng:senior_manager -> eng:manager arrives (sam is a senior manager).
// Expected everywhere: manager = {mandy, sam}, member = {mandy, sam}.
func TestE2E_IncrementalMatchesFullRebuild(t *testing.T) {
	ctx := context.Background()
	ents := []string{"eng:senior_manager", "eng:manager", "eng:member"}

	// --- Path 1: base full expansion, persist graph, then incremental edge ---
	incStore := NewMockExpanderStore()
	// Base only knows manager -> member (senior_manager has no edge yet).
	baseGraph := seedChainStore(t, ctx, incStore,
		ents,
		[]string{"sam", "mandy", ""},
		[][2]string{{"eng:manager", "eng:member"}},
	)
	require.NoError(t, NewExpander(incStore, baseGraph).Run(ctx))

	// Persist + reload the graph across the "event boundary".
	loaded := roundTripGraph(t, baseGraph)

	// The event: senior_manager -> manager.
	ie := NewIncrementalExpander(incStore, loaded)
	res, err := ie.ExpandChanges(ctx, []NewEdge{
		{SourceEntitlementID: "eng:senior_manager", DestEntitlementID: "eng:manager"},
	}, nil)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"eng:manager", "eng:member"}, res.EntitlementsWalked)

	// --- Path 2: full expansion from scratch with BOTH edges present ---
	fullStore := NewMockExpanderStore()
	fullGraph := seedChainStore(t, ctx, fullStore,
		ents,
		[]string{"sam", "mandy", ""},
		[][2]string{{"eng:senior_manager", "eng:manager"}, {"eng:manager", "eng:member"}},
	)
	require.NoError(t, NewExpander(fullStore, fullGraph).Run(ctx))

	// --- The two must agree on full rows, INCLUDING sources/provenance ---
	incGrants := grantSet(t, ctx, incStore, ents...)
	fullGrants := grantSet(t, ctx, fullStore, ents...)
	require.Equal(t, keys(fullGrants), keys(incGrants),
		"incremental result must equal a full rebuild, sources included")

	// Sanity: the expected access is present (sources vary, so match by prefix).
	require.NotEmpty(t, incGrants)
	requireHasGrant(t, incGrants, "eng:manager|user|sam")
	requireHasGrant(t, incGrants, "eng:member|user|sam")
	requireHasGrant(t, incGrants, "eng:member|user|mandy")
}

// TestE2E_ShallowEdgeDirectnessMatchesFull (C1 differential): a principal who
// is BOTH a direct member of the source and expanded into it (sources map
// carries the source entitlement) must propagate over a new shallow edge, and
// a principal who is only expanded into the source must not — exactly matching
// a full rebuild, sources included.
//
// alice: direct on X and direct on B. bob: direct on X only (reaches B via
// X -> B deep). New shallow edge B -> Y arrives: alice qualifies (direct on
// B), bob does not.
func TestE2E_ShallowEdgeDirectnessMatchesFull(t *testing.T) {
	ctx := context.Background()
	ents := []string{"x:member", "b:member", "y:access"}

	seed := func(store *MockExpanderStore) *EntitlementGraph {
		g := NewEntitlementGraph(ctx)
		for _, e := range ents {
			g.AddEntitlementID(e)
			store.AddEntitlement(makeEntitlement(e, makeResource("group", e)))
		}
		store.AddGrant(directGrant("x:member", makeResource("user", "alice")))
		store.AddGrant(directGrant("x:member", makeResource("user", "bob")))
		store.AddGrant(directGrant("b:member", makeResource("user", "alice")))
		require.NoError(t, g.AddEdge(ctx, "x:member", "b:member", false, nil)) // deep
		return g
	}

	// --- Path 1: expand the base (X->B), persist+reload, then the shallow edge ---
	incStore := NewMockExpanderStore()
	baseGraph := seed(incStore)
	require.NoError(t, NewExpander(incStore, baseGraph).Run(ctx))
	loaded := roundTripGraph(t, baseGraph)

	ie := NewIncrementalExpander(incStore, loaded)
	_, err := ie.ExpandChanges(ctx, []NewEdge{
		{SourceEntitlementID: "b:member", DestEntitlementID: "y:access", Shallow: true},
	}, nil)
	require.NoError(t, err)

	// --- Path 2: full expansion from scratch with BOTH edges present ---
	fullStore := NewMockExpanderStore()
	fullGraph := seed(fullStore)
	require.NoError(t, fullGraph.AddEdge(ctx, "b:member", "y:access", true, nil)) // shallow
	require.NoError(t, NewExpander(fullStore, fullGraph).Run(ctx))

	// --- Full-row parity, sources included ---
	incGrants := grantSet(t, ctx, incStore, ents...)
	fullGrants := grantSet(t, ctx, fullStore, ents...)
	require.Equal(t, keys(fullGrants), keys(incGrants),
		"shallow-edge incremental must equal a full rebuild, sources included")

	// alice (direct on B) crossed the shallow edge; bob (expanded-only) did not.
	requireHasGrant(t, incGrants, "y:access|user|alice")
	for k := range incGrants {
		require.False(t, strings.HasPrefix(k, "y:access|user|bob|"),
			"bob is not direct on b:member and must not cross a shallow edge")
	}
}

// requireHasGrant asserts some key in the set starts with the given
// entitlement|type|resource prefix (ignoring the sources suffix).
func requireHasGrant(t *testing.T, set map[string]struct{}, prefix string) {
	t.Helper()
	for k := range set {
		if strings.HasPrefix(k, prefix+"|") {
			return
		}
	}
	t.Fatalf("expected a grant with prefix %q in %v", prefix, keys(set))
}
