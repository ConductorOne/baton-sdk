package expand

import (
	"context"
	"encoding/json"
	"sort"
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
// of "entitlement|principalType|principalResource" keys — the access outcome,
// independent of provenance details.
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
			set[entID+"|"+pid.GetResourceType()+"|"+pid.GetResource()] = struct{}{}
		}
	}
	return set
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

	// --- The two must agree on the access outcome ---
	incGrants := grantSet(t, ctx, incStore, ents...)
	fullGrants := grantSet(t, ctx, fullStore, ents...)
	require.Equal(t, keys(fullGrants), keys(incGrants),
		"incremental result must equal a full rebuild")

	// And spell out the expected access explicitly.
	require.Equal(t, []string{
		"eng:manager|user|mandy",
		"eng:manager|user|sam",
		"eng:member|user|mandy",
		"eng:member|user|sam",
		"eng:senior_manager|user|sam",
	}, keys(incGrants))
}
