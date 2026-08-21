package expand

import (
	"context"
	"fmt"
	"math/rand"
	"slices"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// Stage-2a's oracle is a fresh full expansion. Each generated script mutates
// an already-expanded store one step at a time. Additive changes must match
// the oracle after every step; revocation-shaped changes must decline before
// the additions-only incremental writer is called.
func TestIncrementalDifferentialRandom(t *testing.T) {
	coverage := make(map[differentialMutation]int)
	incrementalRuns := 0
	declines := 0
	for seed := int64(0); seed < 200; seed++ {
		seed := seed
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			runs, declined, seen := runDifferentialScript(t, seed)
			incrementalRuns += runs
			declines += declined
			for mutation, count := range seen {
				coverage[mutation] += count
			}
		})
	}

	for mutation := differentialMutation(0); mutation < mutationCount; mutation++ {
		require.Positive(t, coverage[mutation], "mutation %s was not generated", mutation)
	}
	require.Positive(t, declines, "the corpus must exercise safe-decline paths")
	require.Greater(t, incrementalRuns, declines,
		"more than half of exercised decisions should run incrementally")
}

// FuzzIncrementalVsFullExpansion extends the deterministic corpus with Go's
// native mutation engine. A failure reports one replayable script seed.
func FuzzIncrementalVsFullExpansion(f *testing.F) {
	for seed := int64(0); seed < 40; seed++ {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, seed int64) {
		runDifferentialScript(t, seed)
	})
}

// TestIncrementalDifferentialMutationAdequacy proves the oracle detects the
// three representative mutants required by the review brief. These tests pass
// only when the deliberately wrong result differs from the fresh-full oracle.
func TestIncrementalDifferentialMutationAdequacy(t *testing.T) {
	t.Run("ghost edge", func(t *testing.T) {
		ctx := context.Background()
		base := sqliteParityCase{
			entitlementIDs: []string{"a", "b"},
			grants: []sqliteGrantSpec{{
				id: "alice-a", entitlementID: "a", principalRT: "user", principalID: "alice",
			}},
			edges: []sqliteEdgeSpec{{src: "a", dst: "b"}},
		}
		wrongStore, wrongGraph := mockStoreFromCase(t, ctx, base)
		require.NoError(t, NewExpander(wrongStore, wrongGraph).Run(ctx))

		// Mutant: the current rule set removed a->b, but incremental wrongly
		// keeps the old graph and old synthesized grant.
		current := base
		current.edges = nil
		fullStore, fullGraph := mockStoreFromCase(t, ctx, current)
		require.NoError(t, NewExpander(fullStore, fullGraph).Run(ctx))
		require.NotEqual(t, snapshotStoreGrants(fullStore), snapshotStoreGrants(wrongStore),
			"the oracle must kill a retained ghost-edge mutant")
	})

	t.Run("skipped re-expansion", func(t *testing.T) {
		ctx := context.Background()
		current := sqliteParityCase{
			entitlementIDs: []string{"a", "b"},
			grants: []sqliteGrantSpec{{
				id: "alice-a", entitlementID: "a", principalRT: "user", principalID: "alice",
			}},
			edges: []sqliteEdgeSpec{{src: "a", dst: "b"}},
		}
		wrongStore, wrongGraph := mockStoreFromCase(t, ctx, current)
		require.NoError(t, NewExpander(wrongStore, wrongGraph).Run(ctx))

		// Mutant: Bob is added to A, but the incremental walk is skipped.
		bob := sqliteGrantSpec{id: "bob-a", entitlementID: "a", principalRT: "user", principalID: "bob"}
		current.grants = append(current.grants, bob)
		wrongStore.AddGrant(directGrant("a", makeResource("user", "bob")))
		fullStore, fullGraph := mockStoreFromCase(t, ctx, current)
		require.NoError(t, NewExpander(fullStore, fullGraph).Run(ctx))
		require.NotEqual(t, snapshotStoreGrants(fullStore), snapshotStoreGrants(wrongStore),
			"the oracle must kill a skipped-re-expansion mutant")
	})

	t.Run("stale sidecar", func(t *testing.T) {
		ctx := context.Background()
		store := NewMockExpanderStore()
		graph := buildExpandedChain(t, ctx, store, "alice", "a", "b")
		graph.Loaded = true
		data, err := MarshalGraphBlob("old-sync", graph)
		require.NoError(t, err)
		got, err := UnmarshalGraphBlob(data, "current-sync")
		require.NoError(t, err)
		require.Nil(t, got, "the guard must kill stale-sidecar acceptance")
	})

	t.Run("one hop affected walk", func(t *testing.T) {
		ctx := context.Background()
		current := sqliteParityCase{
			entitlementIDs: []string{"a", "b", "c"},
			grants: []sqliteGrantSpec{{
				id: "alice-a", entitlementID: "a", principalRT: "user", principalID: "alice",
			}},
			edges: []sqliteEdgeSpec{{src: "a", dst: "b"}, {src: "b", dst: "c"}},
		}
		wrongStore, wrongGraph := mockStoreFromCase(t, ctx, current)
		require.NoError(t, NewExpander(wrongStore, wrongGraph).Run(ctx))
		bob := sqliteGrantSpec{id: "bob-a", entitlementID: "a", principalRT: "user", principalID: "bob"}
		current.grants = append(current.grants, bob)
		wrongStore.AddGrant(directGrant("a", makeResource("user", "bob")))
		// Mutant: stop affected traversal after B by hiding B->C.
		bNode, cNode := wrongGraph.GetNode("b"), wrongGraph.GetNode("c")
		edgeID := wrongGraph.SourcesToDestinations[bNode.Id][cNode.Id]
		delete(wrongGraph.SourcesToDestinations[bNode.Id], cNode.Id)
		delete(wrongGraph.DestinationsToSources[cNode.Id], bNode.Id)
		delete(wrongGraph.Edges, edgeID)
		_, err := NewIncrementalExpander(wrongStore, wrongGraph).ExpandChanges(ctx, nil, []string{"a"})
		require.NoError(t, err)

		fullStore, fullGraph := mockStoreFromCase(t, ctx, current)
		require.NoError(t, NewExpander(fullStore, fullGraph).Run(ctx))
		require.NotEqual(t, snapshotStoreGrants(fullStore), snapshotStoreGrants(wrongStore),
			"the oracle must kill a one-hop traversal mutant")
	})

	t.Run("stale provenance", func(t *testing.T) {
		ctx := context.Background()
		current := sqliteParityCase{
			entitlementIDs: []string{"a", "b", "c"},
			grants: []sqliteGrantSpec{
				{id: "carol-a", entitlementID: "a", principalRT: "user", principalID: "carol"},
				{id: "carol-b", entitlementID: "b", principalRT: "user", principalID: "carol"},
			},
			edges: []sqliteEdgeSpec{{src: "a", dst: "c"}, {src: "b", dst: "c"}},
		}
		fullStore, fullGraph := mockStoreFromCase(t, ctx, current)
		require.NoError(t, NewExpander(fullStore, fullGraph).Run(ctx))

		wrongStore, _ := mockStoreFromCase(t, ctx, current)
		wrongStore.AddGrant(expandedGrantWithSource("c", makeResource("user", "carol"), "a"))
		require.NotEqual(t, snapshotStoreGrants(fullStore), snapshotStoreGrants(wrongStore),
			"the oracle must kill a missing-contributor provenance mutant")
	})
}

type differentialMutation int

const (
	mutationAddMember differentialMutation = iota
	mutationAddEdge
	mutationWidenShallow
	mutationWidenFilter
	mutationRemoveMember
	mutationRemoveEdge
	mutationNarrowShallow
	mutationNarrowFilter
	mutationCloseCycle
	mutationNoOp
	mutationCount
)

func (m differentialMutation) String() string {
	return [...]string{
		"add-member", "add-edge", "widen-shallow", "widen-filter",
		"remove-member", "remove-edge", "narrow-shallow", "narrow-filter",
		"close-cycle", "no-op",
	}[m]
}

func runDifferentialScript(t *testing.T, seed int64) (int, int, map[differentialMutation]int) {
	t.Helper()
	ctx := context.Background()
	rng := rand.New(rand.NewSource(seed ^ 0x5eed)) //nolint:gosec // deterministic fixture generation
	model := randomExpansionCase(seed)
	incStore, incGraph := mockStoreFromCase(t, ctx, model)
	require.NoError(t, NewExpander(incStore, incGraph).Run(ctx))
	incGraph.Loaded = true

	incrementalRuns := 0
	declines := 0
	seen := make(map[differentialMutation]int)
	steps := 1 + rng.Intn(10)
	for step := 0; step < steps; step++ {
		mutation := differentialMutation((int(seed) + step) % int(mutationCount))
		seen[mutation]++

		newEdges, changed, decline := applyDifferentialMutation(t, rng, &model, incStore, mutation, seed, step)
		if decline {
			// Compaction must route these changes to full expansion. Calling the
			// additions-only writer would leave stale grants or provenance.
			declines++
			return incrementalRuns, declines, seen
		}

		_, err := NewIncrementalExpander(incStore, incGraph).ExpandChanges(ctx, newEdges, changed)
		if err != nil {
			if mutation == mutationCloseCycle {
				require.ErrorIs(t, err, ErrIncrementalFallback)
				declines++
				return incrementalRuns, declines, seen
			}
			require.NoError(t, err, "seed=%d step=%d mutation=%s", seed, step, mutation)
		}
		incrementalRuns++

		fullStore, fullGraph := mockStoreFromCase(t, ctx, model)
		require.NoError(t, NewExpander(fullStore, fullGraph).Run(ctx))
		require.Equal(t, snapshotStoreGrants(fullStore), snapshotStoreGrants(incStore),
			"seed=%d step=%d mutation=%s", seed, step, mutation)
		require.Equal(t, independentAccessOracle(model), snapshotAccessPairs(incStore),
			"seed=%d step=%d mutation=%s independent access oracle", seed, step, mutation)
		assertReusableGraph(t, incGraph, seed, step, mutation)
	}
	return incrementalRuns, declines, seen
}

type modelMembership struct {
	resourceType string
	principalID  string
	direct       bool
}

// independentAccessOracle is deliberately graph-library-free. It computes a
// fixed point over plain maps and sets, providing a second oracle independent
// of both IncrementalExpander and the production full Expander.
func independentAccessOracle(model sqliteParityCase) map[string]struct{} {
	members := make(map[string]map[string]modelMembership, len(model.entitlementIDs))
	for _, entitlementID := range model.entitlementIDs {
		members[entitlementID] = make(map[string]modelMembership)
	}
	for _, grant := range model.grants {
		key := grant.principalRT + "\x00" + grant.principalID
		direct := len(grant.sources) == 0 || grant.sources[grant.entitlementID]
		current, exists := members[grant.entitlementID][key]
		if !exists || direct {
			members[grant.entitlementID][key] = modelMembership{
				resourceType: grant.principalRT, principalID: grant.principalID, direct: current.direct || direct,
			}
		}
	}
	changed := true
	for changed {
		changed = false
		for _, edge := range model.edges {
			for key, member := range members[edge.src] {
				if edge.shallow && !member.direct {
					continue
				}
				if len(edge.rtids) > 0 && !slices.Contains(edge.rtids, member.resourceType) {
					continue
				}
				if _, exists := members[edge.dst][key]; exists {
					continue
				}
				members[edge.dst][key] = modelMembership{
					resourceType: member.resourceType, principalID: member.principalID,
				}
				changed = true
			}
		}
	}
	out := make(map[string]struct{})
	for entitlementID, entitlementMembers := range members {
		for _, member := range entitlementMembers {
			out[entitlementID+"\x00"+member.resourceType+"\x00"+member.principalID] = struct{}{}
		}
	}
	return out
}

func snapshotAccessPairs(store *MockExpanderStore) map[string]struct{} {
	out := make(map[string]struct{})
	for entitlementID, grants := range store.grants {
		for _, grant := range grants {
			principal := grant.GetPrincipal().GetId()
			out[entitlementID+"\x00"+principal.GetResourceType()+"\x00"+principal.GetResource()] = struct{}{}
		}
	}
	return out
}

func applyDifferentialMutation(
	t *testing.T,
	rng *rand.Rand,
	model *sqliteParityCase,
	store *MockExpanderStore,
	mutation differentialMutation,
	seed int64,
	step int,
) ([]NewEdge, []string, bool) {
	t.Helper()
	ents := model.entitlementIDs
	switch mutation {
	case mutationAddMember:
		entitlementID := ents[rng.Intn(len(ents))]
		principalID := fmt.Sprintf("delta-%d-%d", seed, step)
		grant := directGrant(entitlementID, makeResource("user", principalID))
		store.AddGrant(grant)
		model.grants = append(model.grants, sqliteGrantSpec{
			id: grant.GetId(), entitlementID: entitlementID, principalRT: "user", principalID: principalID,
		})
		return nil, []string{entitlementID}, false

	case mutationAddEdge:
		edge, ok := missingForwardEdge(*model)
		if !ok {
			return nil, nil, false
		}
		edge.shallow = rng.Intn(2) == 0
		if rng.Intn(2) == 0 {
			edge.rtids = []string{"user"}
		}
		model.edges = append(model.edges, edge)
		return []NewEdge{newEdgeFromSpec(edge)}, nil, false

	case mutationWidenShallow:
		index := findEdge(model.edges, func(edge sqliteEdgeSpec) bool { return edge.shallow })
		if index < 0 {
			return nil, nil, false
		}
		model.edges[index].shallow = false
		return []NewEdge{newEdgeFromSpec(model.edges[index])}, nil, false

	case mutationWidenFilter:
		index := findEdge(model.edges, func(edge sqliteEdgeSpec) bool { return len(edge.rtids) > 0 })
		if index < 0 {
			return nil, nil, false
		}
		model.edges[index].rtids = nil // no filter means all principal types
		return []NewEdge{newEdgeFromSpec(model.edges[index])}, nil, false

	case mutationRemoveMember:
		// Membership removals need deletion-aware writes. The current contract
		// declines them to a fresh full expansion.
		return nil, nil, true

	case mutationRemoveEdge:
		if len(model.edges) == 0 {
			return nil, nil, false
		}
		return nil, nil, true

	case mutationNarrowShallow:
		index := findEdge(model.edges, func(edge sqliteEdgeSpec) bool { return !edge.shallow })
		if index < 0 {
			return nil, nil, false
		}
		return nil, nil, true

	case mutationNarrowFilter:
		index := findEdge(model.edges, func(edge sqliteEdgeSpec) bool {
			return len(edge.rtids) == 0 || !slices.Equal(edge.rtids, []string{"user"})
		})
		if index < 0 {
			return nil, nil, false
		}
		return nil, nil, true

	case mutationCloseCycle:
		if len(model.edges) == 0 {
			return nil, nil, false
		}
		base := model.edges[rng.Intn(len(model.edges))]
		reverse := sqliteEdgeSpec{src: base.dst, dst: base.src}
		model.edges = append(model.edges, reverse)
		return []NewEdge{newEdgeFromSpec(reverse)}, nil, false

	case mutationNoOp:
		if len(model.edges) == 0 {
			return nil, nil, false
		}
		return []NewEdge{newEdgeFromSpec(model.edges[rng.Intn(len(model.edges))])}, nil, false
	case mutationCount:
		return nil, nil, false
	}
	return nil, nil, false
}

func findEdge(edges []sqliteEdgeSpec, match func(sqliteEdgeSpec) bool) int {
	for i, edge := range edges {
		if match(edge) {
			return i
		}
	}
	return -1
}

func newEdgeFromSpec(edge sqliteEdgeSpec) NewEdge {
	return NewEdge{
		SourceEntitlementID: edge.src,
		DestEntitlementID:   edge.dst,
		Shallow:             edge.shallow,
		ResourceTypeIDs:     append([]string(nil), edge.rtids...),
	}
}

func assertReusableGraph(t *testing.T, graph *EntitlementGraph, seed int64, step int, mutation differentialMutation) {
	t.Helper()
	require.True(t, graph.IsExpanded(), "seed=%d step=%d mutation=%s", seed, step, mutation)
	require.True(t, graph.HasNoCycles, "seed=%d step=%d mutation=%s", seed, step, mutation)
	require.NoError(t, graph.ValidateCompleted(), "seed=%d step=%d mutation=%s", seed, step, mutation)

	data, err := MarshalGraphBlob("differential", graph)
	require.NoError(t, err)
	roundTrip, err := UnmarshalGraphBlob(data, "differential")
	require.NoError(t, err)
	require.NotNil(t, roundTrip)
	require.NoError(t, roundTrip.ValidateCompleted())
	require.Equal(t, canonicalDifferentialGraph(graph), canonicalDifferentialGraph(roundTrip))
}

// canonicalGraph removes map iteration from diagnostics and checks the
// durable graph structure, not transient action/metric state.
func canonicalDifferentialGraph(graph *EntitlementGraph) []string {
	out := make([]string, 0, len(graph.Edges)+len(graph.EntitlementsToNodes))
	for entitlementID, nodeID := range graph.EntitlementsToNodes {
		out = append(out, fmt.Sprintf("node:%s=%d", entitlementID, nodeID))
	}
	for _, edge := range graph.Edges {
		source := graph.Nodes[edge.SourceID]
		destination := graph.Nodes[edge.DestinationID]
		out = append(out, fmt.Sprintf("edge:%v->%v:%t:%v:%t",
			source.EntitlementIDs, destination.EntitlementIDs,
			edge.IsShallow, edge.ResourceTypeIDs, edge.IsExpanded))
	}
	sort.Strings(out)
	return out
}

func missingForwardEdge(tc sqliteParityCase) (sqliteEdgeSpec, bool) {
	existing := make(map[string]struct{}, len(tc.edges))
	for _, edge := range tc.edges {
		existing[edge.src+"\x00"+edge.dst] = struct{}{}
	}
	for i := 0; i < len(tc.entitlementIDs); i++ {
		for j := i + 1; j < len(tc.entitlementIDs); j++ {
			src, dst := tc.entitlementIDs[i], tc.entitlementIDs[j]
			if _, ok := existing[src+"\x00"+dst]; ok {
				continue
			}
			return sqliteEdgeSpec{src: src, dst: dst}, true
		}
	}
	return sqliteEdgeSpec{}, false
}
