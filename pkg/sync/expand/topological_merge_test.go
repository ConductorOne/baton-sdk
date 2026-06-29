package expand

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/stretchr/testify/require"
)

type cancelingBaseGroupStream struct {
	cancel    context.CancelFunc
	remaining int
	nextIndex int
}

func (s *cancelingBaseGroupStream) next(context.Context) (contributionGroup, bool, error) {
	if s.remaining == 0 {
		return contributionGroup{}, false, nil
	}
	if s.nextIndex == 0 {
		s.cancel()
	}
	group := contributionGroup{
		key:    topoPrincipalKey{resourceType: "user", resource: "p" + strconv.Itoa(s.nextIndex)},
		isBase: true,
	}
	s.nextIndex++
	s.remaining--
	return group, true, nil
}

func (s *cancelingBaseGroupStream) close() error { return nil }

func TestMergeContributionGroupStreamsChecksContextDuringMergeLoop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	stream := &cancelingBaseGroupStream{cancel: cancel, remaining: 3}
	sinkCalled := false

	err := mergeContributionGroupStreams(ctx, makeEntitlement("ent:dest", makeResource("group", "dest")), []contributionGroupStream{stream}, func(context.Context, []*v2.Grant) error {
		sinkCalled = true
		return nil
	}, nil)

	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled))
	require.False(t, sinkCalled)
}

type grantSnapshot struct {
	id           string
	entitlement  string
	principalRT  string
	principalID  string
	sourceDirect map[string]bool
}

func snapshotStoreGrants(store *MockExpanderStore) map[string]grantSnapshot {
	byID := make(map[string]*v2.Grant)
	order := make([]string, 0)
	entIDs := make([]string, 0, len(store.grants))
	for entID := range store.grants {
		entIDs = append(entIDs, entID)
	}
	sort.Strings(entIDs)
	for _, entID := range entIDs {
		for _, grant := range store.grants[entID] {
			if grant == nil {
				continue
			}
			if _, ok := byID[grant.GetId()]; !ok {
				order = append(order, grant.GetId())
			}
			byID[grant.GetId()] = grant
		}
	}

	out := make(map[string]grantSnapshot, len(byID))
	for _, id := range order {
		grant := byID[id]
		if grant == nil || grant.GetPrincipal() == nil || grant.GetPrincipal().GetId() == nil {
			continue
		}
		sources := grant.GetSources().GetSources()
		sourceDirect := make(map[string]bool, len(sources))
		for sourceID, source := range sources {
			sourceDirect[sourceID] = source.GetIsDirect()
		}
		out[id] = grantSnapshot{
			id:           grant.GetId(),
			entitlement:  grant.GetEntitlement().GetId(),
			principalRT:  grant.GetPrincipal().GetId().GetResourceType(),
			principalID:  grant.GetPrincipal().GetId().GetResource(),
			sourceDirect: sourceDirect,
		}
	}
	return out
}

func compareCurrentAndTopologicalStreaming(
	t *testing.T,
	setup func(context.Context) (*MockExpanderStore, *EntitlementGraph),
) {
	t.Helper()
	ctx := context.Background()

	currentStore, currentGraph := setup(ctx)
	require.NoError(t, NewExpander(currentStore, currentGraph).Run(ctx))
	current := snapshotStoreGrants(currentStore)

	streamingStore, streamingGraph := setup(ctx)
	require.NoError(t, NewExpander(streamingStore, streamingGraph).RunTopologicalMergeStreaming(ctx))
	streaming := snapshotStoreGrants(streamingStore)

	assertGrantSnapshotsEqual(t, current, streaming)
}

func compareCurrentAndTopologicalProjection(
	t *testing.T,
	setup func(context.Context) (*MockExpanderStore, *EntitlementGraph),
) {
	t.Helper()
	ctx := context.Background()

	currentStore, currentGraph := setup(ctx)
	require.NoError(t, NewExpander(currentStore, currentGraph).Run(ctx))
	current := snapshotStoreGrants(currentStore)

	projectionStore, projectionGraph := setup(ctx)
	require.NoError(t, NewExpander(projectionStore, projectionGraph).RunTopologicalMergeProjection(ctx))
	projection := snapshotStoreGrants(projectionStore)

	assertGrantSnapshotsEqual(t, current, projection)
}

func compareCurrentAndAllTopological(
	t *testing.T,
	setup func(context.Context) (*MockExpanderStore, *EntitlementGraph),
) {
	t.Helper()
	compareCurrentAndTopologicalStreaming(t, setup)
	compareCurrentAndTopologicalProjection(t, setup)
}

func assertGrantSnapshotsEqual(t *testing.T, expected, actual map[string]grantSnapshot) {
	t.Helper()
	require.Equal(t, len(expected), len(actual), "grant snapshot count mismatch")
	for id, exp := range expected {
		got, ok := actual[id]
		require.True(t, ok, "missing grant %q", id)
		require.Equal(t, exp.entitlement, got.entitlement, "entitlement mismatch for grant %q", id)
		require.Equal(t, exp.principalRT, got.principalRT, "principal resource type mismatch for grant %q", id)
		require.Equal(t, exp.principalID, got.principalID, "principal resource id mismatch for grant %q", id)
		require.Equal(t, exp.sourceDirect, got.sourceDirect, "sources mismatch for grant %q", id)
	}
	for id := range actual {
		require.Contains(t, expected, id, "unexpected grant %q", id)
	}
}

func TestTopologicalMergeMatchesCurrentMultiLevel(t *testing.T) {
	setup := func(ctx context.Context) (*MockExpanderStore, *EntitlementGraph) {
		store := NewMockExpanderStore()
		group := makeResource("group", "org")
		alice := makeResource("user", "alice")

		entA := makeEntitlement("ent:a", group)
		entB := makeEntitlement("ent:b", group)
		entC := makeEntitlement("ent:c", group)
		store.AddEntitlement(entA)
		store.AddEntitlement(entB)
		store.AddEntitlement(entC)
		store.AddGrant(makeGrant("grant:alice:a", entA, alice))

		graph := NewEntitlementGraph(ctx)
		graph.AddEntitlementID(entA.GetId())
		graph.AddEntitlementID(entB.GetId())
		graph.AddEntitlementID(entC.GetId())
		require.NoError(t, graph.AddEdge(ctx, entA.GetId(), entB.GetId(), false, []string{"user"}))
		require.NoError(t, graph.AddEdge(ctx, entB.GetId(), entC.GetId(), false, []string{"user"}))
		return store, graph
	}
	compareCurrentAndAllTopological(t, setup)
}

func TestTopologicalMergeMatchesCurrentDiamond(t *testing.T) {
	compareCurrentAndAllTopological(t, func(ctx context.Context) (*MockExpanderStore, *EntitlementGraph) {
		store := NewMockExpanderStore()
		group := makeResource("group", "org")
		alice := makeResource("user", "alice")

		entA := makeEntitlement("ent:a", group)
		entB := makeEntitlement("ent:b", group)
		entC := makeEntitlement("ent:c", group)
		store.AddEntitlement(entA)
		store.AddEntitlement(entB)
		store.AddEntitlement(entC)
		store.AddGrant(makeGrant("grant:alice:a", entA, alice))
		store.AddGrant(makeGrant("grant:alice:b", entB, alice))

		graph := NewEntitlementGraph(ctx)
		graph.AddEntitlementID(entA.GetId())
		graph.AddEntitlementID(entB.GetId())
		graph.AddEntitlementID(entC.GetId())
		require.NoError(t, graph.AddEdge(ctx, entA.GetId(), entC.GetId(), false, []string{"user"}))
		require.NoError(t, graph.AddEdge(ctx, entB.GetId(), entC.GetId(), false, []string{"user"}))
		return store, graph
	})
}

func TestTopologicalMergeMatchesCurrentMixedDirectness(t *testing.T) {
	compareCurrentAndAllTopological(t, func(ctx context.Context) (*MockExpanderStore, *EntitlementGraph) {
		store := NewMockExpanderStore()
		group := makeResource("group", "org")
		alice := makeResource("user", "alice")

		entA := makeEntitlement("ent:a", group)
		entB := makeEntitlement("ent:b", group)
		entC := makeEntitlement("ent:c", group)
		store.AddEntitlement(entA)
		store.AddEntitlement(entB)
		store.AddEntitlement(entC)
		store.AddGrant(makeGrant("grant:alice:a", entA, alice))

		graph := NewEntitlementGraph(ctx)
		graph.AddEntitlementID(entA.GetId())
		graph.AddEntitlementID(entB.GetId())
		graph.AddEntitlementID(entC.GetId())
		require.NoError(t, graph.AddEdge(ctx, entA.GetId(), entB.GetId(), false, []string{"user"}))
		require.NoError(t, graph.AddEdge(ctx, entA.GetId(), entC.GetId(), false, []string{"user"}))
		require.NoError(t, graph.AddEdge(ctx, entB.GetId(), entC.GetId(), false, []string{"user"}))
		return store, graph
	})
}

func TestTopologicalMergeMatchesCurrentShallow(t *testing.T) {
	compareCurrentAndAllTopological(t, func(ctx context.Context) (*MockExpanderStore, *EntitlementGraph) {
		store := NewMockExpanderStore()
		group := makeResource("group", "org")
		alice := makeResource("user", "alice")

		source := makeEntitlement("ent:source", group)
		other := makeEntitlement("ent:other", group)
		destShallow := makeEntitlement("ent:dest:shallow", group)
		destDeep := makeEntitlement("ent:dest:deep", group)
		store.AddEntitlement(source)
		store.AddEntitlement(destShallow)
		store.AddEntitlement(destDeep)

		transitive := makeGrant("grant:alice:source", source, alice)
		transitive.SetSources(v2.GrantSources_builder{Sources: map[string]*v2.GrantSources_GrantSource{
			other.GetId(): {IsDirect: false},
		}}.Build())
		store.AddGrant(transitive)

		graph := NewEntitlementGraph(ctx)
		graph.AddEntitlementID(source.GetId())
		graph.AddEntitlementID(destShallow.GetId())
		graph.AddEntitlementID(destDeep.GetId())
		require.NoError(t, graph.AddEdge(ctx, source.GetId(), destShallow.GetId(), true, nil))
		require.NoError(t, graph.AddEdge(ctx, source.GetId(), destDeep.GetId(), false, nil))
		return store, graph
	})
}

func TestTopologicalMergeMatchesCurrentExistingDestinationGrant(t *testing.T) {
	compareCurrentAndAllTopological(t, func(ctx context.Context) (*MockExpanderStore, *EntitlementGraph) {
		store := NewMockExpanderStore()
		group := makeResource("group", "org")
		alice := makeResource("user", "alice")

		source := makeEntitlement("ent:source", group)
		dest := makeEntitlement("ent:dest", group)
		store.AddEntitlement(source)
		store.AddEntitlement(dest)
		store.AddGrant(makeGrant("grant:alice:source", source, alice))
		store.AddGrant(makeGrant("grant:alice:dest", dest, alice))

		graph := NewEntitlementGraph(ctx)
		graph.AddEntitlementID(source.GetId())
		graph.AddEntitlementID(dest.GetId())
		require.NoError(t, graph.AddEdge(ctx, source.GetId(), dest.GetId(), false, nil))
		return store, graph
	})
}

func TestTopologicalMergeMatchesCurrentDirectnessUpgrade(t *testing.T) {
	compareCurrentAndAllTopological(t, func(ctx context.Context) (*MockExpanderStore, *EntitlementGraph) {
		store := NewMockExpanderStore()
		group := makeResource("group", "team")
		alice := makeResource("user", "alice")

		source := makeEntitlement("ent:source", group)
		dest := makeEntitlement("ent:dest", group)
		other := makeEntitlement("ent:other", group)
		store.AddEntitlement(source)
		store.AddEntitlement(dest)

		indirect := makeGrant("grant:alice:source:indirect", source, alice)
		indirect.SetSources(v2.GrantSources_builder{Sources: map[string]*v2.GrantSources_GrantSource{
			other.GetId(): {IsDirect: false},
		}}.Build())
		store.AddGrant(indirect)
		store.AddGrant(makeGrant("grant:alice:source:direct", source, alice))

		graph := NewEntitlementGraph(ctx)
		graph.AddEntitlementID(source.GetId())
		graph.AddEntitlementID(dest.GetId())
		require.NoError(t, graph.AddEdge(ctx, source.GetId(), dest.GetId(), false, []string{"user"}))
		return store, graph
	})
}

func TestTopologicalMergeMatchesCurrentResourceTypeFilters(t *testing.T) {
	compareCurrentAndAllTopological(t, func(ctx context.Context) (*MockExpanderStore, *EntitlementGraph) {
		store := NewMockExpanderStore()
		group := makeResource("group", "org")
		alice := makeResource("user", "alice")
		robot := makeResource("service", "robot")

		source := makeEntitlement("ent:source", group)
		destUser := makeEntitlement("ent:dest:user", group)
		destService := makeEntitlement("ent:dest:service", group)
		store.AddEntitlement(source)
		store.AddEntitlement(destUser)
		store.AddEntitlement(destService)
		store.AddGrant(makeGrant("grant:alice:source", source, alice))
		store.AddGrant(makeGrant("grant:robot:source", source, robot))

		graph := NewEntitlementGraph(ctx)
		graph.AddEntitlementID(source.GetId())
		graph.AddEntitlementID(destUser.GetId())
		graph.AddEntitlementID(destService.GetId())
		require.NoError(t, graph.AddEdge(ctx, source.GetId(), destUser.GetId(), false, []string{"user"}))
		require.NoError(t, graph.AddEdge(ctx, source.GetId(), destService.GetId(), false, []string{"service"}))
		return store, graph
	})
}

func TestTopologicalMergeMatchesCurrentDuplicatePrincipalDedup(t *testing.T) {
	compareCurrentAndAllTopological(t, func(ctx context.Context) (*MockExpanderStore, *EntitlementGraph) {
		store := NewMockExpanderStore()
		group := makeResource("group", "team")
		alice := makeResource("user", "alice")

		source := makeEntitlement("ent:source", group)
		dest := makeEntitlement("ent:dest", group)
		store.AddEntitlement(source)
		store.AddEntitlement(dest)
		store.AddGrant(makeGrant("grant:alice:source:1", source, alice))
		store.AddGrant(makeGrant("grant:alice:source:2", source, alice))

		graph := NewEntitlementGraph(ctx)
		graph.AddEntitlementID(source.GetId())
		graph.AddEntitlementID(dest.GetId())
		require.NoError(t, graph.AddEdge(ctx, source.GetId(), dest.GetId(), false, []string{"user"}))
		return store, graph
	})
}

func TestTopologicalMergeMatchesCurrentNilPrincipalSourceGrant(t *testing.T) {
	compareCurrentAndAllTopological(t, func(ctx context.Context) (*MockExpanderStore, *EntitlementGraph) {
		store := NewMockExpanderStore()
		group := makeResource("group", "team")
		bob := makeResource("user", "bob")

		source := makeEntitlement("ent:source", group)
		dest := makeEntitlement("ent:dest", group)
		store.AddEntitlement(source)
		store.AddEntitlement(dest)
		store.AddGrant(v2.Grant_builder{
			Id:          "grant:nil:source",
			Entitlement: source,
			Principal:   nil,
		}.Build())
		bobGrant := makeGrant("grant:bob:dest", dest, bob)
		bobGrant.SetSources(v2.GrantSources_builder{Sources: map[string]*v2.GrantSources_GrantSource{
			dest.GetId(): {IsDirect: true},
		}}.Build())
		store.AddGrant(bobGrant)

		graph := NewEntitlementGraph(ctx)
		graph.AddEntitlementID(source.GetId())
		graph.AddEntitlementID(dest.GetId())
		require.NoError(t, graph.AddEdge(ctx, source.GetId(), dest.GetId(), false, []string{"user"}))
		return store, graph
	})
}

func TestTopologicalMergeMatchesCurrentCollapsedCycle(t *testing.T) {
	compareCurrentAndAllTopological(t, func(ctx context.Context) (*MockExpanderStore, *EntitlementGraph) {
		store := NewMockExpanderStore()
		group := makeResource("group", "cycle")
		alice := makeResource("user", "alice")

		entA := makeEntitlement("ent:a", group)
		entB := makeEntitlement("ent:b", group)
		store.AddEntitlement(entA)
		store.AddEntitlement(entB)
		store.AddGrant(makeGrant("grant:alice:a", entA, alice))

		graph := NewEntitlementGraph(ctx)
		graph.AddEntitlementID(entA.GetId())
		graph.AddEntitlementID(entB.GetId())
		require.NoError(t, graph.AddEdge(ctx, entA.GetId(), entB.GetId(), false, []string{"user"}))
		require.NoError(t, graph.AddEdge(ctx, entB.GetId(), entA.GetId(), false, []string{"user"}))
		comps, _ := graph.ComputeCyclicComponents(ctx)
		require.NoError(t, graph.FixCyclesFromComponents(ctx, comps))
		return store, graph
	})
}
