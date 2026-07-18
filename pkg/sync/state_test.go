package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/stretchr/testify/require"
)

var allActionOps = []ActionOp{
	UnknownOp,
	InitOp,
	SyncResourceTypesOp,
	SyncResourcesOp,
	SyncEntitlementsOp,
	ListResourcesForEntitlementsOp,
	SyncGrantsOp,
	SyncExternalResourcesOp,
	SyncAssetsOp,
	SyncGrantExpansionOp,
	SyncTargetedResourceOp,
	SyncStaticEntitlementsOp,
}

func compareSyncerState(t *testing.T, expected Action, actual Action) {
	require.Equal(t, expected.Op, actual.Op)
	require.Equal(t, expected.PageToken, actual.PageToken)
	require.Equal(t, expected.ResourceID, actual.ResourceID)
	require.Equal(t, expected.ResourceTypeID, actual.ResourceTypeID)
}

func TestActionOps(t *testing.T) {
	for _, op := range allActionOps {
		require.Equal(t, op, newActionOp(op.String()), "action op %s should be equal to %s", op.String(), op.String())
	}
}

func TestSyncerToken(t *testing.T) {
	ctx := t.Context()
	st := newState()
	op1 := Action{Op: InitOp, PageToken: ""}
	op2 := Action{Op: SyncResourcesOp, PageToken: "", ResourceTypeID: "user", ResourceID: "userID1"}
	op3 := Action{Op: SyncEntitlementsOp, PageToken: "1234", ResourceTypeID: "repo", ResourceID: "repo42"}
	op4 := Action{Op: SyncEntitlementsOp, PageToken: "5678", ResourceTypeID: "repo", ResourceID: "repo42"}

	st.PushAction(ctx, op1)
	compareSyncerState(t, op1, *st.Current())
	require.Len(t, st.actions, 1)
	st.PushAction(ctx, op2)
	compareSyncerState(t, op2, *st.Current())
	require.Len(t, st.actions, 2)
	compareSyncerState(t, op1, st.actions[st.actionOrder[0]])
	compareSyncerState(t, op2, st.actions[st.actionOrder[1]])

	compareSyncerState(t, op2, *st.Current())
	st.FinishAction(ctx, st.Current())
	compareSyncerState(t, op1, *st.Current())
	require.Len(t, st.actions, 1)

	st.PushAction(ctx, op3)
	compareSyncerState(t, op3, *st.Current())
	require.Len(t, st.actions, 2)
	compareSyncerState(t, op1, st.actions[st.actionOrder[0]])
	compareSyncerState(t, op3, st.actions[st.actionOrder[1]])

	st.PushAction(ctx, op4)
	compareSyncerState(t, op4, *st.Current())
	require.Len(t, st.actions, 3)

	compareSyncerState(t, op1, st.actions[st.actionOrder[0]])
	compareSyncerState(t, op3, st.actions[st.actionOrder[1]])
	compareSyncerState(t, op4, st.actions[st.actionOrder[2]])

	st.FinishAction(ctx, st.Current())
	compareSyncerState(t, op3, *st.Current())
	require.Len(t, st.actions, 2)
	compareSyncerState(t, op1, st.actions[st.actionOrder[0]])

	st.FinishAction(ctx, st.Current())
	compareSyncerState(t, op1, *st.Current())
	require.Len(t, st.actions, 1)

	st.FinishAction(ctx, st.Current())
	require.Nil(t, st.Current())
	require.Len(t, st.actions, 0)
}

func TestSyncerTokenMarshalUnmarshal(t *testing.T) {
	ctx := t.Context()
	st := newState()
	states := []Action{
		{Op: InitOp, PageToken: ""},
		{Op: SyncResourcesOp, PageToken: "", ResourceTypeID: "user", ResourceID: "userID1"},
		{Op: SyncEntitlementsOp, PageToken: "1234", ResourceTypeID: "repo", ResourceID: "repo42"},
		{Op: SyncEntitlementsOp, PageToken: "5678", ResourceTypeID: "repo", ResourceID: "repo42"},
	}

	for _, s := range states {
		st.PushAction(ctx, s)
	}

	tokenString, err := st.Marshal()
	require.NoError(t, err)

	newToken := newState()
	err = newToken.Unmarshal(tokenString)
	require.NoError(t, err)

	i := len(states) - 1
	for newToken.Current() != nil {
		compareSyncerState(t, states[i], *newToken.Current())
		newToken.FinishAction(ctx, newToken.Current())
		i--
	}

	require.Equal(t, i, -1)
}

func TestSyncerTokenTimingStatsMarshalUnmarshal(t *testing.T) {
	st := newState()
	st.AddStepDuration("list-resources", 1500*time.Millisecond)
	st.AddStepDuration("list-resources", 500*time.Millisecond)
	st.RecordConnectorCall("list-resources", 1250*time.Millisecond)
	st.RecordConnectorCall("list-resources", 750*time.Millisecond)

	tokenString, err := st.Marshal()
	require.NoError(t, err)

	got := newState()
	require.NoError(t, got.Unmarshal(tokenString))
	require.Equal(t, map[string]int64{"list-resources": 2000}, got.StepDurations())
	require.Equal(t, map[string]ConnectorCallStat{
		"list-resources": {Count: 2, TotalMs: 2000, MaxMs: 1250},
	}, got.ConnectorCallStats())

	durations := got.StepDurations()
	durations["list-resources"] = 0
	stats := got.ConnectorCallStats()
	stats["list-resources"] = ConnectorCallStat{}
	require.EqualValues(t, 2000, got.StepDurations()["list-resources"])
	require.EqualValues(t, 2, got.ConnectorCallStats()["list-resources"].Count)
}

func TestSyncerTokenSessionStatsMarshalUnmarshal(t *testing.T) {
	st := newState()
	st.RecordSessionOp("get", 30*time.Second, context.DeadlineExceeded, true)
	st.RecordSessionOp("get", time.Millisecond, nil, false)
	st.RecordSessionOp("set", 5*time.Millisecond, errors.New("boom"), false)

	tokenString, err := st.Marshal()
	require.NoError(t, err)

	got := newState()
	require.NoError(t, got.Unmarshal(tokenString))
	stats := got.SessionStoreStats()
	require.Equal(t, SessionStoreStat{Count: 2, Errors: 1, Timeouts: 1, TotalMs: 30_001, MaxMs: 30_000}, stats["get"])
	require.Equal(t, SessionStoreStat{Count: 1, Errors: 1, TotalMs: 5, MaxMs: 5}, stats["set"])

	// Legacy tokens without the field yield an empty-but-usable map.
	legacy, err := json.Marshal(serializedTokenV1{Version: StateTokenVersion})
	require.NoError(t, err)
	fresh := newState()
	require.NoError(t, fresh.Unmarshal(string(legacy)))
	require.Empty(t, fresh.SessionStoreStats())
	fresh.RecordSessionOp("get", time.Millisecond, nil, false)
	require.EqualValues(t, 1, fresh.SessionStoreStats()["get"].Count)
}

func TestSyncerTokenLegacyTimingStatsAreUsable(t *testing.T) {
	tokenBytes, err := json.Marshal(serializedTokenV1{Version: StateTokenVersion})
	require.NoError(t, err)

	st := newState()
	require.NoError(t, st.Unmarshal(string(tokenBytes)))
	require.Empty(t, st.StepDurations())
	require.Empty(t, st.ConnectorCallStats())

	st.AddStepDuration("checkpoint", time.Millisecond)
	st.RecordConnectorCall("list-grants", time.Millisecond)
	require.EqualValues(t, 1, st.StepDurations()["checkpoint"])
	require.EqualValues(t, 1, st.ConnectorCallStats()["list-grants"].Count)
}

func TestSyncerTokenUnmarshalEmptyString(t *testing.T) {
	ctx := t.Context()
	st := newState()
	op1 := Action{Op: InitOp}

	err := st.Unmarshal("")
	require.NoError(t, err)

	st.PushAction(ctx, op1)
	compareSyncerState(t, op1, *st.Current())
}

func TestPrepareExpansionReplayTokenPreservesState(t *testing.T) {
	st := newState()
	st.SetShouldSkipGrants()
	_, conflict := st.CheckAndSetExclusionGroupResourceType("grp", "user")
	require.False(t, conflict, "unexpected conflict seeding exclusion group")
	require.False(t, st.NeedsExpansion())

	token, err := st.Marshal()
	require.NoError(t, err)

	replayToken, err := PrepareExpansionReplayToken(token)
	require.NoError(t, err)

	got := newState()
	require.NoError(t, got.Unmarshal(replayToken))

	// The flag the rollback sets.
	require.True(t, got.NeedsExpansion(), "expansion must be re-flagged")
	// The rest of the token must survive rather than be cleared.
	require.True(t, got.ShouldSkipGrants(), "skip-grants flag must be preserved")
	existing, conflict := got.CheckAndSetExclusionGroupResourceType("grp", "group")
	require.True(t, conflict, "preserved exclusion-group mapping must still conflict")
	require.Equal(t, "user", existing)
	// A finished token has no current action, so an InitOp is queued to drive
	// the resumed run.
	require.NotNil(t, got.Current())
	require.Equal(t, InitOp, got.Current().Op)
}

func TestSyncerTokenNextPage(t *testing.T) {
	ctx := t.Context()
	st := newState()
	op1 := Action{Op: InitOp}
	op2 := Action{Op: InitOp, PageToken: "next-page"}

	st.PushAction(ctx, op1)
	compareSyncerState(t, op1, *st.Current())
	require.Len(t, st.actions, 1)

	err := st.NextPage(ctx, st.Current().ID, "next-page")
	require.NoError(t, err)
	require.Len(t, st.actions, 1)
	compareSyncerState(t, op2, *st.Current())
}

func TestSyncerTokenUnmarshalBackwardsCompatible(t *testing.T) {
	initOp := Action{Op: InitOp}
	syncResourcesOp := Action{Op: SyncResourcesOp, PageToken: "", ResourceTypeID: "user", ResourceID: "userID1"}
	tokenV0 := serializedTokenV0{
		Actions:                         []Action{initOp},
		CurrentAction:                   &syncResourcesOp,
		NeedsExpansion:                  false,
		EntitlementGraph:                nil,
		HasExternalResourceGrants:       false,
		ShouldFetchRelatedResources:     false,
		ShouldSkipEntitlementsAndGrants: false,
		ShouldSkipGrants:                false,
		CompletedActionsCount:           2,
	}
	tokenV0Bytes, err := json.Marshal(tokenV0)
	require.NoError(t, err)
	require.NotEmpty(t, tokenV0Bytes)
	st := newState()
	err = st.Unmarshal(string(tokenV0Bytes))
	require.NoError(t, err)

	tokenV1String, err := st.Marshal()
	require.NoError(t, err)
	require.NotEmpty(t, tokenV1String)
	tokenV1 := serializedTokenV1{}
	err = json.Unmarshal([]byte(tokenV1String), &tokenV1)
	require.NoError(t, err)
	require.NotEmpty(t, tokenV0Bytes)

	expectedToken := serializedTokenV1{
		ActionsMap: map[string]Action{
			"0000000000": {
				Op: InitOp,
				ID: "0000000000",
			},
			"0000000001": {
				Op:             SyncResourcesOp,
				ID:             "0000000001",
				PageToken:      "",
				ResourceTypeID: "user",
				ResourceID:     "userID1",
			},
		},
		ActionOrder:                     []string{"0000000000", "0000000001"},
		CurrentActionID:                 2,
		NeedsExpansion:                  false,
		EntitlementGraph:                nil,
		HasExternalResourceGrants:       false,
		ShouldFetchRelatedResources:     false,
		ShouldSkipEntitlementsAndGrants: false,
		ShouldSkipGrants:                false,
		CompletedActionsCount:           2,
		Version:                         1,
	}
	require.Equal(t, expectedToken, tokenV1)
}

func TestUnmarshalV0ThenPushAction(t *testing.T) {
	ctx := t.Context()

	tokenV0 := serializedTokenV0{
		Actions:       []Action{{Op: SyncGrantsOp}},
		CurrentAction: &Action{Op: SyncResourcesOp, ResourceTypeID: "user"},
	}
	tokenV0Bytes, err := json.Marshal(tokenV0)
	require.NoError(t, err)

	st := newState()
	err = st.Unmarshal(string(tokenV0Bytes))
	require.NoError(t, err)

	// Current should be the old CurrentAction (top of stack)
	require.NotNil(t, st.Current())
	require.Equal(t, SyncResourcesOp, st.Current().Op)

	// This must not panic. After migrating a V0 token, pushing a new action
	// should produce a fresh ID that doesn't collide with any migrated ID.
	st.PushAction(ctx, Action{Op: SyncEntitlementsOp})

	require.Equal(t, SyncEntitlementsOp, st.Current().Op)
}

func TestSyncTokenV0FromC1Z(t *testing.T) {
	ctx := t.Context()

	store, err := dotc1z.NewC1ZFile(ctx, "testdata/sync-in-progress.c1z")
	require.NoError(t, err)
	defer store.Close(ctx)

	resp, err := store.ListSyncs(ctx, reader_v2.SyncsReaderServiceListSyncsRequest_builder{}.Build())
	require.NoError(t, err)
	require.NotEmpty(t, resp.GetSyncs())

	var tokenStr string
	for _, s := range resp.GetSyncs() {
		if s.GetEndedAt() == nil {
			tokenStr = s.GetSyncToken()
			break
		}
	}
	require.NotEmpty(t, tokenStr, "expected an in-progress sync with a non-empty token")

	// Verify it parses as a v0 token with the expected structure.
	var tokenV0 serializedTokenV0
	err = json.Unmarshal([]byte(tokenStr), &tokenV0)
	require.NoError(t, err)
	require.Len(t, tokenV0.Actions, 2)
	require.Equal(t, SyncGrantExpansionOp, tokenV0.Actions[0].Op)
	require.Equal(t, SyncGrantsOp, tokenV0.Actions[1].Op)
	require.NotNil(t, tokenV0.CurrentAction)
	require.Equal(t, SyncEntitlementsOp, tokenV0.CurrentAction.Op)
	require.Equal(t, uint64(45), tokenV0.CompletedActionsCount)

	// Migrate v0 -> v1 through state.Unmarshal.
	st := newState()
	err = st.Unmarshal(tokenStr)
	require.NoError(t, err)

	// Verify the migrated state has the correct current action (top of stack).
	require.NotNil(t, st.Current())
	require.Equal(t, SyncEntitlementsOp, st.Current().Op)
	require.Equal(t, "0000000002", st.Current().ID)

	// Marshal back to v1 and validate the full structure.
	v1Str, err := st.Marshal()
	require.NoError(t, err)
	require.NotEmpty(t, v1Str)

	var tokenV1 serializedTokenV1
	err = json.Unmarshal([]byte(v1Str), &tokenV1)
	require.NoError(t, err)

	require.Equal(t, serializedTokenV1{
		ActionsMap: map[string]Action{
			"0000000000": {Op: SyncGrantExpansionOp, ID: "0000000000"},
			"0000000001": {Op: SyncGrantsOp, ID: "0000000001"},
			"0000000002": {Op: SyncEntitlementsOp, ID: "0000000002"},
		},
		ActionOrder:           []string{"0000000000", "0000000001", "0000000002"},
		CurrentActionID:       3,
		CompletedActionsCount: 45,
		Version:               1,
	}, tokenV1)
}

// buildLoadedGraphState returns a state mid-grant-expansion: a populated
// entitlement graph and a SyncGrantExpansionOp action paginating the load,
// stacked on a non-expansion action whose own pagination must survive the
// checkpoint normalization untouched.
func buildLoadedGraphState(t *testing.T, ctx context.Context, pageToken string) *state {
	t.Helper()
	st := newState()

	st.PushAction(ctx, Action{Op: SyncGrantsOp})
	require.NoError(t, st.NextPage(ctx, st.Current().ID, "grants-p9"))

	st.PushAction(ctx, Action{Op: SyncGrantExpansionOp})
	if pageToken != "" {
		require.NoError(t, st.NextPage(ctx, st.Current().ID, pageToken))
	}

	graph := st.EntitlementGraph(ctx)
	require.NotNil(t, graph)
	graph.AddEntitlementID("ent1")
	graph.AddEntitlementID("ent2")
	graph.AddEntitlementID("ent3")
	require.NoError(t, graph.AddEdge(ctx, "ent1", "ent2", false, []string{"user"}))
	require.NoError(t, graph.AddEdge(ctx, "ent2", "ent3", true, []string{"group"}))
	graph.Depth = 5
	return st
}

// The entitlement graph is a projection of store data and is deliberately
// omitted from checkpoints (see state.Marshal). A resumed state must instead
// restart the expansion load from the first page, so the serialized expansion
// action's page token is blanked while the live state keeps paginating.
func TestSyncerTokenOmitsEntitlementGraph(t *testing.T) {
	ctx := t.Context()
	st := buildLoadedGraphState(t, ctx, "page37")

	tokenString, err := st.Marshal()
	require.NoError(t, err)
	require.NotEmpty(t, tokenString)

	// The serialized token carries neither the graph nor the pagination that
	// was accumulating into it.
	require.NotContains(t, tokenString, `"entitlement_graph"`)
	var raw serializedTokenV1
	require.NoError(t, json.Unmarshal([]byte(tokenString), &raw))
	require.Nil(t, raw.EntitlementGraph)
	// The normalized copy blanks ONLY expansion pagination: other actions keep
	// theirs, and no map entries are dropped.
	require.Len(t, raw.ActionsMap, 2)
	require.Len(t, raw.ActionOrder, 2)
	for _, a := range raw.ActionsMap {
		switch a.Op {
		case SyncGrantExpansionOp:
			require.Empty(t, a.PageToken, "serialized expansion action must restart the load")
		case SyncGrantsOp:
			require.Equal(t, "grants-p9", a.PageToken, "non-expansion pagination must survive")
		default:
			t.Fatalf("unexpected action op in serialized token: %v", a.Op)
		}
	}

	// Marshal must not mutate the live state: the in-process sync keeps its
	// graph and continues from its current page.
	require.NotNil(t, st.entitlementGraph)
	require.Equal(t, "page37", st.Current().PageToken)

	// A resumed state starts expansion over: fresh graph, first page — with
	// the rest of the action stack intact.
	resumed := newState()
	require.NoError(t, resumed.Unmarshal(tokenString))
	require.Nil(t, resumed.entitlementGraph)
	require.Equal(t, SyncGrantExpansionOp, resumed.Current().Op)
	require.Empty(t, resumed.Current().PageToken)
	require.False(t, resumed.EntitlementGraph(ctx).Loaded)
	require.Len(t, resumed.actions, 2)
	for _, a := range resumed.actions {
		if a.Op == SyncGrantsOp {
			require.Equal(t, "grants-p9", a.PageToken)
		}
	}
}

// Tokens written by older SDKs carry the graph inline. They must decode and
// resume exactly as before: graph restored, pagination preserved.
func TestSyncerTokenLegacyInlineGraphStillDecodes(t *testing.T) {
	ctx := t.Context()
	st := buildLoadedGraphState(t, ctx, "page37")
	st.entitlementGraph.Loaded = true
	st.entitlementGraph.HasNoCycles = true

	// Serialize the way pre-omission SDKs did: graph inline, page token kept.
	legacy, err := json.Marshal(serializedTokenV1{
		ActionsMap:       st.actions,
		ActionOrder:      st.actionOrder,
		CurrentActionID:  st.currentActionID,
		EntitlementGraph: st.entitlementGraph,
		Version:          1,
	})
	require.NoError(t, err)

	resumed := newState()
	require.NoError(t, resumed.Unmarshal(string(legacy)))

	restored := resumed.entitlementGraph
	require.NotNil(t, restored, "inline graph must be restored")
	require.Len(t, restored.Nodes, 3)
	require.Len(t, restored.Edges, 2)
	require.Equal(t, 5, restored.Depth)
	require.True(t, restored.Loaded)
	require.True(t, restored.HasNoCycles)
	require.Equal(t, st.entitlementGraph.NextNodeID, restored.NextNodeID)
	require.Equal(t, st.entitlementGraph.NextEdgeID, restored.NextEdgeID)
	// With the graph present, the pagination is still valid and must survive.
	require.Equal(t, "page37", resumed.Current().PageToken)

	// The upgrade chain: the next checkpoint after resuming a legacy token
	// must write the new format — graph omitted AND the in-flight expansion
	// pagination blanked (a graph-less token carrying the page token would be
	// exactly the orphan shape Unmarshal defends against) — while the live
	// resumed state keeps both and continues unaffected.
	out, err := resumed.Marshal()
	require.NoError(t, err)
	var reserialized serializedTokenV1
	require.NoError(t, json.Unmarshal([]byte(out), &reserialized))
	require.Nil(t, reserialized.EntitlementGraph)
	for _, a := range reserialized.ActionsMap {
		if a.Op == SyncGrantExpansionOp {
			require.Empty(t, a.PageToken)
		}
	}
	require.NotNil(t, resumed.entitlementGraph)
	require.Equal(t, "page37", resumed.Current().PageToken)
}

// The defensive blanking also applies to tokens decoded through the V0
// fallback: a graph-less V0 token cannot resume expansion pagination either,
// while other actions keep theirs.
func TestSyncerTokenV0OrphanExpansionPageTokenBlanked(t *testing.T) {
	legacy, err := json.Marshal(serializedTokenV0{
		Actions:       []Action{{Op: SyncGrantsOp, PageToken: "grants-p9"}},
		CurrentAction: &Action{Op: SyncGrantExpansionOp, PageToken: "page37"},
	})
	require.NoError(t, err)

	resumed := newState()
	require.NoError(t, resumed.Unmarshal(string(legacy)))
	require.Nil(t, resumed.entitlementGraph)
	require.Equal(t, SyncGrantExpansionOp, resumed.Current().Op)
	require.Empty(t, resumed.Current().PageToken)
	require.Len(t, resumed.actions, 2)
	for _, a := range resumed.actions {
		if a.Op == SyncGrantsOp {
			require.Equal(t, "grants-p9", a.PageToken)
		}
	}
}

// A graph-less token whose expansion action still carries a page token (a
// writer that did not normalize) cannot safely resume that pagination —
// Unmarshal must blank it so the load restarts from the first page.
func TestSyncerTokenUnmarshalBlanksOrphanExpansionPageToken(t *testing.T) {
	ctx := t.Context()
	st := newState()
	st.PushAction(ctx, Action{Op: SyncGrantExpansionOp})
	require.NoError(t, st.NextPage(ctx, st.Current().ID, "page37"))

	orphan, err := json.Marshal(serializedTokenV1{
		ActionsMap:      st.actions,
		ActionOrder:     st.actionOrder,
		CurrentActionID: st.currentActionID,
		Version:         1,
	})
	require.NoError(t, err)

	resumed := newState()
	require.NoError(t, resumed.Unmarshal(string(orphan)))
	require.Nil(t, resumed.entitlementGraph)
	require.Equal(t, SyncGrantExpansionOp, resumed.Current().Op)
	require.Empty(t, resumed.Current().PageToken)
}

func TestCheckAndSetExclusionGroupResourceType(t *testing.T) {
	st := newState()

	// First write for a group records the resource type and reports no conflict.
	existing, conflict := st.CheckAndSetExclusionGroupResourceType("group-a", "user")
	require.False(t, conflict)
	require.Empty(t, existing)

	// Re-recording the same (group, resource type) pair is idempotent and not a conflict.
	existing, conflict = st.CheckAndSetExclusionGroupResourceType("group-a", "user")
	require.False(t, conflict)
	require.Empty(t, existing)

	// A different group on a different resource type is independent and not a conflict.
	existing, conflict = st.CheckAndSetExclusionGroupResourceType("group-b", "team")
	require.False(t, conflict)
	require.Empty(t, existing)

	// Reusing an existing group id on a different resource type is a conflict, and
	// the originally-recorded resource type is returned.
	existing, conflict = st.CheckAndSetExclusionGroupResourceType("group-a", "team")
	require.True(t, conflict)
	require.Equal(t, "user", existing)

	// Conflicting attempt must not have rewritten the map; the original mapping holds.
	existing, conflict = st.CheckAndSetExclusionGroupResourceType("group-a", "user")
	require.False(t, conflict)
	require.Empty(t, existing)
}

func TestSyncerTokenExclusionGroupResourceTypesRoundTrip(t *testing.T) {
	st := newState()

	// Seed the map.
	_, conflict := st.CheckAndSetExclusionGroupResourceType("group-a", "user")
	require.False(t, conflict)
	_, conflict = st.CheckAndSetExclusionGroupResourceType("group-b", "team")
	require.False(t, conflict)

	tokenString, err := st.Marshal()
	require.NoError(t, err)

	restored := newState()
	require.NoError(t, restored.Unmarshal(tokenString))

	// Reusing a group id on the same resource type should still not conflict after resume.
	existing, conflict := restored.CheckAndSetExclusionGroupResourceType("group-a", "user")
	require.False(t, conflict)
	require.Empty(t, existing)

	// And reusing across resource types should conflict — proving the map crossed Marshal/Unmarshal.
	existing, conflict = restored.CheckAndSetExclusionGroupResourceType("group-a", "team")
	require.True(t, conflict)
	require.Equal(t, "user", existing)
}

func TestSyncerTokenUnmarshalEmptyStringInitsExclusionGroupMap(t *testing.T) {
	st := newState()
	require.NoError(t, st.Unmarshal(""))

	// A fresh-but-unmarshalled state must accept writes without panicking on a nil map.
	existing, conflict := st.CheckAndSetExclusionGroupResourceType("group-a", "user")
	require.False(t, conflict)
	require.Empty(t, existing)

	existing, conflict = st.CheckAndSetExclusionGroupDefault("group-a", "user:user1:admin")
	require.False(t, conflict)
	require.Empty(t, existing)
}

func TestCheckAndSetExclusionGroupDefault(t *testing.T) {
	st := newState()

	// First default for a group is recorded with no conflict.
	existing, conflict := st.CheckAndSetExclusionGroupDefault("role", "user:user1:admin")
	require.False(t, conflict)
	require.Empty(t, existing)

	// Re-recording the same (group, entitlement) pair is idempotent.
	existing, conflict = st.CheckAndSetExclusionGroupDefault("role", "user:user1:admin")
	require.False(t, conflict)
	require.Empty(t, existing)

	// A default in a different group is independent.
	existing, conflict = st.CheckAndSetExclusionGroupDefault("other", "user:user2:owner")
	require.False(t, conflict)
	require.Empty(t, existing)

	// A second, different default in the same group is a conflict; the original is returned.
	existing, conflict = st.CheckAndSetExclusionGroupDefault("role", "user:user2:editor")
	require.True(t, conflict)
	require.Equal(t, "user:user1:admin", existing)

	// Conflicting attempt must not have rewritten the map.
	existing, conflict = st.CheckAndSetExclusionGroupDefault("role", "user:user1:admin")
	require.False(t, conflict)
	require.Empty(t, existing)
}

func TestSyncerTokenExclusionGroupDefaultsRoundTrip(t *testing.T) {
	st := newState()

	_, conflict := st.CheckAndSetExclusionGroupDefault("role-a", "user:user1:admin")
	require.False(t, conflict)
	_, conflict = st.CheckAndSetExclusionGroupDefault("role-b", "team:team1:lead")
	require.False(t, conflict)

	tokenString, err := st.Marshal()
	require.NoError(t, err)

	restored := newState()
	require.NoError(t, restored.Unmarshal(tokenString))

	// Re-recording the same default is idempotent.
	existing, conflict := restored.CheckAndSetExclusionGroupDefault("role-a", "user:user1:admin")
	require.False(t, conflict)
	require.Empty(t, existing)

	// A conflicting default after resume is correctly rejected — proving the
	// map crossed Marshal/Unmarshal.
	existing, conflict = restored.CheckAndSetExclusionGroupDefault("role-a", "user:user2:editor")
	require.True(t, conflict)
	require.Equal(t, "user:user1:admin", existing)
}

func TestIncrementExclusionGroupCount(t *testing.T) {
	st := newState()

	require.Equal(t, uint32(1), st.IncrementExclusionGroupCount("role"))
	require.Equal(t, uint32(2), st.IncrementExclusionGroupCount("role"))
	require.Equal(t, uint32(3), st.IncrementExclusionGroupCount("role"))

	// Counts are independent per group.
	require.Equal(t, uint32(1), st.IncrementExclusionGroupCount("other"))
	require.Equal(t, uint32(4), st.IncrementExclusionGroupCount("role"))
}

func TestSyncerTokenExclusionGroupCountsRoundTrip(t *testing.T) {
	st := newState()

	st.IncrementExclusionGroupCount("role")
	st.IncrementExclusionGroupCount("role")
	st.IncrementExclusionGroupCount("other")

	tokenString, err := st.Marshal()
	require.NoError(t, err)

	restored := newState()
	require.NoError(t, restored.Unmarshal(tokenString))

	// Counts resume where they left off — the next increment continues from the
	// persisted value rather than starting over.
	require.Equal(t, uint32(3), restored.IncrementExclusionGroupCount("role"))
	require.Equal(t, uint32(2), restored.IncrementExclusionGroupCount("other"))
}

func TestClearExclusionGroupTracking(t *testing.T) {
	ctx := t.Context()
	st := newState()

	_, conflict := st.CheckAndSetExclusionGroupResourceType("role", "user")
	require.False(t, conflict)
	_, conflict = st.CheckAndSetExclusionGroupDefault("role", "user:user1:admin")
	require.False(t, conflict)
	st.IncrementExclusionGroupCount("role")

	tokenBefore, err := st.Marshal()
	require.NoError(t, err)
	require.Contains(t, tokenBefore, "exclusion_group")

	st.ClearExclusionGroupTracking(ctx)

	tokenAfter, err := st.Marshal()
	require.NoError(t, err)
	// The omitempty maps drop out of the serialized token once cleared.
	require.NotContains(t, tokenAfter, "exclusion_group")

	// After clearing, tracking starts fresh without panicking on nil maps.
	require.Equal(t, uint32(1), st.IncrementExclusionGroupCount("role"))
	existing, conflict := st.CheckAndSetExclusionGroupResourceType("role", "team")
	require.False(t, conflict)
	require.Empty(t, existing)
}
