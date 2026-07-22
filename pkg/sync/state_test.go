package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/sync/expand"
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
	st.AddStepDuration("checkpoint", time.Millisecond)
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
	require.EqualValues(t, 1, got.StepDurations()["checkpoint"], "preserved step durations must survive the rewrite")
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

func TestSyncerTokenEntitlementGraphMarshalUnmarshal(t *testing.T) {
	ctx := t.Context()
	st := newState()

	// Push an action to initialize the state
	op1 := Action{Op: SyncGrantExpansionOp}
	st.PushAction(ctx, op1)

	// Get the entitlement graph from state
	graph := st.EntitlementGraph(ctx)
	require.NotNil(t, graph)

	// Create some test entitlements and add them to the graph
	entitlement1 := &v2.Entitlement{Id: "ent1"}
	entitlement2 := &v2.Entitlement{Id: "ent2"}
	entitlement3 := &v2.Entitlement{Id: "ent3"}

	graph.AddEntitlementID(entitlement1.GetId())
	graph.AddEntitlementID(entitlement2.GetId())
	graph.AddEntitlementID(entitlement3.GetId())

	// Add edges between entitlements
	err := graph.AddEdge(ctx, "ent1", "ent2", false, []string{"user"})
	require.NoError(t, err)
	err = graph.AddEdge(ctx, "ent2", "ent3", true, []string{"group"})
	require.NoError(t, err)

	// Add some actions to the graph
	graph.Actions = []*expand.EntitlementGraphAction{
		{
			SourceEntitlementID:     "ent1",
			DescendantEntitlementID: "ent2",
			Shallow:                 false,
			ResourceTypeIDs:         []string{"user"},
			PageToken:               "page1",
		},
		{
			SourceEntitlementID:     "ent2",
			DescendantEntitlementID: "ent3",
			Shallow:                 true,
			ResourceTypeIDs:         []string{"group", "role"},
			PageToken:               "",
		},
	}

	// Set some graph state
	graph.Depth = 5
	graph.Loaded = true
	graph.HasNoCycles = true

	// Marshal the state
	tokenString, err := st.Marshal()
	require.NoError(t, err)
	require.NotEmpty(t, tokenString)

	// Create a new state and unmarshal
	newState := newState()
	err = newState.Unmarshal(tokenString)
	require.NoError(t, err)

	// Verify the action was restored
	compareSyncerState(t, op1, *newState.Current())

	// Get the entitlement graph from the new state
	restoredGraph := newState.entitlementGraph
	require.NotNil(t, restoredGraph, "entitlement graph should be restored")

	// Verify nodes were restored
	require.Len(t, restoredGraph.Nodes, 3)
	require.NotNil(t, restoredGraph.GetNode("ent1"))
	require.NotNil(t, restoredGraph.GetNode("ent2"))
	require.NotNil(t, restoredGraph.GetNode("ent3"))

	// Verify edges were restored
	require.Len(t, restoredGraph.Edges, 2)

	// Verify actions were restored
	require.Len(t, restoredGraph.Actions, 2)
	require.Equal(t, "ent1", restoredGraph.Actions[0].SourceEntitlementID)
	require.Equal(t, "ent2", restoredGraph.Actions[0].DescendantEntitlementID)
	require.Equal(t, false, restoredGraph.Actions[0].Shallow)
	require.Equal(t, []string{"user"}, restoredGraph.Actions[0].ResourceTypeIDs)
	require.Equal(t, "page1", restoredGraph.Actions[0].PageToken)

	require.Equal(t, "ent2", restoredGraph.Actions[1].SourceEntitlementID)
	require.Equal(t, "ent3", restoredGraph.Actions[1].DescendantEntitlementID)
	require.Equal(t, true, restoredGraph.Actions[1].Shallow)
	require.Equal(t, []string{"group", "role"}, restoredGraph.Actions[1].ResourceTypeIDs)
	require.Equal(t, "", restoredGraph.Actions[1].PageToken)

	// Verify graph state was restored
	require.Equal(t, 5, restoredGraph.Depth)
	require.Equal(t, true, restoredGraph.Loaded)
	require.Equal(t, true, restoredGraph.HasNoCycles)

	// Verify NextNodeID and NextEdgeID were restored
	require.Equal(t, graph.NextNodeID, restoredGraph.NextNodeID)
	require.Equal(t, graph.NextEdgeID, restoredGraph.NextEdgeID)
}
