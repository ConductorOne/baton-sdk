package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"context"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
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

func compareSyncerState(t *testing.T, expected Action, actual *Action) {
	require.NotNil(t, actual)
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
	st := &state{}
	op1 := Action{Op: InitOp, PageToken: ""}
	op2 := Action{Op: SyncResourcesOp, PageToken: "", ResourceTypeID: "user", ResourceID: "userID1"}
	op3 := Action{Op: SyncEntitlementsOp, PageToken: "1234", ResourceTypeID: "repo", ResourceID: "repo42"}
	op4 := Action{Op: SyncEntitlementsOp, PageToken: "5678", ResourceTypeID: "repo", ResourceID: "repo42"}

	st.push(op1)
	compareSyncerState(t, op1, st.Current())
	require.Empty(t, st.actions)
	st.push(op2)
	compareSyncerState(t, op2, st.Current())
	require.Len(t, st.actions, 1)
	compareSyncerState(t, op1, &st.actions[0])

	popped := st.pop()
	compareSyncerState(t, op2, popped)
	compareSyncerState(t, op1, st.Current())
	require.Len(t, st.actions, 0)

	st.push(op3)
	compareSyncerState(t, op3, st.Current())
	require.Len(t, st.actions, 1)
	compareSyncerState(t, op1, &st.actions[0])

	st.push(op4)
	compareSyncerState(t, op4, st.Current())
	require.Len(t, st.actions, 2)
	compareSyncerState(t, op1, &st.actions[0])
	compareSyncerState(t, op3, &st.actions[1])

	popped = st.pop()
	compareSyncerState(t, op4, popped)
	compareSyncerState(t, op3, st.Current())
	require.Len(t, st.actions, 1)
	compareSyncerState(t, op1, &st.actions[0])

	popped = st.pop()
	compareSyncerState(t, op3, popped)
	compareSyncerState(t, op1, st.Current())
	require.Len(t, st.actions, 0)

	popped = st.pop()
	compareSyncerState(t, op1, popped)
	require.Nil(t, st.Current())
	require.Len(t, st.actions, 0)
}

func TestSyncerTokenMarshalUnmarshal(t *testing.T) {
	st := &state{}
	states := []Action{
		{Op: InitOp, PageToken: ""},
		{Op: SyncResourcesOp, PageToken: "", ResourceTypeID: "user", ResourceID: "userID1"},
		{Op: SyncEntitlementsOp, PageToken: "1234", ResourceTypeID: "repo", ResourceID: "repo42"},
		{Op: SyncEntitlementsOp, PageToken: "5678", ResourceTypeID: "repo", ResourceID: "repo42"},
	}

	for _, s := range states {
		st.push(s)
	}

	tokenString, err := st.Marshal()
	require.NoError(t, err)

	newToken := &state{}
	err = newToken.Unmarshal(tokenString)
	require.NoError(t, err)

	i := len(states) - 1
	for newToken.Current() != nil {
		compareSyncerState(t, states[i], newToken.pop())
		i--
	}

	require.Equal(t, i, -1)
}

func TestSyncerTokenUnmarshalEmptyString(t *testing.T) {
	st := &state{}
	op1 := Action{Op: InitOp}

	err := st.Unmarshal("")
	require.NoError(t, err)

	st.push(op1)
	compareSyncerState(t, op1, st.Current())
}

func TestSyncerTokenNextPage(t *testing.T) {
	ctx := context.Background()
	st := &state{}
	op1 := Action{Op: InitOp}
	op2 := Action{Op: InitOp, PageToken: "next-page"}

	st.PushAction(ctx, op1)
	compareSyncerState(t, op1, st.Current())
	require.Len(t, st.actions, 0)

	err := st.NextPage(ctx, "next-page")
	require.NoError(t, err)
	require.Len(t, st.actions, 0)
	compareSyncerState(t, op2, st.Current())
}

func TestSyncerTokenEntitlementGraphMarshalUnmarshal(t *testing.T) {
	ctx := context.Background()
	st := &state{}

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
	newState := &state{}
	err = newState.Unmarshal(tokenString)
	require.NoError(t, err)

	// Verify the action was restored
	compareSyncerState(t, op1, newState.Current())

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
