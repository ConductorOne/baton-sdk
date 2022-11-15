package sync

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func compareSyncerState(t *testing.T, expected Action, actual *Action) {
	require.NotNil(t, actual)
	require.Equal(t, expected.Op, actual.Op)
	require.Equal(t, expected.PageToken, actual.PageToken)
	require.Equal(t, expected.ResourceID, actual.ResourceID)
	require.Equal(t, expected.ResourceTypeID, actual.ResourceTypeID)
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
