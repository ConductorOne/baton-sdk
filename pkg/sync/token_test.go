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
	st := newRunState()
	op1 := Action{Op: InitOp, PageToken: ""}
	op2 := Action{Op: SyncResourcesOp, PageToken: "", ResourceTypeID: "user", ResourceID: "userID1"}
	op3 := Action{Op: SyncEntitlementsOp, PageToken: "1234", ResourceTypeID: "repo", ResourceID: "repo42"}
	op4 := Action{Op: SyncEntitlementsOp, PageToken: "5678", ResourceTypeID: "repo", ResourceID: "repo42"}

	st.pushAction(ctx, op1)
	compareSyncerState(t, op1, *st.current())
	require.Len(t, st.actions, 1)
	st.pushAction(ctx, op2)
	compareSyncerState(t, op2, *st.current())
	require.Len(t, st.actions, 2)
	compareSyncerState(t, op1, st.actions[st.actionOrder[0]])
	compareSyncerState(t, op2, st.actions[st.actionOrder[1]])

	compareSyncerState(t, op2, *st.current())
	st.finishAction(ctx, st.current())
	compareSyncerState(t, op1, *st.current())
	require.Len(t, st.actions, 1)

	st.pushAction(ctx, op3)
	compareSyncerState(t, op3, *st.current())
	require.Len(t, st.actions, 2)
	compareSyncerState(t, op1, st.actions[st.actionOrder[0]])
	compareSyncerState(t, op3, st.actions[st.actionOrder[1]])

	st.pushAction(ctx, op4)
	compareSyncerState(t, op4, *st.current())
	require.Len(t, st.actions, 3)

	compareSyncerState(t, op1, st.actions[st.actionOrder[0]])
	compareSyncerState(t, op3, st.actions[st.actionOrder[1]])
	compareSyncerState(t, op4, st.actions[st.actionOrder[2]])

	st.finishAction(ctx, st.current())
	compareSyncerState(t, op3, *st.current())
	require.Len(t, st.actions, 2)
	compareSyncerState(t, op1, st.actions[st.actionOrder[0]])

	st.finishAction(ctx, st.current())
	compareSyncerState(t, op1, *st.current())
	require.Len(t, st.actions, 1)

	st.finishAction(ctx, st.current())
	require.Nil(t, st.current())
	require.Len(t, st.actions, 0)
}

func TestSyncerTokenMarshalUnmarshal(t *testing.T) {
	ctx := t.Context()
	run, stats, _ := newTestRun()
	states := []Action{
		{Op: InitOp, PageToken: ""},
		{Op: SyncResourcesOp, PageToken: "", ResourceTypeID: "user", ResourceID: "userID1"},
		{Op: SyncEntitlementsOp, PageToken: "1234", ResourceTypeID: "repo", ResourceID: "repo42"},
		{Op: SyncEntitlementsOp, PageToken: "5678", ResourceTypeID: "repo", ResourceID: "repo42"},
	}

	for _, s := range states {
		run.pushAction(ctx, s)
	}

	tokenString := encodeTestRun(t, run, stats)

	resumed, _, _ := decodeTestRun(t, tokenString)

	i := len(states) - 1
	for resumed.current() != nil {
		compareSyncerState(t, states[i], *resumed.current())
		resumed.finishAction(ctx, resumed.current())
		i--
	}

	require.Equal(t, i, -1)
}

func TestActionCountsIncrementAndCheckpoint(t *testing.T) {
	ctx := t.Context()
	st := newRunState()
	st.pushAction(ctx, Action{Op: SyncResourcesOp, ResourceTypeID: "user"})
	parent := st.current()
	require.Equal(t, uint64(0), st.getActionCount(SyncResourcesOp).CompletedCount)
	require.Equal(t, uint64(0), st.completedActionsCount())

	_, err := st.transitionAction(ctx, parent, "page-2", nil)
	require.NoError(t, err)
	require.Equal(t, uint64(0), st.getActionCount(SyncResourcesOp).CompletedCount, "pagination must not count as completion")
	require.Equal(t, uint64(0), st.completedActionsCount())

	parent = st.current()
	_, err = st.transitionAction(ctx, parent, "", nil)
	require.NoError(t, err)
	require.Equal(t, uint64(1), st.getActionCount(SyncResourcesOp).CompletedCount)
	require.Equal(t, uint64(1), st.completedActionsCount())

	st.pushAction(ctx, Action{Op: SyncResourcesOp, ResourceTypeID: "group"})
	st.finishAction(ctx, st.current())
	require.Equal(t, uint64(2), st.getActionCount(SyncResourcesOp).CompletedCount)
	require.Equal(t, uint64(0), st.getActionCount(SyncResourcesOp).WarningCount)
	require.Equal(t, uint64(2), st.completedActionsCount())

	st.pushAction(ctx, Action{Op: SyncGrantsOp, ResourceTypeID: "user"})
	st.finishActionWithWarning(ctx, st.current())
	require.Equal(t, uint64(2), st.getActionCount(SyncResourcesOp).CompletedCount)
	require.Equal(t, uint64(1), st.getActionCount(SyncGrantsOp).CompletedCount)
	require.Equal(t, uint64(1), st.getActionCount(SyncGrantsOp).WarningCount)
	require.Equal(t, uint64(0), st.getActionCount(SyncResourcesOp).WarningCount)
	require.Equal(t, uint64(3), st.completedActionsCount())

	st.pushAction(ctx, Action{Op: SyncResourcesOp, ResourceTypeID: "role"})
	st.finishActionWithWarning(ctx, st.current())
	require.Equal(t, uint64(3), st.getActionCount(SyncResourcesOp).CompletedCount)
	require.Equal(t, uint64(1), st.getActionCount(SyncResourcesOp).WarningCount)

	tokenString, err := marshalToken(st, newRunStats())
	require.NoError(t, err)
	got, err := unmarshalToken(tokenString)
	require.NoError(t, err)
	require.Equal(t, uint64(3), got.run.getActionCount(SyncResourcesOp).CompletedCount)
	require.Equal(t, uint64(1), got.run.getActionCount(SyncResourcesOp).WarningCount)
	require.Equal(t, uint64(1), got.run.getActionCount(SyncGrantsOp).CompletedCount)
	require.Equal(t, uint64(1), got.run.getActionCount(SyncGrantsOp).WarningCount)
	require.Equal(t, uint64(4), got.run.completedActionsCount())

	tokenV0Bytes, err := json.Marshal(serializedTokenV0{
		Actions:               []Action{{Op: InitOp}},
		CompletedActionsCount: 45,
	})
	require.NoError(t, err)
	v0, err := unmarshalToken(string(tokenV0Bytes))
	require.NoError(t, err)
	require.Equal(t, uint64(0), v0.run.getActionCount(SyncResourcesOp).CompletedCount)
	require.Equal(t, uint64(0), v0.run.getActionCount(SyncResourcesOp).WarningCount)
	require.Equal(t, uint64(45), v0.run.completedActionsCount())

	legacy, err := json.Marshal(serializedTokenV1{Version: StateTokenVersion, CompletedActionsCount: 5})
	require.NoError(t, err)
	freshParts, err := unmarshalToken(string(legacy))
	require.NoError(t, err)
	fresh := freshParts.run
	require.Equal(t, uint64(0), fresh.getActionCount(SyncResourcesOp).CompletedCount)
	require.Equal(t, uint64(0), fresh.getActionCount(SyncResourcesOp).WarningCount)
	fresh.pushAction(ctx, Action{Op: SyncResourcesOp, ResourceTypeID: "user"})
	fresh.finishAction(ctx, fresh.current())
	require.Equal(t, uint64(1), fresh.getActionCount(SyncResourcesOp).CompletedCount)
	require.Equal(t, uint64(0), fresh.getActionCount(SyncResourcesOp).WarningCount)
}

func TestResumedActionWarningCountsTripThreshold(t *testing.T) {
	tokenBytes, err := json.Marshal(serializedTokenV1{
		Version:               StateTokenVersion,
		CompletedActionsCount: 220,
		ActionCountsMap: map[string]ActionCount{
			SyncResourcesOp.String(): {CompletedCount: 20, WarningCount: 11},
			SyncGrantsOp.String():    {CompletedCount: 200, WarningCount: 2},
		},
	})
	require.NoError(t, err)

	parts, err := unmarshalToken(string(tokenBytes))
	require.NoError(t, err)
	st := parts.run
	require.Equal(t, uint64(220), st.completedActionsCount())
	require.Equal(t, uint64(2), st.getActionCount(SyncGrantsOp).WarningCount)

	listResources := st.getActionCount(SyncResourcesOp)
	require.True(t, tooManyWarnings(listResources.WarningCount, listResources.CompletedCount, 0.05),
		"resume must use checkpointed list-resource warnings, not an empty in-memory slice")
	require.False(t, tooManyWarnings(st.getActionCount(SyncGrantsOp).WarningCount, st.getActionCount(SyncGrantsOp).CompletedCount, 0.1),
		"grant warnings stay on the grant bucket and do not trip the list-resource threshold")

	// ErrTooManyWarnings is preservable, so this token is what the next run
	// resumes. The ratio alone must not stop that run before this process
	// finishes more than ten list-resource actions, or every resume exits
	// with zero progress.
	require.False(t, tooManyListResourceWarnings(listResources, 0),
		"a resumed run must not abort before completing a list-resource action")
	require.False(t, tooManyListResourceWarnings(listResources, 10),
		"ten completions this run are not enough to re-arm the durable ratio")
	require.True(t, tooManyListResourceWarnings(listResources, 11),
		"more than ten completions this run re-arm the durable ratio")
}

func TestSyncerTokenTimingStatsMarshalUnmarshal(t *testing.T) {
	run, stats, _ := newTestRun()
	stats.addStepDuration("list-resources", 1500*time.Millisecond)
	stats.addStepDuration("list-resources", 500*time.Millisecond)
	stats.recordConnectorCall("list-resources", 1250*time.Millisecond)
	stats.recordConnectorCall("list-resources", 750*time.Millisecond)

	tokenString := encodeTestRun(t, run, stats)

	_, got, _ := decodeTestRun(t, tokenString)
	require.Equal(t, map[string]int64{"list-resources": 2000}, got.stepDurations())
	require.Equal(t, map[string]ConnectorCallStat{
		"list-resources": {Count: 2, TotalMs: 2000, MaxMs: 1250},
	}, got.connectorCallStats())

	durations := got.stepDurations()
	durations["list-resources"] = 0
	callStats := got.connectorCallStats()
	callStats["list-resources"] = ConnectorCallStat{}
	require.EqualValues(t, 2000, got.stepDurations()["list-resources"])
	require.EqualValues(t, 2, got.connectorCallStats()["list-resources"].Count)
}

func TestSyncerTokenSessionStatsMarshalUnmarshal(t *testing.T) {
	run, stats, _ := newTestRun()
	stats.recordSessionOp("get", 30*time.Second, context.DeadlineExceeded, true)
	stats.recordSessionOp("get", time.Millisecond, nil, false)
	stats.recordSessionOp("set", 5*time.Millisecond, errors.New("boom"), false)

	tokenString := encodeTestRun(t, run, stats)

	_, got, _ := decodeTestRun(t, tokenString)
	sessionStats := got.sessionStoreStats()
	require.Equal(t, SessionStoreStat{Count: 2, Errors: 1, Timeouts: 1, TotalMs: 30_001, MaxMs: 30_000}, sessionStats["get"])
	require.Equal(t, SessionStoreStat{Count: 1, Errors: 1, TotalMs: 5, MaxMs: 5}, sessionStats["set"])

	// Legacy tokens without the field yield an empty-but-usable map.
	legacy, err := json.Marshal(serializedTokenV1{Version: StateTokenVersion})
	require.NoError(t, err)
	_, fresh, _ := decodeTestRun(t, string(legacy))
	require.Empty(t, fresh.sessionStoreStats())
	fresh.recordSessionOp("get", time.Millisecond, nil, false)
	require.EqualValues(t, 1, fresh.sessionStoreStats()["get"].Count)
}

func TestSyncerTokenLegacyTimingStatsAreUsable(t *testing.T) {
	tokenBytes, err := json.Marshal(serializedTokenV1{Version: StateTokenVersion})
	require.NoError(t, err)

	_, stats, _ := decodeTestRun(t, string(tokenBytes))
	require.Empty(t, stats.stepDurations())
	require.Empty(t, stats.connectorCallStats())

	stats.addStepDuration("checkpoint", time.Millisecond)
	stats.recordConnectorCall("list-grants", time.Millisecond)
	require.EqualValues(t, 1, stats.stepDurations()["checkpoint"])
	require.EqualValues(t, 1, stats.connectorCallStats()["list-grants"].Count)
}

func TestSyncerTokenUnmarshalEmptyString(t *testing.T) {
	ctx := t.Context()
	run, _, _ := decodeTestRun(t, "")
	op1 := Action{Op: InitOp}

	// The empty token seeds an InitOp, so the pushed one lands on top of it.
	require.Equal(t, InitOp, run.current().Op)
	run.pushAction(ctx, op1)
	compareSyncerState(t, op1, *run.current())
}

func TestPrepareExpansionReplayTokenPreservesState(t *testing.T) {
	run, stats, _ := newTestRun()
	run.setFact(factShouldSkipGrants)
	stats.addStepDuration("checkpoint", time.Millisecond)
	require.False(t, run.hasFact(factNeedsExpansion))

	token := encodeTestRun(t, run, stats)

	replayToken, err := PrepareExpansionReplayToken(token)
	require.NoError(t, err)

	replayRun, replayStats, _ := decodeTestRun(t, replayToken)

	// The fact the rollback sets.
	require.True(t, replayRun.hasFact(factNeedsExpansion), "expansion must be re-flagged")
	// The rest of the token must survive rather than be cleared.
	require.True(t, replayRun.hasFact(factShouldSkipGrants), "skip-grants fact must be preserved")
	require.EqualValues(t, 1, replayStats.stepDurations()["checkpoint"], "preserved step durations must survive the rewrite")
	// A finished token has no current action, so an InitOp is queued to drive
	// the resumed run.
	require.NotNil(t, replayRun.current())
	require.Equal(t, InitOp, replayRun.current().Op)
}

func TestSyncerTokenNextPage(t *testing.T) {
	ctx := t.Context()
	st := newRunState()
	op1 := Action{Op: InitOp}
	op2 := Action{Op: InitOp, PageToken: "next-page"}

	st.pushAction(ctx, op1)
	compareSyncerState(t, op1, *st.current())
	require.Len(t, st.actions, 1)

	err := st.nextPage(ctx, st.current().ID, "next-page")
	require.NoError(t, err)
	require.Len(t, st.actions, 1)
	compareSyncerState(t, op2, *st.current())
}

func TestPeekMatchingActionsCapsBatchesAndDrainsRemainder(t *testing.T) {
	ctx := t.Context()
	st := newEmptySchedulerState(t)
	for range maxPeekActionsCount + 5 {
		st.pushAction(ctx, Action{Op: SyncGrantsOp})
	}

	firstBatch := st.peekMatchingActions(ctx, SyncGrantsOp)
	require.Len(t, firstBatch, maxPeekActionsCount)
	for _, action := range firstBatch {
		st.finishAction(ctx, action)
	}

	require.Len(t, st.peekMatchingActions(ctx, SyncGrantsOp), 5)
}

func TestPeekMatchingActionsStopsAtDifferentOperation(t *testing.T) {
	ctx := t.Context()
	st := newEmptySchedulerState(t)
	st.pushAction(ctx, Action{Op: SyncEntitlementsOp})
	st.pushAction(ctx, Action{Op: SyncGrantsOp, ResourceID: "first"})
	st.pushAction(ctx, Action{Op: SyncGrantsOp, ResourceID: "second"})

	actions := st.peekMatchingActions(ctx, SyncGrantsOp)
	require.Len(t, actions, 2)
	require.Equal(t, "second", actions[0].ResourceID)
	require.Equal(t, "first", actions[1].ResourceID)
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

	run, stats, _ := decodeTestRun(t, string(tokenV0Bytes))
	tokenV1String := encodeTestRun(t, run, stats)
	require.NotEmpty(t, tokenV1String)
	tokenV1 := serializedTokenV1{}
	err = json.Unmarshal([]byte(tokenV1String), &tokenV1)
	require.NoError(t, err)

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

	run, _, _ := decodeTestRun(t, string(tokenV0Bytes))

	// Current should be the old CurrentAction (top of stack)
	require.NotNil(t, run.current())
	require.Equal(t, SyncResourcesOp, run.current().Op)

	// This must not panic. After migrating a V0 token, pushing a new action
	// should produce a fresh ID that doesn't collide with any migrated ID.
	run.pushAction(ctx, Action{Op: SyncEntitlementsOp})

	require.Equal(t, SyncEntitlementsOp, run.current().Op)
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

	// Migrate v0 -> v1 through unmarshalToken.
	run, stats, _ := decodeTestRun(t, tokenStr)

	// Verify the migrated run has the correct current action (top of stack).
	require.NotNil(t, run.current())
	require.Equal(t, SyncEntitlementsOp, run.current().Op)
	require.Equal(t, "0000000002", run.current().ID)

	// Encode back to v1 and validate the full structure.
	v1Str := encodeTestRun(t, run, stats)
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

// buildLoadedGraphRun returns a run mid-grant-expansion: a populated
// entitlement graph and a SyncGrantExpansionOp action paginating the load,
// stacked on a non-expansion action whose own pagination must survive the
// checkpoint normalization untouched.
func buildLoadedGraphRun(t *testing.T, ctx context.Context, pageToken string) (*runState, *runStats, *expansionGraph) {
	t.Helper()
	run, stats, holder := newTestRun()

	run.pushAction(ctx, Action{Op: SyncGrantsOp})
	require.NoError(t, run.nextPage(ctx, run.current().ID, "grants-p9"))

	run.pushAction(ctx, Action{Op: SyncGrantExpansionOp})
	if pageToken != "" {
		require.NoError(t, run.nextPage(ctx, run.current().ID, pageToken))
	}

	graph := holder.get(ctx)
	require.NotNil(t, graph)
	graph.AddEntitlementID("ent1")
	graph.AddEntitlementID("ent2")
	graph.AddEntitlementID("ent3")
	require.NoError(t, graph.AddEdge(ctx, "ent1", "ent2", false, []string{"user"}))
	require.NoError(t, graph.AddEdge(ctx, "ent2", "ent3", true, []string{"group"}))
	graph.Depth = 5
	return run, stats, holder
}

// The entitlement graph is a projection of store data and is deliberately
// omitted from checkpoints (see marshalToken). A resumed run must instead
// restart the expansion load from the first page, so the serialized expansion
// action's page token is blanked while the live run keeps paginating.
func TestSyncerTokenOmitsEntitlementGraph(t *testing.T) {
	ctx := t.Context()
	run, stats, holder := buildLoadedGraphRun(t, ctx, "page37")

	tokenString := encodeTestRun(t, run, stats)
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

	// Encoding must not mutate the live run: the in-process sync keeps its
	// graph and continues from its current page.
	require.NotNil(t, holder.peek())
	require.Equal(t, "page37", run.current().PageToken)

	// A resumed run starts expansion over: fresh graph, first page — with
	// the rest of the action stack intact.
	resumed, _, resumedHolder := decodeTestRun(t, tokenString)
	require.Nil(t, resumedHolder.peek())
	require.Equal(t, SyncGrantExpansionOp, resumed.current().Op)
	require.Empty(t, resumed.current().PageToken)
	require.False(t, resumedHolder.get(ctx).Loaded)
	require.Len(t, resumed.actions, 2)
	for _, a := range resumed.actions {
		if a.Op == SyncGrantsOp {
			require.Equal(t, "grants-p9", a.PageToken)
		}
	}
}

// Graph omission rewrites the serialized actions map, and the type-scoped
// version stamp is computed from it. The rewrite must not drop the markers that
// drive the stamp: a token that lands on version 1 while carrying them is
// silently misparsed by an older SDK.
func TestSyncerTokenVersionStampSurvivesGraphOmission(t *testing.T) {
	ctx := t.Context()
	run, stats, _ := buildLoadedGraphRun(t, ctx, "page37")

	// A type-scoped marker alongside the in-flight expansion that triggers
	// the actions-map rewrite.
	for id, a := range run.actions {
		if a.Op == SyncGrantsOp {
			a.TypeScoped = true
			run.actions[id] = a
		}
	}

	tokenString := encodeTestRun(t, run, stats)

	var raw serializedTokenV1
	require.NoError(t, json.Unmarshal([]byte(tokenString), &raw))
	require.Nil(t, raw.EntitlementGraph, "graph still omitted")
	require.EqualValues(t, StateTokenVersionTypeScoped, raw.Version,
		"type-scoped marker must still stamp version 2 through the rewrite")
	for _, a := range raw.ActionsMap {
		if a.Op == SyncGrantsOp {
			require.True(t, a.TypeScoped, "rewrite must preserve non-PageToken action fields")
		}
	}
}

// Tokens written by older SDKs carry the graph inline. They must decode and
// resume exactly as before: graph restored, pagination preserved.
func TestSyncerTokenLegacyInlineGraphStillDecodes(t *testing.T) {
	ctx := t.Context()
	run, _, holder := buildLoadedGraphRun(t, ctx, "page37")
	holder.peek().Loaded = true
	holder.peek().HasNoCycles = true

	// Serialize the way pre-omission SDKs did: graph inline, page token kept.
	legacy := marshalLegacyInlineGraphToken(t, run, holder.peek())

	resumed, resumedStats, resumedHolder := decodeTestRun(t, legacy)

	restored := resumedHolder.peek()
	require.NotNil(t, restored, "inline graph must be restored")
	require.Len(t, restored.Nodes, 3)
	require.Len(t, restored.Edges, 2)
	require.Equal(t, 5, restored.Depth)
	require.True(t, restored.Loaded)
	require.True(t, restored.HasNoCycles)
	require.Equal(t, holder.peek().NextNodeID, restored.NextNodeID)
	require.Equal(t, holder.peek().NextEdgeID, restored.NextEdgeID)
	// With the graph present, the pagination is still valid and must survive.
	require.Equal(t, "page37", resumed.current().PageToken)

	// The upgrade chain: the next checkpoint after resuming a legacy token
	// must write the new format — graph omitted AND the in-flight expansion
	// pagination blanked (a graph-less token carrying the page token would be
	// exactly the orphan shape decoding defends against) — while the live
	// resumed run keeps both and continues unaffected.
	out := encodeTestRun(t, resumed, resumedStats)
	var reserialized serializedTokenV1
	require.NoError(t, json.Unmarshal([]byte(out), &reserialized))
	require.Nil(t, reserialized.EntitlementGraph)
	for _, a := range reserialized.ActionsMap {
		if a.Op == SyncGrantExpansionOp {
			require.Empty(t, a.PageToken)
		}
	}
	require.NotNil(t, resumedHolder.peek())
	require.Equal(t, "page37", resumed.current().PageToken)
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

	resumed, _, resumedHolder := decodeTestRun(t, string(legacy))
	require.Nil(t, resumedHolder.peek())
	require.Equal(t, SyncGrantExpansionOp, resumed.current().Op)
	require.Empty(t, resumed.current().PageToken)
	require.Len(t, resumed.actions, 2)
	for _, a := range resumed.actions {
		if a.Op == SyncGrantsOp {
			require.Equal(t, "grants-p9", a.PageToken)
		}
	}
}

// A graph-less token whose expansion action still carries a page token (a
// writer that did not normalize) cannot safely resume that pagination —
// decoding must blank it so the load restarts from the first page.
func TestSyncerTokenUnmarshalBlanksOrphanExpansionPageToken(t *testing.T) {
	ctx := t.Context()
	st := newRunState()
	st.pushAction(ctx, Action{Op: SyncGrantExpansionOp})
	require.NoError(t, st.nextPage(ctx, st.current().ID, "page37"))

	orphan, err := json.Marshal(serializedTokenV1{
		ActionsMap:      st.actions,
		ActionOrder:     st.actionOrder,
		CurrentActionID: st.currentActionID,
		Version:         1,
	})
	require.NoError(t, err)

	resumed, _, resumedHolder := decodeTestRun(t, string(orphan))
	require.Nil(t, resumedHolder.peek())
	require.Equal(t, SyncGrantExpansionOp, resumed.current().Op)
	require.Empty(t, resumed.current().PageToken)
}

func TestStateIngestQualityRoundTripPreservesCleanPresence(t *testing.T) {
	for _, quality := range []*IngestQualityCheckpoint{
		{},
		{
			SourceCacheReplayBlocked:      true,
			EntitlementsDropped:           1,
			GrantsDropped:                 2,
			GrantResourcesDropped:         3,
			ExpansionResourceTypesDropped: 4,
			ExpansionsDropped:             5,
			InvalidResourceTypesObserved:  6,
			InvalidResourcesObserved:      7,
			InvalidEntitlementsObserved:   8,
			ReasonFlags:                   63,
		},
	} {
		run, stats, _ := decodeTestRun(t, "")
		stats.setIngestQuality(quality)

		token := encodeTestRun(t, run, stats)
		require.Contains(t, token, `"ingest_quality"`)

		_, resumedStats, _ := decodeTestRun(t, token)
		require.Equal(t, quality, resumedStats.ingestQuality())
	}
}

func TestStateLegacyTokenLeavesIngestQualityUnknown(t *testing.T) {
	_, stats, _ := decodeTestRun(t, `{"version":1}`)
	require.Nil(t, stats.ingestQuality())
}
