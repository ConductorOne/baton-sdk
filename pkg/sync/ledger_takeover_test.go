package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/stretchr/testify/require"
)

func TestLedgerTakeoverLegacyFixtures(t *testing.T) {
	for _, name := range []string{
		"v0_current_action.json", "v0_no_current_action.json", "v1_actions_multi.json",
		"v1_facts_all.json", "v1_run_stats.json", "v1_action_counts.json", "v2_type_scoped.json",
		"v1_inline_graph.json", "v1_compaction.json",
	} {
		t.Run(name, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join("testdata", "tokens", name))
			require.NoError(t, err)
			state := string(data)
			expected, expectedFacts, expectedCounters, err := decodeLedgerCheckpoint(state)
			require.NoError(t, err)
			f := newLedgerFixture(t)
			require.NoError(t, f.store.CheckpointSync(t.Context(), state))
			got, err := loadLedgerResume(t.Context(), f.store, f.ledger, "takeover-attempt")
			require.NoError(t, err)
			require.True(t, got.initialized)
			assertPendingCheckpointActions(t, f.ledger, expected.actions)
			token, err := f.store.CurrentSyncStep(t.Context())
			require.NoError(t, err)
			require.Empty(t, token)
			frontier, found, err := f.ledger.LedgerFrontier(t.Context())
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, state, frontier.State)
			facts, err := f.ledger.LedgerFacts(t.Context())
			require.NoError(t, err)
			for _, fact := range expectedFacts {
				require.Contains(t, facts, fact)
			}
			counters, err := f.ledger.LedgerCounters(t.Context())
			require.NoError(t, err)
			require.Equal(t, addLedgerCounters(c1zstore.LedgerCounters{}, expectedCounters), addLedgerCounters(c1zstore.LedgerCounters{}, counters))
			before := ledgerRawSnapshot(t, f.engine)
			f.audit.enter(ledgerWalk)
			for i := 0; i < 3; i++ {
				resumed, err := loadLedgerResume(t.Context(), f.store, f.ledger, "new-attempt")
				require.NoError(t, err)
				require.True(t, resumed.initialized)
				assertPendingCheckpointActions(t, f.ledger, expected.actions)
			}
			f.audit.enter(ledgerLifecycle)
			require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
		})
	}
}

func TestLedgerTakeoverRejectsInvalidStateBeforeConsumption(t *testing.T) {
	for _, state := range []string{
		"{", `{"version":1,"action_order":["missing"]}`,
		`{"version":1,"actions_map":{"a":{"operation":"materialize-static-entitlements"}},"action_order":["a"]}`,
		`{"version":1,"actions_map":{"a":{"operation":"not-supported"}},"action_order":["a"]}`,
	} {
		t.Run(state, func(t *testing.T) {
			f := newLedgerFixture(t)
			require.NoError(t, f.store.CheckpointSync(t.Context(), state))
			before := ledgerRawSnapshot(t, f.engine)
			_, err := loadLedgerResume(t.Context(), f.store, f.ledger, "attempt")
			require.ErrorContains(t, err, "invalid ledger resume")
			require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
		})
	}
}

func TestLedgerTakeoverCountersImportedOnlyWhenAbsent(t *testing.T) {
	for _, prior := range []c1zstore.LedgerCounters{
		{},
		{Counters: map[string]uint64{"existing": 9}},
		{ConnectorCalls: map[string]c1zstore.CallStat{"existing": {Count: 3, MaxMs: 12}}},
		{StepDurationsMs: map[string]int64{"existing": 7}},
		{SessionCalls: map[string]c1zstore.CallStat{"existing": {Count: 2, Timeouts: 1}}},
	} {
		f := newLedgerFixture(t)
		state := `{"version":1,"completed_actions_count":7,"connector_call_stats":{"list":{"count":2,"total_ms":9,"max_ms":6}},"ingest_quality":{}}`
		require.NoError(t, f.store.CheckpointSync(t.Context(), state))
		if !prior.IsZero() {
			require.NoError(t, f.ledger.PutCounterBucket(t.Context(), "prior", 2, prior))
		}
		_, err := loadLedgerResume(t.Context(), f.store, f.ledger, "import")
		require.NoError(t, err)
		imported, err := f.ledger.LedgerCounters(t.Context())
		require.NoError(t, err)
		if prior.IsZero() {
			require.Equal(t, uint64(7), imported.Counters[ledgerCompletedActions])
			require.Equal(t, int64(2), imported.ConnectorCalls["list"].Count)
		} else {
			require.Equal(t, addLedgerCounters(c1zstore.LedgerCounters{}, prior), addLedgerCounters(c1zstore.LedgerCounters{}, imported))
		}
		runtime, err := newTestLedgerRuntime(t.Context(), f.ledger, "worker-attempt")
		require.NoError(t, err)
		_, err = runtime.runPage(t.Context(), 0, c1zstore.LedgerActionIdentity{Op: "init"}, func(_ context.Context, page *ledgerPage) error {
			page.observations.Counters = map[string]uint64{ledgerCompletedActions: 1}
			return page.transition("")
		})
		require.NoError(t, err)
		for i := 0; i < 3; i++ {
			_, err = loadLedgerResume(t.Context(), f.store, f.ledger, "again")
			require.NoError(t, err)
		}
		after, err := f.ledger.LedgerCounters(t.Context())
		require.NoError(t, err)
		require.Equal(t, imported.Counters[ledgerCompletedActions]+1, after.Counters[ledgerCompletedActions])
	}
}

func TestLedgerTakeoverIngestQuality(t *testing.T) {
	for _, test := range []struct {
		quality        string
		known, blocked bool
		reasons        uint64
	}{
		{quality: "", blocked: true, reasons: ingestQualityReasonUnknownPriorCheckpoint},
		{quality: `,"ingest_quality":{}`, known: true},
		{quality: `,"ingest_quality":{"source_cache_replay_blocked":true,"reason_flags":8,"grants_dropped":4}`, known: true, blocked: true, reasons: 8},
	} {
		f := newLedgerFixture(t)
		require.NoError(t, f.store.CheckpointSync(t.Context(), `{"version":1`+test.quality+`}`))
		_, err := loadLedgerResume(t.Context(), f.store, f.ledger, "attempt")
		require.NoError(t, err)
		facts, err := f.ledger.LedgerFacts(t.Context())
		require.NoError(t, err)
		_, known := facts[ledgerFactIngestKnown]
		_, blocked := facts[ledgerFactIngestBlocked]
		require.Equal(t, test.known, known)
		require.Equal(t, test.blocked, blocked)
		counters, err := f.ledger.LedgerCounters(t.Context())
		require.NoError(t, err)
		require.Equal(t, test.reasons, counters.Flags)
		if test.reasons == 8 {
			require.Equal(t, uint64(4), counters.Counters["ingest.grants_dropped"])
		}
	}
}

type ledgerFrontierReadOverride struct {
	c1zstore.PageLedgerStore
	frontier *c1zstore.LedgerFrontier
	found    bool
	err      error
}

func (s ledgerFrontierReadOverride) LedgerFrontier(context.Context) (*c1zstore.LedgerFrontier, bool, error) {
	return s.frontier, s.found, s.err
}

func TestLedgerTakeoverRejectsConflictingAndEmptyFrontier(t *testing.T) {
	for _, token := range []string{"", `{"version":1}`} {
		f := newLedgerFixture(t)
		if token != "" {
			require.NoError(t, f.store.CheckpointSync(t.Context(), token))
		}
		source := ledgerFrontierReadOverride{PageLedgerStore: f.ledger, frontier: &c1zstore.LedgerFrontier{}, found: true}
		before := ledgerRawSnapshot(t, f.engine)
		_, err := loadLedgerResume(t.Context(), f.store, source, "attempt")
		require.Error(t, err)
		require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
	}
}

func TestLedgerTakeoverV0CursorAndParentIdentity(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("testdata", "tokens", "v0_current_action.json"))
	require.NoError(t, err)
	f := newLedgerFixture(t)
	require.NoError(t, f.store.CheckpointSync(t.Context(), string(data)))
	resume, err := loadLedgerResume(t.Context(), f.store, f.ledger, "attempt")
	require.NoError(t, err)
	require.True(t, resume.initialized)
	assertPendingCheckpointActions(t, f.ledger, []ledgerAction{
		{identity: c1zstore.LedgerActionIdentity{Op: "list-resource-types"}},
		{identity: c1zstore.LedgerActionIdentity{Op: "list-resources", ResourceTypeID: "group", PageToken: "p2"}},
		{identity: c1zstore.LedgerActionIdentity{Op: "list-grants", ResourceTypeID: "group", ResourceID: "grp-1", ParentResourceTypeID: "org", ParentResourceID: "org-1", PageToken: "g7"}},
	})
}

func assertPendingCheckpointActions(t *testing.T, store c1zstore.PageLedgerStore, expected []ledgerAction) {
	t.Helper()
	if len(expected) == 0 {
		expected = []ledgerAction{{identity: c1zstore.LedgerActionIdentity{Op: InitOp.String()}}}
	}
	pending, initialized, err := store.PendingWork(t.Context(), 0, 100)
	require.NoError(t, err)
	require.True(t, initialized)
	require.Len(t, pending, len(expected))
	for i, before := range expected {
		after := pending[len(pending)-1-i]
		if before.identity.Op == SyncGrantExpansionOp.String() {
			before.identity.PageToken = ""
		}
		require.Equal(t, before.identity, after.Action.Identity)
		require.Equal(t, before.spawned, after.Action.Spawned)
		require.Equal(t, before.typeScopedPlanned, after.TypeScopedPlanned)
		require.EqualValues(t, i+1, after.ID)
	}
}
