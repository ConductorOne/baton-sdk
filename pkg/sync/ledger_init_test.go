package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/stretchr/testify/require"
)

type initialActionCase struct {
	name      string
	cfg       syncConfig
	external  bool
	priorSkip bool
	targeted  bool
	ops       []ActionOp
}

func initialActionCases() []initialActionCase {
	return []initialActionCase{
		{name: "full", ops: []ActionOp{SyncGrantExpansionOp, SyncGrantsOp, SyncEntitlementsOp, SyncStaticEntitlementsOp, SyncResourcesOp, SyncResourceTypesOp}},
		{name: "skip-grants", cfg: syncConfig{skipGrants: true}, ops: []ActionOp{SyncGrantExpansionOp, SyncEntitlementsOp, SyncStaticEntitlementsOp, SyncResourcesOp, SyncResourceTypesOp}},
		{name: "skip-entitlements", cfg: syncConfig{skipEntitlementsAndGrants: true}, ops: []ActionOp{SyncResourcesOp, SyncResourceTypesOp}},
		{name: "expand-only", cfg: syncConfig{onlyExpandGrants: true}, ops: []ActionOp{SyncGrantExpansionOp}},
		{name: "expand-and-external", cfg: syncConfig{onlyExpandGrants: true}, external: true, ops: []ActionOp{SyncGrantExpansionOp, SyncExternalResourcesOp}},
		{name: "prior-skip", cfg: syncConfig{onlyExpandGrants: true}, priorSkip: true, ops: []ActionOp{}},
		{name: "targeted", cfg: syncConfig{onlyExpandGrants: true}, external: true, targeted: true, ops: []ActionOp{SyncTargetedResourceOp, SyncResourceTypesOp}},
		{name: "deferred-expansion", cfg: syncConfig{dontExpandGrants: true},
			ops: []ActionOp{SyncGrantExpansionOp, SyncGrantsOp, SyncEntitlementsOp, SyncStaticEntitlementsOp, SyncResourcesOp, SyncResourceTypesOp}},
	}
}

func configureInitialActionCase(t *testing.T, s *syncer, f *ledgerFixture, tc initialActionCase) []*v2.Resource {
	t.Helper()
	s.cfg = tc.cfg
	s.cfg.workerCount = 1
	if tc.external {
		s.externalResourceReader = f.store
	}
	if tc.priorSkip {
		s.run.setFact(factShouldSkipEntitlementsAndGrants)
	}
	s.run.pushAction(t.Context(), Action{Op: InitOp})
	if tc.targeted {
		return []*v2.Resource{v2.Resource_builder{
			Id:               v2.ResourceId_builder{ResourceType: "type", Resource: "target"}.Build(),
			ParentResourceId: v2.ResourceId_builder{ResourceType: "parent-type", Resource: "parent"}.Build(),
		}.Build()}
	}
	return nil
}

func assertInitialActionCase(t *testing.T, s *syncer, tc initialActionCase) {
	t.Helper()
	ops := make([]ActionOp, 0, len(s.run.actionOrder))
	for _, id := range s.run.actionOrder {
		ops = append(ops, s.run.actions[id].Op)
	}
	require.Equal(t, tc.ops, ops)
	require.EqualValues(t, 1, s.run.completedActionsCount())
	require.Equal(t, tc.cfg.skipEntitlementsAndGrants || tc.priorSkip, s.run.hasFact(factShouldSkipEntitlementsAndGrants))
	require.Equal(t, tc.cfg.skipGrants, s.run.hasFact(factShouldSkipGrants))
	require.Equal(t, tc.targeted, s.run.hasFact(factShouldFetchRelatedResources))
	require.Equal(t, tc.cfg.onlyExpandGrants && !tc.targeted, s.run.hasFact(factNeedsExpansion))
	if tc.targeted {
		action := s.run.actions[s.run.actionOrder[0]]
		require.Equal(t, "target", action.ResourceID)
		require.Equal(t, "parent-type", action.ParentResourceTypeID)
		require.Equal(t, "parent", action.ParentResourceID)
	}
}

func TestInitialActionBaseline(t *testing.T) {
	for _, tc := range initialActionCases() {
		t.Run(tc.name, func(t *testing.T) {
			s, f := newLedgerSchedulerFixture(t, 1)
			s.ledgered = false
			targeted := configureInitialActionCase(t, s, f, tc)
			runCtx, cancel := context.WithCancel(t.Context())
			defer cancel()
			s.testHooks.checkpointHook = func(string) {
				if current := s.run.current(); current == nil || current.Op != InitOp {
					cancel()
				}
			}
			_, err := runLedgerTestSync(t, s, t.Context(), runCtx, targeted)
			if len(tc.ops) > 0 {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.NoError(t, err)
			}
			assertInitialActionCase(t, s, tc)
		})
	}
}

func TestLedgerInitialActionCommitFailureIsAtomic(t *testing.T) {
	for _, stage := range []string{"fact", "counter", "commit"} {
		t.Run(stage, func(t *testing.T) {
			s, f := newLedgerSchedulerFixture(t, 1)
			configureInitialActionCase(t, s, f, initialActionCase{cfg: syncConfig{skipGrants: true}})
			s.ledger.store = ledgerFailingPageStore{PageLedgerStore: f.ledger, stage: stage}
			seedLedgerTestRun(t, s, nil)
			before := ledgerRawSnapshot(t, f.engine)
			f.audit.enter(ledgerHandler)
			_, err := runLedgerTestSync(t, s, t.Context(), t.Context(), nil)
			f.audit.enter(ledgerLifecycle)
			require.ErrorIs(t, err, errLedgerInjectedPage)
			require.Equal(t, InitOp, s.run.current().Op)
			require.False(t, s.run.hasFact(factShouldSkipGrants))
			require.Zero(t, s.run.completedActionsCount())
			require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
			require.Zero(t, f.audit.writers)
		})
	}
}

func TestLedgerInitialActionMatchesBaseline(t *testing.T) {
	for _, tc := range initialActionCases() {
		t.Run(tc.name, func(t *testing.T) {
			s, f := newLedgerSchedulerFixture(t, 1)
			targeted := configureInitialActionCase(t, s, f, tc)
			action := s.run.current()
			f.audit.enter(ledgerHandler)
			err := invokeLedgerTestPage(t, s, t.Context(), action, func(ctx context.Context, action *Action) error {
				return s.initializeAction(ctx, action, targeted)
			}, false)
			f.audit.enter(ledgerLifecycle)
			require.NoError(t, err)
			assertInitialActionCase(t, s, tc)
			row, found, err := f.ledger.GetLedgerRow(t.Context(), ledgerIdentity(action))
			require.NoError(t, err)
			require.True(t, found)
			require.Len(t, row.Children, len(tc.ops))
			for i, child := range row.Children {
				planned := s.run.actions[s.run.actionOrder[i]]
				require.Equal(t, ledgerIdentity(&planned), child.Identity)
			}
			facts, err := f.ledger.LedgerFacts(t.Context())
			require.NoError(t, err)
			for _, fact := range []string{factShouldSkipGrants, factShouldFetchRelatedResources, factNeedsExpansion} {
				_, found := facts[fact]
				require.Equal(t, s.run.hasFact(fact), found)
			}
			counters, err := f.ledger.LedgerCounters(t.Context())
			require.NoError(t, err)
			require.EqualValues(t, 1, counters.Counters[ledgerCompletedActions])
		})
	}
}

func TestLedgerInitialQualitySurvivesResume(t *testing.T) {
	for _, fresh := range []bool{true, false} {
		t.Run(map[bool]string{true: "fresh", false: "unknown-prior"}[fresh], func(t *testing.T) {
			s, f := newLedgerSchedulerFixture(t, 1)
			roots := []ledgerAction{{identity: c1zstore.LedgerActionIdentity{Op: InitOp.String()}}}
			require.NoError(t, restoreLedgerTestState(t, s, t.Context(), ledgerResume{actions: roots}, fresh))
			seedLedgerTestRun(t, s, nil)
			before := ledgerRawSnapshot(t, f.engine)
			action := s.run.current()
			f.audit.enter(ledgerHandler)
			err := invokeLedgerTestPage(t, s, t.Context(), action, func(ctx context.Context, action *Action) error {
				return s.initializeAction(ctx, action, nil)
			}, false)
			f.audit.enter(ledgerLifecycle)
			require.NoError(t, err)
			require.False(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
			require.NoError(t, f.store.Close(t.Context()))
			f = openLedgerFixtureAt(t, f.path, false)
			facts, err := f.ledger.LedgerFacts(t.Context())
			require.NoError(t, err)
			_, known := facts[ledgerFactIngestKnown]
			require.Equal(t, fresh, known)
			_, found, err := f.ledger.GetLedgerRow(t.Context(), ledgerIdentity(action))
			require.NoError(t, err)
			require.True(t, found)
			runtime, err := newLedgerRuntime(t.Context(), f.ledger, "quality-resume")
			require.NoError(t, err)
			resumed := &syncer{ledgered: true, ledger: runtime}
			before = ledgerRawSnapshot(t, f.engine)
			f.audit.enter(ledgerWalk)
			err = restoreLedgerTestState(t, resumed, t.Context(), ledgerResume{actions: roots}, false)
			f.audit.enter(ledgerLifecycle)
			require.NoError(t, err)
			require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
			quality := resumed.stats.ingestQuality()
			filterQuality := resumed.ingestFilterStats.snapshot()
			require.NotNil(t, filterQuality)
			require.Equal(t, !fresh, filterQuality.SourceCacheReplayBlocked)
			if fresh {
				require.Equal(t, &IngestQualityCheckpoint{}, quality)
			} else {
				require.Nil(t, quality)
				require.Equal(t, ingestQualityReasonUnknownPriorCheckpoint, filterQuality.ReasonFlags)
			}
		})
	}
}

func TestLedgerInitialQualityCommitFailure(t *testing.T) {
	for _, stage := range []string{"fact", "counter", "commit"} {
		t.Run(stage, func(t *testing.T) {
			s, f := newLedgerSchedulerFixture(t, 1)
			roots := []ledgerAction{{identity: c1zstore.LedgerActionIdentity{Op: InitOp.String()}}}
			require.NoError(t, restoreLedgerTestState(t, s, t.Context(), ledgerResume{actions: roots}, true))
			s.ledger.store = ledgerFailingPageStore{PageLedgerStore: f.ledger, stage: stage}
			seedLedgerTestRun(t, s, nil)
			before := ledgerRawSnapshot(t, f.engine)
			f.audit.enter(ledgerHandler)
			_, err := runLedgerTestSync(t, s, t.Context(), t.Context(), nil)
			f.audit.enter(ledgerLifecycle)
			require.ErrorIs(t, err, errLedgerInjectedPage)
			require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
			require.False(t, s.run.hasFact(ledgerFactIngestKnown))
			require.Equal(t, InitOp, s.run.current().Op)
			require.NoError(t, f.store.Close(t.Context()))
			f = openLedgerFixtureAt(t, f.path, false)
			facts, err := f.ledger.LedgerFacts(t.Context())
			require.NoError(t, err)
			require.NotContains(t, facts, ledgerFactIngestKnown)
			_, found, err := f.ledger.GetLedgerRow(t.Context(), roots[0].identity)
			require.NoError(t, err)
			require.False(t, found)
		})
	}
}
