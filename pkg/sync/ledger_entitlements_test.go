package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"sync"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/sync/progresslog"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/anypb"
)

type ledgerEntitlementsConnector struct {
	*mockConnector
	mu        sync.Mutex
	requests  []*v2.EntitlementsServiceListEntitlementsRequest
	spawned   bool
	duplicate bool
}

func (c *ledgerEntitlementsConnector) ListEntitlements(
	_ context.Context, req *v2.EntitlementsServiceListEntitlementsRequest, _ ...grpc.CallOption,
) (*v2.EntitlementsServiceListEntitlementsResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.requests = append(c.requests, req)
	resource := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "selected", Resource: "one"}.Build()}.Build()
	valid := v2.Entitlement_builder{Id: "valid" + req.GetPageToken(), Resource: resource}.Build()
	if req.GetPageToken() != "" {
		return v2.EntitlementsServiceListEntitlementsResponse_builder{List: []*v2.Entitlement{valid}}.Build(), nil
	}
	disabled := v2.Entitlement_builder{Id: "disabled", Resource: v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "disabled", Resource: "one"}.Build()}.Build()}.Build()
	var annos []*anypb.Any
	if c.spawned {
		token := "sibling"
		if c.duplicate {
			token = "next"
		}
		a, err := anypb.New(v2.EnqueuePageTokens_builder{PageTokens: []string{token}}.Build())
		if err != nil {
			return nil, err
		}
		annos = append(annos, a)
	}
	return v2.EntitlementsServiceListEntitlementsResponse_builder{
		List: []*v2.Entitlement{nil, {Id: "missing-resource"}, valid, disabled}, NextPageToken: "next", Annotations: annos,
	}.Build(), nil
}
func entitlementPageFixture(t *testing.T, scoped bool) (*syncer, *ledgerFixture, *ledgerEntitlementsConnector) {
	t.Helper()
	s, f := newLedgerSchedulerFixture(t, 4)
	s.counts = progresslog.NewProgressCounts(t.Context())
	s.recordStats = true
	s.cfg.syncType = connectorstore.SyncTypeFull
	s.ingestFilterStats.markKnown()
	s.ledger.facts[ledgerFactIngestKnown] = ""
	require.NoError(t, f.store.PutResourceTypes(t.Context(), &v2.ResourceType{Id: "selected"}))
	resource := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "selected", Resource: "one"}.Build()}.Build()
	require.NoError(t, f.store.PutResources(t.Context(), resource))
	c := &ledgerEntitlementsConnector{mockConnector: &mockConnector{}, spawned: scoped}
	s.connector = c
	action := Action{Op: SyncEntitlementsOp, ResourceTypeID: "selected", ResourceID: "one", TypeScoped: scoped}
	if scoped {
		action.ResourceID = ""
	}
	s.run.pushAction(t.Context(), action)
	return s, f, c
}
func TestLedgerEntitlementPages(t *testing.T) {
	for _, scoped := range []bool{false, true} {
		name := "resource"
		if scoped {
			name = "type"
		}
		t.Run(name, func(t *testing.T) {
			s, f, c := entitlementPageFixture(t, scoped)
			f.audit.enter(ledgerHandler)
			_, err := runLedgerTestSync(t, s, t.Context(), t.Context(), nil)
			require.NoError(t, err)
			f.audit.enter(ledgerLifecycle)
			expected := 2
			if scoped {
				expected++
			}
			require.Len(t, c.requests, expected)
			for _, req := range c.requests {
				annos := annotations.Annotations(req.GetAnnotations())
				require.Equal(t, scoped, annos.Contains(&v2.TypeScopedEntitlements{}))
			}
			counters, err := f.ledger.LedgerCounters(t.Context())
			require.NoError(t, err)
			require.EqualValues(t, expected, counters.ConnectorCalls["list-entitlements"].Count)
			require.EqualValues(t, 1, counters.Counters["ingest.entitlements_dropped"])
			require.EqualValues(t, 2, counters.Counters["ingest.invalid_entitlements_observed"])
			require.Equal(t, ingestQualityReasonEntitlementDropped, counters.Flags)
			require.NoError(t, f.store.Close(t.Context()))
			f = openLedgerFixtureAt(t, f.path, false)
			reopenedCounters, err := f.ledger.LedgerCounters(t.Context())
			require.NoError(t, err)
			require.Equal(t, counters, reopenedCounters)
			facts, err := f.ledger.LedgerFacts(t.Context())
			require.NoError(t, err)
			require.Contains(t, facts, ledgerFactIngestKnown)
			require.Contains(t, facts, ledgerFactIngestBlocked)
			resp, err := f.store.ListEntitlements(t.Context(), &v2.EntitlementsServiceListEntitlementsRequest{})
			require.NoError(t, err)
			require.Len(t, resp.GetList(), expected)
		})
	}
}
func TestLedgerEntitlementCommitFailureRetry(t *testing.T) {
	s, f, c := entitlementPageFixture(t, false)
	seedLedgerTestRun(t, s, nil)
	before := ledgerRawSnapshot(t, f.engine)
	s.ledger.store = ledgerFailingPageStore{PageLedgerStore: f.ledger, stage: "commit"}
	action := s.run.current()
	f.audit.enter(ledgerHandler)
	require.ErrorIs(t, invokeLedgerTestPage(t, s, t.Context(), action, s.SyncEntitlements, false), errLedgerInjectedPage)
	f.audit.enter(ledgerLifecycle)
	require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
	require.Zero(t, s.ingestFilterStats.entitlementsDropped.Load())
	require.False(t, s.ingestFilterStats.replayBlocked.Load())
	require.Empty(t, s.stats.connectorCallStats())
	s.ledger.store = f.ledger
	f.audit.enter(ledgerHandler)
	_, err := runLedgerTestSync(t, s, t.Context(), t.Context(), nil)
	require.NoError(t, err)
	f.audit.enter(ledgerLifecycle)
	require.Len(t, c.requests, 3)
	require.EqualValues(t, 1, s.ingestFilterStats.entitlementsDropped.Load())
	require.True(t, s.ingestFilterStats.replayBlocked.Load())
	counters, err := f.ledger.LedgerCounters(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 2, counters.ConnectorCalls["list-entitlements"].Count)
}

type ledgerEntitlementPlannerStore struct{ c1zstore.Store }

func (s ledgerEntitlementPlannerStore) ListResources(_ context.Context, req *v2.ResourcesServiceListResourcesRequest) (*v2.ResourcesServiceListResourcesResponse, error) {
	next := ""
	if req.GetPageToken() == "" {
		next = "store-next"
	}
	return v2.ResourcesServiceListResourcesResponse_builder{NextPageToken: next}.Build(), nil
}
func TestLedgerEntitlementPlannerCommitAndReplay(t *testing.T) {
	s, f, _ := entitlementPageFixture(t, true)
	marker, err := anypb.New(&v2.TypeScopedEntitlements{})
	require.NoError(t, err)
	require.NoError(t, f.store.PutResourceTypes(t.Context(), v2.ResourceType_builder{Id: "selected", Annotations: []*anypb.Any{marker}}.Build()))
	s.store = ledgerEntitlementPlannerStore{Store: f.store}
	s.run = newRunState()
	s.run.pushAction(t.Context(), Action{Op: SyncEntitlementsOp})
	action := s.run.current()
	s.ledger.store = ledgerFailingPageStore{PageLedgerStore: f.ledger, stage: "commit"}
	f.audit.enter(ledgerHandler)
	require.ErrorIs(t, invokeLedgerTestPage(t, s, t.Context(), action, s.SyncEntitlements, false), errLedgerInjectedPage)
	f.audit.enter(ledgerLifecycle)
	require.False(t, s.run.getAction(action.ID).TypeScopedPlanned)
	s.ledger.store = f.ledger
	f.audit.enter(ledgerHandler)
	require.NoError(t, invokeLedgerTestPage(t, s, t.Context(), action, s.SyncEntitlements, false))
	f.audit.enter(ledgerLifecycle)
	continued := s.run.getAction(action.ID)
	require.True(t, continued.TypeScopedPlanned)
	require.Equal(t, "store-next", continued.PageToken)
	f.audit.enter(ledgerHandler)
	require.NoError(t, invokeLedgerTestPage(t, s, t.Context(), continued, s.SyncEntitlements, false))
	f.audit.enter(ledgerLifecycle)
	row, found, err := f.ledger.GetLedgerRow(t.Context(), ledgerIdentity(continued))
	require.NoError(t, err)
	require.True(t, found)
	require.Empty(t, row.Children)
	s.run = newRunState()
	before := ledgerRawSnapshot(t, f.engine)
	f.audit.enter(ledgerWalk)
	require.NoError(t, s.restoreLedgerState(t.Context(), ledgerResume{initialized: true}, false))
	f.audit.enter(ledgerLifecycle)
	require.Nil(t, s.run.getAction(action.ID))
	require.True(t, s.run.current().TypeScoped)
	require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
}

func TestLedgerEntitlementReplay(t *testing.T) {
	s, f, c := entitlementPageFixture(t, true)
	f.audit.enter(ledgerHandler)
	_, err := runLedgerTestSync(t, s, t.Context(), t.Context(), nil)
	require.NoError(t, err)
	f.audit.enter(ledgerLifecycle)
	require.NoError(t, f.store.Close(t.Context()))
	f = openLedgerFixtureAt(t, f.path, false)
	s.store, s.caps = f.store, resolveStoreCaps(f.store)
	s.ledger, err = newLedgerRuntime(t.Context(), f.ledger, "reopened-entitlements")
	require.NoError(t, err)
	s.run = newRunState()
	s.run.pushAction(t.Context(), Action{Op: SyncEntitlementsOp, ResourceTypeID: "selected", TypeScoped: true})
	seedLedgerTestRun(t, s, nil)
	before := ledgerRawSnapshot(t, f.engine)
	f.audit.enter(ledgerWalk)
	_, err = runLedgerTestSync(t, s, t.Context(), t.Context(), nil)
	require.NoError(t, err)
	f.audit.enter(ledgerLifecycle)
	require.Len(t, c.requests, 3)
	require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
}

func TestLedgerEntitlementPartialRetention(t *testing.T) {
	s, f, _ := entitlementPageFixture(t, false)
	s.cfg.syncType = connectorstore.SyncTypePartial
	f.audit.enter(ledgerHandler)
	_, err := runLedgerTestSync(t, s, t.Context(), t.Context(), nil)
	require.NoError(t, err)
	f.audit.enter(ledgerLifecycle)
	resp, err := f.store.ListEntitlements(t.Context(), &v2.EntitlementsServiceListEntitlementsRequest{})
	require.NoError(t, err)
	require.Len(t, resp.GetList(), 3)
	counters, err := f.ledger.LedgerCounters(t.Context())
	require.NoError(t, err)
	require.Zero(t, counters.Counters["ingest.entitlements_dropped"])
	facts, err := f.ledger.LedgerFacts(t.Context())
	require.NoError(t, err)
	require.NotContains(t, facts, ledgerFactIngestBlocked)
}

func TestLedgerEntitlementDuplicateCursor(t *testing.T) {
	s, f, c := entitlementPageFixture(t, true)
	c.duplicate = true
	seedLedgerTestRun(t, s, nil)
	before := ledgerRawSnapshot(t, f.engine)
	f.audit.enter(ledgerHandler)
	_, err := runLedgerTestSync(t, s, t.Context(), t.Context(), nil)
	require.ErrorContains(t, err, "duplicate or cyclic spawned cursor")
	f.audit.enter(ledgerLifecycle)
	require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
	require.False(t, s.ingestFilterStats.replayBlocked.Load())
}

func TestLedgerEntitlementReadFailures(t *testing.T) {
	for _, kind := range []string{"resource", "type"} {
		t.Run(kind, func(t *testing.T) {
			s, f, _ := entitlementPageFixture(t, false)
			if kind == "resource" {
				s.store = ledgerResourceReadFailure{Store: f.store}
			} else {
				s.store = ledgerTypeReadFailure{Store: f.store}
			}
			seedLedgerTestRun(t, s, nil)
			before := ledgerRawSnapshot(t, f.engine)
			f.audit.enter(ledgerHandler)
			require.ErrorIs(t, invokeLedgerTestPage(t, s, t.Context(), s.run.current(), s.SyncEntitlements, false), errLedgerInjectedPage)
			f.audit.enter(ledgerLifecycle)
			require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
			require.False(t, s.ingestFilterStats.replayBlocked.Load())
			require.Empty(t, s.stats.connectorCallStats())
		})
	}
}
