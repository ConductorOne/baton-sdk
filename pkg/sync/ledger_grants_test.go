package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"sync"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/sync/progresslog"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/anypb"
)

type ledgerGrantsConnector struct {
	*mockConnector
	mu       sync.Mutex
	requests []*v2.GrantsServiceListGrantsRequest
	gets     []*v2.ResourceGetterServiceGetResourceRequest
	grants   []*v2.Grant
	insert   bool
}

func (c *ledgerGrantsConnector) ListGrants(
	_ context.Context, req *v2.GrantsServiceListGrantsRequest, _ ...grpc.CallOption,
) (*v2.GrantsServiceListGrantsResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.requests = append(c.requests, req)
	if req.GetPageToken() != "" {
		return &v2.GrantsServiceListGrantsResponse{}, nil
	}
	var annos []*anypb.Any
	if c.insert {
		a, err := anypb.New(&v2.InsertResourceGrants{})
		if err != nil {
			return nil, err
		}
		annos = append(annos, a)
	}
	return v2.GrantsServiceListGrantsResponse_builder{List: c.grants, NextPageToken: "next", Annotations: annos}.Build(), nil
}
func (c *ledgerGrantsConnector) GetResource(
	_ context.Context, req *v2.ResourceGetterServiceGetResourceRequest, _ ...grpc.CallOption,
) (*v2.ResourceGetterServiceGetResourceResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.gets = append(c.gets, req)
	return v2.ResourceGetterServiceGetResourceResponse_builder{Resource: v2.Resource_builder{Id: req.GetResourceId(), DisplayName: "fetched"}.Build()}.Build(), nil
}
func ledgerGrant(id, resourceType, resourceID, principalType string) *v2.Grant {
	r := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: resourceType, Resource: resourceID}.Build(), DisplayName: "discovered"}.Build()
	return v2.Grant_builder{
		Id: id, Entitlement: v2.Entitlement_builder{Id: "entitlement-" + id, Resource: r}.Build(),
		Principal: v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: principalType, Resource: "one"}.Build()}.Build(),
	}.Build()
}
func grantPageFixture(t *testing.T, scoped bool) (*syncer, *ledgerFixture, *ledgerGrantsConnector) {
	t.Helper()
	s, f := newLedgerSchedulerFixture(t, 4)
	s.counts = progresslog.NewProgressCounts(t.Context())
	s.recordStats = true
	s.cfg.syncType = connectorstore.SyncTypeFull
	s.ingestFilterStats.markKnown()
	require.NoError(t, f.store.PutResourceTypes(t.Context(), &v2.ResourceType{Id: "selected"}))
	r := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "selected", Resource: "one"}.Build()}.Build()
	require.NoError(t, f.store.PutResources(t.Context(), r))
	c := &ledgerGrantsConnector{mockConnector: &mockConnector{}, insert: true, grants: []*v2.Grant{
		ledgerGrant("kept", "selected", "found-kept", "selected"), ledgerGrant("dropped", "selected", "found-dropped", "disabled"),
	}}
	expandable, err := anypb.New(v2.GrantExpandable_builder{EntitlementIds: []string{"source"}, ResourceTypeIds: []string{"selected", "disabled"}}.Build())
	require.NoError(t, err)
	c.grants[0].SetAnnotations([]*anypb.Any{expandable})
	s.connector = c
	action := Action{Op: SyncGrantsOp, ResourceTypeID: "selected", ResourceID: "one", TypeScoped: scoped}
	if scoped {
		action.ResourceID = ""
	}
	s.run.pushAction(t.Context(), action)
	return s, f, c
}
func TestLedgerGrantPages(t *testing.T) {
	for _, scoped := range []bool{false, true} {
		name := "resource"
		if scoped {
			name = "type"
		}
		t.Run(name, func(t *testing.T) {
			s, f, c := grantPageFixture(t, scoped)
			f.audit.enter(ledgerHandler)
			_, err := runLedgerTestSync(t, s, t.Context(), t.Context(), nil)
			require.NoError(t, err)
			f.audit.enter(ledgerLifecycle)
			require.Len(t, c.requests, 2)
			for _, req := range c.requests {
				annos := annotations.Annotations(req.GetAnnotations())
				require.Equal(t, scoped, annos.Contains(&v2.TypeScopedGrants{}))
			}
			for _, id := range []string{"found-kept", "found-dropped"} {
				got, err := f.store.GetResource(t.Context(), reader_v2.ResourcesReaderServiceGetResourceRequest_builder{
					ResourceId: v2.ResourceId_builder{ResourceType: "selected", Resource: id}.Build(),
				}.Build())
				require.NoError(t, err)
				require.Equal(t, "discovered", got.GetResource().GetDisplayName())
			}
			grants, err := f.store.ListGrants(t.Context(), &v2.GrantsServiceListGrantsRequest{})
			require.NoError(t, err)
			require.Len(t, grants.GetList(), 1)
			pending, _, err := f.store.Grants().PendingExpansionPage(t.Context(), "")
			require.NoError(t, err)
			require.Len(t, pending, 1)
			require.Equal(t, []string{"selected"}, pending[0].Annotation.GetResourceTypeIds())
			grantAnnotations := annotations.Annotations(grants.GetList()[0].GetAnnotations())
			require.True(t, grantAnnotations.Contains(&v2.InsertResourceGrants{}))
			facts, err := f.ledger.LedgerFacts(t.Context())
			require.NoError(t, err)
			require.Contains(t, facts, factNeedsExpansion)
			require.Contains(t, facts, ledgerFactIngestBlocked)
			counters, err := f.ledger.LedgerCounters(t.Context())
			require.NoError(t, err)
			require.EqualValues(t, 1, counters.Counters["ingest.grants_dropped"])
			require.EqualValues(t, 1, counters.Counters["ingest.expansion_resource_types_dropped"])
		})
	}
}
func TestLedgerGrantCommitFailureRetry(t *testing.T) {
	s, f, c := grantPageFixture(t, false)
	seedLedgerTestRun(t, s, nil)
	before := ledgerRawSnapshot(t, f.engine)
	s.ledger.store = ledgerFailingPageStore{PageLedgerStore: f.ledger, stage: "commit"}
	f.audit.enter(ledgerHandler)
	require.ErrorIs(t, invokeLedgerTestPage(t, s, t.Context(), s.run.current(), s.SyncGrants, false), errLedgerInjectedPage)
	f.audit.enter(ledgerLifecycle)
	require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
	require.False(t, s.run.hasFact(factNeedsExpansion))
	require.False(t, s.ingestFilterStats.replayBlocked.Load())
	require.Zero(t, s.ingestFilterStats.grantsDropped.Load())
	s.ledger.store = f.ledger
	f.audit.enter(ledgerHandler)
	_, err := runLedgerTestSync(t, s, t.Context(), t.Context(), nil)
	require.NoError(t, err)
	f.audit.enter(ledgerLifecycle)
	require.Len(t, c.requests, 3)
	require.EqualValues(t, 1, s.ingestFilterStats.grantsDropped.Load())
	require.True(t, s.run.hasFact(factNeedsExpansion))
}
func TestLedgerGrantRelatedResourceReadThrough(t *testing.T) {
	s, f, c := grantPageFixture(t, false)
	s.cfg.syncType = connectorstore.SyncTypePartial
	s.run.setFact(factShouldFetchRelatedResources)
	c.insert = false
	c.grants = []*v2.Grant{ledgerGrant("first", "related", "one", "selected"), ledgerGrant("second", "related", "one", "selected")}
	f.audit.enter(ledgerHandler)
	_, err := runLedgerTestSync(t, s, t.Context(), t.Context(), nil)
	require.NoError(t, err)
	f.audit.enter(ledgerLifecycle)
	require.Len(t, c.gets, 1)
	require.Equal(t, "related", c.gets[0].GetResourceId().GetResourceType())
	counters, err := f.ledger.LedgerCounters(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 1, counters.ConnectorCalls["get-resource:related"].Count)
	require.Zero(t, counters.ConnectorCalls["get-resource:selected"].Count)
}

func TestLedgerGrantExternalMatchFact(t *testing.T) {
	s, f, c := grantPageFixture(t, false)
	c.grants = []*v2.Grant{ledgerGrant("external", "selected", "found", "disabled")}
	external, err := anypb.New(&v2.ExternalResourceMatchAll{})
	require.NoError(t, err)
	c.grants[0].SetAnnotations([]*anypb.Any{external})
	f.audit.enter(ledgerHandler)
	_, err = runLedgerTestSync(t, s, t.Context(), t.Context(), nil)
	require.NoError(t, err)
	f.audit.enter(ledgerLifecycle)
	facts, err := f.ledger.LedgerFacts(t.Context())
	require.NoError(t, err)
	require.Contains(t, facts, factHasExternalResourceGrants)
	require.NotContains(t, facts, ledgerFactIngestBlocked)
	grants, err := f.store.ListGrants(t.Context(), &v2.GrantsServiceListGrantsRequest{})
	require.NoError(t, err)
	require.Len(t, grants.GetList(), 1)
}

func TestLedgerGrantRemovedExpansion(t *testing.T) {
	s, f, c := grantPageFixture(t, false)
	c.grants = c.grants[:1]
	expansion, err := anypb.New(v2.GrantExpandable_builder{EntitlementIds: []string{"source"}, ResourceTypeIds: []string{"disabled"}}.Build())
	require.NoError(t, err)
	c.grants[0].SetAnnotations([]*anypb.Any{expansion})
	f.audit.enter(ledgerHandler)
	_, err = runLedgerTestSync(t, s, t.Context(), t.Context(), nil)
	require.NoError(t, err)
	f.audit.enter(ledgerLifecycle)
	facts, err := f.ledger.LedgerFacts(t.Context())
	require.NoError(t, err)
	require.NotContains(t, facts, factNeedsExpansion)
	require.Contains(t, facts, ledgerFactIngestBlocked)
	pending, _, err := f.store.Grants().PendingExpansionPage(t.Context(), "")
	require.NoError(t, err)
	require.Empty(t, pending)
	counters, err := f.ledger.LedgerCounters(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 1, counters.Counters["ingest.expansions_dropped"])
}

func TestLedgerGrantReplay(t *testing.T) {
	s, f, c := grantPageFixture(t, true)
	action := s.run.current()
	f.audit.enter(ledgerHandler)
	_, err := runLedgerTestSync(t, s, t.Context(), t.Context(), nil)
	require.NoError(t, err)
	f.audit.enter(ledgerLifecycle)
	require.NoError(t, f.store.Close(t.Context()))
	f = openLedgerFixtureAt(t, f.path, false)
	s.store, s.caps = f.store, resolveStoreCaps(f.store)
	s.ledger, err = newLedgerRuntime(t.Context(), f.ledger, "reopened-grants")
	require.NoError(t, err)
	seedLedgerTestRun(t, s, nil)
	before := ledgerRawSnapshot(t, f.engine)
	f.audit.enter(ledgerWalk)
	require.NoError(t, s.restoreLedgerState(t.Context(), ledgerResume{actions: []ledgerAction{{identity: ledgerIdentity(action)}}}, false))
	_, err = runLedgerTestSync(t, s, t.Context(), t.Context(), nil)
	require.NoError(t, err)
	f.audit.enter(ledgerLifecycle)
	require.Len(t, c.requests, 2)
	require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
	require.True(t, s.run.hasFact(factNeedsExpansion))
	require.True(t, s.ingestFilterStats.replayBlocked.Load())
	require.EqualValues(t, 1, s.ingestFilterStats.grantsDropped.Load())
}

func TestLedgerGrantFullIdentities(t *testing.T) {
	s, f, c := grantPageFixture(t, false)
	c.grants = []*v2.Grant{ledgerGrant("shared", "selected", "alpha", "selected"), ledgerGrant("shared", "selected", "beta", "selected")}
	action := s.run.current()
	f.audit.enter(ledgerHandler)
	_, err := runLedgerTestSync(t, s, t.Context(), t.Context(), nil)
	require.NoError(t, err)
	f.audit.enter(ledgerLifecycle)
	row, found, err := f.ledger.GetLedgerRow(t.Context(), ledgerIdentity(action))
	require.NoError(t, err)
	require.True(t, found)
	require.EqualValues(t, 2, row.GrantsWritten)
	grants, err := f.store.ListGrants(t.Context(), &v2.GrantsServiceListGrantsRequest{})
	require.NoError(t, err)
	require.Len(t, grants.GetList(), 2)
	var resources []string
	for _, grant := range grants.GetList() {
		resources = append(resources, grant.GetEntitlement().GetResource().GetId().GetResource())
	}
	require.ElementsMatch(t, []string{"alpha", "beta"}, resources)
}

func TestLedgerGrantPlanner(t *testing.T) {
	s, f, _ := grantPageFixture(t, true)
	marker, err := anypb.New(&v2.TypeScopedGrants{})
	require.NoError(t, err)
	require.NoError(t, f.store.PutResourceTypes(t.Context(), v2.ResourceType_builder{Id: "selected", Annotations: []*anypb.Any{marker}}.Build()))
	s.store = ledgerEntitlementPlannerStore{Store: f.store}
	s.run = newRunState()
	s.run.pushAction(t.Context(), Action{Op: SyncGrantsOp})
	action := s.run.current()
	s.ledger.store = ledgerFailingPageStore{PageLedgerStore: f.ledger, stage: "commit"}
	f.audit.enter(ledgerHandler)
	require.ErrorIs(t, invokeLedgerTestPage(t, s, t.Context(), action, s.SyncGrants, false), errLedgerInjectedPage)
	f.audit.enter(ledgerLifecycle)
	require.False(t, s.run.getAction(action.ID).TypeScopedPlanned)
	s.ledger.store = f.ledger
	f.audit.enter(ledgerHandler)
	require.NoError(t, invokeLedgerTestPage(t, s, t.Context(), action, s.SyncGrants, false))
	f.audit.enter(ledgerLifecycle)
	continued := s.run.getAction(action.ID)
	require.True(t, continued.TypeScopedPlanned)
	f.audit.enter(ledgerHandler)
	require.NoError(t, invokeLedgerTestPage(t, s, t.Context(), continued, s.SyncGrants, false))
	f.audit.enter(ledgerLifecycle)
	row, found, err := f.ledger.GetLedgerRow(t.Context(), ledgerIdentity(continued))
	require.NoError(t, err)
	require.True(t, found)
	require.Empty(t, row.Children)
}
