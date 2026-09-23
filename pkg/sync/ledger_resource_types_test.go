package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/sync/progresslog"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/anypb"
)

type ledgerTypesConnector struct {
	*mockConnector
	calls   []string
	failure error
}

func (c *ledgerTypesConnector) ListResourceTypes(
	_ context.Context, req *v2.ResourceTypesServiceListResourceTypesRequest, _ ...grpc.CallOption,
) (*v2.ResourceTypesServiceListResourceTypesResponse, error) {
	c.calls = append(c.calls, req.GetPageToken())
	report, err := anypb.New(v2.RateLimitWaitReport_builder{WaitMs: 7}.Build())
	if err != nil {
		return nil, err
	}
	if c.failure != nil {
		return nil, c.failure
	}
	if req.GetPageToken() == "" {
		return v2.ResourceTypesServiceListResourceTypesResponse_builder{List: []*v2.ResourceType{nil, {Id: "first"}}, NextPageToken: "page-2", Annotations: []*anypb.Any{report}}.Build(), nil
	}
	return v2.ResourceTypesServiceListResourceTypesResponse_builder{List: []*v2.ResourceType{{Id: "second"}, {Id: "excluded"}}, Annotations: []*anypb.Any{report}}.Build(), nil
}

func resourceTypePageFixture(t *testing.T, ledger bool) (*syncer, *ledgerFixture, *ledgerTypesConnector) {
	t.Helper()
	s, f := newLedgerSchedulerFixture(t, 1)
	s.ledgered = ledger
	s.recordStats = true
	s.counts = progresslog.NewProgressCounts(t.Context())
	s.cfg.syncResourceTypes = []string{"first", "second"}
	c := &ledgerTypesConnector{mockConnector: &mockConnector{}}
	s.connector = c
	s.run.pushAction(t.Context(), Action{Op: SyncResourceTypesOp})
	return s, f, c
}

func runResourceTypePages(t *testing.T, s *syncer) error {
	t.Helper()
	_, err := runLedgerTestSync(t, s, t.Context(), t.Context(), nil)
	return err
}

func TestResourceTypeFilterAcrossConnectorPages(t *testing.T) {
	s, _, c := resourceTypePageFixture(t, false)
	require.NoError(t, runResourceTypePages(t, s))
	require.Equal(t, []string{"", "page-2"}, c.calls)
	require.Nil(t, s.run.current())
}

func TestLedgerResourceTypePages(t *testing.T) {
	s, f, c := resourceTypePageFixture(t, true)
	f.audit.enter(ledgerHandler)
	require.NoError(t, runResourceTypePages(t, s))
	f.audit.enter(ledgerLifecycle)
	require.Equal(t, []string{"", "page-2"}, c.calls)
	require.Nil(t, s.run.current())
	counters, err := f.ledger.LedgerCounters(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 2, counters.ConnectorCalls["list-resource-types"].Count)
	require.EqualValues(t, 14, counters.StepDurationsMs["rate_limit_wait"])
	require.EqualValues(t, 1, counters.Counters["ingest.invalid_resource_types_observed"])
	require.NoError(t, f.store.Close(t.Context()))
	f = openLedgerFixtureAt(t, f.path, false)
	response, err := f.store.ListResourceTypes(t.Context(), &v2.ResourceTypesServiceListResourceTypesRequest{})
	require.NoError(t, err)
	var ids []string
	for _, rt := range response.GetList() {
		ids = append(ids, rt.GetId())
	}
	require.ElementsMatch(t, []string{"first", "second"}, ids)
}

func TestLedgerResourceTypeFailureRetryAndReplay(t *testing.T) {
	s, f, c := resourceTypePageFixture(t, true)
	var progressTokens []string
	s.cfg.progressHandler = func(p *Progress) { progressTokens = append(progressTokens, "progress") }
	seedLedgerTestRun(t, s, nil)
	before := ledgerRawSnapshot(t, f.engine)
	s.ledger.store = ledgerFailingPageStore{PageLedgerStore: f.ledger, stage: "commit"}
	f.audit.enter(ledgerHandler)
	require.ErrorIs(t, runResourceTypePages(t, s), errLedgerInjectedPage)
	f.audit.enter(ledgerLifecycle)
	require.Equal(t, "", s.run.current().PageToken)
	require.Empty(t, progressTokens)
	require.Zero(t, s.ingestFilterStats.invalidResourceTypesObserved.Load())
	require.Empty(t, s.stats.connectorCallStats())
	require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
	s.ledger.store = f.ledger
	f.audit.enter(ledgerHandler)
	require.NoError(t, runResourceTypePages(t, s))
	f.audit.enter(ledgerLifecycle)
	require.Len(t, progressTokens, 2)
	require.Equal(t, []string{"", "", "page-2"}, c.calls)
	require.EqualValues(t, 1, s.ingestFilterStats.invalidResourceTypesObserved.Load())
	counters, err := f.ledger.LedgerCounters(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 2, counters.ConnectorCalls["list-resource-types"].Count)
	require.EqualValues(t, 14, counters.StepDurationsMs["rate_limit_wait"])
	require.NoError(t, f.store.Close(t.Context()))
	f = openLedgerFixtureAt(t, f.path, false)
	s.store, s.caps = f.store, resolveStoreCaps(f.store)
	s.ledger, err = newLedgerRuntime(t.Context(), f.ledger, "reopened-attempt")
	require.NoError(t, err)
	s.run = newRunState()
	before = ledgerRawSnapshot(t, f.engine)
	s.run.pushAction(t.Context(), Action{Op: SyncResourceTypesOp})
	f.audit.enter(ledgerWalk)
	require.NoError(t, runResourceTypePages(t, s))
	f.audit.enter(ledgerLifecycle)
	require.Len(t, c.calls, 3)
	require.Len(t, progressTokens, 2)
	require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
}

func TestLedgerResourceTypeErrors(t *testing.T) {
	t.Run("connector", func(t *testing.T) {
		s, f, c := resourceTypePageFixture(t, true)
		c.failure = errLedgerInjectedPage
		seedLedgerTestRun(t, s, nil)
		before := ledgerRawSnapshot(t, f.engine)
		f.audit.enter(ledgerHandler)
		require.ErrorIs(t, runResourceTypePages(t, s), errLedgerInjectedPage)
		f.audit.enter(ledgerLifecycle)
		require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
		require.Empty(t, s.stats.connectorCallStats())
		require.NotNil(t, s.run.current())
	})
	t.Run("missing-filter", func(t *testing.T) {
		s, f, _ := resourceTypePageFixture(t, true)
		s.cfg.syncResourceTypes = append(s.cfg.syncResourceTypes, "missing")
		f.audit.enter(ledgerHandler)
		require.ErrorContains(t, runResourceTypePages(t, s), "invalid resource type 'missing' in filter")
		f.audit.enter(ledgerLifecycle)
		require.Equal(t, "page-2", s.run.current().PageToken)
		response, err := f.store.ListResourceTypes(t.Context(), &v2.ResourceTypesServiceListResourceTypesRequest{})
		require.NoError(t, err)
		require.Len(t, response.GetList(), 1)
		require.Equal(t, "first", response.GetList()[0].GetId())
	})
}

type ledgerTypeReadFailure struct{ c1zstore.Store }

func (s ledgerTypeReadFailure) GetResourceType(context.Context, *reader_v2.ResourceTypesReaderServiceGetResourceTypeRequest) (*reader_v2.ResourceTypesReaderServiceGetResourceTypeResponse, error) {
	return nil, errLedgerInjectedPage
}

func TestLedgerResourceTypeReadFailure(t *testing.T) {
	s, f, _ := resourceTypePageFixture(t, true)
	s.store = ledgerTypeReadFailure{Store: f.store}
	f.audit.enter(ledgerHandler)
	require.ErrorIs(t, runResourceTypePages(t, s), errLedgerInjectedPage)
	f.audit.enter(ledgerLifecycle)
	require.Equal(t, "page-2", s.run.current().PageToken)
	_, found, err := f.ledger.GetLedgerRow(t.Context(), c1zstore.LedgerActionIdentity{Op: SyncResourceTypesOp.String(), PageToken: "page-2"})
	require.NoError(t, err)
	require.False(t, found)
}

func TestLedgerResourceTypeSelection(t *testing.T) {
	for _, tc := range []struct {
		name            string
		selection, want []string
		progress        []uint32
	}{
		{name: "all", want: []string{"first", "second", "excluded"}, progress: []uint32{1, 2}},
		{name: "earlier-page", selection: []string{"first"}, want: []string{"first"}, progress: []uint32{1, 0}},
		{name: "terminal-page", selection: []string{"second"}, want: []string{"second"}, progress: []uint32{0, 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, f, _ := resourceTypePageFixture(t, true)
			s.cfg.syncResourceTypes = tc.selection
			s.recordStats = false
			var progress []uint32
			s.cfg.progressHandler = func(p *Progress) { progress = append(progress, p.Count) }
			f.audit.enter(ledgerHandler)
			require.NoError(t, runResourceTypePages(t, s))
			f.audit.enter(ledgerLifecycle)
			response, err := f.store.ListResourceTypes(t.Context(), &v2.ResourceTypesServiceListResourceTypesRequest{})
			require.NoError(t, err)
			var ids []string
			for _, rt := range response.GetList() {
				ids = append(ids, rt.GetId())
			}
			require.ElementsMatch(t, tc.want, ids)
			require.Equal(t, tc.progress, progress)
			counters, err := f.ledger.LedgerCounters(t.Context())
			require.NoError(t, err)
			require.Empty(t, counters.ConnectorCalls)
			require.Empty(t, counters.StepDurationsMs)
		})
	}
}

type ledgerTypeReadAudit struct {
	c1zstore.Store
	syncIDs []string
}

func (s *ledgerTypeReadAudit) GetResourceType(
	ctx context.Context, req *reader_v2.ResourceTypesReaderServiceGetResourceTypeRequest,
) (*reader_v2.ResourceTypesReaderServiceGetResourceTypeResponse, error) {
	id, err := annotations.GetSyncIdFromAnnotations(req.GetAnnotations())
	if err != nil {
		return nil, err
	}
	s.syncIDs = append(s.syncIDs, id)
	return s.Store.GetResourceType(ctx, req)
}

func TestLedgerResourceTypeSelectedSync(t *testing.T) {
	s, f, _ := resourceTypePageFixture(t, true)
	s.injectSyncIDAnnotation = true
	s.syncID = f.engine.CurrentSyncID()
	reader := &ledgerTypeReadAudit{Store: f.store}
	s.store = reader
	f.audit.enter(ledgerHandler)
	require.NoError(t, runResourceTypePages(t, s))
	f.audit.enter(ledgerLifecycle)
	require.Equal(t, []string{s.syncID}, reader.syncIDs)
}
