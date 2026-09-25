package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/sync/progresslog"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

type ledgerResourcesConnector struct {
	*mockConnector
	mu       sync.Mutex
	requests []*v2.ResourcesServiceListResourcesRequest
	barrier  *sync.WaitGroup
	distinct bool
}

func (c *ledgerResourcesConnector) ListResources(_ context.Context, req *v2.ResourcesServiceListResourcesRequest, _ ...grpc.CallOption) (*v2.ResourcesServiceListResourcesResponse, error) {
	c.mu.Lock()
	c.requests = append(c.requests, req)
	c.mu.Unlock()
	if c.barrier != nil && req.GetResourceTypeId() != "child" && req.GetPageToken() == "" {
		c.barrier.Done()
		c.barrier.Wait()
	}
	if req.GetResourceTypeId() == "child" {
		return &v2.ResourcesServiceListResourcesResponse{}, nil
	}
	child, err := anypb.New(v2.ChildResourceType_builder{ResourceTypeId: "child"}.Build())
	if err != nil {
		return nil, err
	}
	r := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "parent", Resource: "one"}.Build(), DisplayName: "first", Annotations: []*anypb.Any{child, child}}.Build()
	if c.distinct {
		r.GetId().SetResource(req.GetResourceTypeId())
	}
	if req.GetPageToken() != "" {
		r.SetDisplayName("latest")
		return v2.ResourcesServiceListResourcesResponse_builder{List: []*v2.Resource{r}}.Build(), nil
	}
	return v2.ResourcesServiceListResourcesResponse_builder{List: []*v2.Resource{nil, r, r}, NextPageToken: "next"}.Build(), nil
}

func resourcePageFixture(t *testing.T, workers int) (*syncer, *ledgerFixture, *ledgerResourcesConnector) {
	t.Helper()
	s, f := newLedgerSchedulerFixture(t, workers)
	s.counts = progresslog.NewProgressCounts(t.Context())
	s.recordStats = true
	require.NoError(t, f.store.PutResourceTypes(t.Context(), &v2.ResourceType{Id: "parent"}, &v2.ResourceType{Id: "child"}))
	c := &ledgerResourcesConnector{mockConnector: &mockConnector{}}
	s.connector = c
	s.run.pushAction(t.Context(), Action{Op: SyncResourcesOp, ResourceTypeID: "parent"})
	return s, f, c
}

func TestLedgerResourcePages(t *testing.T) {
	for _, workers := range []int{1, 4} {
		t.Run(strconv.Itoa(workers), func(t *testing.T) {
			s, f, c := resourcePageFixture(t, workers)
			f.audit.enter(ledgerHandler)
			_, err := runLedgerTestSync(t, s, t.Context(), t.Context(), nil)
			require.NoError(t, err)
			f.audit.enter(ledgerLifecycle)
			require.Nil(t, s.run.current())
			require.Len(t, c.requests, 3)
			children := 0
			for _, req := range c.requests {
				if req.GetResourceTypeId() == "child" {
					children++
					require.Equal(t, "parent", req.GetParentResourceId().GetResourceType())
					require.Equal(t, "one", req.GetParentResourceId().GetResource())
				}
			}
			require.Equal(t, 1, children)
			got, err := f.store.GetResource(t.Context(), reader_v2.ResourcesReaderServiceGetResourceRequest_builder{
				ResourceId: v2.ResourceId_builder{ResourceType: "parent", Resource: "one"}.Build(),
			}.Build())
			require.NoError(t, err)
			require.Equal(t, "latest", got.GetResource().GetDisplayName())
			counters, err := f.ledger.LedgerCounters(t.Context())
			require.NoError(t, err)
			require.EqualValues(t, 3, counters.ConnectorCalls["list-resources"].Count)
			require.EqualValues(t, 1, counters.Counters["ingest.invalid_resources_observed"])
		})
	}
}

func TestLedgerResourceCommitFailureRetry(t *testing.T) {
	s, f, c := resourcePageFixture(t, 1)
	var progress []uint32
	s.cfg.progressHandler = func(p *Progress) { progress = append(progress, p.Count) }
	seedLedgerTestRun(t, s, nil)
	before := ledgerRawSnapshot(t, f.engine)
	s.ledger.store = ledgerFailingPageStore{PageLedgerStore: f.ledger, stage: "commit"}
	f.audit.enter(ledgerHandler)
	_, err := runLedgerTestSync(t, s, t.Context(), t.Context(), nil)
	require.ErrorIs(t, err, errLedgerInjectedPage)
	f.audit.enter(ledgerLifecycle)
	require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
	require.False(t, hasLedgerScheduledChild(t, s, "child", "parent", "one"))
	require.Empty(t, progress)
	require.Zero(t, s.ingestFilterStats.invalidResourcesObserved.Load())
	require.Empty(t, s.stats.connectorCallStats())
	s.ledger.store = f.ledger
	f.audit.enter(ledgerHandler)
	_, err = runLedgerTestSync(t, s, t.Context(), t.Context(), nil)
	require.NoError(t, err)
	f.audit.enter(ledgerLifecycle)
	require.Len(t, c.requests, 4)
	require.ElementsMatch(t, []uint32{3, 1, 0}, progress)
	require.True(t, hasLedgerScheduledChild(t, s, "child", "parent", "one"))
}

type ledgerTargetConnector struct {
	*mockConnector
	resource *v2.Resource
	failure  error
	request  *v2.ResourceGetterServiceGetResourceRequest
	calls    int
}

func (c *ledgerTargetConnector) GetResource(_ context.Context, req *v2.ResourceGetterServiceGetResourceRequest, _ ...grpc.CallOption) (*v2.ResourceGetterServiceGetResourceResponse, error) {
	c.calls++
	c.request = req
	return v2.ResourceGetterServiceGetResourceResponse_builder{Resource: c.resource}.Build(), c.failure
}

func TestLedgerTargetedResourcePage(t *testing.T) {
	s, f, _ := resourcePageFixture(t, 1)
	child, err := anypb.New(v2.ChildResourceType_builder{ResourceTypeId: "child"}.Build())
	require.NoError(t, err)
	c := &ledgerTargetConnector{mockConnector: &mockConnector{}, resource: v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "parent", Resource: "one"}.Build(), Annotations: []*anypb.Any{child, child}}.Build()}
	s.connector = c
	s.run = newRunState()
	s.run.pushAction(t.Context(), Action{Op: SyncTargetedResourceOp, ResourceTypeID: "parent", ResourceID: "one", ParentResourceTypeID: "root", ParentResourceID: "two"})
	action := s.run.current()
	seedLedgerTestRun(t, s, nil)
	before := ledgerRawSnapshot(t, f.engine)
	s.ledger.store = ledgerFailingPageStore{PageLedgerStore: f.ledger, stage: "commit"}
	f.audit.enter(ledgerHandler)
	require.ErrorIs(t, invokeLedgerTestPage(t, s, t.Context(), action, s.SyncTargetedResource, false), errLedgerInjectedPage)
	f.audit.enter(ledgerLifecycle)
	require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
	require.False(t, hasLedgerScheduledChild(t, s, "child", "parent", "one"))
	s.ledger.store = f.ledger
	f.audit.enter(ledgerHandler)
	require.NoError(t, invokeLedgerTestPage(t, s, t.Context(), action, s.SyncTargetedResource, false))
	f.audit.enter(ledgerLifecycle)
	require.Equal(t, "root", c.request.GetParentResourceId().GetResourceType())
	require.Equal(t, "two", c.request.GetParentResourceId().GetResource())
	row, found, err := f.ledger.GetLedgerRow(t.Context(), ledgerIdentity(action))
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, row.Children, 3)
	require.Equal(t, SyncGrantsOp.String(), row.Children[0].Identity.Op)
	require.Equal(t, SyncEntitlementsOp.String(), row.Children[1].Identity.Op)
	require.Equal(t, SyncResourcesOp.String(), row.Children[2].Identity.Op)
	require.Equal(t, SyncResourcesOp, s.run.current().Op)
}

func TestLedgerConcurrentChildDiscovery(t *testing.T) {
	for _, distinct := range []bool{false, true} {
		t.Run(strconv.FormatBool(distinct), func(t *testing.T) {
			s, f, c := resourcePageFixture(t, 4)
			c.barrier = &sync.WaitGroup{}
			c.barrier.Add(2)
			c.distinct = distinct
			s.run.pushAction(t.Context(), Action{Op: SyncResourcesOp, ResourceTypeID: "another"})
			f.audit.enter(ledgerHandler)
			_, err := runLedgerTestSync(t, s, t.Context(), t.Context(), nil)
			require.NoError(t, err)
			f.audit.enter(ledgerLifecycle)
			expected := 5
			if distinct {
				expected++
			}
			require.Len(t, c.requests, expected)
		})
	}
}

func TestLedgerResourceReplay(t *testing.T) {
	s, f, c := resourcePageFixture(t, 4)
	f.audit.enter(ledgerHandler)
	_, err := runLedgerTestSync(t, s, t.Context(), t.Context(), nil)
	require.NoError(t, err)
	f.audit.enter(ledgerLifecycle)
	require.NoError(t, f.store.Close(t.Context()))
	f = openLedgerFixtureAt(t, f.path, false)
	s.store, s.caps = f.store, resolveStoreCaps(f.store)
	s.ledger, err = newTestLedgerRuntime(t.Context(), f.ledger, "reopened-resources")
	require.NoError(t, err)
	s.run = newRunState()
	s.childSchedule = childScheduleSet{}
	s.run.pushAction(t.Context(), Action{Op: SyncResourcesOp, ResourceTypeID: "parent"})
	seedLedgerTestRun(t, s, nil)
	before := ledgerRawSnapshot(t, f.engine)
	f.audit.enter(ledgerWalk)
	_, err = runLedgerTestSync(t, s, t.Context(), t.Context(), nil)
	require.NoError(t, err)
	f.audit.enter(ledgerLifecycle)
	require.Len(t, c.requests, 3)
	require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
	require.True(t, hasLedgerScheduledChild(t, s, "child", "parent", "one"))
}

func TestLedgerTargetedResourceEmptyAndFailure(t *testing.T) {
	for _, code := range []codes.Code{codes.OK, codes.NotFound, codes.Unimplemented, codes.Internal} {
		t.Run(code.String(), func(t *testing.T) {
			s, f, _ := resourcePageFixture(t, 1)
			c := &ledgerTargetConnector{mockConnector: &mockConnector{}, failure: status.Error(code, "connector response")}
			s.connector = c
			s.run = newRunState()
			s.run.pushAction(t.Context(), Action{Op: SyncTargetedResourceOp, ResourceTypeID: "parent", ResourceID: "one"})
			action := s.run.current()
			seedLedgerTestRun(t, s, nil)
			before := ledgerRawSnapshot(t, f.engine)
			f.audit.enter(ledgerHandler)
			err := invokeLedgerTestPage(t, s, t.Context(), action, s.SyncTargetedResource, false)
			f.audit.enter(ledgerLifecycle)
			if code == codes.Internal {
				require.ErrorIs(t, err, c.failure)
				require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
				require.NotNil(t, s.run.current())
			} else {
				require.NoError(t, err)
				require.Nil(t, s.run.current())
				row, found, err := f.ledger.GetLedgerRow(t.Context(), ledgerIdentity(action))
				require.NoError(t, err)
				require.True(t, found)
				require.Empty(t, row.Children)
			}
		})
	}
}

func TestLedgerConnectorObservationsAccumulate(t *testing.T) {
	s, f := newLedgerSchedulerFixture(t, 1)
	s.recordStats = true
	wait, err := anypb.New(v2.RateLimitWaitReport_builder{WaitMs: 7}.Build())
	require.NoError(t, err)
	usage, err := anypb.New(v2.SessionStoreUsage_builder{Ops: []*v2.SessionStoreUsage_OpStats{
		v2.SessionStoreUsage_OpStats_builder{Op: "get", Count: 2, Errors: 1, TotalMs: 6, MaxMs: 4}.Build(),
		v2.SessionStoreUsage_OpStats_builder{Op: "", Count: 99}.Build(),
	}}.Build())
	require.NoError(t, err)
	annos := []*anypb.Any{wait, usage}
	s.run.pushAction(t.Context(), Action{Op: SyncResourcesOp, ResourceTypeID: "parent"})
	s.testHooks.ledgerHandler = func(ctx context.Context, action *Action, _ *ledgerPage) error {
		invocation := ctx.Value(ledgerInvocationKey{}).(*ledgerInvocation)
		s.recordLedgerConnectorResponse(ctx, invocation, "list-resources", 2*time.Millisecond, annos)
		s.recordLedgerSessionUsage(invocation, annos)
		s.recordLedgerConnectorResponse(ctx, invocation, "list-resources", 3*time.Millisecond, annos)
		s.recordLedgerSessionUsage(invocation, annos)
		return s.nextPageOrFinishAction(ctx, action, "")
	}
	f.audit.enter(ledgerHandler)
	require.NoError(t, invokeLedgerTestPage(t, s, t.Context(), s.run.current(), s.SyncResources, false))
	f.audit.enter(ledgerLifecycle)
	counters, err := f.ledger.LedgerCounters(t.Context())
	require.NoError(t, err)
	require.Equal(t, c1zstore.CallStat{Count: 2, TotalMs: 5, MaxMs: 3}, counters.ConnectorCalls["list-resources"])
	require.EqualValues(t, 14, counters.StepDurationsMs["rate_limit_wait"])
	require.Equal(t, c1zstore.CallStat{Count: 4, Errors: 2, TotalMs: 12, MaxMs: 4}, counters.SessionCalls["connector.get"])
	require.Len(t, counters.SessionCalls, 1)
}

func TestLedgerResourceControlPage(t *testing.T) {
	s, f, _ := resourcePageFixture(t, 1)
	s.run = newRunState()
	s.run.pushAction(t.Context(), Action{Op: SyncResourcesOp, ParentResourceTypeID: "root", ParentResourceID: "one"})
	action := s.run.current()
	s.ledger.store = ledgerFailingPageStore{PageLedgerStore: f.ledger, stage: "commit"}
	f.audit.enter(ledgerHandler)
	require.ErrorIs(t, invokeLedgerTestPage(t, s, t.Context(), action, s.SyncResources, false), errLedgerInjectedPage)
	f.audit.enter(ledgerLifecycle)
	require.False(t, s.resourcesPhaseRanHere)
	s.ledger.store = f.ledger
	f.audit.enter(ledgerHandler)
	require.NoError(t, invokeLedgerTestPage(t, s, t.Context(), action, s.SyncResources, false))
	f.audit.enter(ledgerLifecycle)
	require.True(t, s.resourcesPhaseRanHere)
	row, found, err := f.ledger.GetLedgerRow(t.Context(), ledgerIdentity(action))
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, row.Children, 2)
	for _, child := range row.Children {
		require.Equal(t, "root", child.Identity.ParentResourceTypeID)
		require.Equal(t, "one", child.Identity.ParentResourceID)
	}
}

func TestLedgerTargetedTypeScoped(t *testing.T) {
	s, f, _ := resourcePageFixture(t, 1)
	grants, err := anypb.New(&v2.TypeScopedGrants{})
	require.NoError(t, err)
	entitlements, err := anypb.New(&v2.TypeScopedEntitlements{})
	require.NoError(t, err)
	require.NoError(t, f.store.PutResourceTypes(t.Context(), v2.ResourceType_builder{Id: "parent", Annotations: []*anypb.Any{grants, entitlements}}.Build()))
	s.connector = &ledgerTargetConnector{mockConnector: &mockConnector{}, resource: v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "parent", Resource: "one"}.Build()}.Build()}
	s.run = newRunState()
	s.run.pushAction(t.Context(), Action{Op: SyncTargetedResourceOp, ResourceTypeID: "parent", ResourceID: "one"})
	action := s.run.current()
	f.audit.enter(ledgerHandler)
	require.NoError(t, invokeLedgerTestPage(t, s, t.Context(), action, s.SyncTargetedResource, false))
	f.audit.enter(ledgerLifecycle)
	row, found, err := f.ledger.GetLedgerRow(t.Context(), ledgerIdentity(action))
	require.NoError(t, err)
	require.True(t, found)
	require.Empty(t, row.Children)
}

type ledgerResourceReadFailure struct{ c1zstore.Store }

func (s ledgerResourceReadFailure) GetResource(context.Context, *reader_v2.ResourcesReaderServiceGetResourceRequest) (*reader_v2.ResourcesReaderServiceGetResourceResponse, error) {
	return nil, errLedgerInjectedPage
}
func TestLedgerResourceReadFailure(t *testing.T) {
	s, f, _ := resourcePageFixture(t, 1)
	s.store = ledgerResourceReadFailure{Store: f.store}
	seedLedgerTestRun(t, s, nil)
	before := ledgerRawSnapshot(t, f.engine)
	f.audit.enter(ledgerHandler)
	require.ErrorIs(t, invokeLedgerTestPage(t, s, t.Context(), s.run.current(), s.SyncResources, false), errLedgerInjectedPage)
	f.audit.enter(ledgerLifecycle)
	require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
	require.Empty(t, s.stats.connectorCallStats())
}

func TestLedgerResourcePendingChildRestore(t *testing.T) {
	for _, check := range []string{"mark", "transition"} {
		t.Run(check, func(t *testing.T) {
			s, f, c := resourcePageFixture(t, 1)
			root := ledgerIdentity(s.run.current())
			f.audit.enter(ledgerHandler)
			require.NoError(t, invokeLedgerTestPage(t, s, t.Context(), s.run.current(), s.SyncResources, false))
			f.audit.enter(ledgerLifecycle)
			require.Len(t, c.requests, 1)
			require.NoError(t, f.store.Close(t.Context()))
			f = openLedgerFixtureAt(t, f.path, false)
			s.store, s.caps = f.store, resolveStoreCaps(f.store)
			var err error
			s.ledger, err = newTestLedgerRuntime(t.Context(), f.ledger, "pending-child-resume")
			require.NoError(t, err)
			s.childSchedule = childScheduleSet{}
			seedLedgerTestRun(t, s, nil)
			before := ledgerRawSnapshot(t, f.engine)
			f.audit.enter(ledgerWalk)
			require.NoError(t, s.restoreLedgerState(t.Context(), s.ledger.store, s.ledger.runID, false))
			f.audit.enter(ledgerLifecycle)
			require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
			if check == "mark" {
				require.True(t, hasLedgerScheduledChild(t, s, "child", "parent", "one"))
			}
			f.audit.enter(ledgerHandler)
			_, err = runLedgerTestSync(t, s, t.Context(), t.Context(), nil)
			require.NoError(t, err)
			f.audit.enter(ledgerLifecycle)
			require.Len(t, c.requests, 3)
			root.PageToken = "next"
			row, found, err := f.ledger.GetLedgerRow(t.Context(), root)
			require.NoError(t, err)
			require.True(t, found)
			require.Empty(t, row.Children)
			baseline, baselineFile, _ := resourcePageFixture(t, 1)
			baselineFile.audit.enter(ledgerHandler)
			_, err = runLedgerTestSync(t, baseline, t.Context(), t.Context(), nil)
			require.NoError(t, err)
			baselineFile.audit.enter(ledgerLifecycle)
			uninterrupted, found, err := baselineFile.ledger.GetLedgerRow(t.Context(), root)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, uninterrupted.Children, row.Children)
			require.Equal(t, uninterrupted.ResourcesWritten, row.ResourcesWritten)
		})
	}
}

type ledgerSchedulingLookup struct {
	c1zstore.PageLedgerStore
	calls int
	mode  string
}

func (s *ledgerSchedulingLookup) HasScheduledWork(ctx context.Context, key string) (bool, error) {
	s.calls++
	switch s.mode {
	case "missing":
		return false, nil
	case "error":
		return false, errLedgerInjectedPage
	default:
		return s.PageLedgerStore.HasScheduledWork(ctx, key)
	}
}

func TestLedgerFailFastUsesDurableChildScheduling(t *testing.T) {
	for _, mode := range []string{"stored", "missing", "error"} {
		t.Run(mode, func(t *testing.T) {
			f := openLedgerFixtureAt(t, filepath.Join(t.TempDir(), "child-check.c1z"), false)
			connector := &ledgerResourcesConnector{mockConnector: newMockConnector()}
			skip, err := anypb.New(&v2.SkipEntitlementsAndGrants{})
			require.NoError(t, err)
			connector.rtDB = []*v2.ResourceType{{Id: "parent", Annotations: []*anypb.Any{skip}}, {Id: "child", Annotations: []*anypb.Any{skip}}}
			created, err := NewSyncer(t.Context(), connector, WithConnectorStore(f.store), WithDontExpandGrants(), WithFailFastInvariants())
			require.NoError(t, err)
			s := created.(*syncer)
			lookup := &ledgerSchedulingLookup{PageLedgerStore: f.ledger, mode: mode}
			s.caps.pageLedger = lookup
			err = s.Sync(t.Context())
			require.Positive(t, lookup.calls, "sync error: %v; resource phase: %t; fail-fast: %t", err, s.resourcesPhaseRanHere, s.cfg.failFastInvariants)
			switch mode {
			case "stored":
				require.NoError(t, err)
			case "missing":
				require.ErrorContains(t, err, "I4 violated")
			case "error":
				require.ErrorIs(t, err, errLedgerInjectedPage)
			}
			scopedEmptyCalls := 0
			for _, request := range connector.requests {
				if request.GetResourceTypeId() == "child" && request.GetParentResourceId() != nil {
					scopedEmptyCalls++
				}
			}
			require.Equal(t, 1, scopedEmptyCalls)
			if mode != "stored" {
				finished, err := f.ledger.BoundSyncFinished(t.Context())
				require.NoError(t, err)
				require.False(t, finished)
			}
		})
	}
}
