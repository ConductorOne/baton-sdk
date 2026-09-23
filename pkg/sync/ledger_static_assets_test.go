package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"errors"
	"io"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/sync/progresslog"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

type ledgerStaticConnector struct {
	*ledgerTypesConnector
	calls   []string
	failure error
}

func (c *ledgerStaticConnector) ListStaticEntitlements(
	_ context.Context, req *v2.EntitlementsServiceListStaticEntitlementsRequest, _ ...grpc.CallOption,
) (*v2.EntitlementsServiceListStaticEntitlementsResponse, error) {
	c.calls = append(c.calls, req.GetPageToken())
	if c.failure != nil {
		return nil, c.failure
	}
	if req.GetPageToken() != "" {
		return &v2.EntitlementsServiceListStaticEntitlementsResponse{}, nil
	}
	group, err := anypb.New(v2.EntitlementExclusionGroup_builder{ExclusionGroupId: "roles", ScopeToResource: true}.Build())
	if err != nil {
		return nil, err
	}
	return v2.EntitlementsServiceListStaticEntitlementsResponse_builder{
		List: []*v2.Entitlement{{Slug: "member", Annotations: []*anypb.Any{group}}}, NextPageToken: "next",
	}.Build(), nil
}
func staticPageFixture(t *testing.T) (*syncer, *ledgerFixture, *ledgerStaticConnector) {
	t.Helper()
	s, f := newLedgerSchedulerFixture(t, 1)
	s.recordStats = true
	s.counts = progresslog.NewProgressCounts(t.Context())
	require.NoError(t, f.store.PutResourceTypes(t.Context(), &v2.ResourceType{Id: "first"}))
	for _, id := range []string{"one", "two"} {
		require.NoError(t, f.store.PutResources(t.Context(), v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "first", Resource: id}.Build(), DisplayName: id, Description: "description " + id,
		}.Build()))
	}
	c := &ledgerStaticConnector{ledgerTypesConnector: &ledgerTypesConnector{mockConnector: &mockConnector{}}}
	s.connector = c
	s.run.pushAction(t.Context(), Action{Op: SyncStaticEntitlementsOp, ResourceTypeID: "first"})
	return s, f, c
}
func TestLedgerStaticEntitlementPages(t *testing.T) {
	s, f, c := staticPageFixture(t)
	f.audit.enter(ledgerHandler)
	_, err := runLedgerTestSync(t, s, t.Context(), t.Context(), nil)
	require.NoError(t, err)
	f.audit.enter(ledgerLifecycle)
	require.Equal(t, []string{"", "next"}, c.calls)
	ents, err := f.store.ListEntitlements(t.Context(), &v2.EntitlementsServiceListEntitlementsRequest{})
	require.NoError(t, err)
	require.Len(t, ents.GetList(), 2)
	for _, ent := range ents.GetList() {
		id := ent.GetResource().GetId().GetResource()
		require.Equal(t, id, ent.GetDisplayName())
		require.Equal(t, "description "+id, ent.GetDescription())
		group := &v2.EntitlementExclusionGroup{}
		annos := annotations.Annotations(ent.GetAnnotations())
		found, err := annos.Pick(group)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, "roles-"+id, group.GetExclusionGroupId())
	}
}
func TestLedgerStaticEntitlementCommitFailure(t *testing.T) {
	s, f, _ := staticPageFixture(t)
	seedLedgerTestRun(t, s, nil)
	before := ledgerRawSnapshot(t, f.engine)
	s.ledger.store = ledgerFailingPageStore{PageLedgerStore: f.ledger, stage: "commit"}
	f.audit.enter(ledgerHandler)
	require.ErrorIs(t, invokeLedgerTestPage(t, s, t.Context(), s.run.current(), s.SyncStaticEntitlements, false), errLedgerInjectedPage)
	f.audit.enter(ledgerLifecycle)
	require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
	require.Empty(t, s.stats.connectorCallStats())
}

type ledgerAssetStream struct {
	grpc.ClientStream
	messages []*v2.AssetServiceGetAssetResponse
	failure  error
}

func (s *ledgerAssetStream) Recv() (*v2.AssetServiceGetAssetResponse, error) {
	if len(s.messages) == 0 {
		if s.failure != nil {
			return nil, s.failure
		}
		return nil, io.EOF
	}
	msg := s.messages[0]
	s.messages = s.messages[1:]
	return msg, nil
}

type ledgerAssetConnector struct {
	*mockConnector
	calls     []string
	metadata  bool
	failure   error
	nilStream bool
	failAsset string
}

func (c *ledgerAssetConnector) GetAsset(
	_ context.Context, req *v2.AssetServiceGetAssetRequest, _ ...grpc.CallOption,
) (grpc.ServerStreamingClient[v2.AssetServiceGetAssetResponse], error) {
	c.calls = append(c.calls, req.GetAsset().GetId())
	if c.nilStream {
		return nil, nil
	}
	stream := &ledgerAssetStream{}
	if c.failAsset == "" || c.failAsset == req.GetAsset().GetId() {
		stream.failure = c.failure
	}
	if c.metadata {
		stream.messages = append(stream.messages, v2.AssetServiceGetAssetResponse_builder{
			Metadata: v2.AssetServiceGetAssetResponse_Metadata_builder{ContentType: "image/example"}.Build(),
		}.Build())
	}
	stream.messages = append(stream.messages, v2.AssetServiceGetAssetResponse_builder{
		Data: v2.AssetServiceGetAssetResponse_Data_builder{Data: []byte("asset " + req.GetAsset().GetId())}.Build(),
	}.Build())
	return stream, nil
}
func assetPageFixture(t *testing.T) (*syncer, *ledgerFixture, *ledgerAssetConnector) {
	t.Helper()
	s, f := newLedgerSchedulerFixture(t, 1)
	s.recordStats = true
	s.counts = progresslog.NewProgressCounts(t.Context())
	require.NoError(t, f.store.PutResources(t.Context(), v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "type", Resource: "one"}.Build(), Icon: &v2.AssetRef{Id: "icon"},
	}.Build()))
	c := &ledgerAssetConnector{mockConnector: &mockConnector{}, metadata: true}
	s.connector = c
	s.run.pushAction(t.Context(), Action{Op: SyncAssetsOp, ResourceTypeID: "type", ResourceID: "one"})
	return s, f, c
}
func TestLedgerAssetHandlerReopen(t *testing.T) {
	s, f, c := assetPageFixture(t)
	f.audit.enter(ledgerHandler)
	_, err := runLedgerTestSync(t, s, t.Context(), t.Context(), nil)
	require.NoError(t, err)
	f.audit.enter(ledgerLifecycle)
	require.Equal(t, []string{"icon"}, c.calls)
	require.NoError(t, f.store.Close(t.Context()))
	f = openLedgerFixtureAt(t, f.path, false)
	asset, err := f.engine.GetAssetRecord(t.Context(), "icon")
	require.NoError(t, err)
	require.Equal(t, []byte("asset icon"), asset.GetData())
	require.Equal(t, "image/example", asset.GetContentType())
}
func TestLedgerAssetHandlerCommitFailure(t *testing.T) {
	s, f, _ := assetPageFixture(t)
	seedLedgerTestRun(t, s, nil)
	before := ledgerRawSnapshot(t, f.engine)
	s.ledger.store = ledgerFailingPageStore{PageLedgerStore: f.ledger, stage: "commit"}
	f.audit.enter(ledgerHandler)
	require.ErrorIs(t, invokeLedgerTestPage(t, s, t.Context(), s.run.current(), s.SyncAssets, false), errLedgerInjectedPage)
	f.audit.enter(ledgerLifecycle)
	require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
	require.Empty(t, s.stats.connectorCallStats())
}

func TestLedgerStaticEntitlementPlanner(t *testing.T) {
	s, f, c := staticPageFixture(t)
	s.cfg.syncResourceTypes = []string{"first"}
	s.run = newRunState()
	s.run.pushAction(t.Context(), Action{Op: SyncStaticEntitlementsOp})
	action := s.run.current()
	f.audit.enter(ledgerHandler)
	require.NoError(t, invokeLedgerTestPage(t, s, t.Context(), action, s.SyncStaticEntitlements, false))
	f.audit.enter(ledgerLifecycle)
	require.Equal(t, []string{"", "page-2"}, c.ledgerTypesConnector.calls)
	row, found, err := f.ledger.GetLedgerRow(t.Context(), ledgerIdentity(action))
	require.NoError(t, err)
	require.True(t, found)
	var types []string
	for _, child := range row.Children {
		types = append(types, child.Identity.ResourceTypeID)
	}
	require.Equal(t, []string{"first", "second", "excluded"}, types)
	counters, err := f.ledger.LedgerCounters(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 2, counters.ConnectorCalls["list-resource-types"].Count)
	require.EqualValues(t, 1, counters.Counters["ingest.invalid_resource_types_observed"])
}

func TestLedgerStaticEntitlementsMatchTokenHandler(t *testing.T) {
	ledger, f, _ := staticPageFixture(t)
	baseline, b, _ := staticPageFixture(t)
	baseline.ledgered = false
	f.audit.enter(ledgerHandler)
	_, err := runLedgerTestSync(t, ledger, t.Context(), t.Context(), nil)
	require.NoError(t, err)
	f.audit.enter(ledgerLifecycle)
	_, err = runLedgerTestSync(t, baseline, t.Context(), t.Context(), nil)
	require.NoError(t, err)
	got, err := f.store.ListEntitlements(t.Context(), &v2.EntitlementsServiceListEntitlementsRequest{})
	require.NoError(t, err)
	expected, err := b.store.ListEntitlements(t.Context(), &v2.EntitlementsServiceListEntitlementsRequest{})
	require.NoError(t, err)
	require.Len(t, got.GetList(), len(expected.GetList()))
	for i, value := range got.GetList() {
		require.True(t, proto.Equal(value, expected.GetList()[i]))
	}
}

func TestLedgerStaticEntitlementReplay(t *testing.T) {
	s, f, c := staticPageFixture(t)
	f.audit.enter(ledgerHandler)
	_, err := runLedgerTestSync(t, s, t.Context(), t.Context(), nil)
	require.NoError(t, err)
	f.audit.enter(ledgerLifecycle)
	require.NoError(t, f.store.Close(t.Context()))
	f = openLedgerFixtureAt(t, f.path, false)
	s.store, s.caps = f.store, resolveStoreCaps(f.store)
	s.ledger, err = newTestLedgerRuntime(t.Context(), f.ledger, "static-resume")
	require.NoError(t, err)
	seedLedgerTestRun(t, s, nil)
	before := ledgerRawSnapshot(t, f.engine)
	f.audit.enter(ledgerWalk)
	require.NoError(t, s.restoreLedgerState(t.Context(), s.ledger.store, s.ledger.runID, false))
	_, err = runLedgerTestSync(t, s, t.Context(), t.Context(), nil)
	require.NoError(t, err)
	f.audit.enter(ledgerLifecycle)
	require.Len(t, c.calls, 2)
	require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
}

func TestLedgerAssetHandlerErrors(t *testing.T) {
	for _, kind := range []string{"metadata", "stream", "nil"} {
		t.Run(kind, func(t *testing.T) {
			s, f, c := assetPageFixture(t)
			seedLedgerTestRun(t, s, nil)
			before := ledgerRawSnapshot(t, f.engine)
			switch kind {
			case "metadata":
				c.metadata = false
			case "stream":
				c.failure = errors.New("asset stream failed")
			case "nil":
				c.nilStream = true
			}
			f.audit.enter(ledgerHandler)
			err := invokeLedgerTestPage(t, s, t.Context(), s.run.current(), s.SyncAssets, false)
			f.audit.enter(ledgerLifecycle)
			if kind == "nil" {
				require.NoError(t, err)
				require.Nil(t, s.run.current())
			} else {
				require.Error(t, err)
				require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
				require.NotNil(t, s.run.current())
			}
		})
	}
}

func TestLedgerAssetHandlerMultipleReferences(t *testing.T) {
	s, f, c := assetPageFixture(t)
	logo, err := anypb.New(v2.AppTrait_builder{Logo: &v2.AssetRef{Id: "logo"}}.Build())
	require.NoError(t, err)
	require.NoError(t, f.store.PutResources(t.Context(), v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "type", Resource: "one"}.Build(), Icon: &v2.AssetRef{Id: "icon"}, Annotations: []*anypb.Any{logo},
	}.Build()))
	c.failure = errors.New("second stream failed")
	c.failAsset = "logo"
	seedLedgerTestRun(t, s, nil)
	before := ledgerRawSnapshot(t, f.engine)
	action := s.run.current()
	f.audit.enter(ledgerHandler)
	require.ErrorIs(t, invokeLedgerTestPage(t, s, t.Context(), action, s.SyncAssets, false), c.failure)
	f.audit.enter(ledgerLifecycle)
	require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
	c.failure = nil
	f.audit.enter(ledgerHandler)
	require.NoError(t, invokeLedgerTestPage(t, s, t.Context(), action, s.SyncAssets, false))
	f.audit.enter(ledgerLifecycle)
	require.Equal(t, []string{"icon", "logo", "icon", "logo"}, c.calls)
	for _, id := range []string{"icon", "logo"} {
		asset, err := f.engine.GetAssetRecord(t.Context(), id)
		require.NoError(t, err)
		require.Equal(t, []byte("asset "+id), asset.GetData())
	}
	counters, err := f.ledger.LedgerCounters(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 2, counters.ConnectorCalls["get-asset"].Count)
}

func TestLedgerStaticEntitlementLegacyPrefixError(t *testing.T) {
	s, f, c := staticPageFixture(t)
	c.failure = errors.New(`unable to resolve \"type.googleapis.com/c1.connector.v2.EntitlementsServiceListStaticEntitlementsRequest\": \"not found\"","errorType":"prefixError"`)
	f.audit.enter(ledgerHandler)
	require.NoError(t, invokeLedgerTestPage(t, s, t.Context(), s.run.current(), s.SyncStaticEntitlements, false))
	f.audit.enter(ledgerLifecycle)
	require.Nil(t, s.run.current())
}

func TestLedgerAssetHandlerReplay(t *testing.T) {
	s, f, c := assetPageFixture(t)
	f.audit.enter(ledgerHandler)
	_, err := runLedgerTestSync(t, s, t.Context(), t.Context(), nil)
	require.NoError(t, err)
	f.audit.enter(ledgerLifecycle)
	require.NoError(t, f.store.Close(t.Context()))
	f = openLedgerFixtureAt(t, f.path, false)
	s.store, s.caps = f.store, resolveStoreCaps(f.store)
	s.ledger, err = newTestLedgerRuntime(t.Context(), f.ledger, "asset-resume")
	require.NoError(t, err)
	seedLedgerTestRun(t, s, nil)
	before := ledgerRawSnapshot(t, f.engine)
	f.audit.enter(ledgerWalk)
	require.NoError(t, s.restoreLedgerState(t.Context(), s.ledger.store, s.ledger.runID, false))
	_, err = runLedgerTestSync(t, s, t.Context(), t.Context(), nil)
	require.NoError(t, err)
	f.audit.enter(ledgerLifecycle)
	require.Len(t, c.calls, 1)
	require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
}

func TestLedgerAssetHandlerNoReferences(t *testing.T) {
	s, f, c := assetPageFixture(t)
	require.NoError(t, f.store.PutResources(t.Context(), v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "type", Resource: "one"}.Build(),
	}.Build()))
	action := s.run.current()
	f.audit.enter(ledgerHandler)
	require.NoError(t, invokeLedgerTestPage(t, s, t.Context(), action, s.SyncAssets, false))
	f.audit.enter(ledgerLifecycle)
	require.Empty(t, c.calls)
	require.Nil(t, s.run.current())
	_, found, err := f.ledger.GetLedgerRow(t.Context(), ledgerIdentity(action))
	require.NoError(t, err)
	require.True(t, found)
}
