package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"bytes"
	"context"
	"errors"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type staticResourcePageStore struct{ c1zstore.Store }

func (s staticResourcePageStore) ListResources(ctx context.Context, req *v2.ResourcesServiceListResourcesRequest) (*v2.ResourcesServiceListResourcesResponse, error) {
	req = proto.Clone(req).(*v2.ResourcesServiceListResourcesRequest)
	req.SetPageSize(1)
	return s.Store.ListResources(ctx, req)
}

type staticBoundStore struct {
	c1zstore.PageLedgerStore
	peak         int
	materialized int
	failAt       int
}

func (s *staticBoundStore) BeginPage() c1zstore.PageWriter {
	return &staticBoundWriter{PageWriter: s.PageLedgerStore.BeginPage(), store: s}
}

type staticBoundWriter struct {
	c1zstore.PageWriter
	store   *staticBoundStore
	pending int
}

func (w *staticBoundWriter) PutEntitlements(ctx context.Context, values ...*v2.Entitlement) error {
	w.pending += len(values)
	return w.PageWriter.PutEntitlements(ctx, values...)
}
func (w *staticBoundWriter) Commit(ctx context.Context, id c1zstore.LedgerActionIdentity, row *c1zstore.LedgerRow) error {
	w.store.peak = max(w.store.peak, w.pending)
	if id.Op == MaterializeStaticEntitlementsOp.String() {
		w.store.materialized++
		if w.store.materialized == w.store.failAt {
			return errLedgerInjectedPage
		}
	}
	return w.PageWriter.Commit(ctx, id, row)
}

func TestLedgerStaticMaterializationBounded(t *testing.T) {
	s, f, c := staticPageFixture(t)
	s.store = staticResourcePageStore{Store: f.store}
	counted := &staticBoundStore{PageLedgerStore: f.ledger}
	s.ledger.store = counted
	_, err := s.parallelSync(t.Context(), t.Context(), nil)
	require.NoError(t, err)
	require.Equal(t, 1, counted.peak)
	result, err := f.store.ListEntitlements(t.Context(), &v2.EntitlementsServiceListEntitlementsRequest{})
	require.NoError(t, err)
	require.Len(t, result.GetList(), 2)
	require.Equal(t, []string{"", "next"}, c.calls)
}

type staticOrderConnector struct {
	*mockConnector
	calls       []string
	forbidFirst bool
	beforeNext  func() error
}

func (c *staticOrderConnector) ListStaticEntitlements(
	_ context.Context, req *v2.EntitlementsServiceListStaticEntitlementsRequest, _ ...grpc.CallOption,
) (*v2.EntitlementsServiceListStaticEntitlementsResponse, error) {
	token := req.GetPageToken()
	c.calls = append(c.calls, token)
	template := func(name string) *v2.Entitlement {
		return v2.Entitlement_builder{Slug: "member", DisplayName: name, Description: name}.Build()
	}
	if token == "" {
		if c.forbidFirst {
			return nil, errors.New("committed template page was fetched again")
		}
		return v2.EntitlementsServiceListStaticEntitlementsResponse_builder{List: []*v2.Entitlement{template("first"), template("first"), template("middle")}, NextPageToken: "second"}.Build(), nil
	}
	if c.beforeNext != nil {
		if err := c.beforeNext(); err != nil {
			return nil, err
		}
	}
	return v2.EntitlementsServiceListStaticEntitlementsResponse_builder{List: []*v2.Entitlement{template("first"), template("last")}}.Build(), nil
}

func TestLedgerStaticMaterializationOrderAndResume(t *testing.T) {
	baseline, baseStore, _ := staticPageFixture(t)
	baseline.ledgered = false
	baseline.store = staticResourcePageStore{Store: baseStore.store}
	baseline.connector = &staticOrderConnector{mockConnector: newMockConnector()}
	_, err := baseline.parallelSync(t.Context(), t.Context(), nil)
	require.NoError(t, err)
	expected, err := baseStore.store.ListEntitlements(t.Context(), &v2.EntitlementsServiceListEntitlementsRequest{})
	require.NoError(t, err)
	require.EqualValues(t, 1, baseline.run.completedActionsCount())
	for _, interrupted := range []bool{false, true} {
		t.Run(map[bool]string{false: "uninterrupted", true: "cold-resume"}[interrupted], func(t *testing.T) {
			s, f, _ := staticPageFixture(t)
			root := ledgerIdentity(s.run.current())
			syncID := f.engine.CurrentSyncID()
			connector := &staticOrderConnector{mockConnector: newMockConnector()}
			connector.beforeNext = func() error {
				response, err := f.store.ListEntitlements(t.Context(), &v2.EntitlementsServiceListEntitlementsRequest{})
				if err != nil {
					return err
				}
				require.Len(t, response.GetList(), 2)
				for _, ent := range response.GetList() {
					require.Equal(t, "middle", ent.GetDisplayName())
				}
				return nil
			}
			s.connector = connector
			s.store = staticResourcePageStore{Store: f.store}
			counted := &staticBoundStore{PageLedgerStore: f.ledger}
			if interrupted {
				counted.failAt = 2
			}
			s.ledger.store = counted
			f.audit.enter(ledgerHandler)
			_, err := s.parallelSync(t.Context(), t.Context(), nil)
			f.audit.enter(ledgerLifecycle)
			if interrupted {
				require.ErrorIs(t, err, errLedgerInjectedPage)
				require.Equal(t, []string{""}, connector.calls)
				committed, err := f.store.ListEntitlements(t.Context(), &v2.EntitlementsServiceListEntitlementsRequest{})
				require.NoError(t, err)
				require.Len(t, committed.GetList(), 1)
				require.NoError(t, f.store.Close(t.Context()))
				f = openLedgerFixtureAt(t, f.path, false)
				require.NoError(t, f.store.SetCurrentSync(t.Context(), syncID))
				s.store = staticResourcePageStore{Store: f.store}
				s.caps = resolveStoreCaps(f.store)
				s.ledger, err = newLedgerRuntime(t.Context(), f.ledger, "static-resume")
				require.NoError(t, err)
				before := ledgerRawSnapshot(t, f.engine)
				f.audit.enter(ledgerWalk)
				require.NoError(t, s.restoreLedgerState(t.Context(), ledgerResume{actions: []ledgerAction{{identity: root}}}, false))
				require.Equal(t, before, ledgerRawSnapshot(t, f.engine))
				f.audit.enter(ledgerHandler)
				connector.forbidFirst = true
				_, err = s.parallelSync(t.Context(), t.Context(), nil)
				f.audit.enter(ledgerLifecycle)
				require.NoError(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, []string{"", "second"}, connector.calls)
			response, err := f.store.ListEntitlements(t.Context(), &v2.EntitlementsServiceListEntitlementsRequest{})
			require.NoError(t, err)
			require.Len(t, response.GetList(), 2)
			for _, ent := range response.GetList() {
				require.Equal(t, "last", ent.GetDisplayName())
			}
			require.Len(t, response.GetList(), len(expected.GetList()))
			for i, ent := range response.GetList() {
				require.True(t, proto.Equal(expected.GetList()[i], ent))
			}
			first, found, err := f.ledger.GetLedgerRow(t.Context(), root)
			require.NoError(t, err)
			require.True(t, found)
			require.Len(t, first.Children, 3)
			require.NotEqual(t, first.Children[1].Identity, first.Children[2].Identity)
			next := root
			next.PageToken = "second"
			second, found, err := f.ledger.GetLedgerRow(t.Context(), next)
			require.NoError(t, err)
			require.True(t, found)
			require.Len(t, second.Children, 2)
			require.NotEqual(t, first.Children[2].Identity, second.Children[1].Identity)
			counters, err := f.ledger.LedgerCounters(t.Context())
			require.NoError(t, err)
			require.EqualValues(t, 5, counters.Counters[ledgerCompletedPrefix+MaterializeStaticEntitlementsOp.String()])
			require.EqualValues(t, 6, counters.Counters[ledgerCompletedActions])
			require.EqualValues(t, 2, counters.ConnectorCalls["list-static-entitlements"].Count)
		})
	}
}

func TestLedgerStaticMaterializationRejectsInvalidCursor(t *testing.T) {
	for _, token := range []string{"", `{"version":2}`, `{"version":1,"ordinal":-1}`, `{"version":1,"origin":"AAAA"}`} {
		s, f, c := staticPageFixture(t)
		s.run = newRunState()
		action := s.run.pushAction(t.Context(), Action{Op: MaterializeStaticEntitlementsOp, ResourceTypeID: "first", PageToken: token})
		before := ledgerRawSnapshot(t, f.engine)
		require.Error(t, s.invokeActionPage(t.Context(), action, s.materializeLedgerStaticEntitlements, false))
		require.Empty(t, c.calls)
		require.Equal(t, before, ledgerRawSnapshot(t, f.engine))
	}
}

func TestLedgerStaticMaterializationTokensScrub(t *testing.T) {
	s, f, _ := staticPageFixture(t)
	s.store = staticResourcePageStore{Store: f.store}
	root := ledgerIdentity(s.run.current())
	_, err := s.parallelSync(t.Context(), t.Context(), nil)
	require.NoError(t, err)
	parent, found, err := f.ledger.GetLedgerRow(t.Context(), root)
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, parent.Children, 1)
	child := parent.Children[0].Identity
	contains := func(rows []ledgerKV) bool {
		for _, row := range rows {
			if bytes.Contains(row.value, []byte(child.PageToken)) {
				return true
			}
		}
		return false
	}
	require.True(t, contains(ledgerRawSnapshot(t, f.engine)))
	require.NoError(t, s.ledger.prepareSeal(t.Context(), c1zstore.LedgerCounters{}))
	require.NoError(t, s.ledger.seal(t.Context()))
	require.NoError(t, f.store.Close(t.Context()))
	f = openLedgerFixtureAt(t, f.path, false)
	require.False(t, contains(ledgerRawSnapshot(t, f.engine)))
	parent, found, err = f.ledger.GetLedgerRow(t.Context(), root)
	require.NoError(t, err)
	require.True(t, found)
	require.True(t, parent.Scrubbed)
	require.Empty(t, parent.Children[0].Identity.PageToken)
	row, found, err := f.ledger.GetLedgerRow(t.Context(), child)
	require.NoError(t, err)
	require.True(t, found)
	require.True(t, row.Scrubbed)
	require.Empty(t, row.NextPageToken)
}

type staticInvalidResourcePageStore struct {
	c1zstore.Store
	oversized bool
}

func (s staticInvalidResourcePageStore) ListResources(context.Context, *v2.ResourcesServiceListResourcesRequest) (*v2.ResourcesServiceListResourcesResponse, error) {
	if s.oversized {
		return v2.ResourcesServiceListResourcesResponse_builder{List: make([]*v2.Resource, staticMaterializationPageSize+1)}.Build(), nil
	}
	return v2.ResourcesServiceListResourcesResponse_builder{NextPageToken: "stuck"}.Build(), nil
}
func TestLedgerStaticMaterializationRejectsInvalidResourcePage(t *testing.T) {
	for _, oversized := range []bool{false, true} {
		s, f, _ := staticPageFixture(t)
		s.store = staticInvalidResourcePageStore{Store: f.store, oversized: oversized}
		cursor := staticMaterializationCursor{Version: 1, Origin: make([]byte, 32), ResourceCursor: "stuck"}
		token, err := cursor.encode()
		require.NoError(t, err)
		s.run = newRunState()
		action := s.run.pushAction(t.Context(), Action{Op: MaterializeStaticEntitlementsOp, ResourceTypeID: "first", PageToken: token})
		before := ledgerRawSnapshot(t, f.engine)
		require.Error(t, s.invokeActionPage(t.Context(), action, s.materializeLedgerStaticEntitlements, false))
		require.Equal(t, before, ledgerRawSnapshot(t, f.engine))
	}
}
