package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"fmt"
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/sync/expand"
	et "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	gt "github.com/conductorone/baton-sdk/pkg/types/grant"
	"github.com/stretchr/testify/require"
)

func ledgerExpansionFixture(t *testing.T) (*syncer, *ledgerFixture) {
	t.Helper()
	s, f := newLedgerSchedulerFixture(t, 1)
	s.graph = newExpansionGraph()
	s.syncID = f.engine.CurrentSyncID()
	t.Cleanup(s.stopLedgerExpansion)
	resources := make([]*v2.Resource, 3)
	for i, name := range []string{"a", "b", "c"} {
		resources[i] = v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: name}.Build()}.Build()
	}
	require.NoError(t, f.store.PutResources(t.Context(), resources...))
	for _, resource := range resources {
		require.NoError(t, f.store.PutEntitlements(t.Context(), et.NewAssignmentEntitlement(resource, "member")))
	}
	grants := []*v2.Grant{gt.NewGrant(resources[0], "member", v2.ResourceId_builder{ResourceType: "user", Resource: "alice"}.Build())}
	for i := 1; i < len(resources); i++ {
		annotation := v2.GrantExpandable_builder{EntitlementIds: []string{et.NewEntitlementID(resources[i-1], "member")}}.Build()
		grants = append(grants, gt.NewGrant(resources[i], "member", resources[i-1].GetId(), gt.WithAnnotation(annotation)))
	}
	require.NoError(t, f.store.PutGrants(t.Context(), grants...))
	f.audit.enter(ledgerHandler)
	_, err := s.ledger.runPage(t.Context(), 0, c1zstore.LedgerActionIdentity{Op: "expansion-fixture"}, func(_ context.Context, page *ledgerPage) error {
		if err := page.setFact(factNeedsExpansion); err != nil {
			return err
		}
		return page.transition("")
	})
	require.NoError(t, err)
	f.audit.enter(ledgerLifecycle)
	s.run.setFact(factNeedsExpansion)
	s.run.pushAction(t.Context(), Action{Op: SyncGrantExpansionOp})
	return s, f
}

func runLedgerExpansionPages(t *testing.T, s *syncer, f *ledgerFixture) int {
	t.Helper()
	count := 0
	for s.run.current() != nil {
		require.Less(t, count, 20)
		f.audit.enter(ledgerHandler)
		require.NoError(t, s.invokeActionPage(t.Context(), s.run.current(), s.SyncGrantExpansion, false))
		f.audit.enter(ledgerLifecycle)
		count++
	}
	return count
}

func TestLedgerExpansionHandlerResume(t *testing.T) {
	for _, cut := range []int{0, 1, 2} {
		t.Run(fmt.Sprint(cut), func(t *testing.T) { testLedgerExpansionHandlerResume(t, cut) })
	}
}
func testLedgerExpansionHandlerResume(t *testing.T, cut int) {
	t.Helper()
	reference, referenceFile := ledgerExpansionFixture(t)
	require.Greater(t, runLedgerExpansionPages(t, reference, referenceFile), 1)
	want, err := referenceFile.store.ListGrants(t.Context(), &v2.GrantsServiceListGrantsRequest{})
	require.NoError(t, err)
	s, f := ledgerExpansionFixture(t)
	root := ledgerIdentity(s.run.current())
	for range cut {
		f.audit.enter(ledgerHandler)
		require.NoError(t, s.invokeActionPage(t.Context(), s.run.current(), s.SyncGrantExpansion, false))
		f.audit.enter(ledgerLifecycle)
	}
	before := ledgerRawSnapshot(t, f.engine)
	s.ledger.store = ledgerFailingPageStore{PageLedgerStore: f.ledger, stage: "commit"}
	f.audit.enter(ledgerHandler)
	require.ErrorIs(t, s.invokeActionPage(t.Context(), s.run.current(), s.SyncGrantExpansion, false), errLedgerInjectedPage)
	f.audit.enter(ledgerLifecycle)
	require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
	require.Nil(t, s.ledgerExpansion, "failed page must release projection iterator")
	require.False(t, s.graph.get(t.Context()).Loaded, "terminal failure must not publish graph")
	require.NoError(t, f.store.Close(t.Context()))
	f = openLedgerFixtureAt(t, f.path, false)
	s.store, s.caps = f.store, resolveStoreCaps(f.store)
	s.graph = newExpansionGraph()
	s.ledger, err = newLedgerRuntime(t.Context(), f.ledger, "expansion-resume")
	require.NoError(t, err)
	before = ledgerRawSnapshot(t, f.engine)
	f.audit.enter(ledgerWalk)
	require.NoError(t, s.restoreLedgerState(t.Context(), ledgerResume{actions: []ledgerAction{{identity: root}}}, false))
	f.audit.enter(ledgerLifecycle)
	require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
	runLedgerExpansionPages(t, s, f)
	got, err := f.store.ListGrants(t.Context(), &v2.GrantsServiceListGrantsRequest{})
	require.NoError(t, err)
	equalProtoLists(t, want.GetList(), got.GetList())
	wantCounters, err := referenceFile.ledger.LedgerCounters(t.Context())
	require.NoError(t, err)
	gotCounters, err := f.ledger.LedgerCounters(t.Context())
	require.NoError(t, err)
	require.Equal(t, wantCounters.Counters, gotCounters.Counters)
	for id := root; ; {
		wantRow, found, err := referenceFile.ledger.GetLedgerRow(t.Context(), id)
		require.NoError(t, err)
		require.True(t, found)
		gotRow, found, err := f.ledger.GetLedgerRow(t.Context(), id)
		require.NoError(t, err)
		require.True(t, found)
		next := wantRow.NextPageToken
		for _, row := range []*c1zstore.LedgerRow{wantRow, gotRow} {
			row.Attempt = ""
			row.CommittedAt = time.Time{}
			row.PageDuration = 0
		}
		require.Equal(t, wantRow, gotRow)
		if next == "" {
			break
		}
		id.PageToken = next
	}
}

func TestLedgerExpansionMatchesMainProjection(t *testing.T) {
	s, f := ledgerExpansionFixture(t)
	baseline, b := ledgerExpansionFixture(t)
	graph, _, err := baseline.buildLedgerExpansionGraph(t.Context())
	require.NoError(t, err)
	require.NoError(t, expand.NewExpander(baseline.expanderStore(), graph).RunTopologicalMergeProjection(t.Context()))
	runLedgerExpansionPages(t, s, f)
	want, err := b.store.ListGrants(t.Context(), &v2.GrantsServiceListGrantsRequest{})
	require.NoError(t, err)
	got, err := f.store.ListGrants(t.Context(), &v2.GrantsServiceListGrantsRequest{})
	require.NoError(t, err)
	equalProtoLists(t, want.GetList(), got.GetList())
}

func TestLedgerExpansionSkipRecordsTransition(t *testing.T) {
	for _, mode := range []string{"disabled", "no-fact"} {
		t.Run(mode, func(t *testing.T) {
			s, f := ledgerExpansionFixture(t)
			if mode == "disabled" {
				s.cfg.dontExpandGrants = true
			} else {
				s.run = newRunState()
				s.run.pushAction(t.Context(), Action{Op: SyncGrantExpansionOp})
			}
			root := ledgerIdentity(s.run.current())
			require.Equal(t, 1, runLedgerExpansionPages(t, s, f))
			row, found, err := f.ledger.GetLedgerRow(t.Context(), root)
			require.NoError(t, err)
			require.True(t, found)
			require.Empty(t, row.NextPageToken)
			require.Zero(t, row.GrantsWritten)
			require.Nil(t, s.ledgerExpansion)
		})
	}
}

type ledgerExpansionReadFailure struct{ c1zstore.Store }

func (s ledgerExpansionReadFailure) GetEntitlement(context.Context, *reader_v2.EntitlementsReaderServiceGetEntitlementRequest) (*reader_v2.EntitlementsReaderServiceGetEntitlementResponse, error) {
	return nil, errLedgerInjectedPage
}

func TestLedgerExpansionReadFailure(t *testing.T) {
	s, f := ledgerExpansionFixture(t)
	s.store = ledgerExpansionReadFailure{Store: s.store}
	before := ledgerRawSnapshot(t, f.engine)
	f.audit.enter(ledgerHandler)
	require.ErrorIs(t, s.invokeActionPage(t.Context(), s.run.current(), s.SyncGrantExpansion, false), errLedgerInjectedPage)
	f.audit.enter(ledgerLifecycle)
	require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
	require.Nil(t, s.ledgerExpansion)
	require.False(t, s.graph.get(t.Context()).Loaded)
}

func TestLedgerExpansionSchedulerCleanup(t *testing.T) {
	for _, cancel := range []bool{false, true} {
		t.Run(fmt.Sprint(cancel), func(t *testing.T) {
			s, f := ledgerExpansionFixture(t)
			if cancel {
				f.audit.enter(ledgerHandler)
				require.NoError(t, s.invokeActionPage(t.Context(), s.run.current(), s.SyncGrantExpansion, false))
				f.audit.enter(ledgerLifecycle)
				require.NotNil(t, s.ledgerExpansion)
			}
			ctx, stop := context.WithCancel(t.Context())
			defer stop()
			if cancel {
				stop()
			}
			_, err := s.parallelSync(ctx, ctx, nil)
			if cancel {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Nil(t, s.ledgerExpansion)
		})
	}
}
