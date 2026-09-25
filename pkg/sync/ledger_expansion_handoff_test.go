package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"path/filepath"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/stretchr/testify/require"
)

type ledgerExpansionHandoffFailure struct {
	c1zstore.Store
	c1zstore.PageLedgerStore
	EntitlementGraphStore
	c1zstore.GrantGenerationDigestReader
	fail  string
	binds int
}

func (s *ledgerExpansionHandoffFailure) SetCurrentSync(ctx context.Context, id string) error {
	s.binds++
	if s.fail == "rebind" && s.binds == 2 {
		return errLedgerInjectedPage
	}
	return s.Store.SetCurrentSync(ctx, id)
}
func (s *ledgerExpansionHandoffFailure) EndSyncWithStats(ctx context.Context, stats c1zstore.SyncStats) error {
	if s.fail == "seal" {
		return errLedgerInjectedPage
	}
	return s.PageLedgerStore.EndSyncWithStats(ctx, stats)
}

func TestLedgerExpansionRequestAcrossSealFailure(t *testing.T) {
	for _, cut := range []string{"none", "seal", "rebind"} {
		t.Run(cut, func(t *testing.T) {
			ctx := t.Context()
			source, base, expanded := ledgerUnexpandedSource(t)
			f := openLedgerFixtureAt(t, filepath.Join(t.TempDir(), "handoff.c1z"), false)
			first, err := NewSyncer(ctx, source, WithConnectorStore(f.store), WithDontExpandGrants(), WithLedgerDebug(true), WithRetainLedgerTokens(true))
			require.NoError(t, err)
			first.(*syncer).caps.pageLedger = ledgerExpansionSealFailure{PageLedgerStore: f.ledger}
			require.ErrorIs(t, first.Sync(ctx), errLedgerInjectedPage)
			id := first.(*syncer).syncID
			require.NoError(t, f.store.Close(ctx))
			f = openLedgerFixtureAt(t, f.path, false)
			wrapper := &ledgerExpansionHandoffFailure{Store: f.store, PageLedgerStore: f.ledger, fail: cut,
				EntitlementGraphStore: f.store.EntitlementGraphStore, GrantGenerationDigestReader: f.store.GrantGenerationDigestReader}
			host, err := NewSyncer(ctx, ledgerExpansionConnector{mockConnector: newMockConnector()},
				WithConnectorStore(wrapper), WithSyncID(id), WithOnlyExpandGrants(), WithPreserveEntitlementGraph())
			require.NoError(t, err)
			observeLedgerRestore(t, host.(*syncer), f)
			err = host.Sync(ctx)
			if cut != "none" {
				require.ErrorIs(t, err, errLedgerInjectedPage)
				grants, readErr := f.store.ListGrants(ctx, &v2.GrantsServiceListGrantsRequest{})
				require.NoError(t, readErr)
				var ids []string
				for _, g := range grants.GetList() {
					ids = append(ids, g.GetId())
				}
				require.ElementsMatch(t, base, ids)
				priorFacts, factsErr := f.ledger.LedgerFacts(ctx)
				require.NoError(t, factsErr)
				require.Contains(t, priorFacts, c1zstore.LedgerFactRetainTokens)
				graph, graphErr := GraphFromStore(ctx, f.store, id)
				require.NoError(t, graphErr)
				require.Nil(t, graph, "no expansion-complete graph before requested expansion runs")
				require.NoError(t, f.store.Close(ctx))
				f = openLedgerFixtureAt(t, f.path, false)
				host, err = NewSyncer(ctx, ledgerExpansionConnector{mockConnector: newMockConnector()},
					WithConnectorStore(f.store), WithSyncID(id), WithOnlyExpandGrants(), WithPreserveEntitlementGraph())
				require.NoError(t, err)
				observeLedgerRestore(t, host.(*syncer), f)
				err = host.Sync(ctx)
			}
			require.NoError(t, err)
			require.False(t, host.(*syncer).ledgerDebug, "old retention must not enable debug for the new request")
			facts, err := f.ledger.LedgerFacts(ctx)
			require.NoError(t, err)
			require.Empty(t, facts)
			require.NoError(t, f.store.Close(ctx))
			f = openLedgerFixtureAt(t, f.path, false)
			grants, err := f.store.ListGrants(ctx, &v2.GrantsServiceListGrantsRequest{})
			require.NoError(t, err)
			var ids []string
			for _, g := range grants.GetList() {
				ids = append(ids, g.GetId())
			}
			require.ElementsMatch(t, expanded, ids)
			graph, err := GraphFromStore(ctx, f.store, id)
			require.NoError(t, err)
			require.NotNil(t, graph)
			opts, err := f.ledger.GetArchivedLedgerOptions(ctx, "")
			require.NoError(t, err)
			require.NotNil(t, opts)
			require.True(t, opts.Requested.OnlyExpandGrants)
			require.False(t, opts.EffectiveRetainLedgerTokens)
			record, err := f.engine.GetSyncRunRecord(ctx, id)
			require.NoError(t, err)
			require.NotNil(t, record.GetEndedAt())
		})
	}
}
