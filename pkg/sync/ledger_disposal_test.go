package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/stretchr/testify/require"
)

type ledgerDiscardUnfinishedImage struct {
	c1zstore.PageLedgerStore
	construct func() error
}

func (s ledgerDiscardUnfinishedImage) EndSyncWithStats(ctx context.Context, stats c1zstore.SyncStats) error {
	if err := s.PageLedgerStore.EndSyncWithStats(ctx, stats); err != nil {
		return err
	}
	if err := s.construct(); err != nil {
		return err
	}
	return errLedgerInjectedPage
}

func TestLedgerDiscardUnfinishedArchiveResumesWithoutCollection(t *testing.T) {
	f := newLedgerFixture(t)
	connector := newMockConnector()
	_, _, err := connector.AddGroup(t.Context(), "team")
	require.NoError(t, err)
	first, err := NewSyncer(t.Context(), connector, WithConnectorStore(f.store))
	require.NoError(t, err)
	s := first.(*syncer)
	s.caps.pageLedger = ledgerDiscardUnfinishedImage{PageLedgerStore: f.ledger, construct: func() error {
		if err := f.store.SetCurrentSync(t.Context(), s.syncID); err != nil {
			return err
		}
		page := f.ledger.BeginPage()
		if err := page.SetFact(c1zstore.LedgerFactDiscardOnSeal); err != nil {
			page.Discard()
			return err
		}
		if err := page.Commit(t.Context(), c1zstore.LedgerActionIdentity{Op: "disposal-image"}, nil); err != nil {
			page.Discard()
			return err
		}
		if err := f.ledger.ClearLedgerRows(t.Context(), nil); err != nil {
			return err
		}
		record, err := f.engine.GetSyncRunRecord(t.Context(), s.syncID)
		if err != nil {
			return err
		}
		record.SetEndedAt(nil)
		return f.engine.PutSyncRunRecord(t.Context(), record)
	}}
	require.ErrorIs(t, s.Sync(t.Context()), errLedgerInjectedPage)
	facts, err := f.ledger.LedgerFacts(t.Context())
	require.NoError(t, err)
	require.Equal(t, map[string]string{c1zstore.LedgerFactDiscardOnSeal: ""}, facts)
	saved, err := f.ledger.GetArchivedLedgerReport(t.Context())
	require.NoError(t, err)
	require.NotEmpty(t, saved)
	syncID := s.syncID
	require.NoError(t, f.store.Close(t.Context()))
	f = openLedgerFixtureAt(t, f.path, false)
	next, err := NewSyncer(t.Context(), ledgerExpansionConnector{mockConnector: newMockConnector()}, WithConnectorStore(f.store), WithSyncID(syncID))
	require.NoError(t, err)
	require.NoError(t, next.Sync(t.Context()))
	report, err := f.ledger.GetArchivedLedgerReport(t.Context())
	require.NoError(t, err)
	require.JSONEq(t, string(saved), string(report))
	require.NoError(t, f.store.SetCurrentSync(t.Context(), syncID))
	finished, err := f.ledger.BoundSyncFinished(t.Context())
	require.NoError(t, err)
	require.True(t, finished)
	facts, err = f.ledger.LedgerFacts(t.Context())
	require.NoError(t, err)
	require.Empty(t, facts)
}

func TestLedgerDiscardPendingFinishedBindingDoesNotRestartProcessing(t *testing.T) {
	_, f := ledgerExpansionFixture(t)
	first := newLedgerExpansionPublicSyncer(t, f)
	first.cfg.ledgerDebug = false
	require.NoError(t, first.Sync(t.Context()))
	id := first.syncID
	require.NoError(t, f.store.SetCurrentSync(t.Context(), id))
	before, err := f.engine.GetSyncRunRecord(t.Context(), id)
	require.NoError(t, err)
	second := newLedgerExpansionPublicSyncer(t, f)
	second.cfg.ledgerDebug = false
	second.caps.pageLedger = ledgerExpansionSealFailure{PageLedgerStore: f.ledger}
	require.ErrorIs(t, second.Sync(t.Context()), errLedgerInjectedPage)
	pending, err := f.engine.GetSyncRunRecord(t.Context(), id)
	require.NoError(t, err)
	require.Equal(t, before.GetEndedAt(), pending.GetEndedAt())
	require.NoError(t, f.store.Close(t.Context()))
	f = openLedgerFixtureAt(t, f.path, false)
	require.NoError(t, f.store.SetCurrentSync(t.Context(), id))
	resumed, err := NewSyncer(t.Context(), ledgerExpansionConnector{mockConnector: newMockConnector()},
		WithConnectorStore(f.store), WithSyncID(id), WithPreserveEntitlementGraph())
	require.NoError(t, err)
	next := resumed.(*syncer)
	next.testHooks.ledgerHandler = func(context.Context, *Action, *ledgerPage) error {
		t.Error("pending seal must not restart processing")
		return errLedgerInjectedPage
	}
	require.NoError(t, next.Sync(t.Context()))
	options, err := f.ledger.GetArchivedLedgerOptions(t.Context(), second.ledger.runID)
	require.NoError(t, err)
	require.NotNil(t, options)
	require.NoError(t, f.store.SetCurrentSync(t.Context(), id))
	require.NoError(t, f.ledger.RestoreLedgerArchive(t.Context()))
	counters, err := f.ledger.LedgerCounters(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 2, counters.Counters[ledgerCompletedPrefix+SyncGrantExpansionOp.String()])
}
