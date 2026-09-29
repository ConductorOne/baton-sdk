package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

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
	// CO-039 §5: options belong to the attempt that ran the pass. The middle
	// attempt began the pass and committed its terminal page, so it is
	// latest; the original run stays first; the attempt that only finished
	// the seal recorded nothing.
	options, err := f.engine.GetArchivedLedgerOptions(t.Context(), next.ledger.runID)
	require.NoError(t, err)
	require.Nil(t, options, "the sealing attempt did none of the pass's work")
	options, err = f.engine.GetArchivedLedgerOptions(t.Context(), first.ledger.runID)
	require.NoError(t, err)
	require.NotNil(t, options)
	options, err = f.engine.GetArchivedLedgerOptions(t.Context(), second.ledger.runID)
	require.NoError(t, err)
	require.NotNil(t, options, "the attempt that ran the pass is latest")
	require.NoError(t, f.store.SetCurrentSync(t.Context(), id))
	require.NoError(t, f.ledger.BeginPass(t.Context(), nil, nil))
	counters, err := f.ledger.LedgerCounters(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 2, counters.Counters[ledgerCompletedPrefix+SyncGrantExpansionOp.String()])
}
