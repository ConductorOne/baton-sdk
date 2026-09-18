package pebble

import (
	"errors"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/stretchr/testify/require"
)

func TestSealCostIncludesFailedFinalize(t *testing.T) {
	e, _ := newTestEngine(t)
	_, err := e.StartNewSync(t.Context(), connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.Equal(t, SealCost{}, e.LastSealCost())
	page := e.Ledger().BeginPage()
	defer page.Discard()
	require.NoError(t, page.Commit(t.Context(), c1zstore.LedgerActionIdentity{Op: "init"}, nil))
	injected := errors.New("stamp failure")
	e.test.endSyncStampHook = func() error { return injected }
	require.ErrorIs(t, e.EndSyncWithStats(t.Context(), c1zstore.SyncStats{}), injected)
	cost := e.LastSealCost()
	require.Positive(t, cost.LedgerScrub)
	require.Positive(t, cost.LedgerPurge)
	cost.LedgerScrub = 0
	require.Positive(t, e.LastSealCost().LedgerScrub)
}
