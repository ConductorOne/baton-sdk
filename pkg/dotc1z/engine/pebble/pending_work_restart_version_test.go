package pebble

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/internal/rawdb"
	"github.com/stretchr/testify/require"
)

func requirePendingWorkVersion(t *testing.T, e *Engine, want byte) {
	t.Helper()
	value, closer, err := e.db.Get(rawdb.LedgerWorkStateKey())
	require.NoError(t, err)
	defer closer.Close()
	require.Len(t, value, 10)
	require.Equal(t, want, value[0])
}

func TestPendingWorkRestartVersion(t *testing.T) {
	e, dir := newTestEngine(t)
	work := pendingTestSeed(t, e)
	requirePendingWorkVersion(t, e, 2)
	work.PageTokenRestarted = true
	injected := errors.New("restart commit failed")
	e.db.SetRecordCommitTestHook(func() error { return injected })
	require.ErrorIs(t, pendingTestCommit(t, e, work, "wrapped"), injected)
	e.db.SetRecordCommitTestHook(nil)
	requirePendingWorkVersion(t, e, 2)
	items, _, err := e.Ledger().PendingWork(t.Context(), 0, 10)
	require.NoError(t, err)
	require.False(t, items[0].PageTokenRestarted)

	require.NoError(t, pendingTestCommit(t, e, work, "wrapped"))
	requirePendingWorkVersion(t, e, 3)
	require.NoError(t, e.Close())
	e, err = Open(t.Context(), filepath.Join(dir, "db"))
	require.NoError(t, err)
	defer e.Close()
	require.NoError(t, e.SetCurrentSync(t.Context(), work.SyncID))
	requirePendingWorkVersion(t, e, 3)
	items, _, err = e.Ledger().PendingWork(t.Context(), 0, 10)
	require.NoError(t, err)
	require.True(t, items[0].PageTokenRestarted)
	require.Equal(t, "wrapped", items[0].Action.Identity.PageToken)
	child := c1zstore.LedgerChild{Identity: c1zstore.LedgerActionIdentity{Op: "grant-expansion"}}
	require.NoError(t, pendingTestCommit(t, e, items[0], "next", child))
	requirePendingWorkVersion(t, e, 3)
	items, _, err = e.Ledger().PendingWork(t.Context(), 0, 10)
	require.NoError(t, err)
	require.Len(t, items, 2)
	require.NoError(t, pendingTestCommit(t, e, items[0], "child-next", child))
	requirePendingWorkVersion(t, e, 3)
	require.NoError(t, pendingTestCommit(t, e, items[1], ""))
	items, _, err = e.Ledger().PendingWork(t.Context(), 0, 10)
	require.NoError(t, err)
	require.NoError(t, pendingTestCommit(t, e, items[1], ""))
	require.NoError(t, e.Ledger().BeginExpanding(t.Context()))
	requirePendingWorkVersion(t, e, 3)
	items, _, err = e.Ledger().PendingWork(t.Context(), 0, 10)
	require.NoError(t, err)
	require.NoError(t, e.Ledger().CompletePendingWork(t.Context(), items[0], "attempt", c1zstore.LedgerCounters{}))
	require.NoError(t, phaseTestTerminal(t, e, nil))
	requirePendingWorkVersion(t, e, 3)
}

func TestPendingWorkRestartSeedVersion(t *testing.T) {
	for _, takeover := range []bool{false, true} {
		t.Run(fmt.Sprintf("takeover=%t", takeover), func(t *testing.T) {
			e, _ := newTestEngine(t)
			_, err := e.StartNewSync(t.Context(), connectorstore.SyncTypeFull, "")
			require.NoError(t, err)
			seeds := []c1zstore.LedgerWork{
				{Action: c1zstore.LedgerChild{Identity: c1zstore.LedgerActionIdentity{Op: "list-grants", PageToken: "wrapped"}}, PageTokenRestarted: true},
				{Action: c1zstore.LedgerChild{Identity: c1zstore.LedgerActionIdentity{Op: "list-resources"}}},
			}
			if takeover {
				require.NoError(t, e.CheckpointSync(t.Context(), "legacy-token"))
				_, err = e.Ledger().BeginFromToken(t.Context(), "attempt", "legacy-token", nil, c1zstore.LedgerCounters{}, seeds, c1zstore.LedgerQueueCollecting)
			} else {
				err = e.Ledger().BeginCollecting(t.Context(), seeds, nil)
			}
			require.NoError(t, err)
			requirePendingWorkVersion(t, e, 3)
			items, _, err := e.Ledger().PendingWork(t.Context(), 0, 10)
			require.NoError(t, err)
			require.Len(t, items, 2)
			require.True(t, items[1].PageTokenRestarted)
		})
	}
}
