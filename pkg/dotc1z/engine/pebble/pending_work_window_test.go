package pebble

import (
	"testing"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/stretchr/testify/require"
)

func TestPendingWorkPreservesWindowPages(t *testing.T) {
	ctx := t.Context()
	e, _ := newTestEngine(t)
	work := pendingTestSeed(t, e)
	writer := e.Ledger().BeginPage()
	defer writer.Discard()
	require.NoError(t, writer.SetPendingWork(work))
	require.NoError(t, writer.Commit(ctx, work.Action.Identity, &c1zstore.LedgerRow{NextPageToken: "B", WindowPages: 2}))
	items := pendingItems(t, e)
	require.Len(t, items, 1)
	require.EqualValues(t, 2, items[0].WindowPages)
	require.Equal(t, "B", items[0].Action.Identity.PageToken)
	row, found := readHistoryRow(t, e, work.Action.Identity, work.ID, work.Revision)
	require.True(t, found)
	require.EqualValues(t, 2, row.GetWindowPages())
	converted := ledgerRowFromProto(row)
	require.EqualValues(t, 2, converted.WindowPages)
	require.EqualValues(t, 2, ledgerRowToProto(converted).GetWindowPages())

	moved := items[0]
	require.NoError(t, restartTestCommit(t, e, moved, "", restartTestCaps, "window-count", 0, 0, nil))
	restarted := pendingItems(t, e)
	require.Len(t, restarted, 1)
	require.EqualValues(t, 2, restarted[0].WindowPages)
	require.Equal(t, "", restarted[0].Action.Identity.PageToken)
	restartRow, found := readHistoryRow(t, e, moved.Action.Identity, moved.ID, moved.Revision)
	require.True(t, found)
	require.True(t, restartRow.GetRestarted())
	require.EqualValues(t, 2, restartRow.GetWindowPages())
}
