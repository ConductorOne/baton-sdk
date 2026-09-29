package pebble

import (
	"context"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// Moves a collecting declaration to sealing with a terminal page. A ledgered
// sync with no declaration gets an empty one first, the state a syncer-driven
// run would have. Returns the terminal page's error so a test
// can assert on EndSync's verdict for a queue that is not drained.
func commitTerminalPage(t testing.TB, e *Engine, ctx context.Context) error {
	t.Helper()
	_, phase, err := e.Ledger().PendingWork(ctx, 0, 1)
	require.NoError(t, err)
	switch phase {
	case c1zstore.LedgerQueueSealing:
		return nil
	case c1zstore.LedgerQueueAbsent:
		active, err := e.ledger.active()
		require.NoError(t, err)
		if !active {
			return nil
		}
		require.NoError(t, e.withWriteAllowSealed(func() error {
			batch := e.db.NewRecordBatch()
			defer batch.Close()
			if err := stageWorkState(batch, 0, c1zstore.LedgerQueueCollecting); err != nil {
				return err
			}
			return batch.Commit(pebble.Sync)
		}))
	case c1zstore.LedgerQueueCollecting, c1zstore.LedgerQueueExpanding:
	}
	writer := e.Ledger().BeginPage()
	defer writer.Discard()
	require.NoError(t, writer.SetTerminal())
	return writer.Commit(ctx, c1zstore.LedgerActionIdentity{Op: "sync-terminal-v1"}, &c1zstore.LedgerRow{})
}

func sealWithStats(t testing.TB, e *Engine, ctx context.Context, stats c1zstore.SyncStats) error {
	t.Helper()
	_ = commitTerminalPage(t, e, ctx)
	return e.EndSyncWithStats(ctx, stats)
}
