package dotc1z

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// The terminal page the syncer commits before EndSyncWithStats.
func commitTerminalPage(t *testing.T, ctx context.Context, ledger c1zstore.PageLedgerStore) {
	t.Helper()
	page := ledger.BeginPage()
	defer page.Discard()
	require.NoError(t, page.SetQueueSealing())
	require.NoError(t, page.Commit(c1zstore.WithOpenPage(ctx), c1zstore.LedgerActionIdentity{Op: "sync-terminal-v1"}, &c1zstore.LedgerRow{}))
}

func sealLedger(t *testing.T, ctx context.Context, ledger c1zstore.PageLedgerStore, stats c1zstore.SyncStats) error {
	t.Helper()
	commitTerminalPage(t, ctx, ledger)
	return ledger.EndSyncWithStats(ctx, stats)
}
