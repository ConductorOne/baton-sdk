package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/stretchr/testify/require"
)

func TestLedgerEarlyEndPublicRecoveryAndReset(t *testing.T) {
	for _, reset := range []bool{false, true} {
		name := "explicit-resume"
		if reset {
			name = "force-reset"
		}
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			f := openLedgerFixtureAt(t, filepath.Join(t.TempDir(), "early-end.c1z"), false)
			c := &ledgerTypesConnector{mockConnector: newMockConnector()}
			stopped, cancel := context.WithCancel(ctx)
			defer cancel()
			first, err := NewSyncer(ctx, c, WithConnectorStore(f.store), WithSkipEntitlementsAndGrants(true), WithProgressHandler(func(*Progress) {
				if len(c.calls) == 1 {
					cancel()
				}
			}))
			require.NoError(t, err)
			require.ErrorIs(t, first.Sync(stopped), context.Canceled)
			id := f.engine.CurrentSyncID()
			before, declared, err := f.ledger.PendingWork(ctx, 0, 100)
			require.NoError(t, err)
			require.True(t, declared)
			require.NotEmpty(t, before)
			require.NoError(t, f.store.Close(ctx))
			f = openLedgerFixtureAt(t, f.path, false)
			resumedID, fresh, err := f.store.StartOrResumeSync(ctx, connectorstore.SyncTypeResourcesOnly, "")
			require.NoError(t, err)
			require.False(t, fresh)
			require.Equal(t, id, resumedID)
			require.NoError(t, f.store.EndSync(ctx))
			require.NoError(t, f.store.Close(ctx))
			f = openLedgerFixtureAt(t, f.path, false)
			require.NoError(t, f.store.SetCurrentSync(ctx, id))
			after, declared, err := f.ledger.PendingWork(ctx, 0, 100)
			require.NoError(t, err)
			require.True(t, declared)
			require.Equal(t, before, after)
			if reset {
				require.NoError(t, f.store.EndSync(ctx))
				require.NoError(t, f.store.Cleanup(ctx))
				next, fresh, err := f.store.StartOrResumeSync(ctx, connectorstore.SyncTypeResourcesOnly, "")
				require.NoError(t, err)
				require.True(t, fresh)
				require.NotEqual(t, id, next)
				id = next
			}
			c = &ledgerTypesConnector{mockConnector: newMockConnector()}
			resumed, err := NewSyncer(ctx, c, WithConnectorStore(f.store), WithSyncID(id), WithSkipEntitlementsAndGrants(true))
			require.NoError(t, err)
			require.NoError(t, resumed.Sync(ctx))
			expected := []string{"page-2"}
			if reset {
				expected = []string{"", "page-2"}
			}
			require.Equal(t, expected, c.calls)
		})
	}
}
