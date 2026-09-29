package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// A pass whose work is all done but whose terminal page never landed is
// still open. A plain rebind seals it; ended_at from an early EndSync does
// not turn it into a finished pass to be recollected.
func TestLedgerDrainedPassSealsWithoutRecollection(t *testing.T) {
	for _, earlyEnd := range []bool{false, true} {
		t.Run(map[bool]string{false: "unfinished", true: "early-ended"}[earlyEnd], func(t *testing.T) {
			ctx := t.Context()
			source, want, _ := ledgerUnexpandedSource(t)
			f := openLedgerFixtureAt(t, t.TempDir()+"/drained.c1z", false)
			first, err := NewSyncer(ctx, source, WithConnectorStore(f.store), WithDontExpandGrants())
			require.NoError(t, err)
			interrupted := errors.New("interrupted before the terminal page")
			first.(*syncer).testHooks.ingestHaltHook = func(stage string) error {
				if stage == haltStageInvariantsComplete {
					return interrupted
				}
				return nil
			}
			require.ErrorIs(t, first.Sync(ctx), interrupted)
			id := first.(*syncer).syncID
			pending, phase, err := f.ledger.PendingWork(ctx, 0, 1)
			require.NoError(t, err)
			require.Equal(t, c1zstore.LedgerQueueCollecting, phase)
			require.Empty(t, pending, "drained")
			if earlyEnd {
				require.NoError(t, f.store.EndSync(ctx))
			}
			require.NoError(t, f.store.Close(ctx))

			f = openLedgerFixtureAt(t, f.path, false)
			next, err := NewSyncer(ctx, ledgerExpansionConnector{mockConnector: newMockConnector()}, WithConnectorStore(f.store), WithSyncID(id), WithDontExpandGrants())
			require.NoError(t, err)
			require.NoError(t, next.Sync(ctx), "the rebind seals the drained pass; the connector refuses any collection call")
			require.NoError(t, f.store.SetCurrentSync(ctx, id))
			_, phase, err = f.ledger.PendingWork(ctx, 0, 1)
			require.NoError(t, err)
			require.Equal(t, c1zstore.LedgerQueueAbsent, phase, "sealed")
			finished, err := f.engine.BoundSyncFinished(ctx)
			require.NoError(t, err)
			require.True(t, finished)
			grants, err := f.store.ListGrants(ctx, &v2.GrantsServiceListGrantsRequest{})
			require.NoError(t, err)
			var ids []string
			for _, g := range grants.GetList() {
				ids = append(ids, g.GetId())
			}
			require.ElementsMatch(t, want, ids, "the first pass's records are the sealed records")
		})
	}
}
