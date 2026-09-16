package pebble

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// ResetForNewSync reaches the fresh-Open state by running
// initKeyspaceStateLocked, which has to assign every keyspace-derived
// flag on every branch. Each flag is forced to its non-fresh value over a
// finished sync; the reset must bring each back.
func TestResetForNewSyncRederivesKeyspaceFlags(t *testing.T) {
	ctx := t.Context()
	e, _ := newTestEngine(t)
	_, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, e.PutResourceRecords(ctx, ledgerTestResource("user", "u1")))
	require.NoError(t, e.EndSync(ctx))

	require.NoError(t, e.db.ArmDeferredGrantIndex())
	e.db.SetGrantDigestsPresent(true)
	e.db.SetSourceScopeMayExist(true)
	e.grantDigestBuildPending.Store(true)
	e.grantDigestAbiStale.Store(true)
	e.ledger.inFlight.Store(true)

	_, err = e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	for name, stale := range map[string]bool{
		"DeferredIdxPending":      e.db.DeferredIdxPending(),
		"GrantDigestsPresent":     e.db.GrantDigestsPresent(),
		"SourceScopeMayExist":     e.db.SourceScopeMayExist(),
		"grantDigestBuildPending": e.grantDigestBuildPending.Load(),
		"grantDigestAbiStale":     e.grantDigestAbiStale.Load(),
		"ledgerInFlight":          e.ledger.inFlight.Load(),
	} {
		require.False(t, stale, "%s not re-derived by ResetForNewSync", name)
	}
	stamp, err := e.keyspaceVersionStamp()
	require.NoError(t, err)
	require.Equal(t, keyspaceVersion, stamp)
}
