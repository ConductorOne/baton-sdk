package pebble

import (
	"context"
	"slices"
	"testing"

	cpebble "github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/internal/rawdb"
)

// Ledger rows are read back from the file when the seal scrubs their page
// tokens. A child without an identity, which this SDK never writes, must fail
// the seal instead of panicking in the scrub.
func TestSecurity_SealRejectsLedgerChildWithoutIdentity(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	_, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	u := e.ledger.newPageUnit()
	require.NoError(t, u.StageResources(ledgerTestResource("user", "u0")))
	require.NoError(t, u.Commit(ctx, grantsPageIdentity("github", "tok"), v3.LedgerRow_builder{
		Children: []*v3.LedgerChild{v3.LedgerChild_builder{Identity: ledgerIdentityToProto(grantsPageIdentity("child", "child-tok"))}.Build()},
	}.Build()))

	db := e.db.UnsafeForTesting()
	iter, err := db.NewIter(&cpebble.IterOptions{LowerBound: rawdb.LedgerKeyPrefix(), UpperBound: upperBoundOf(rawdb.LedgerKeyPrefix())})
	require.NoError(t, err)
	require.True(t, iter.First(), "premise: the page committed a ledger row")
	key := slices.Clone(iter.Key())
	row := &v3.LedgerRow{}
	require.NoError(t, unmarshalRecord(iter.Value(), row))
	require.NoError(t, iter.Close())
	row.GetChildren()[0].ClearIdentity()
	val, err := marshalRecord(row)
	require.NoError(t, err)
	require.NoError(t, db.Set(key, val, cpebble.Sync))

	require.ErrorContains(t, e.EndSyncWithStats(ctx, c1zstore.SyncStats{}), "ledger row child has no identity")
}
