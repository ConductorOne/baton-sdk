package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"testing"

	"github.com/cockroachdb/pebble/v2"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/stretchr/testify/require"
)

func TestLedgerAssetPageSurvivesReopen(t *testing.T) {
	f := newLedgerFixture(t)
	require.NoError(t, f.store.Close(t.Context()))
	f = openLedgerFixtureAt(t, f.path, false)
	before := ledgerRawSnapshot(t, f.engine)
	ctx := c1zstore.WithOpenPage(t.Context())
	id := c1zstore.LedgerActionIdentity{Op: SyncAssetsOp.String(), ResourceTypeID: "type", ResourceID: "resource"}
	data := []byte("asset bytes")
	f.audit.enter(ledgerHandler)
	page := f.ledger.BeginPage()
	defer page.Discard()
	require.NoError(t, page.PutAsset(ctx, v2.AssetRef_builder{Id: "asset"}.Build(), "image/png", data))
	data[0] = 'X'
	_, err := f.engine.GetAssetRecord(ctx, "asset")
	require.ErrorIs(t, err, pebble.ErrNotFound)
	require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
	require.NoError(t, page.Commit(ctx, id, &c1zstore.LedgerRow{Attempt: "assets"}))
	f.audit.enter(ledgerLifecycle)
	require.NoError(t, f.store.Close(t.Context()))
	f = openLedgerFixtureAt(t, f.path, false)
	asset, err := f.engine.GetAssetRecord(t.Context(), "asset")
	require.NoError(t, err)
	require.Equal(t, []byte("asset bytes"), asset.GetData())
	require.Equal(t, "image/png", asset.GetContentType())
	require.Equal(t, f.engine.CurrentSyncID(), asset.GetSyncId())
	require.NotNil(t, asset.GetDiscoveredAt())
	_, found, err := f.ledger.GetLedgerRow(t.Context(), id)
	require.NoError(t, err)
	require.True(t, found)
}

func TestLedgerAssetPageDiscardKeepsPriorValue(t *testing.T) {
	f := newLedgerFixture(t)
	ref := v2.AssetRef_builder{Id: "asset"}.Build()
	require.NoError(t, f.store.PutAsset(t.Context(), ref, "old/type", []byte("old")))
	require.NoError(t, f.store.Close(t.Context()))
	f = openLedgerFixtureAt(t, f.path, false)
	before := ledgerRawSnapshot(t, f.engine)
	ctx := c1zstore.WithOpenPage(t.Context())
	f.audit.enter(ledgerHandler)
	page := f.ledger.BeginPage()
	require.NoError(t, page.PutAsset(ctx, ref, "new/type", []byte("new")))
	page.Discard()
	f.audit.enter(ledgerLifecycle)
	require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
	require.NoError(t, f.store.Close(t.Context()))
	f = openLedgerFixtureAt(t, f.path, false)
	asset, err := f.engine.GetAssetRecord(t.Context(), "asset")
	require.NoError(t, err)
	require.Equal(t, []byte("old"), asset.GetData())
	require.Equal(t, "old/type", asset.GetContentType())
}
