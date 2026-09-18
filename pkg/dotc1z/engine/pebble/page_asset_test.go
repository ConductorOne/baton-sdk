package pebble

import (
	"errors"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/stretchr/testify/require"
)

func TestPageWriterAssetCommitFailureAndRetry(t *testing.T) {
	ctx := t.Context()
	e, _ := newTestEngine(t)
	_, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	ref := v2.AssetRef_builder{Id: "asset"}.Build()
	require.NoError(t, e.PutAsset(ctx, ref, "old/type", []byte("old")))
	w := e.Ledger().BeginPage()
	defer w.Discard()
	require.NoError(t, w.PutAsset(ctx, ref, "first/type", []byte("first")))
	require.NoError(t, w.PutAsset(ctx, ref, "last/type", []byte("last")))
	id := c1zstore.LedgerActionIdentity{Op: "assets", ResourceID: "resource"}
	injected := errors.New("asset page commit failed")
	e.db.SetRecordCommitTestHook(func() error { return injected })
	require.ErrorIs(t, w.Commit(ctx, id, nil), injected)
	e.db.SetRecordCommitTestHook(nil)
	asset, err := e.GetAssetRecord(ctx, ref.GetId())
	require.NoError(t, err)
	require.Equal(t, []byte("old"), asset.GetData())
	require.Equal(t, "old/type", asset.GetContentType())
	_, found, err := e.Ledger().GetRow(ctx, id)
	require.NoError(t, err)
	require.False(t, found)
	require.NoError(t, w.Commit(ctx, id, nil))
	asset, err = e.GetAssetRecord(ctx, ref.GetId())
	require.NoError(t, err)
	require.Equal(t, []byte("last"), asset.GetData())
	require.Equal(t, "last/type", asset.GetContentType())
	require.Equal(t, e.CurrentSyncID(), asset.GetSyncId())
	require.NotNil(t, asset.GetDiscoveredAt())
	_, found, err = e.Ledger().GetRow(ctx, id)
	require.NoError(t, err)
	require.True(t, found)
	require.ErrorIs(t, w.PutAsset(ctx, ref, "ignored/type", nil), ErrPageUnitCommitted)
}

func TestPageWriterAssetValidationAndDiscard(t *testing.T) {
	ctx := t.Context()
	e, _ := newTestEngine(t)
	unbound := e.Ledger().BeginPage()
	require.ErrorIs(t, unbound.PutAsset(ctx, v2.AssetRef_builder{Id: "asset"}.Build(), "type", nil), ErrNoCurrentSync)
	unbound.Discard()
	_, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	w := e.Ledger().BeginPage()
	require.ErrorContains(t, w.PutAsset(ctx, nil, "type", nil), "nil assetRef")
	require.ErrorContains(t, w.PutAsset(ctx, &v2.AssetRef{}, "type", nil), "empty assetRef.Id")
	ref := v2.AssetRef_builder{Id: "asset"}.Build()
	require.NoError(t, w.PutAsset(ctx, ref, "type", nil))
	w.Discard()
	require.ErrorIs(t, w.PutAsset(ctx, ref, "type", nil), ErrPageUnitCommitted)
	require.ErrorIs(t, w.Commit(ctx, c1zstore.LedgerActionIdentity{Op: "assets"}, nil), ErrPageUnitCommitted)
	_, err = e.GetAssetRecord(ctx, "asset")
	require.ErrorIs(t, err, pebble.ErrNotFound)
}

func TestPageWriterAssetRefusesReplacementSync(t *testing.T) {
	ctx := t.Context()
	e, _ := newTestEngine(t)
	_, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	w := e.Ledger().BeginPage()
	defer w.Discard()
	require.NoError(t, w.PutAsset(ctx, v2.AssetRef_builder{Id: "asset"}.Build(), "type", []byte("old sync")))
	require.NoError(t, e.EndSync(ctx))
	_, err = e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	id := c1zstore.LedgerActionIdentity{Op: "assets"}
	require.ErrorIs(t, w.Commit(ctx, id, nil), ErrPageUnitForeignSync)
	_, err = e.GetAssetRecord(ctx, "asset")
	require.ErrorIs(t, err, pebble.ErrNotFound)
	_, found, err := e.Ledger().GetRow(ctx, id)
	require.NoError(t, err)
	require.False(t, found)
}

func TestPageAssetStageRejectsOtherKeyFamilies(t *testing.T) {
	e, _ := newTestEngine(t)
	batch := e.db.NewRecordBatch()
	defer batch.Close()
	require.ErrorContains(t, batch.StageAssetPut(encodeLedgerKey(c1zstore.LedgerActionIdentity{Op: "assets"}), nil), "outside this op's keyspace family")
	require.True(t, batch.Empty())
}
