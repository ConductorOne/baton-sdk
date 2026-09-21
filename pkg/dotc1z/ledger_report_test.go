package dotc1z

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/stretchr/testify/require"
)

func TestPebbleStoreGenerateLedgerReport(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "report.c1z")
	store, err := NewStore(ctx, path, WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	store.(c1zstore.WriteHookStore).SetWriteHook(func(_ context.Context, _ c1zstore.WriteHookEvent) error {
		return errors.New("report attempted a direct write")
	})
	ledger := store.(c1zstore.PageLedgerStore)
	_, err = store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	page := ledger.BeginPage()
	require.NoError(t, page.PutGrants(ctx, mkV2Grant("grant", "entitlement", "user", "principal")))
	require.NoError(t, page.Commit(ctx, c1zstore.LedgerActionIdentity{Op: "SyncGrants", ResourceTypeID: "group", PageToken: "secret-cursor"},
		&c1zstore.LedgerRow{ObservationsRecorded: true, ConnectorAttempts: 3, ConnectorErrors: 2,
			Collection: &c1zstore.LedgerCollectionStats{ListResponses: 1, GrantsReceived: 2, GrantsExcludedByType: 1}}))
	before, err := ledger.GenerateLedgerReport(c1zstore.WithOpenPage(ctx))
	require.NoError(t, err)
	var payload map[string]any
	require.NoError(t, json.Unmarshal(before, &payload))
	require.EqualValues(t, 1, payload["pages"])
	require.EqualValues(t, 1, payload["record_writes"])
	require.EqualValues(t, 3, payload["attempt_observations"].(map[string]any)["connector_attempts"])
	require.EqualValues(t, 1, payload["collection_observations"].(map[string]any)["grants_excluded_by_type"])
	require.NotContains(t, string(before), "secret-cursor")
	require.NoError(t, ledger.EndSyncWithStats(ctx, c1zstore.SyncStats{}))
	require.NoError(t, store.Close(ctx))
	bytesBefore, err := os.ReadFile(path)
	require.NoError(t, err)
	reopened, err := NewStore(ctx, path, WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	reopened.(c1zstore.WriteHookStore).SetWriteHook(func(_ context.Context, _ c1zstore.WriteHookEvent) error {
		return errors.New("report attempted a write")
	})
	after, err := reopened.(c1zstore.PageLedgerStore).GenerateLedgerReport(c1zstore.WithOpenPage(ctx))
	require.NoError(t, err)
	require.JSONEq(t, string(before), string(after))
	require.False(t, reopened.(*pebbleStore).dirty)
	require.NoError(t, reopened.Close(ctx))
	bytesAfter, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, bytesBefore, bytesAfter)
}
