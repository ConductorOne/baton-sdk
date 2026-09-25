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
	require.NoError(t, ledger.InitializePendingWork(ctx, nil))
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
	var sealedPayload map[string]any
	require.NoError(t, json.Unmarshal(after, &sealedPayload))
	require.EqualValues(t, 2, payload["ledger_keys_scanned"])
	require.EqualValues(t, 1, sealedPayload["ledger_keys_scanned"])
	delete(payload, "ledger_keys_scanned")
	delete(sealedPayload, "ledger_keys_scanned")
	require.Equal(t, payload, sealedPayload)
	require.False(t, reopened.(*pebbleStore).dirty)
	require.NoError(t, reopened.Close(ctx))
	bytesAfter, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, bytesBefore, bytesAfter)
}

func TestPebbleStoreArchivedReportSurvivesDropAndReopen(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "archived.c1z")
	store, err := NewStore(ctx, path, WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	ledger := store.(c1zstore.PageLedgerStore)
	store.(c1zstore.WriteHookStore).SetWriteHook(func(_ context.Context, ev c1zstore.WriteHookEvent) error {
		if ev.Bypass == "" {
			return errors.New("unregistered page write")
		}
		return nil
	})
	id, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, ledger.InitializePendingWork(ctx, nil))
	writer := ledger.BeginPage()
	options, err := json.Marshal(c1zstore.LedgerReportOptions{Attempt: "one", Requested: c1zstore.LedgerRequestedOptions{SkipGrants: true}})
	require.NoError(t, err)
	require.NoError(t, writer.SetFactValue(c1zstore.LedgerFactReportOptions, string(options)))
	require.NoError(t, writer.SetFact("skip_grants"))
	require.NoError(t, writer.SetCounterBucket("one", 0, c1zstore.LedgerCounters{Counters: map[string]uint64{"completed": 9}}))
	require.NoError(t, writer.Commit(c1zstore.WithOpenPage(ctx), c1zstore.LedgerActionIdentity{Op: "test"}, &c1zstore.LedgerRow{}))
	require.NoError(t, ledger.EndSyncWithStats(ctx, c1zstore.SyncStats{}))
	report, err := ledger.ArchiveLedgerReport(ctx)
	require.NoError(t, err)
	require.NoError(t, ledger.DropLedger(ctx))
	require.NoError(t, store.Close(ctx))
	store, err = NewStore(ctx, path, WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close(ctx)) }()
	ledger = store.(c1zstore.PageLedgerStore)
	saved, err := ledger.GetArchivedLedgerReport(ctx)
	require.NoError(t, err)
	require.JSONEq(t, string(report), string(saved))
	savedOptions, err := ledger.GetArchivedLedgerOptions(ctx, "")
	require.NoError(t, err)
	require.True(t, savedOptions.Requested.SkipGrants)
	facts, err := ledger.LedgerFacts(ctx)
	require.NoError(t, err)
	require.Empty(t, facts)
	require.NoError(t, store.SetCurrentSync(ctx, id))
	require.NoError(t, ledger.RestoreLedgerArchive(ctx))
	counters, err := ledger.LedgerCounters(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 9, counters.Counters["completed"])
	facts, err = ledger.LedgerFacts(ctx)
	require.NoError(t, err)
	require.Contains(t, facts, "skip_grants")
}
