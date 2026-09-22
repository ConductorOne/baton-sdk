package pebble

import (
	"context"
	"errors"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestLedgerClearRowsPreservesHistory(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	syncID, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	id := c1zstore.LedgerActionIdentity{Op: "list-resource-types"}
	page := e.Ledger().BeginPage()
	defer page.Discard()
	require.NoError(t, page.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "type"}.Build()))
	require.NoError(t, page.SetFact("finished"))
	require.NoError(t, page.SetFactValue("history", "retained"))
	history := c1zstore.LedgerCounters{Counters: map[string]uint64{"completed": 17}}
	require.NoError(t, page.SetCounterBucket("prior", 0, history))
	require.NoError(t, page.Commit(ctx, id, &c1zstore.LedgerRow{}))
	require.NoError(t, e.EndSyncWithStats(ctx, c1zstore.SyncStats{}))
	require.NoError(t, e.SetCurrentSync(ctx, syncID))
	before, err := e.GetSyncRunRecord(ctx, syncID)
	require.NoError(t, err)
	require.NoError(t, e.Ledger().ClearRows(ctx, []string{"finished"}))
	after, err := e.GetSyncRunRecord(ctx, syncID)
	require.NoError(t, err)
	require.True(t, proto.Equal(before, after))
	_, found, err := e.Ledger().GetRow(ctx, id)
	require.NoError(t, err)
	require.False(t, found)
	facts, err := e.Ledger().Facts(ctx)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"history": "retained"}, facts)
	counts, err := e.Ledger().Counters(ctx)
	require.NoError(t, err)
	require.Equal(t, history.Counters, counts.Counters)
	records, err := e.ListResourceTypes(ctx, &v2.ResourceTypesServiceListResourceTypesRequest{})
	require.NoError(t, err)
	require.Len(t, records.GetList(), 1)
	require.ErrorIs(t, e.CheckpointSync(ctx, "forbidden"), ErrLedgeredSyncWritesNoToken)
}

func TestLedgerClearRowsRefusesUnfinishedSync(t *testing.T) {
	e, _ := newTestEngine(t)
	_, err := e.StartNewSync(t.Context(), connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	id := c1zstore.LedgerActionIdentity{Op: "unfinished"}
	page := e.Ledger().BeginPage()
	defer page.Discard()
	require.NoError(t, page.SetFact("keep"))
	require.NoError(t, page.Commit(t.Context(), id, nil))
	require.ErrorContains(t, e.Ledger().ClearRows(t.Context(), []string{"keep"}), "unfinished")
	_, found, err := e.Ledger().GetRow(t.Context(), id)
	require.NoError(t, err)
	require.True(t, found)
	facts, err := e.Ledger().Facts(t.Context())
	require.NoError(t, err)
	require.Contains(t, facts, "keep")
}

func TestLedgerClearRowsFailureCuts(t *testing.T) {
	for _, cut := range []string{"stamped", "staged", "committed"} {
		t.Run(cut, func(t *testing.T) {
			e, _ := newTestEngine(t)
			ctx := t.Context()
			syncID, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
			require.NoError(t, err)
			require.NoError(t, e.CheckpointSync(ctx, "legacy"))
			_, err = e.Ledger().Takeover(ctx, "old", []string{"finished", "history"}, c1zstore.LedgerCounters{Counters: map[string]uint64{"completed": 9}})
			require.NoError(t, err)
			id := c1zstore.LedgerActionIdentity{Op: "old-page"}
			page := e.Ledger().BeginPage()
			defer page.Discard()
			require.NoError(t, page.Commit(ctx, id, nil))
			require.NoError(t, e.EndSyncWithStats(ctx, c1zstore.SyncStats{}))
			require.NoError(t, e.SetCurrentSync(ctx, syncID))
			before, err := e.GetSyncRunRecord(ctx, syncID)
			require.NoError(t, err)
			injected := errors.New("interrupted clear")
			e.test.ledgerClearRowsHook = func(stage string) error {
				if stage == cut {
					return injected
				}
				return nil
			}
			require.ErrorIs(t, e.Ledger().ClearRows(ctx, []string{"finished"}), injected)
			e.test.ledgerClearRowsHook = nil
			after, err := e.GetSyncRunRecord(ctx, syncID)
			require.NoError(t, err)
			require.True(t, proto.Equal(before, after))
			_, found, err := e.Ledger().GetRow(ctx, id)
			require.NoError(t, err)
			require.Equal(t, cut != "committed", found)
			_, frontier, err := e.Ledger().Frontier(ctx)
			require.NoError(t, err)
			require.Equal(t, cut != "committed", frontier)
			facts, err := e.Ledger().Facts(ctx)
			require.NoError(t, err)
			_, finished := facts["finished"]
			require.Equal(t, cut != "committed", finished)
			require.Contains(t, facts, "history")
			counters, err := e.Ledger().Counters(ctx)
			require.NoError(t, err)
			require.EqualValues(t, 9, counters.Counters["completed"])
			require.ErrorIs(t, e.CheckpointSync(ctx, "forbidden"), ErrLedgeredSyncWritesNoToken)
		})
	}
}

func TestLedgerClearRowsCrashImages(t *testing.T) {
	skipOnWindowsMemFS(t)
	for _, cut := range []string{"stamped", "staged", "committed"} {
		t.Run(cut, func(t *testing.T) {
			ctx := t.Context()
			fs := vfs.NewCrashableMem()
			cache := pebble.NewCache(8 << 20)
			defer cache.Unref()
			e, err := Open(ctx, "clear-rows", WithVFS(fs), WithSharedCache(cache), withPanicOnFatalLogger())
			require.NoError(t, err)
			defer func() { require.NoError(t, e.Close()) }()
			syncID, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
			require.NoError(t, err)
			id := c1zstore.LedgerActionIdentity{Op: "completed"}
			page := e.Ledger().BeginPage()
			defer page.Discard()
			require.NoError(t, page.SetFact("finished"))
			require.NoError(t, page.SetFact("history"))
			require.NoError(t, page.SetCounterBucket("prior", 0, c1zstore.LedgerCounters{Counters: map[string]uint64{"completed": 3}}))
			require.NoError(t, page.Commit(ctx, id, nil))
			require.NoError(t, e.EndSyncWithStats(ctx, c1zstore.SyncStats{}))
			require.NoError(t, e.SetCurrentSync(ctx, syncID))
			before, err := e.GetSyncRunRecord(ctx, syncID)
			require.NoError(t, err)
			var image *vfs.MemFS
			interrupted := errors.New("crash cut")
			e.test.ledgerClearRowsHook = func(stage string) error {
				if stage == cut {
					image = fs.CrashClone(vfs.CrashCloneCfg{})
					return interrupted
				}
				return nil
			}
			require.ErrorIs(t, e.Ledger().ClearRows(ctx, []string{"finished"}), interrupted)
			require.NotNil(t, image)
			recovered, err := Open(ctx, "clear-rows", WithVFS(image), WithSharedCache(cache), withPanicOnFatalLogger())
			require.NoError(t, err)
			defer func() { require.NoError(t, recovered.Close()) }()
			after, err := recovered.GetSyncRunRecord(ctx, syncID)
			require.NoError(t, err)
			require.True(t, proto.Equal(before, after))
			_, found, err := recovered.Ledger().GetRow(ctx, id)
			require.NoError(t, err)
			require.Equal(t, cut != "committed", found)
			facts, err := recovered.Ledger().Facts(ctx)
			require.NoError(t, err)
			_, finished := facts["finished"]
			require.Equal(t, found, finished)
			require.Contains(t, facts, "history")
			counters, err := recovered.Ledger().Counters(ctx)
			require.NoError(t, err)
			require.EqualValues(t, 3, counters.Counters["completed"])
			pending, err := recovered.Ledger().residuePending()
			require.NoError(t, err)
			require.True(t, pending)
			require.NoError(t, recovered.SetCurrentSync(ctx, syncID))
			require.NoError(t, recovered.EndSyncWithStats(ctx, c1zstore.SyncStats{}))
			pending, err = recovered.Ledger().residuePending()
			require.NoError(t, err)
			require.False(t, pending)
		})
	}
}

func TestLedgerClearRowsDefersPurgeUntilSeal(t *testing.T) {
	e, _ := newTestEngine(t)
	id, err := e.StartNewSync(t.Context(), connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	e.Ledger().SetRetainTokens(true)
	page := e.Ledger().BeginPage()
	require.NoError(t, page.Commit(t.Context(), grantsPageIdentity("group", "old-secret"), nil))
	require.NoError(t, e.EndSyncWithStats(t.Context(), c1zstore.SyncStats{}))
	require.Zero(t, e.test.ledgerResiduePurges.Load())
	require.Positive(t, checkpointNeedleHits(t, e, []byte("old-secret")))
	require.NoError(t, e.SetCurrentSync(t.Context(), id))
	require.NoError(t, e.Ledger().ClearRows(t.Context(), nil))
	require.Zero(t, e.test.ledgerResiduePurges.Load())
	pending, err := e.Ledger().residuePending()
	require.NoError(t, err)
	require.True(t, pending)
	e.Ledger().SetRetainTokens(false)
	next := e.Ledger().BeginPage()
	require.NoError(t, next.SetFact(c1zstore.LedgerFactDiscardOnSeal))
	require.NoError(t, next.Commit(t.Context(), grantsPageIdentity("group", "new-secret"), nil))
	require.NoError(t, e.EndSyncWithStats(t.Context(), c1zstore.SyncStats{}))
	require.EqualValues(t, 1, e.test.ledgerResiduePurges.Load())
	require.Zero(t, checkpointNeedleHits(t, e, []byte("old-secret")))
	require.Zero(t, checkpointNeedleHits(t, e, []byte("new-secret")))
}
