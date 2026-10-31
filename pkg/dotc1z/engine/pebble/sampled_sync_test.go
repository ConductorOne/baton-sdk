package pebble

import (
	"fmt"
	"testing"

	cpebble "github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/stretchr/testify/require"
)

func commitSampledTerminal(t *testing.T, e *Engine, count uint64) {
	t.Helper()
	ctx := t.Context()
	require.NoError(t, e.Ledger().BeginCollecting(ctx, nil, map[string]string{c1zstore.LedgerFactDiscardOnSeal: "true"}))
	writer := e.Ledger().BeginPage()
	defer writer.Discard()
	require.NoError(t, writer.SetTerminal())
	require.NoError(t, writer.SetCounterBucket("sampled-marker", 0, c1zstore.LedgerCounters{
		Counters: map[string]uint64{c1zstore.ListPageWindowCounterPrefix + "sampled": count},
	}))
	require.NoError(t, writer.Commit(ctx, c1zstore.LedgerActionIdentity{Op: "sampled-marker-terminal"}, &c1zstore.LedgerRow{}))
}

func TestSampledSyncMarkerAtSeal(t *testing.T) {
	for _, mode := range []string{"token", "ledger-overlay", "ledger-recovery"} {
		for _, sampled := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/sampled=%t", mode, sampled), func(t *testing.T) {
				ctx := t.Context()
				e, _ := newTestEngine(t)
				syncID, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
				require.NoError(t, err)
				count := uint64(0)
				if sampled {
					count = 1
				}
				before, err := e.GetSyncRunRecord(ctx, syncID)
				require.NoError(t, err)
				require.False(t, before.GetSampled())
				if mode == "token" {
					require.NoError(t, e.CheckpointSync(ctx, fmt.Sprintf(`{"counters":{"list_page_window.sampled":%d}}`, count)))
					require.NoError(t, e.EndSync(ctx))
				} else {
					commitSampledTerminal(t, e, count)
					if mode == "ledger-overlay" {
						require.NoError(t, e.EndSyncWithStats(ctx, c1zstore.SyncStats{Run: c1zstore.RunStats{
							Counters: map[string]uint64{c1zstore.ListPageWindowCounterPrefix + "sampled": count},
						}}))
					} else {
						require.NoError(t, e.EndSync(ctx))
					}
				}
				record, err := e.GetSyncRunRecord(ctx, syncID)
				require.NoError(t, err)
				require.NotNil(t, record.GetEndedAt())
				require.Equal(t, sampled, record.GetSampled())
				run, err := e.SyncMeta().LatestFullSync(ctx)
				require.NoError(t, err)
				require.NotNil(t, run)
				require.Equal(t, sampled, run.Sampled)
				require.Equal(t, !sampled, run.UsableAsReplaySource())
				response, err := e.GetLatestFinishedSync(ctx, &reader_v2.SyncsReaderServiceGetLatestFinishedSyncRequest{})
				require.NoError(t, err)
				require.Equal(t, sampled, response.GetSync().GetSampled())
				listed, err := e.sortedSyncRuns(ctx)
				require.NoError(t, err)
				require.Len(t, listed, 1)
				require.Equal(t, sampled, listed[0].Sampled)
			})
		}
	}
}

func TestSampledSyncMarkerSurvivesResealWithoutCounters(t *testing.T) {
	ctx := t.Context()
	e, _ := newTestEngine(t)
	syncID, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, e.CheckpointSync(ctx, `{"counters":{"list_page_window.sampled":1}}`))
	require.NoError(t, e.EndSync(ctx))
	require.NoError(t, e.SetCurrentSync(ctx, syncID))
	require.NoError(t, e.CheckpointSync(ctx, `{"version":1}`))
	require.NoError(t, e.EndSyncWithStats(ctx, c1zstore.SyncStats{}))
	run, err := e.GetSyncRunRecord(ctx, syncID)
	require.NoError(t, err)
	require.True(t, run.GetSampled())
}

func TestSampledSyncMarkerStampDurability(t *testing.T) {
	skipOnWindowsMemFS(t)
	for _, mode := range []string{"token", "ledger"} {
		t.Run(mode, func(t *testing.T) {
			ctx := t.Context()
			fs := vfs.NewCrashableMem()
			cache := cpebble.NewCache(8 << 20)
			defer cache.Unref()
			e, err := Open(ctx, "sampled-seal", WithVFS(fs), WithSharedCache(cache), withPanicOnFatalLogger(), WithGrantDigestIndex(false))
			require.NoError(t, err)
			defer e.Close()
			syncID, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
			require.NoError(t, err)
			require.NoError(t, e.PutResources(ctx, V3ResourceToV2(ledgerTestResource("group", "one"))))
			if mode == "token" {
				require.NoError(t, e.CheckpointSync(ctx, `{"counters":{"list_page_window.sampled":1}}`))
			} else {
				commitSampledTerminal(t, e, 1)
			}
			beforeStamp := false
			e.test.endSyncStampHook = func() error {
				beforeStamp = true
				run, err := e.GetSyncRunRecord(ctx, syncID)
				require.NoError(t, err)
				require.Nil(t, run.GetEndedAt())
				require.False(t, run.GetSampled())
				e.takeSyncStatsOverlay(syncID)
				return nil
			}
			var image *vfs.MemFS
			e.test.endSyncPreFlushHook = func() { image = fs.CrashClone(vfs.CrashCloneCfg{}) }
			if mode == "token" {
				require.NoError(t, e.EndSync(ctx))
			} else {
				require.NoError(t, e.EndSyncWithStats(ctx, c1zstore.SyncStats{Run: c1zstore.RunStats{
					Counters: map[string]uint64{c1zstore.ListPageWindowCounterPrefix + "sampled": 1},
				}}))
			}
			require.True(t, beforeStamp)
			require.NotNil(t, image)
			recovered, err := Open(ctx, "sampled-seal", WithVFS(image), WithSharedCache(cache), withPanicOnFatalLogger(), WithGrantDigestIndex(false))
			require.NoError(t, err)
			defer recovered.Close()
			run, err := recovered.GetSyncRunRecord(ctx, syncID)
			require.NoError(t, err)
			require.NotNil(t, run.GetEndedAt())
			require.True(t, run.GetSampled())
			_, err = recovered.GetResourceRecord(ctx, "group", "one")
			require.NoError(t, err)
			stats, err := recovered.readSyncStats(ctx, syncID)
			require.NoError(t, err)
			require.Nil(t, stats, "marker must be durable before the optional stats sidecar")
		})
	}
}
