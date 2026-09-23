package pebble

import (
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/stretchr/testify/require"
)

func TestLedgerArchiveDurableCrashImages(t *testing.T) {
	skipOnWindowsMemFS(t)
	fs := vfs.NewCrashableMem()
	e, err := Open(t.Context(), "archive-crash", WithVFS(fs), withPanicOnFatalLogger())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, e.Close()) })
	syncID, err := e.StartNewSync(t.Context(), connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, e.Ledger().InitializePendingWork(t.Context(), nil))
	page := e.Ledger().BeginPage()
	require.NoError(t, page.SetFact("skip-grants"))
	require.NoError(t, page.SetCounterBucket("attempt", 0, c1zstore.LedgerCounters{Counters: map[string]uint64{"completed": 7}}))
	identity := c1zstore.LedgerActionIdentity{Op: "resources", PageToken: "private-cursor"}
	require.NoError(t, page.Commit(t.Context(), identity, &c1zstore.LedgerRow{}))
	require.NoError(t, e.EndSyncWithStats(t.Context(), c1zstore.SyncStats{}))
	images := map[string]*vfs.MemFS{"sealed": fs.CrashClone(vfs.CrashCloneCfg{})}
	report, err := e.ArchiveLedgerReport(t.Context())
	require.NoError(t, err)
	images["archived"] = fs.CrashClone(vfs.CrashCloneCfg{})
	require.NoError(t, e.Ledger().Drop(t.Context()))
	images["disposed"] = fs.CrashClone(vfs.CrashCloneCfg{})
	require.NoError(t, e.SetCurrentSync(t.Context(), syncID))
	e.db.SetRecordCommitTestHook(func() error {
		images["restore-stamped"] = fs.CrashClone(vfs.CrashCloneCfg{})
		return nil
	})
	require.NoError(t, e.RestoreLedgerArchive(t.Context()))
	e.db.SetRecordCommitTestHook(nil)
	images["restored"] = fs.CrashClone(vfs.CrashCloneCfg{})
	require.Len(t, images, 5)
	for label, image := range images {
		t.Run(label, func(t *testing.T) {
			reopened, err := Open(t.Context(), "archive-crash", WithVFS(image), withPanicOnFatalLogger())
			require.NoError(t, err)
			defer func() { require.NoError(t, reopened.Close()) }()
			saved, err := reopened.GetArchivedLedgerReport(t.Context())
			require.NoError(t, err)
			if label == "sealed" {
				require.Empty(t, saved)
			} else {
				require.JSONEq(t, string(report), string(saved))
			}
			require.NoError(t, reopened.SetCurrentSync(t.Context(), syncID))
			finished, err := reopened.BoundSyncFinished(t.Context())
			require.NoError(t, err)
			require.True(t, finished)
			facts, err := reopened.Ledger().Facts(t.Context())
			require.NoError(t, err)
			if label == "disposed" || label == "restore-stamped" {
				require.Empty(t, facts)
			} else {
				require.Contains(t, facts, "skip-grants")
			}
			require.NoError(t, reopened.RestoreLedgerArchive(t.Context()))
			require.NoError(t, reopened.RestoreLedgerArchive(t.Context()))
			facts, err = reopened.Ledger().Facts(t.Context())
			require.NoError(t, err)
			require.Contains(t, facts, "skip-grants")
			counters, err := reopened.Ledger().Counters(t.Context())
			require.NoError(t, err)
			require.EqualValues(t, 7, counters.Counters["completed"])
			token, err := reopened.CurrentSyncStep(t.Context())
			require.NoError(t, err)
			require.Empty(t, token)
		})
	}
}
