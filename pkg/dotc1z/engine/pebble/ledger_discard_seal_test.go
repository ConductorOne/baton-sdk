package pebble

import (
	"context"
	"errors"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/require"
)

func TestLedgerDiscardSealPurgesOnce(t *testing.T) {
	e, _ := newTestEngine(t)
	_, err := e.StartNewSync(t.Context(), connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	w := e.Ledger().BeginPage()
	require.NoError(t, w.SetFact(c1zstore.LedgerFactDiscardOnSeal))
	require.NoError(t, w.SetFact("sync.seal_ready"))
	require.NoError(t, w.Commit(t.Context(), grantsPageIdentity("group", "secret-token"), nil))
	require.NoError(t, e.Flush(t.Context()))
	require.Positive(t, checkpointNeedleHits(t, e, []byte("secret-token")))
	writes := 0
	e.test.ledgerArchiveHook = func(stage string) error {
		if stage == "before-write" {
			writes++
		}
		return nil
	}
	require.NoError(t, sealWithStats(t, e, t.Context(), c1zstore.SyncStats{}))
	facts, err := e.Ledger().Facts(t.Context())
	require.NoError(t, err)
	require.Empty(t, facts)
	require.Zero(t, checkpointNeedleHits(t, e, []byte("secret-token")))
	report, err := e.ArchiveLedgerReport(t.Context())
	require.NoError(t, err)
	require.NotEmpty(t, report)
	require.NoError(t, e.Ledger().Drop(t.Context()))
	require.EqualValues(t, 1, e.test.ledgerResiduePurges.Load())
	require.Zero(t, e.LastSealCost().LedgerScrub)
	require.Equal(t, 1, writes, "post-seal report access must reuse the archive")
}

func TestLedgerDiscardDisposalFailureRetries(t *testing.T) {
	e, _ := newTestEngine(t)
	_, err := e.StartNewSync(t.Context(), connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	w := e.Ledger().BeginPage()
	require.NoError(t, w.SetFact(c1zstore.LedgerFactDiscardOnSeal))
	id := grantsPageIdentity("group", "private-token")
	require.NoError(t, w.Commit(t.Context(), id, nil))
	require.NoError(t, commitTerminalPage(t, e, t.Context()))
	injected := errors.New("disposal unavailable")
	e.db.SetRecordCommitTestHook(func() error { return injected })
	require.ErrorIs(t, e.EndSyncWithStats(t.Context(), c1zstore.SyncStats{}), injected)
	e.db.SetRecordCommitTestHook(nil)
	finished, err := e.BoundSyncFinished(t.Context())
	require.NoError(t, err)
	require.False(t, finished)
	_, found, err := e.Ledger().GetRow(t.Context(), id)
	require.NoError(t, err)
	require.True(t, found)
	saved, err := e.GetArchivedLedgerReport(t.Context())
	require.NoError(t, err)
	require.Empty(t, saved, "the archive rides the disposal batch; a failed batch lands neither")
	require.NoError(t, e.EndSyncWithStats(t.Context(), c1zstore.SyncStats{}))
	_, found, err = e.Ledger().GetRow(t.Context(), id)
	require.NoError(t, err)
	require.False(t, found)
	require.EqualValues(t, 1, e.test.ledgerResiduePurges.Load())
	require.Zero(t, checkpointNeedleHits(t, e, []byte("private-token")))
}

func TestLedgerDiscardDurableSealCuts(t *testing.T) {
	for _, finished := range []bool{false, true} {
		t.Run(map[bool]string{false: "fresh", true: "finished-binding"}[finished], func(t *testing.T) { testLedgerDiscardDurableSealCuts(t, finished) })
	}
}

func testLedgerDiscardDurableSealCuts(t *testing.T, previouslyFinished bool) {
	skipOnWindowsMemFS(t)
	fs := vfs.NewCrashableMem()
	e, err := Open(t.Context(), "discard-crash", WithVFS(fs), withPanicOnFatalLogger())
	require.NoError(t, err)
	defer func() { require.NoError(t, e.Close()) }()
	syncID, err := e.StartNewSync(t.Context(), connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	if previouslyFinished {
		require.NoError(t, e.EndSync(t.Context()))
		require.NoError(t, e.SetCurrentSync(t.Context(), syncID))
	}
	w := e.Ledger().BeginPage()
	require.NoError(t, w.PutResources(t.Context(), v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "type", Resource: "one"}.Build()}.Build()))
	require.NoError(t, w.SetFact(c1zstore.LedgerFactDiscardOnSeal))
	require.NoError(t, w.SetFact("sync.seal_ready"))
	require.NoError(t, w.SetCounterBucket("run", 0, c1zstore.LedgerCounters{Counters: map[string]uint64{"completed": 7}}))
	require.NoError(t, w.Commit(t.Context(), grantsPageIdentity("group", "private-token"), nil))
	require.NoError(t, commitTerminalPage(t, e, t.Context()))
	require.NoError(t, e.Flush(t.Context()))
	images := map[string]*vfs.MemFS{}
	e.test.ledgerArchiveHook = func(stage string) error { images[stage] = fs.CrashClone(vfs.CrashCloneCfg{}); return nil }
	e.test.endSyncStampHook = func() error { images["before-ended"] = fs.CrashClone(vfs.CrashCloneCfg{}); return nil }
	require.NoError(t, e.EndSyncWithStats(t.Context(), c1zstore.SyncStats{}))
	images["ended"] = fs.CrashClone(vfs.CrashCloneCfg{})
	report, err := e.GetArchivedLedgerReport(t.Context())
	require.NoError(t, err)
	require.Len(t, images, 4, "after-delete, after-purge, before-ended, ended")
	for stage, image := range images {
		t.Run(stage, func(t *testing.T) {
			reopened, err := Open(t.Context(), "discard-crash", WithVFS(image), withPanicOnFatalLogger())
			require.NoError(t, err)
			defer func() { require.NoError(t, reopened.Close()) }()
			require.NoError(t, reopened.SetCurrentSync(t.Context(), syncID))
			finished, err := reopened.BoundSyncFinished(t.Context())
			require.NoError(t, err)
			complete := stage == "ended"
			require.Equal(t, previouslyFinished || complete, finished, "only the stamp batch carries the verdict")
			stamp, err := reopened.keyspaceVersionStamp()
			require.NoError(t, err)
			if complete {
				require.Equal(t, keyspaceVersion, stamp, "a finished file opens under a token-only SDK")
			} else {
				require.Equal(t, keyspaceVersionLedgerInFlight, stamp, "an unfinished file refuses a token-only SDK; it would resume from Init over the sealed data")
				require.ErrorIs(t, reopened.CheckpointSync(t.Context(), "forbidden-token"), ErrLedgeredSyncWritesNoToken)
			}

			facts, err := reopened.Ledger().Facts(t.Context())
			require.NoError(t, err)
			_, phase, err := reopened.Ledger().PendingWork(t.Context(), 0, 1)
			require.NoError(t, err)
			if complete {
				require.Empty(t, facts, "the stamp batch removes the remaining family")
				require.Equal(t, c1zstore.LedgerQueueAbsent, phase)
			} else {
				require.Contains(t, facts, c1zstore.LedgerFactDiscardOnSeal, "facts survive until the stamp")
				require.Contains(t, facts, "sync.seal_ready")
				require.Equal(t, c1zstore.LedgerQueueSealing, phase, "the declaration survives until the stamp")
			}
			saved, err := reopened.GetArchivedLedgerReport(t.Context())
			require.NoError(t, err)
			require.JSONEq(t, string(report), string(saved), "the archive is durable from the disposal batch on")
			_, err = reopened.GetResourceRecord(t.Context(), "type", "one")
			require.NoError(t, err)
			if complete {
				require.NoError(t, reopened.Ledger().BeginPass(t.Context(), nil, nil))
				facts, err = reopened.Ledger().Facts(t.Context())
				require.NoError(t, err)
				require.Contains(t, facts, "sync.seal_ready", "the next pass starts from the archived facts")
				counters, err := reopened.Ledger().Counters(t.Context())
				require.NoError(t, err)
				require.EqualValues(t, 7, counters.Counters["completed"], "and the archived totals")
				return
			}
			counters, err := reopened.Ledger().Counters(t.Context())
			require.NoError(t, err)
			require.EqualValues(t, 7, counters.Counters["completed"])
			require.NoError(t, reopened.EndSyncWithStats(t.Context(), c1zstore.SyncStats{}))
			saved, err = reopened.GetArchivedLedgerReport(t.Context())
			require.NoError(t, err)
			require.JSONEq(t, string(report), string(saved), "seal retry must not replace the report with an empty ledger")
		})
	}
}

func TestLedgerDiscardFailureRetriesSeal(t *testing.T) {
	for _, stage := range []string{"purge", "ended"} {
		t.Run(stage, func(t *testing.T) {
			e, _ := newTestEngine(t)
			id, err := e.StartNewSync(t.Context(), connectorstore.SyncTypeFull, "")
			require.NoError(t, err)
			w := e.Ledger().BeginPage()
			require.NoError(t, w.SetFact(c1zstore.LedgerFactDiscardOnSeal))
			require.NoError(t, w.SetFact("sync.seal_ready"))
			require.NoError(t, w.Commit(t.Context(), grantsPageIdentity("group", "secret-token"), nil))
			require.NoError(t, commitTerminalPage(t, e, t.Context()))
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			injected := errors.New("seal stamp failed")
			if stage == "purge" {
				e.test.ledgerArchiveHook = func(cut string) error {
					if cut == "after-delete" {
						cancel()
					}
					return nil
				}
			} else {
				e.test.endSyncStampHook = func() error { return injected }
			}
			err = e.EndSyncWithStats(ctx, c1zstore.SyncStats{})
			if stage == "purge" {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.ErrorIs(t, err, injected)
			}
			require.NoError(t, e.SetCurrentSync(t.Context(), id))
			finished, err := e.BoundSyncFinished(t.Context())
			require.NoError(t, err)
			require.False(t, finished)
			report, err := e.GetArchivedLedgerReport(t.Context())
			require.NoError(t, err)
			require.NotEmpty(t, report)
			facts, err := e.Ledger().Facts(t.Context())
			require.NoError(t, err)
			require.Contains(t, facts, c1zstore.LedgerFactDiscardOnSeal, "facts survive a failed stamp")
			_, phase, err := e.Ledger().PendingWork(t.Context(), 0, 1)
			require.NoError(t, err)
			require.Equal(t, c1zstore.LedgerQueueSealing, phase)
			if stage == "purge" {
				pending, err := e.ledger.residuePending()
				require.NoError(t, err)
				require.True(t, pending)
			}
			e.test.ledgerArchiveHook = nil
			e.test.endSyncStampHook = nil
			require.NoError(t, e.EndSyncWithStats(t.Context(), c1zstore.SyncStats{}))
			saved, err := e.GetArchivedLedgerReport(t.Context())
			require.NoError(t, err)
			require.JSONEq(t, string(report), string(saved))
			require.NoError(t, e.SetCurrentSync(t.Context(), id))
			finished, err = e.BoundSyncFinished(t.Context())
			require.NoError(t, err)
			require.True(t, finished)
			require.Zero(t, checkpointNeedleHits(t, e, []byte("secret-token")))
		})
	}
}

func TestLedgerDiscardArchiveDoesNotBeginPassOnUnfinishedRun(t *testing.T) {
	e, _ := newTestEngine(t)
	id, err := e.StartNewSync(t.Context(), connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	w := e.Ledger().BeginPage()
	require.NoError(t, w.SetFact(c1zstore.LedgerFactDiscardOnSeal))
	require.NoError(t, w.SetFact("sync.seal_ready"))
	require.NoError(t, w.Commit(t.Context(), grantsPageIdentity("group", ""), nil))
	require.NoError(t, sealWithStats(t, e, t.Context(), c1zstore.SyncStats{}))
	record, err := e.GetSyncRunRecord(t.Context(), id)
	require.NoError(t, err)
	differentID := ksuid.New().String()
	record.SetSyncId(differentID)
	record.SetCompacted(true)
	record.SetEndedAt(nil)
	require.NoError(t, e.PutSyncRunRecord(t.Context(), record))
	require.NoError(t, e.SetCurrentSync(t.Context(), differentID))
	require.ErrorContains(t, e.Ledger().BeginPass(t.Context(), nil, nil), "unfinished")
	facts, err := e.Ledger().Facts(t.Context())
	require.NoError(t, err)
	require.Empty(t, facts)
}

func TestLedgerDiscardBatchFailure(t *testing.T) {
	for _, failAt := range []int{1, 2} {
		t.Run(map[int]string{1: "disposal", 2: "stamp"}[failAt], func(t *testing.T) {
			e, _ := newTestEngine(t)
			id, err := e.StartNewSync(t.Context(), connectorstore.SyncTypeFull, "")
			require.NoError(t, err)
			w := e.Ledger().BeginPage()
			require.NoError(t, w.SetFact(c1zstore.LedgerFactDiscardOnSeal))
			require.NoError(t, w.SetFact("sync.seal_ready"))
			page := grantsPageIdentity("group", "private-token")
			require.NoError(t, w.Commit(t.Context(), page, nil))
			require.NoError(t, commitTerminalPage(t, e, t.Context()))
			injected := errors.New("seal batch failed")
			calls := 0
			e.db.SetRecordCommitTestHook(func() error {
				calls++
				if calls == failAt {
					return injected
				}
				return nil
			})
			require.ErrorIs(t, e.EndSyncWithStats(t.Context(), c1zstore.SyncStats{}), injected)
			require.Equal(t, failAt, calls, "disposal then stamp; nothing after")
			e.db.SetRecordCommitTestHook(nil)
			require.NoError(t, e.SetCurrentSync(t.Context(), id))
			require.ErrorIs(t, e.CheckpointSync(t.Context(), "forbidden-token"), ErrLedgeredSyncWritesNoToken)
			_, found, err := e.Ledger().GetRow(t.Context(), page)
			require.NoError(t, err)
			require.Equal(t, failAt == 1, found, "rows go with the disposal batch")
			report, err := e.GetArchivedLedgerReport(t.Context())
			require.NoError(t, err)
			require.Equal(t, failAt == 2, len(report) > 0, "the archive lands with the disposal batch")
			require.NoError(t, e.EndSyncWithStats(t.Context(), c1zstore.SyncStats{}), "the declaration is still sealing; the retry needs no restore")
			saved, err := e.GetArchivedLedgerReport(t.Context())
			require.NoError(t, err)
			require.NotEmpty(t, saved)
			if failAt == 2 {
				require.JSONEq(t, string(report), string(saved), "a seal retried after disposal keeps the report the rows produced")
			}
		})
	}
}
