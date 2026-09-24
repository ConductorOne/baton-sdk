package pebble

import (
	"context"
	"errors"
	"fmt"
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
	require.NoError(t, e.EndSyncWithStats(t.Context(), c1zstore.SyncStats{}))
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

func TestLedgerDiscardArchiveWriteFailureRetries(t *testing.T) {
	e, _ := newTestEngine(t)
	_, err := e.StartNewSync(t.Context(), connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	w := e.Ledger().BeginPage()
	require.NoError(t, w.SetFact(c1zstore.LedgerFactDiscardOnSeal))
	id := grantsPageIdentity("group", "private-token")
	require.NoError(t, w.Commit(t.Context(), id, nil))
	e.test.ledgerArchiveHook = func(stage string) error {
		if stage == "before-write" {
			return errors.New("archive unavailable")
		}
		return nil
	}
	require.ErrorContains(t, e.EndSyncWithStats(t.Context(), c1zstore.SyncStats{}), "archive unavailable")
	finished, err := e.BoundSyncFinished(t.Context())
	require.NoError(t, err)
	require.False(t, finished)
	_, found, err := e.Ledger().GetRow(t.Context(), id)
	require.NoError(t, err)
	require.True(t, found)
	e.test.ledgerArchiveHook = nil
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
	require.NoError(t, e.Flush(t.Context()))
	images := map[string]*vfs.MemFS{}
	e.test.ledgerArchiveHook = func(stage string) error { images[stage] = fs.CrashClone(vfs.CrashCloneCfg{}); return nil }
	e.test.endSyncStampHook = func() error { images["before-ended"] = fs.CrashClone(vfs.CrashCloneCfg{}); return nil }
	require.NoError(t, e.EndSyncWithStats(t.Context(), c1zstore.SyncStats{}))
	images["ended"] = fs.CrashClone(vfs.CrashCloneCfg{})
	report, err := e.GetArchivedLedgerReport(t.Context())
	require.NoError(t, err)
	require.Len(t, images, 8)
	for stage, image := range images {
		t.Run(stage, func(t *testing.T) {
			reopened, err := Open(t.Context(), "discard-crash", WithVFS(image), withPanicOnFatalLogger())
			require.NoError(t, err)
			defer func() { require.NoError(t, reopened.Close()) }()
			require.NoError(t, reopened.SetCurrentSync(t.Context(), syncID))
			finished, err := reopened.BoundSyncFinished(t.Context())
			require.NoError(t, err)
			complete := stage == "ended" || stage == "after-marker-clear"
			require.Equal(t, previouslyFinished || complete || stage == "before-marker-clear", finished)
			if !complete {
				require.ErrorIs(t, reopened.CheckpointSync(t.Context(), "forbidden-token"), ErrLedgeredSyncWritesNoToken)
				require.ErrorIs(t, reopened.EndSync(t.Context()), ErrLedgeredSyncNeedsStats)
			}

			facts, err := reopened.Ledger().Facts(t.Context())
			require.NoError(t, err)
			switch stage {
			case "before-write", "after-write":
				require.NotEmpty(t, facts)
			case "ended", "after-marker-clear":
				require.Empty(t, facts)
			default:
				require.Equal(t, map[string]string{c1zstore.LedgerFactDiscardOnSeal: ""}, facts)
			}
			saved, err := reopened.GetArchivedLedgerReport(t.Context())
			require.NoError(t, err)
			if stage == "before-write" {
				require.Empty(t, saved)
			} else {
				require.JSONEq(t, string(report), string(saved))
			}
			require.NoError(t, reopened.RestoreLedgerArchive(t.Context()))
			facts, err = reopened.Ledger().Facts(t.Context())
			require.NoError(t, err)
			require.Contains(t, facts, "sync.seal_ready")
			counters, err := reopened.Ledger().Counters(t.Context())
			require.NoError(t, err)
			require.EqualValues(t, 7, counters.Counters["completed"])
			_, err = reopened.GetResourceRecord(t.Context(), "type", "one")
			require.NoError(t, err)
			require.NoError(t, reopened.EndSyncWithStats(t.Context(), c1zstore.SyncStats{}))
			saved, err = reopened.GetArchivedLedgerReport(t.Context())
			require.NoError(t, err)
			require.JSONEq(t, string(report), string(saved), "seal retry must not replace the report with an empty ledger")
		})
	}
}

func TestLedgerDiscardFailureRetriesSeal(t *testing.T) {
	for _, stage := range []string{"purge", "ended", "marker"} {
		t.Run(stage, func(t *testing.T) {
			e, _ := newTestEngine(t)
			id, err := e.StartNewSync(t.Context(), connectorstore.SyncTypeFull, "")
			require.NoError(t, err)
			w := e.Ledger().BeginPage()
			require.NoError(t, w.SetFact(c1zstore.LedgerFactDiscardOnSeal))
			require.NoError(t, w.SetFact("sync.seal_ready"))
			require.NoError(t, w.Commit(t.Context(), grantsPageIdentity("group", "secret-token"), nil))
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			injected := errors.New("seal stamp failed")
			switch stage {
			case "purge":
				e.test.ledgerArchiveHook = func(cut string) error {
					if cut == "after-delete" {
						cancel()
					}
					return nil
				}
			case "marker":
				e.test.ledgerArchiveHook = func(cut string) error {
					if cut == "before-marker-clear" {
						return injected
					}
					return nil
				}
			default:
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
			require.Equal(t, stage == "marker", finished)
			report, err := e.GetArchivedLedgerReport(t.Context())
			require.NoError(t, err)
			require.NotEmpty(t, report)
			facts, err := e.Ledger().Facts(t.Context())
			require.NoError(t, err)
			require.Equal(t, map[string]string{c1zstore.LedgerFactDiscardOnSeal: ""}, facts)
			if stage == "purge" {
				pending, err := e.ledger.residuePending()
				require.NoError(t, err)
				require.True(t, pending)
			}
			e.test.ledgerArchiveHook = nil
			e.test.endSyncStampHook = nil
			require.NoError(t, e.RestoreLedgerArchive(t.Context()))
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

func TestLedgerDiscardArchiveDoesNotRestoreIntoAnotherUnfinishedRun(t *testing.T) {
	e, _ := newTestEngine(t)
	id, err := e.StartNewSync(t.Context(), connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	w := e.Ledger().BeginPage()
	require.NoError(t, w.SetFact(c1zstore.LedgerFactDiscardOnSeal))
	require.NoError(t, w.SetFact("sync.seal_ready"))
	require.NoError(t, w.Commit(t.Context(), grantsPageIdentity("group", ""), nil))
	require.NoError(t, e.EndSyncWithStats(t.Context(), c1zstore.SyncStats{}))
	record, err := e.GetSyncRunRecord(t.Context(), id)
	require.NoError(t, err)
	differentID := ksuid.New().String()
	record.SetSyncId(differentID)
	record.SetCompacted(true)
	record.SetEndedAt(nil)
	require.NoError(t, e.PutSyncRunRecord(t.Context(), record))
	require.NoError(t, e.SetCurrentSync(t.Context(), differentID))
	require.NoError(t, e.RestoreLedgerArchive(t.Context()))
	facts, err := e.Ledger().Facts(t.Context())
	require.NoError(t, err)
	require.Empty(t, facts)
}

func TestLedgerDiscardBatchFailure(t *testing.T) {
	for _, failAt := range []int{1, 2} {
		t.Run(fmt.Sprint(failAt), func(t *testing.T) {
			e, _ := newTestEngine(t)
			id, err := e.StartNewSync(t.Context(), connectorstore.SyncTypeFull, "")
			require.NoError(t, err)
			w := e.Ledger().BeginPage()
			require.NoError(t, w.SetFact(c1zstore.LedgerFactDiscardOnSeal))
			require.NoError(t, w.SetFact("sync.seal_ready"))
			require.NoError(t, w.Commit(t.Context(), grantsPageIdentity("group", "private-token"), nil))
			injected := errors.New("disposal batch failed")
			calls := 0
			e.db.SetRecordCommitTestHook(func() error {
				calls++
				if calls == failAt {
					return injected
				}
				return nil
			})
			require.ErrorIs(t, e.EndSyncWithStats(t.Context(), c1zstore.SyncStats{}), injected)
			require.Equal(t, failAt, calls)
			e.db.SetRecordCommitTestHook(nil)
			require.NoError(t, e.SetCurrentSync(t.Context(), id))
			require.ErrorIs(t, e.CheckpointSync(t.Context(), "forbidden-token"), ErrLedgeredSyncWritesNoToken)
			report, err := e.GetArchivedLedgerReport(t.Context())
			require.NoError(t, err)
			require.NotEmpty(t, report)
			require.NoError(t, e.RestoreLedgerArchive(t.Context()))
			require.NoError(t, e.EndSyncWithStats(t.Context(), c1zstore.SyncStats{}))
			saved, err := e.GetArchivedLedgerReport(t.Context())
			require.NoError(t, err)
			require.JSONEq(t, string(report), string(saved))
		})
	}
}
