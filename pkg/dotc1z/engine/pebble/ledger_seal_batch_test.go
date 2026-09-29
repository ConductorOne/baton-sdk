package pebble

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// The finished verdict is the last ledger write. A rebind that finds
// ended_at therefore finds the family in its final shape, never mid-seal.
func TestLedgerSealWritesNothingAfterStamp(t *testing.T) {
	for _, retained := range []bool{false, true} {
		t.Run(map[bool]string{false: "default", true: "retained"}[retained], func(t *testing.T) {
			e, _ := newTestEngine(t)
			syncID, err := e.StartNewSync(t.Context(), connectorstore.SyncTypeFull, "")
			require.NoError(t, err)
			w := e.Ledger().BeginPage()
			if !retained {
				require.NoError(t, w.SetFact(c1zstore.LedgerFactDiscardOnSeal))
			}
			require.NoError(t, w.SetFact("known"))
			require.NoError(t, w.SetCounterBucket("run", 0, c1zstore.LedgerCounters{Counters: map[string]uint64{"pages": 1}}))
			page := grantsPageIdentity("group", "cursor")
			require.NoError(t, w.Commit(t.Context(), page, nil))
			require.NoError(t, commitTerminalPage(t, e, t.Context()))

			stamped := false
			afterStamp := 0
			e.test.endSyncPreFlushHook = func() { stamped = true }
			e.db.SetRecordCommitTestHook(func() error {
				if stamped {
					afterStamp++
				}
				return nil
			})
			require.NoError(t, e.EndSyncWithStats(t.Context(), c1zstore.SyncStats{}))
			e.db.SetRecordCommitTestHook(nil)
			e.test.endSyncPreFlushHook = nil
			require.Zero(t, afterStamp, "no record batch follows the stamp")

			require.NoError(t, e.SetCurrentSync(t.Context(), syncID))
			_, phase, err := e.Ledger().PendingWork(t.Context(), 0, 1)
			require.NoError(t, err)
			require.Equal(t, c1zstore.LedgerQueueAbsent, phase, "the declaration goes with the stamp")
			facts, err := e.Ledger().Facts(t.Context())
			require.NoError(t, err)
			_, found, err := e.Ledger().GetRow(t.Context(), page)
			require.NoError(t, err)
			saved, err := e.GetArchivedLedgerReport(t.Context())
			require.NoError(t, err)
			require.NotEmpty(t, saved, "both modes archive at the stamp")
			if retained {
				require.Contains(t, facts, "known")
				require.True(t, found)
			} else {
				require.Empty(t, facts)
				require.False(t, found)
			}
		})
	}
}

// A retained seal keeps rows, facts and counters. The frontier holds the
// legacy token verbatim and the scheduling relations belong to the pass; both
// go with the declaration.
func TestLedgerRetainedSealDropsFrontierAndScheduling(t *testing.T) {
	e, _ := newTestEngine(t)
	ctx := t.Context()
	_, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, e.CheckpointSync(ctx, "legacy-secret-token"))
	seed := c1zstore.LedgerWork{Action: c1zstore.LedgerChild{Identity: c1zstore.LedgerActionIdentity{Op: "list-resources"}}, SchedulingKey: "resource:root"}
	_, err = e.Ledger().BeginCollectingFromToken(ctx, "old", "legacy-secret-token", nil, c1zstore.LedgerCounters{}, []c1zstore.LedgerWork{seed})
	require.NoError(t, err)
	pending, _, err := e.Ledger().PendingWork(ctx, 0, 1)
	require.NoError(t, err)
	require.NoError(t, pendingTestCommit(t, e, pending[0], ""))
	scheduled, err := e.Ledger().HasScheduledWork(ctx, "resource:root")
	require.NoError(t, err)
	require.True(t, scheduled)
	_, frontier, err := e.Ledger().Frontier(ctx)
	require.NoError(t, err)
	require.True(t, frontier)

	require.NoError(t, sealWithStats(t, e, ctx, c1zstore.SyncStats{}))
	scheduled, err = e.Ledger().HasScheduledWork(ctx, "resource:root")
	require.NoError(t, err)
	require.False(t, scheduled, "scheduling relations belong to the pass")
	_, frontier, err = e.Ledger().Frontier(ctx)
	require.NoError(t, err)
	require.False(t, frontier, "the frontier carried the token verbatim")
	require.Zero(t, checkpointNeedleHits(t, e, []byte("legacy-secret-token")))
	facts, err := e.Ledger().Facts(ctx)
	require.NoError(t, err)
	require.Contains(t, facts, "work-committed", "retained history keeps its facts")
}

// A seal retried after the disposal batch cannot rebuild the report (the rows
// are gone) but must not freeze the accounting: the retrying attempt's bucket
// lands in the archive before the stamp deletes the family's counters.
func TestLedgerSealRetryArchivesTheRetryBucket(t *testing.T) {
	e, _ := newTestEngine(t)
	ctx := t.Context()
	syncID, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	w := e.Ledger().BeginPage()
	require.NoError(t, w.SetFact(c1zstore.LedgerFactDiscardOnSeal))
	require.NoError(t, w.SetCounterBucket("first", 0, c1zstore.LedgerCounters{Counters: map[string]uint64{"pages": 1}}))
	require.NoError(t, w.Commit(ctx, grantsPageIdentity("group", "cursor"), nil))
	require.NoError(t, commitTerminalPage(t, e, ctx))
	injected := errors.New("stamp failed")
	e.test.endSyncStampHook = func() error { return injected }
	require.ErrorIs(t, e.EndSyncWithStats(ctx, c1zstore.SyncStats{}), injected)
	e.test.endSyncStampHook = nil

	// The retrying attempt records its own accounting, as prepareSeal does.
	require.NoError(t, e.Ledger().PutCounterBucket(ctx, "retry", c1zstore.RunBucketWorker, c1zstore.LedgerCounters{Counters: map[string]uint64{"retries": 1}}))
	require.NoError(t, e.EndSyncWithStats(ctx, c1zstore.SyncStats{}))
	require.NoError(t, e.SetCurrentSync(ctx, syncID))
	archive, err := e.readLedgerArchive(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 1, archive.Counters.Counters["pages"])
	require.EqualValues(t, 1, archive.Counters.Counters["retries"], "the retry's bucket is in the archive")
	require.Contains(t, archive.Facts, c1zstore.LedgerFactDiscardOnSeal)
}
