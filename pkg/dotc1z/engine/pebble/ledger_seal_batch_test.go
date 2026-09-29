package pebble

import (
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
