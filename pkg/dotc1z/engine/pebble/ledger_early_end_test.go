package pebble

import (
	"errors"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/stretchr/testify/require"
)

func TestLedgerEarlyEndPreservesRecovery(t *testing.T) {
	skipOnWindowsMemFS(t)
	for _, policy := range []string{"default", "retain", "discard"} {
		t.Run(policy, func(t *testing.T) {
			ctx := t.Context()
			fs := vfs.NewCrashableMem()
			e, err := Open(ctx, "early-end", WithVFS(fs), withPanicOnFatalLogger())
			require.NoError(t, err)
			defer func() { require.NoError(t, e.Close()) }()
			work := pendingTestSeed(t, e)
			syncID := e.CurrentSyncID()
			resource := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "one"}.Build()}.Build()
			w := e.Ledger().BeginPage()
			defer w.Discard()
			require.NoError(t, w.SetPendingWork(work))
			require.NoError(t, w.PutResources(ctx, resource))
			require.NoError(t, w.SetFact("sync.ingest_known"))
			require.NoError(t, w.SetCounterBucket("attempt", 0, c1zstore.LedgerCounters{Counters: map[string]uint64{"actions.completed": 7}, StepDurationsMs: map[string]int64{"resources": 123}}))
			if policy == "retain" {
				require.NoError(t, w.SetFact(c1zstore.LedgerFactRetainTokens))
			}
			if policy == "discard" {
				require.NoError(t, w.SetFact(c1zstore.LedgerFactDiscardOnSeal))
			}
			require.NoError(t, w.Commit(ctx, work.Action.Identity, &c1zstore.LedgerRow{NextPageToken: "private-next-token"}))
			before, initialized, err := e.Ledger().PendingWork(ctx, 0, 100)
			require.NoError(t, err)
			require.True(t, initialized)
			facts, err := e.Ledger().Facts(ctx)
			require.NoError(t, err)
			require.NoError(t, e.Flush(ctx))
			images := map[string]*vfs.MemFS{}
			e.test.endSyncStampHook = func() error {
				images["before-stamp"] = fs.CrashClone(vfs.CrashCloneCfg{})
				return errors.New("stamp failure")
			}
			require.ErrorContains(t, e.EndSync(ctx), "stamp failure")
			require.Equal(t, syncID, e.CurrentSyncID())
			e.test.endSyncStampHook = nil
			e.test.endSyncPreFlushHook = func() { images["after-stamp"] = fs.CrashClone(vfs.CrashCloneCfg{}) }
			require.NoError(t, e.EndSync(ctx))
			require.Empty(t, e.CurrentSyncID())
			images["after-finish"] = fs.CrashClone(vfs.CrashCloneCfg{})
			for stage, image := range images {
				t.Run(stage, func(t *testing.T) {
					r, err := Open(ctx, "early-end", WithVFS(image), withPanicOnFatalLogger())
					require.NoError(t, err)
					defer func() { require.NoError(t, r.Close()) }()
					require.NoError(t, r.SetCurrentSync(ctx, syncID))
					ended, err := r.BoundSyncFinished(ctx)
					require.NoError(t, err)
					require.Equal(t, stage != "before-stamp", ended)
					got, declared, err := r.Ledger().PendingWork(ctx, 0, 100)
					require.NoError(t, err)
					require.True(t, declared)
					require.Equal(t, before, got)
					gotFacts, err := r.Ledger().Facts(ctx)
					require.NoError(t, err)
					require.Equal(t, facts, gotFacts)
					row, found, err := r.Ledger().GetRow(ctx, work.Action.Identity)
					require.NoError(t, err)
					require.True(t, found)
					require.False(t, row.Scrubbed)
					require.Equal(t, "private-next-token", row.NextPageToken)
					_, err = r.GetResourceRecord(ctx, "group", "one")
					require.NoError(t, err)
					require.NoError(t, r.EndSync(ctx))
					stats, err := r.readSyncStats(ctx, syncID)
					require.NoError(t, err)
					require.NotNil(t, stats)
					require.EqualValues(t, 123, stats.GetStepDurationsMs()["resources"])
					require.NotNil(t, stats.GetIngestQuality())
					require.NoError(t, r.Cleanup(ctx))
					nextID, fresh, err := r.StartOrResumeSync(ctx, connectorstore.SyncTypeFull, "")
					require.NoError(t, err)
					require.True(t, fresh)
					require.NotEqual(t, syncID, nextID)
					_, declared, err = r.Ledger().PendingWork(ctx, 0, 100)
					require.NoError(t, err)
					require.False(t, declared)
					require.NoError(t, r.EndSync(ctx))
				})
			}
		})
	}
}

func TestLedgerEarlyEndAfterInterruptedDisposalKeepsStats(t *testing.T) {
	e, _ := newTestEngine(t)
	ctx := t.Context()
	id, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	w := e.Ledger().BeginPage()
	defer w.Discard()
	require.NoError(t, w.SetFact(c1zstore.LedgerFactDiscardOnSeal))
	require.NoError(t, w.SetFact("sync.ingest_known"))
	require.NoError(t, w.SetCounterBucket("attempt", 0, c1zstore.LedgerCounters{StepDurationsMs: map[string]int64{"resources": 321}}))
	require.NoError(t, w.Commit(ctx, grantsPageIdentity("group", "private-token"), nil))
	e.test.ledgerArchiveHook = func(stage string) error {
		if stage == "after-delete" {
			return errors.New("interrupted disposal")
		}
		return nil
	}
	require.ErrorContains(t, e.EndSyncWithStats(ctx, c1zstore.SyncStats{}), "interrupted disposal")
	e.test.ledgerArchiveHook = nil
	require.NoError(t, e.EndSync(ctx))
	stats, err := e.readSyncStats(ctx, id)
	require.NoError(t, err)
	require.NotNil(t, stats)
	require.EqualValues(t, 321, stats.GetStepDurationsMs()["resources"])
	require.NotNil(t, stats.GetIngestQuality())
	require.NoError(t, e.SetCurrentSync(ctx, id))
	require.NoError(t, e.RestoreLedgerArchive(ctx))
	counters, err := e.Ledger().Counters(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 321, counters.StepDurationsMs["resources"])
}
