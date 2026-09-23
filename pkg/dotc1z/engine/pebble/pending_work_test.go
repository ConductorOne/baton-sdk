package pebble

import (
	"context"
	"errors"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/internal/rawdb"
	"github.com/stretchr/testify/require"
)

func pendingTestSeed(t *testing.T, e *Engine) c1zstore.LedgerWork {
	t.Helper()
	_, err := e.StartNewSync(t.Context(), connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	seed := c1zstore.LedgerWork{Action: c1zstore.LedgerChild{Identity: c1zstore.LedgerActionIdentity{Op: "list-resources", ResourceTypeID: "group", PageToken: "A"}}}
	require.NoError(t, e.Ledger().InitializePendingWork(t.Context(), []c1zstore.LedgerWork{seed}))
	items, initialized, err := e.Ledger().PendingWork(t.Context(), 0, 100)
	require.NoError(t, err)
	require.True(t, initialized)
	require.Len(t, items, 1)
	return items[0]
}

func pendingTestCommit(t *testing.T, e *Engine, work c1zstore.LedgerWork, next string, children ...c1zstore.LedgerChild) error {
	t.Helper()
	writer := e.Ledger().BeginPage()
	defer writer.Discard()
	require.NoError(t, writer.SetPendingWork(work))
	require.NoError(t, writer.SetFact("work-committed"))
	require.NoError(t, writer.SetCounterBucket("attempt", 0, c1zstore.LedgerCounters{Counters: map[string]uint64{"pages": work.Revision + 1}}))
	return writer.Commit(t.Context(), work.Action.Identity, &c1zstore.LedgerRow{NextPageToken: next, Children: children})
}

func TestPendingWorkRepeatedArgumentsAndCompletion(t *testing.T) {
	e, _ := newTestEngine(t)
	work := pendingTestSeed(t, e)
	require.NoError(t, pendingTestCommit(t, e, work, "A", work.Action, work.Action))
	items, initialized, err := e.Ledger().PendingWork(t.Context(), 0, 100)
	require.NoError(t, err)
	require.True(t, initialized)
	require.Len(t, items, 3)
	require.EqualValues(t, 3, items[0].ID)
	require.EqualValues(t, 2, items[1].ID)
	require.Equal(t, work.ID, items[2].ID)
	require.EqualValues(t, 1, items[2].Revision)
	for _, item := range items {
		require.Equal(t, work.Action.Identity, item.Action.Identity)
	}
	require.ErrorIs(t, pendingTestCommit(t, e, work, "lost"), ErrStaleLedgerWork)
	for _, item := range items {
		require.NoError(t, pendingTestCommit(t, e, item, ""))
	}
	items, initialized, err = e.Ledger().PendingWork(t.Context(), 0, 100)
	require.NoError(t, err)
	require.True(t, initialized)
	require.Empty(t, items)
	lo, hi := rawdb.LedgerRowBounds()
	it, err := e.db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	require.NoError(t, err)
	defer it.Close()
	n := 0
	for it.First(); it.Valid(); it.Next() {
		n++
	}
	require.NoError(t, it.Error())
	require.Equal(t, 4, n)
}

func TestPendingWorkReadWindows(t *testing.T) {
	e, _ := newTestEngine(t)
	_, err := e.StartNewSync(t.Context(), connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	seed := make([]c1zstore.LedgerWork, 1001)
	for i := range seed {
		seed[i].Action.Identity.Op = "list-resources"
	}
	require.NoError(t, e.Ledger().InitializePendingWork(t.Context(), seed))
	var before uint64
	count := 0
	expected := uint64(len(seed))
	for {
		items, initialized, err := e.Ledger().PendingWork(t.Context(), before, 64)
		require.NoError(t, err)
		require.True(t, initialized)
		require.LessOrEqual(t, len(items), 64)
		if len(items) == 0 {
			break
		}
		for _, item := range items {
			require.Equal(t, expected, item.ID)
			expected--
			count++
		}
		before = items[len(items)-1].ID
	}
	require.Equal(t, len(seed), count)
	_, _, err = e.Ledger().PendingWork(t.Context(), 0, 101)
	require.Error(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, _, err = e.Ledger().PendingWork(ctx, 0, 64)
	require.ErrorIs(t, err, context.Canceled)
}

func TestPendingWorkDurableImages(t *testing.T) {
	skipOnWindowsMemFS(t)
	for _, cut := range []string{"before", "after-nosync", "after-flush"} {
		t.Run(cut, func(t *testing.T) {
			fs := vfs.NewCrashableMem()
			cache := pebble.NewCache(8 << 20)
			defer cache.Unref()
			e, err := Open(t.Context(), "pending", WithVFS(fs), WithSharedCache(cache), withPanicOnFatalLogger())
			require.NoError(t, err)
			defer e.Close()
			work := pendingTestSeed(t, e)
			writer := e.Ledger().BeginPage()
			defer writer.Discard()
			require.NoError(t, writer.SetPendingWork(work))
			require.NoError(t, writer.PutResources(t.Context(), V3ResourceToV2(ledgerTestResource("group", "one"))))
			require.NoError(t, writer.SetFact("observed"))
			require.NoError(t, writer.SetCounterBucket("attempt", 0, c1zstore.LedgerCounters{Counters: map[string]uint64{"pages": 1}}))
			if cut != "before" {
				require.NoError(t, writer.Commit(t.Context(), work.Action.Identity, &c1zstore.LedgerRow{NextPageToken: "A", Children: []c1zstore.LedgerChild{work.Action}}))
			}
			if cut == "after-flush" {
				require.NoError(t, e.db.FlushMemtables())
			}
			image := fs.CrashClone(vfs.CrashCloneCfg{})
			recovered, err := Open(t.Context(), "pending", WithVFS(image), WithSharedCache(cache), withPanicOnFatalLogger())
			require.NoError(t, err)
			defer recovered.Close()
			items, initialized, err := recovered.Ledger().PendingWork(t.Context(), 0, 64)
			require.NoError(t, err)
			require.True(t, initialized)
			committed := len(items) == 2
			if cut == "before" {
				require.False(t, committed)
			}
			if cut == "after-flush" {
				require.True(t, committed)
			}
			_, recordErr := recovered.GetResourceRecord(t.Context(), "group", "one")
			facts, err := recovered.Ledger().Facts(t.Context())
			require.NoError(t, err)
			counters, err := recovered.Ledger().Counters(t.Context())
			require.NoError(t, err)
			_, rowCloser, rowErr := recovered.db.Get(encodeWorkHistoryKey(work.Action.Identity, work.ID, work.Revision))
			if rowErr == nil {
				require.NoError(t, rowCloser.Close())
			}
			if committed {
				require.NoError(t, recordErr)
				require.NoError(t, rowErr)
				require.Contains(t, facts, "observed")
				require.EqualValues(t, 1, counters.Counters["pages"])
				require.EqualValues(t, 1, items[1].Revision)
			} else {
				require.Len(t, items, 1)
				require.Equal(t, work, items[0])
				require.ErrorIs(t, recordErr, pebble.ErrNotFound)
				require.ErrorIs(t, rowErr, pebble.ErrNotFound)
				require.NotContains(t, facts, "observed")
				require.True(t, counters.IsZero())
			}
		})
	}
}

func TestPendingWorkStaleCommitWritesNothing(t *testing.T) {
	e, _ := newTestEngine(t)
	work := pendingTestSeed(t, e)
	require.NoError(t, pendingTestCommit(t, e, work, "B"))
	writer := e.Ledger().BeginPage()
	defer writer.Discard()
	require.NoError(t, writer.SetPendingWork(work))
	require.NoError(t, writer.PutResources(t.Context(), V3ResourceToV2(ledgerTestResource("group", "stale"))))
	require.NoError(t, writer.SetFact("stale"))
	require.ErrorIs(t, writer.Commit(t.Context(), work.Action.Identity, &c1zstore.LedgerRow{}), ErrStaleLedgerWork)
	_, err := e.GetResourceRecord(t.Context(), "group", "stale")
	require.ErrorIs(t, err, pebble.ErrNotFound)
	facts, err := e.Ledger().Facts(t.Context())
	require.NoError(t, err)
	require.NotContains(t, facts, "stale")
}

func TestPendingWorkSealAndFinishedClear(t *testing.T) {
	e, _ := newTestEngine(t)
	work := pendingTestSeed(t, e)
	require.ErrorContains(t, e.EndSyncWithStats(t.Context(), c1zstore.SyncStats{}), "pending work")
	require.NoError(t, pendingTestCommit(t, e, work, ""))
	syncID := e.CurrentSyncID()
	require.NoError(t, e.EndSyncWithStats(t.Context(), c1zstore.SyncStats{}))
	require.NoError(t, e.SetCurrentSync(t.Context(), syncID))
	_, initialized, err := e.Ledger().PendingWork(t.Context(), 0, 64)
	require.NoError(t, err)
	require.True(t, initialized)
	require.NoError(t, e.Ledger().ClearRows(t.Context(), nil))
	_, initialized, err = e.Ledger().PendingWork(t.Context(), 0, 64)
	require.NoError(t, err)
	require.False(t, initialized)
}

func TestPendingWorkCommitFailureKeepsRevisionAndAllocator(t *testing.T) {
	e, _ := newTestEngine(t)
	work := pendingTestSeed(t, e)
	injected := errors.New("pending batch failure")
	e.db.SetRecordCommitTestHook(func() error { return injected })
	require.ErrorIs(t, pendingTestCommit(t, e, work, "B", work.Action), injected)
	e.db.SetRecordCommitTestHook(nil)
	items, _, err := e.Ledger().PendingWork(t.Context(), 0, 64)
	require.NoError(t, err)
	require.Equal(t, []c1zstore.LedgerWork{work}, items)
	facts, err := e.Ledger().Facts(t.Context())
	require.NoError(t, err)
	require.Empty(t, facts)
	counters, err := e.Ledger().Counters(t.Context())
	require.NoError(t, err)
	require.True(t, counters.IsZero())
	require.NoError(t, pendingTestCommit(t, e, work, "B", work.Action))
	items, _, err = e.Ledger().PendingWork(t.Context(), 0, 64)
	require.NoError(t, err)
	require.Len(t, items, 2)
	require.EqualValues(t, 2, items[0].ID)
}

func TestPendingWorkInitializationFailureAndRetry(t *testing.T) {
	e, _ := newTestEngine(t)
	_, err := e.StartNewSync(t.Context(), connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	injected := errors.New("seed failure")
	e.db.SetRecordCommitTestHook(func() error { return injected })
	require.ErrorIs(t, e.Ledger().InitializePendingWork(t.Context(), nil), injected)
	e.db.SetRecordCommitTestHook(nil)
	items, initialized, err := e.Ledger().PendingWork(t.Context(), 0, 64)
	require.NoError(t, err)
	require.False(t, initialized)
	require.Empty(t, items)
	require.NoError(t, e.Ledger().InitializePendingWork(t.Context(), nil))
	items, initialized, err = e.Ledger().PendingWork(t.Context(), 0, 64)
	require.NoError(t, err)
	require.True(t, initialized)
	require.Empty(t, items)
}
