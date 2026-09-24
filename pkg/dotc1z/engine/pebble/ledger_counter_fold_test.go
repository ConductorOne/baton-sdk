package pebble

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/internal/rawdb"
	"github.com/stretchr/testify/require"
)

func counterBucketCount(t *testing.T, e *Engine) int {
	t.Helper()
	lo, hi := rawdb.LedgerCounterBounds()
	it, err := e.db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	require.NoError(t, err)
	n := 0
	for it.First(); it.Valid(); it.Next() {
		n++
	}
	require.NoError(t, it.Error())
	require.NoError(t, it.Close())
	return n
}

func foldTestCounters(n uint32) c1zstore.LedgerCounters {
	return c1zstore.LedgerCounters{
		Counters: map[string]uint64{"completed": uint64(n)}, Flags: uint64(n) % 8,
		StepDurationsMs: map[string]int64{"resources": int64(n)},
		ConnectorCalls:  map[string]c1zstore.CallStat{"resources": {Count: int64(n), TotalMs: int64(n) * 3, MaxMs: int64(n), Errors: int64(n), Timeouts: int64(n)}},
		SessionCalls:    map[string]c1zstore.CallStat{"get": {Count: int64(n), TotalMs: int64(n) * 2, MaxMs: int64(n), Errors: int64(n), Timeouts: int64(n)}},
	}
}

func TestLedgerCounterFoldBoundsAttempts(t *testing.T) {
	e, _ := newTestEngine(t, WithVFS(vfs.NewMem()))
	_, err := e.StartNewSync(t.Context(), connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	for i := 1; i <= 1000; i++ {
		run := fmt.Sprintf("attempt-%d", i)
		require.NoError(t, e.Ledger().FoldCounters(t.Context(), run))
		value, err := marshalRecord(ledgerCountersToProto(foldTestCounters(1)))
		require.NoError(t, err)
		batch := e.db.NewRecordBatch()
		for _, worker := range []uint32{0, 1, c1zstore.RunBucketWorker} {
			require.NoError(t, batch.StageLedgerCounterBucket(encodeLedgerCounterKey(run, worker), value))
		}
		require.NoError(t, batch.Commit(pebble.NoSync))
		require.NoError(t, batch.Close())
		require.LessOrEqual(t, counterBucketCount(t, e), 4)
	}
	totals, err := e.Ledger().Counters(t.Context())
	require.NoError(t, err)
	expected := foldTestCounters(3000)
	expected.Flags = 1
	call := expected.ConnectorCalls["resources"]
	call.MaxMs = 1
	expected.ConnectorCalls["resources"] = call
	call = expected.SessionCalls["get"]
	call.MaxMs = 1
	expected.SessionCalls["get"] = call
	require.Equal(t, expected, totals)
}

func TestLedgerCounterFoldKeepsCurrentCumulativeBuckets(t *testing.T) {
	e, _ := newTestEngine(t)
	_, err := e.StartNewSync(t.Context(), connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, e.Ledger().PutCounterBucket(t.Context(), "prior", 0, foldTestCounters(2)))
	require.NoError(t, e.Ledger().PutCounterBucket(t.Context(), "current", 0, foldTestCounters(3)))
	require.NoError(t, e.Ledger().PutCounterBucket(t.Context(), "current-extra", 0, foldTestCounters(7)))
	require.NoError(t, e.Ledger().FoldCounters(t.Context(), "current"))
	require.NoError(t, e.Ledger().FoldCounters(t.Context(), "current"))
	require.Equal(t, 2, counterBucketCount(t, e))
	require.NoError(t, e.Ledger().PutCounterBucket(t.Context(), "current", 0, foldTestCounters(5)))
	total, err := e.Ledger().Counters(t.Context())
	require.NoError(t, err)
	expected := foldTestCounters(14)
	expected.Flags = 7
	call := expected.ConnectorCalls["resources"]
	call.MaxMs = 7
	expected.ConnectorCalls["resources"] = call
	call = expected.SessionCalls["get"]
	call.MaxMs = 7
	expected.SessionCalls["get"] = call
	require.Equal(t, expected, total)
}

func TestLedgerCounterFoldFailureAndCrash(t *testing.T) {
	skipOnWindowsMemFS(t)
	fs := vfs.NewCrashableMem()
	e, err := Open(t.Context(), "fold", WithVFS(fs), withPanicOnFatalLogger())
	require.NoError(t, err)
	defer func() { require.NoError(t, e.Close()) }()
	syncID, err := e.StartNewSync(t.Context(), connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, e.Ledger().PutCounterBucket(t.Context(), "old-a", 0, foldTestCounters(2)))
	require.NoError(t, e.Ledger().PutCounterBucket(t.Context(), "old-b", 0, foldTestCounters(3)))
	before, err := e.Ledger().Counters(t.Context())
	require.NoError(t, err)
	injected := errors.New("fold commit failed")
	e.db.SetRecordCommitTestHook(func() error { return injected })
	require.ErrorIs(t, e.Ledger().FoldCounters(t.Context(), "next"), injected)
	require.Equal(t, 2, counterBucketCount(t, e))
	e.db.SetRecordCommitTestHook(nil)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, e.Ledger().FoldCounters(ctx, "next"), context.Canceled)
	require.Equal(t, 2, counterBucketCount(t, e))
	require.Error(t, e.Ledger().FoldCounters(t.Context(), ""))
	images := map[string]*vfs.MemFS{}
	e.db.SetRecordCommitTestHook(func() error { images["before"] = fs.CrashClone(vfs.CrashCloneCfg{}); return nil })
	require.NoError(t, e.Ledger().FoldCounters(t.Context(), "next"))
	e.db.SetRecordCommitTestHook(nil)
	images["after"] = fs.CrashClone(vfs.CrashCloneCfg{})
	require.Len(t, images, 2)
	for cut, image := range images {
		t.Run(cut, func(t *testing.T) {
			reopened, err := Open(t.Context(), "fold", WithVFS(image), withPanicOnFatalLogger())
			require.NoError(t, err)
			defer func() { require.NoError(t, reopened.Close()) }()
			require.NoError(t, reopened.SetCurrentSync(t.Context(), syncID))
			expectedKeys := 1
			if cut == "before" {
				expectedKeys = 2
			}
			require.Equal(t, expectedKeys, counterBucketCount(t, reopened))
			total, err := reopened.Ledger().Counters(t.Context())
			require.NoError(t, err)
			require.Equal(t, before, total)
			require.NoError(t, reopened.Ledger().FoldCounters(t.Context(), "next"))
			require.NoError(t, reopened.Ledger().FoldCounters(t.Context(), "next"))
			total, err = reopened.Ledger().Counters(t.Context())
			require.NoError(t, err)
			require.Equal(t, before, total)
			require.Equal(t, 1, counterBucketCount(t, reopened))
		})
	}
}

func TestLedgerCounterFoldNoOpAndReservedBucket(t *testing.T) {
	e, _ := newTestEngine(t)
	_, err := e.StartNewSync(t.Context(), connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, e.Ledger().PutCounterBucket(t.Context(), "old", 0, foldTestCounters(2)))
	require.NoError(t, e.Ledger().PutCounterBucket(t.Context(), "current", 0, foldTestCounters(3)))
	require.NoError(t, e.Ledger().FoldCounters(t.Context(), "current"))
	commits := 0
	e.db.SetRecordCommitTestHook(func() error { commits++; return errors.New("unexpected repeated fold write") })
	require.NoError(t, e.Ledger().FoldCounters(t.Context(), "current"))
	require.Zero(t, commits)
	e.db.SetRecordCommitTestHook(nil)
	require.Error(t, e.Ledger().PutCounterBucket(t.Context(), "", c1zstore.TakeoverBucketWorker, foldTestCounters(10)))
	page := e.Ledger().BeginPage()
	defer page.Discard()
	require.Error(t, page.SetCounterBucket("", c1zstore.TakeoverBucketWorker, foldTestCounters(10)))
	total, err := e.Ledger().Counters(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 5, total.Counters["completed"])
}

func TestLedgerCounterFoldUnreadableBucketDoesNotDelete(t *testing.T) {
	e, _ := newTestEngine(t)
	_, err := e.StartNewSync(t.Context(), connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, e.Ledger().PutCounterBucket(t.Context(), "good", 0, foldTestCounters(2)))
	badKey := encodeLedgerCounterKey("bad", 0)
	require.NoError(t, e.db.MetaSet(badKey, []byte{0xff}, pebble.Sync))
	require.Error(t, e.Ledger().FoldCounters(t.Context(), "next"))
	require.Equal(t, 2, counterBucketCount(t, e))
	value, closer, err := e.db.Get(badKey)
	require.NoError(t, err)
	require.Equal(t, []byte{0xff}, value)
	require.NoError(t, closer.Close())
}
