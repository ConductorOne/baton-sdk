package pebble

import (
	"errors"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

func TestLedgerBeginPassPreservesHistory(t *testing.T) {
	ctx := t.Context()
	e, _ := newTestEngine(t)
	syncID, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, e.Ledger().BeginCollecting(ctx, nil, nil))
	id := c1zstore.LedgerActionIdentity{Op: "list-resource-types"}
	page := e.Ledger().BeginPage()
	defer page.Discard()
	require.NoError(t, page.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "type"}.Build()))
	require.NoError(t, page.SetFact("finished"))
	require.NoError(t, page.SetFactValue("history", "retained"))
	history := c1zstore.LedgerCounters{Counters: map[string]uint64{"completed": 17}}
	require.NoError(t, page.SetCounterBucket("prior", 0, history))
	require.NoError(t, page.Commit(ctx, id, &c1zstore.LedgerRow{}))
	require.NoError(t, sealWithStats(t, e, ctx, c1zstore.SyncStats{}))
	require.NoError(t, e.SetCurrentSync(ctx, syncID))
	before, err := e.GetSyncRunRecord(ctx, syncID)
	require.NoError(t, err)

	seed := c1zstore.LedgerWork{Action: c1zstore.LedgerChild{Identity: c1zstore.LedgerActionIdentity{Op: "init"}}}
	require.NoError(t, e.Ledger().BeginPass(ctx, []c1zstore.LedgerWork{seed}, []string{"finished"}))
	after, err := e.GetSyncRunRecord(ctx, syncID)
	require.NoError(t, err)
	require.True(t, proto.Equal(before, after))
	_, found, err := e.Ledger().GetRow(ctx, id)
	require.NoError(t, err)
	require.False(t, found)
	facts, err := e.Ledger().Facts(ctx)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"history": "retained", c1zstore.LedgerFactFollowOnPass: ""}, facts)
	counts, err := e.Ledger().Counters(ctx)
	require.NoError(t, err)
	require.Equal(t, history.Counters, counts.Counters, "a retained seal keeps its buckets; the archive's copy is not imported")
	pending, phase, err := e.Ledger().PendingWork(ctx, 0, 10)
	require.NoError(t, err)
	require.Equal(t, c1zstore.LedgerQueueCollecting, phase)
	require.Len(t, pending, 1)
	require.Equal(t, seed.Action.Identity, pending[0].Action.Identity)
	records, err := e.ListResourceTypes(ctx, &v2.ResourceTypesServiceListResourceTypesRequest{})
	require.NoError(t, err)
	require.Len(t, records.GetList(), 1)
	require.ErrorIs(t, e.CheckpointSync(ctx, "forbidden"), ErrLedgeredSyncWritesNoToken)
}

func TestLedgerBeginPassRefusals(t *testing.T) {
	t.Run("unfinished", func(t *testing.T) {
		e, _ := newTestEngine(t)
		_, err := e.StartNewSync(t.Context(), connectorstore.SyncTypeFull, "")
		require.NoError(t, err)
		id := c1zstore.LedgerActionIdentity{Op: "unfinished"}
		page := e.Ledger().BeginPage()
		defer page.Discard()
		require.NoError(t, page.SetFact("keep"))
		require.NoError(t, page.Commit(t.Context(), id, nil))
		require.ErrorContains(t, e.Ledger().BeginPass(t.Context(), nil, []string{"keep"}), "unfinished")
		_, found, err := e.Ledger().GetRow(t.Context(), id)
		require.NoError(t, err)
		require.True(t, found)
		facts, err := e.Ledger().Facts(t.Context())
		require.NoError(t, err)
		require.Contains(t, facts, "keep")
	})
	t.Run("legacy checkpoint", func(t *testing.T) {
		e, _ := newTestEngine(t)
		syncID, err := e.StartNewSync(t.Context(), connectorstore.SyncTypeFull, "")
		require.NoError(t, err)
		require.NoError(t, e.CheckpointSync(t.Context(), "legacy"))
		require.NoError(t, e.EndSync(t.Context()))
		require.NoError(t, e.SetCurrentSync(t.Context(), syncID))
		require.ErrorContains(t, e.Ledger().BeginPass(t.Context(), nil, nil), "taken over")
	})
	t.Run("early-ended declaration", func(t *testing.T) {
		e, _ := newTestEngine(t)
		work := pendingTestSeed(t, e)
		syncID := e.CurrentSyncID()
		require.NoError(t, e.EndSync(t.Context()))
		require.NoError(t, e.SetCurrentSync(t.Context(), syncID))
		require.ErrorContains(t, e.Ledger().BeginPass(t.Context(), nil, nil), "declaration is collecting")
		pending, phase, err := e.Ledger().PendingWork(t.Context(), 0, 10)
		require.NoError(t, err)
		require.Equal(t, c1zstore.LedgerQueueCollecting, phase)
		require.Equal(t, []c1zstore.LedgerWork{work}, pending, "the early-ended pass's work is untouched")
	})
	t.Run("assigned seed", func(t *testing.T) {
		e, _ := newTestEngine(t)
		syncID, err := e.StartNewSync(t.Context(), connectorstore.SyncTypeFull, "")
		require.NoError(t, err)
		require.NoError(t, e.EndSync(t.Context()))
		require.NoError(t, e.SetCurrentSync(t.Context(), syncID))
		require.Error(t, e.Ledger().BeginPass(t.Context(), []c1zstore.LedgerWork{{ID: 3}}, nil))
	})
}

// Cuts: the hook fires after staging, before the commit; the record-commit
// hook fails the commit itself. Either way nothing lands.
func TestLedgerBeginPassFailureCuts(t *testing.T) {
	for _, cut := range []string{"staged", "commit"} {
		t.Run(cut, func(t *testing.T) {
			e, _ := newTestEngine(t)
			ctx := t.Context()
			syncID, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
			require.NoError(t, err)
			require.NoError(t, e.CheckpointSync(ctx, "legacy"))
			_, err = e.Ledger().Takeover(ctx, "old", []string{"finished", "history"}, c1zstore.LedgerCounters{Counters: map[string]uint64{"completed": 9}})
			require.NoError(t, err)
			require.NoError(t, e.Ledger().BeginCollecting(ctx, nil, nil))
			id := c1zstore.LedgerActionIdentity{Op: "old-page"}
			page := e.Ledger().BeginPage()
			defer page.Discard()
			require.NoError(t, page.Commit(ctx, id, nil))
			require.NoError(t, sealWithStats(t, e, ctx, c1zstore.SyncStats{}))
			require.NoError(t, e.SetCurrentSync(ctx, syncID))
			before, err := e.GetSyncRunRecord(ctx, syncID)
			require.NoError(t, err)
			injected := errors.New("interrupted pass")
			if cut == "staged" {
				e.test.ledgerBeginPassHook = func() error { return injected }
			} else {
				e.db.SetRecordCommitTestHook(func() error { return injected })
			}
			require.ErrorIs(t, e.Ledger().BeginPass(ctx, nil, []string{"finished"}), injected)
			e.test.ledgerBeginPassHook = nil
			e.db.SetRecordCommitTestHook(nil)
			after, err := e.GetSyncRunRecord(ctx, syncID)
			require.NoError(t, err)
			require.True(t, proto.Equal(before, after))
			_, found, err := e.Ledger().GetRow(ctx, id)
			require.NoError(t, err)
			require.True(t, found)
			_, phase, err := e.Ledger().PendingWork(ctx, 0, 1)
			require.NoError(t, err)
			require.Equal(t, c1zstore.LedgerQueueAbsent, phase)
			facts, err := e.Ledger().Facts(ctx)
			require.NoError(t, err)
			require.Contains(t, facts, "finished")
			require.Contains(t, facts, "history")
			counters, err := e.Ledger().Counters(ctx)
			require.NoError(t, err)
			require.EqualValues(t, 9, counters.Counters["completed"])
			require.ErrorIs(t, e.CheckpointSync(ctx, "forbidden"), ErrLedgeredSyncWritesNoToken)

			require.NoError(t, e.Ledger().BeginPass(ctx, nil, []string{"finished"}))
			facts, err = e.Ledger().Facts(ctx)
			require.NoError(t, err)
			require.NotContains(t, facts, "finished")
			require.Contains(t, facts, "history")
			_, found, err = e.Ledger().GetRow(ctx, id)
			require.NoError(t, err)
			require.False(t, found)
			_, frontier, err := e.Ledger().Frontier(ctx)
			require.NoError(t, err)
			require.False(t, frontier)
		})
	}
}

func TestLedgerBeginPassCrashImage(t *testing.T) {
	skipOnWindowsMemFS(t)
	ctx := t.Context()
	fs := vfs.NewCrashableMem()
	cache := pebble.NewCache(8 << 20)
	defer cache.Unref()
	e, err := Open(ctx, "begin-pass", WithVFS(fs), WithSharedCache(cache), withPanicOnFatalLogger())
	require.NoError(t, err)
	defer func() { require.NoError(t, e.Close()) }()
	syncID, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, e.Ledger().BeginCollecting(ctx, nil, nil))
	id := c1zstore.LedgerActionIdentity{Op: "completed"}
	page := e.Ledger().BeginPage()
	defer page.Discard()
	require.NoError(t, page.SetFact("finished"))
	require.NoError(t, page.SetFact("history"))
	require.NoError(t, page.SetCounterBucket("prior", 0, c1zstore.LedgerCounters{Counters: map[string]uint64{"completed": 3}}))
	require.NoError(t, page.Commit(ctx, id, nil))
	require.NoError(t, sealWithStats(t, e, ctx, c1zstore.SyncStats{}))
	require.NoError(t, e.SetCurrentSync(ctx, syncID))
	before, err := e.GetSyncRunRecord(ctx, syncID)
	require.NoError(t, err)
	var image *vfs.MemFS
	interrupted := errors.New("crash cut")
	e.test.ledgerBeginPassHook = func() error {
		image = fs.CrashClone(vfs.CrashCloneCfg{})
		return interrupted
	}
	require.ErrorIs(t, e.Ledger().BeginPass(ctx, nil, []string{"finished"}), interrupted)
	e.test.ledgerBeginPassHook = nil
	require.NotNil(t, image)
	recovered, err := Open(ctx, "begin-pass", WithVFS(image), WithSharedCache(cache), withPanicOnFatalLogger())
	require.NoError(t, err)
	defer func() { require.NoError(t, recovered.Close()) }()
	after, err := recovered.GetSyncRunRecord(ctx, syncID)
	require.NoError(t, err)
	require.True(t, proto.Equal(before, after))
	_, found, err := recovered.Ledger().GetRow(ctx, id)
	require.NoError(t, err)
	require.True(t, found)
	_, phase, err := recovered.Ledger().PendingWork(ctx, 0, 1)
	require.NoError(t, err)
	require.Equal(t, c1zstore.LedgerQueueAbsent, phase)
	facts, err := recovered.Ledger().Facts(ctx)
	require.NoError(t, err)
	require.Contains(t, facts, "finished")
	require.Contains(t, facts, "history")
	counters, err := recovered.Ledger().Counters(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 3, counters.Counters["completed"])
	pending, err := recovered.Ledger().residuePending()
	require.NoError(t, err)
	require.True(t, pending, "the residue marker precedes the batch")
	require.NoError(t, recovered.SetCurrentSync(ctx, syncID))
	require.NoError(t, recovered.Ledger().BeginPass(ctx, nil, []string{"finished"}))
	require.NoError(t, sealWithStats(t, recovered, ctx, c1zstore.SyncStats{}))
	pending, err = recovered.Ledger().residuePending()
	require.NoError(t, err)
	require.False(t, pending, "the next seal consumes it")
}

func TestLedgerBeginPassDefersPurgeUntilSeal(t *testing.T) {
	e, _ := newTestEngine(t)
	id, err := e.StartNewSync(t.Context(), connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, e.Ledger().BeginCollecting(t.Context(), nil, nil))
	e.Ledger().SetRetainTokens(true)
	page := e.Ledger().BeginPage()
	require.NoError(t, page.Commit(t.Context(), grantsPageIdentity("group", "old-secret"), nil))
	require.NoError(t, sealWithStats(t, e, t.Context(), c1zstore.SyncStats{}))
	require.Zero(t, e.test.ledgerResiduePurges.Load())
	require.Positive(t, checkpointNeedleHits(t, e, []byte("old-secret")))
	require.NoError(t, e.SetCurrentSync(t.Context(), id))
	require.NoError(t, e.Ledger().BeginPass(t.Context(), nil, []string{c1zstore.LedgerFactRetainTokens}))
	require.Zero(t, e.test.ledgerResiduePurges.Load())
	pending, err := e.Ledger().residuePending()
	require.NoError(t, err)
	require.True(t, pending)
	e.Ledger().SetRetainTokens(false)
	next := e.Ledger().BeginPage()
	require.NoError(t, next.SetFact(c1zstore.LedgerFactDiscardOnSeal))
	require.NoError(t, next.Commit(t.Context(), grantsPageIdentity("group", "new-secret"), nil))
	require.NoError(t, sealWithStats(t, e, t.Context(), c1zstore.SyncStats{}))
	require.EqualValues(t, 1, e.test.ledgerResiduePurges.Load())
	require.Zero(t, checkpointNeedleHits(t, e, []byte("old-secret")))
	require.Zero(t, checkpointNeedleHits(t, e, []byte("new-secret")))
}

// Default-mode seal leaves an empty family and an archive; the next pass
// starts from the archive. Facts the caller clears stay cleared.
func TestLedgerBeginPassImportsArchiveIntoEmptyFamily(t *testing.T) {
	e, _ := newTestEngine(t)
	id, err := e.StartNewSync(t.Context(), connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, e.Ledger().BeginCollecting(t.Context(), nil, nil))
	page := e.Ledger().BeginPage()
	require.NoError(t, page.SetFact(c1zstore.LedgerFactDiscardOnSeal))
	require.NoError(t, page.SetFact("skip-grants"))
	require.NoError(t, page.SetCounterBucket("run", 0, c1zstore.LedgerCounters{Counters: map[string]uint64{"completed": 5}}))
	require.NoError(t, page.Commit(t.Context(), grantsPageIdentity("group", "cursor"), nil))
	require.NoError(t, sealWithStats(t, e, t.Context(), c1zstore.SyncStats{}))
	require.NoError(t, e.SetCurrentSync(t.Context(), id))
	facts, err := e.Ledger().Facts(t.Context())
	require.NoError(t, err)
	require.Empty(t, facts)

	require.NoError(t, e.Ledger().BeginPass(t.Context(), nil, []string{c1zstore.LedgerFactDiscardOnSeal}))
	facts, err = e.Ledger().Facts(t.Context())
	require.NoError(t, err)
	require.Equal(t, map[string]string{"skip-grants": "", c1zstore.LedgerFactFollowOnPass: ""}, facts)
	counters, err := e.Ledger().Counters(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 5, counters.Counters["completed"])
	_, phase, err := e.Ledger().PendingWork(t.Context(), 0, 1)
	require.NoError(t, err)
	require.Equal(t, c1zstore.LedgerQueueCollecting, phase)
}
