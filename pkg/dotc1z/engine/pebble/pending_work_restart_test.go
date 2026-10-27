package pebble

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/internal/rawdb"
)

var restartTestCaps = c1zstore.RestartLimits{PerAction: 2, PerSync: 8}

// restartTestCommit stages a restart page for work at the given restart
// token: bucket counter LedgerActionRestarts = committed + 1 for the
// worker, as the syncer does.
func restartTestCommit(t *testing.T, e *Engine, work c1zstore.LedgerWork, restartToken string, caps c1zstore.RestartLimits,
	runID string, worker uint32, restartsSoFar uint64, stage func(c1zstore.PageWriter)) error {
	t.Helper()
	writer := e.Ledger().BeginPage()
	defer writer.Discard()
	require.NoError(t, writer.SetPendingWork(work))
	require.NoError(t, writer.SetRestart(caps))
	require.NoError(t, writer.SetCounterBucket(runID, worker, c1zstore.LedgerCounters{Counters: map[string]uint64{c1zstore.LedgerActionRestarts: restartsSoFar + 1}}))
	if stage != nil {
		stage(writer)
	}
	return writer.Commit(t.Context(), work.Action.Identity, &c1zstore.LedgerRow{NextPageToken: restartToken, Restarted: true})
}

func pendingItems(t *testing.T, e *Engine) []c1zstore.LedgerWork {
	t.Helper()
	items, phase, err := e.Ledger().PendingWork(t.Context(), 0, 100)
	require.NoError(t, err)
	require.NotEqual(t, c1zstore.LedgerQueueAbsent, phase)
	return items
}

func readHistoryRow(t *testing.T, e *Engine, id c1zstore.LedgerActionIdentity, workID, revision uint64) (*v3.LedgerRow, bool) {
	t.Helper()
	val, closer, err := e.db.Get(encodeWorkHistoryKey(id, workID, revision))
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, false
	}
	require.NoError(t, err)
	defer closer.Close()
	row := &v3.LedgerRow{}
	require.NoError(t, unmarshalRecord(val, row))
	return row, true
}

func countRecordKeys(t *testing.T, e *Engine) int {
	t.Helper()
	n := 0
	for _, family := range []byte{rawdb.TypeResourceType, rawdb.TypeResource, rawdb.TypeEntitlement, rawdb.TypeGrant} {
		prefix := []byte{rawdb.VersionV3, family}
		it, err := e.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: upperBoundOf(prefix)})
		require.NoError(t, err)
		for it.First(); it.Valid(); it.Next() {
			n++
		}
		require.NoError(t, it.Close())
	}
	return n
}

// C06, C07, C08, C09 at F5: the restarted row, the moved item, no records,
// the counter. Restart tokens "" (the reference case), a distinct token,
// and one equal to the expired token (the P4 degenerate cell).
func TestPendingWorkRestartCommitsRowAndMovesItem(t *testing.T) {
	for _, restartToken := range []string{"", "B", "A"} {
		t.Run(fmt.Sprintf("restart=%q", restartToken), func(t *testing.T) {
			e, _ := newTestEngine(t)
			work := pendingTestSeed(t, e)
			require.Equal(t, "A", work.Action.Identity.PageToken)
			require.NoError(t, pendingTestCommit(t, e, work, "T"))
			work = pendingItems(t, e)[0]
			require.EqualValues(t, 1, work.Revision)
			require.Equal(t, "T", work.Action.Identity.PageToken)

			require.NoError(t, restartTestCommit(t, e, work, restartToken, restartTestCaps, "run", 0, 0, nil))

			items := pendingItems(t, e)
			require.Len(t, items, 1, "a restart to %q is a restart, not a finish", restartToken)
			want := work
			want.Revision = work.Revision + 1
			want.Action.Identity.PageToken = restartToken
			want.Restarts = []uint64{work.Revision}
			require.Equal(t, want, items[0], "every other field of the pending item is unchanged")

			row, found := readHistoryRow(t, e, work.Action.Identity, work.ID, work.Revision)
			require.True(t, found, "the restarted row is keyed at the expired token and the expired revision")
			require.True(t, row.GetRestarted())
			require.Equal(t, restartToken, row.GetNextPageToken())
			require.Equal(t, ledgerTokenHash(restartToken), row.GetNextPageTokenHash())
			require.Empty(t, row.GetChildren())
			require.Zero(t, row.GetResourcesWritten()+row.GetGrantsWritten()+row.GetEntitlementsWritten()+row.GetResourceTypesWritten())
			require.Zero(t, countRecordKeys(t, e))

			counters, err := e.Ledger().Counters(t.Context())
			require.NoError(t, err)
			require.EqualValues(t, 1, counters.Counters[c1zstore.LedgerActionRestarts])

			// The restarted chain's first page lands at the restart token
			// and revision r+1; the second restart appends to the list.
			require.NoError(t, pendingTestCommit(t, e, items[0], "T2"))
			moved := pendingItems(t, e)[0]
			require.Equal(t, []uint64{work.Revision}, moved.Restarts, "a continuation keeps the list")
			require.NoError(t, restartTestCommit(t, e, moved, "", restartTestCaps, "run", 0, 1, nil))
			require.Equal(t, []uint64{work.Revision, moved.Revision}, pendingItems(t, e)[0].Restarts)
		})
	}
}

// The engine refuses a restart page that carries anything but the row and
// the transition, and a page whose SetRestart and row.Restarted disagree.
func TestPendingWorkRestartRefusesPayload(t *testing.T) {
	e, _ := newTestEngine(t)
	work := pendingTestSeed(t, e)
	ent := V3EntitlementToV2(ledgerTestEntitlement("group", "one", "member"))
	grant := v2.Grant_builder{
		Id:          "grant-one",
		Entitlement: ent,
		Principal:   V3ResourceToV2(ledgerTestResource("user", "u1")),
	}.Build()
	cases := map[string]func(c1zstore.PageWriter){
		"resource-types": func(w c1zstore.PageWriter) {
			require.NoError(t, w.PutResourceTypes(t.Context(), v2.ResourceType_builder{Id: "group"}.Build()))
		},
		"resources": func(w c1zstore.PageWriter) {
			require.NoError(t, w.PutResources(t.Context(), V3ResourceToV2(ledgerTestResource("group", "one"))))
		},
		"entitlements": func(w c1zstore.PageWriter) {
			require.NoError(t, w.PutEntitlements(t.Context(), ent))
		},
		"grants": func(w c1zstore.PageWriter) {
			require.NoError(t, w.PutGrants(t.Context(), grant))
		},
		"grant-deletes": func(w c1zstore.PageWriter) {
			require.NoError(t, w.DeleteGrants(t.Context(), grant))
		},
		"assets": func(w c1zstore.PageWriter) {
			require.NoError(t, w.PutAsset(t.Context(), v2.AssetRef_builder{Id: "asset-one"}.Build(), "text/plain", []byte("x")))
		},
		"children": func(w c1zstore.PageWriter) {
			require.NoError(t, w.SetPendingWork(work, "resource:child"))
		},
	}
	for name, stage := range cases {
		t.Run(name, func(t *testing.T) {
			err := restartTestCommit(t, e, work, "", restartTestCaps, "run", 0, 0, stage)
			if name == "children" {
				// Scheduling keys without row children: the shape check
				// refuses, and with it gone the arity check in
				// stageWorkTransition would refuse the same input, so this
				// case does not pin the shape check; children-aligned does.
				require.Error(t, err)
			} else {
				require.ErrorIs(t, err, ErrPageUnitRestart)
			}
			require.Equal(t, []c1zstore.LedgerWork{work}, pendingItems(t, e))
			_, found := readHistoryRow(t, e, work.Action.Identity, work.ID, work.Revision)
			require.False(t, found)
			require.Zero(t, countRecordKeys(t, e))
		})
	}
	// Row children aligned with the scheduling keys pass the arity check in
	// stageWorkTransition; only requireRestartShape refuses them.
	t.Run("children-aligned", func(t *testing.T) {
		writer := e.Ledger().BeginPage()
		defer writer.Discard()
		require.NoError(t, writer.SetPendingWork(work, "resource:child"))
		require.NoError(t, writer.SetRestart(restartTestCaps))
		require.NoError(t, writer.SetCounterBucket("run", 0, c1zstore.LedgerCounters{Counters: map[string]uint64{c1zstore.LedgerActionRestarts: 1}}))
		child := c1zstore.LedgerChild{Identity: c1zstore.LedgerActionIdentity{Op: "list-grants", ResourceTypeID: "group", ResourceID: "child"}}
		err := writer.Commit(t.Context(), work.Action.Identity, &c1zstore.LedgerRow{Restarted: true, Children: []c1zstore.LedgerChild{child}})
		require.ErrorIs(t, err, ErrPageUnitRestart)
		require.Equal(t, []c1zstore.LedgerWork{work}, pendingItems(t, e))
		require.Zero(t, countRecordKeys(t, e))
	})
	t.Run("terminal", func(t *testing.T) {
		writer := e.Ledger().BeginPage()
		defer writer.Discard()
		require.NoError(t, writer.SetPendingWork(work))
		require.NoError(t, writer.SetRestart(restartTestCaps))
		require.NoError(t, writer.SetCounterBucket("run", 0, c1zstore.LedgerCounters{Counters: map[string]uint64{c1zstore.LedgerActionRestarts: 1}}))
		require.NoError(t, writer.SetTerminal())
		err := writer.Commit(t.Context(), work.Action.Identity, &c1zstore.LedgerRow{Restarted: true})
		require.ErrorIs(t, err, ErrPageUnitRestart)
		require.Equal(t, []c1zstore.LedgerWork{work}, pendingItems(t, e))
	})
	t.Run("row-without-SetRestart", func(t *testing.T) {
		writer := e.Ledger().BeginPage()
		defer writer.Discard()
		require.NoError(t, writer.SetPendingWork(work))
		require.NoError(t, writer.SetCounterBucket("run", 0, c1zstore.LedgerCounters{}))
		require.ErrorIs(t, writer.Commit(t.Context(), work.Action.Identity, &c1zstore.LedgerRow{Restarted: true}), ErrPageUnitRestart)
	})
	t.Run("SetRestart-without-row", func(t *testing.T) {
		writer := e.Ledger().BeginPage()
		defer writer.Discard()
		require.NoError(t, writer.SetPendingWork(work))
		require.NoError(t, writer.SetRestart(restartTestCaps))
		require.NoError(t, writer.SetCounterBucket("run", 0, c1zstore.LedgerCounters{}))
		require.ErrorIs(t, writer.Commit(t.Context(), work.Action.Identity, &c1zstore.LedgerRow{NextPageToken: "B"}), ErrPageUnitRestart)
	})
	t.Run("zero-caps", func(t *testing.T) {
		writer := e.Ledger().BeginPage()
		defer writer.Discard()
		require.ErrorIs(t, writer.SetRestart(c1zstore.RestartLimits{PerAction: 1}), ErrPageUnitRestart)
		require.ErrorIs(t, writer.SetRestart(c1zstore.RestartLimits{PerSync: 1}), ErrPageUnitRestart)
	})
	t.Run("bucket-not-incremented", func(t *testing.T) {
		writer := e.Ledger().BeginPage()
		defer writer.Discard()
		require.NoError(t, writer.SetPendingWork(work))
		require.NoError(t, writer.SetRestart(restartTestCaps))
		require.NoError(t, writer.SetCounterBucket("run", 0, c1zstore.LedgerCounters{Counters: map[string]uint64{"pages": 1}}))
		require.ErrorIs(t, writer.Commit(t.Context(), work.Action.Identity, &c1zstore.LedgerRow{Restarted: true}), ErrPageUnitRestart)
	})
	t.Run("spawned", func(t *testing.T) {
		e, _ := newTestEngine(t)
		_, err := e.StartNewSync(t.Context(), connectorstore.SyncTypeFull, "")
		require.NoError(t, err)
		seed := c1zstore.LedgerWork{Action: c1zstore.LedgerChild{Identity: c1zstore.LedgerActionIdentity{Op: "list-grants", ResourceTypeID: "group", ResourceID: "g", PageToken: "S"}, Spawned: true}}
		require.NoError(t, e.Ledger().BeginCollecting(t.Context(), []c1zstore.LedgerWork{seed}, nil))
		spawned := pendingItems(t, e)[0]
		require.ErrorIs(t, restartTestCommit(t, e, spawned, "", restartTestCaps, "run", 0, 0, nil), ErrPageUnitRestart)
		require.Equal(t, []c1zstore.LedgerWork{spawned}, pendingItems(t, e))
	})
}

// S6 per item: k restarts honor, the (k+1)th is refused with the item's
// list, and the refusal leaves the item and the row untouched.
func TestPendingWorkRestartPerItemCap(t *testing.T) {
	e, _ := newTestEngine(t)
	_ = pendingTestSeed(t, e)
	caps := c1zstore.RestartLimits{PerAction: 2, PerSync: 100}
	for i := uint64(0); i < caps.PerAction; i++ {
		item := pendingItems(t, e)[0]
		require.NoError(t, restartTestCommit(t, e, item, "", caps, "run", 0, i, nil))
	}
	item := pendingItems(t, e)[0]
	require.Equal(t, []uint64{0, 1}, item.Restarts)
	err := restartTestCommit(t, e, item, "", caps, "run", 0, caps.PerAction, nil)
	var capErr *c1zstore.RestartLimitError
	require.ErrorAs(t, err, &capErr)
	require.ErrorIs(t, err, c1zstore.ErrRestartLimitExceeded)
	require.Equal(t, c1zstore.RestartScopeAction, capErr.Scope)
	require.EqualValues(t, 2, capErr.Limit)
	require.EqualValues(t, 2, capErr.Count)
	require.Equal(t, item.ID, capErr.WorkID)
	require.Equal(t, []uint64{0, 1}, capErr.Restarts)
	require.Equal(t, []c1zstore.LedgerWork{item}, pendingItems(t, e), "the refused page leaves the item at the expired token")
	_, found := readHistoryRow(t, e, item.Action.Identity, item.ID, item.Revision)
	require.False(t, found)
	counters, err := e.Ledger().Counters(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, caps.PerAction, counters.Counters[c1zstore.LedgerActionRestarts], "the refused page's bucket did not land")
}

// S6 per sync: the budget is the fold across buckets of every attempt and
// worker, so it survives a resume under a new run id and a page from
// another worker counts.
func TestPendingWorkRestartPerSyncBudgetFoldsBuckets(t *testing.T) {
	e, _ := newTestEngine(t)
	_, err := e.StartNewSync(t.Context(), connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	seeds := []c1zstore.LedgerWork{
		{Action: c1zstore.LedgerChild{Identity: c1zstore.LedgerActionIdentity{Op: "list-grants", ResourceTypeID: "group", ResourceID: "a", PageToken: "A"}}},
		{Action: c1zstore.LedgerChild{Identity: c1zstore.LedgerActionIdentity{Op: "list-grants", ResourceTypeID: "group", ResourceID: "b", PageToken: "B"}}},
	}
	require.NoError(t, e.Ledger().BeginCollecting(t.Context(), seeds, nil))
	caps := c1zstore.RestartLimits{PerAction: 100, PerSync: 3}
	byID := func() map[uint64]c1zstore.LedgerWork {
		out := map[uint64]c1zstore.LedgerWork{}
		for _, item := range pendingItems(t, e) {
			out[item.ID] = item
		}
		return out
	}
	// Attempt one, worker 0: two restarts of item 1.
	require.NoError(t, restartTestCommit(t, e, byID()[1], "", caps, "run-1", 0, 0, nil))
	require.NoError(t, restartTestCommit(t, e, byID()[1], "", caps, "run-1", 0, 1, nil))
	// Attempt two, worker 3: one restart of item 2 reaches the budget.
	require.NoError(t, restartTestCommit(t, e, byID()[2], "", caps, "run-2", 3, 0, nil))
	err = restartTestCommit(t, e, byID()[1], "", caps, "run-2", 3, 1, nil)
	var capErr *c1zstore.RestartLimitError
	require.ErrorAs(t, err, &capErr)
	require.Equal(t, c1zstore.RestartScopeSync, capErr.Scope)
	require.EqualValues(t, 3, capErr.Limit)
	require.EqualValues(t, 3, capErr.Count, "committed restarts, not the refused one")
	require.Equal(t, []uint64{0, 1}, capErr.Restarts)
	counters, err := e.Ledger().Counters(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 3, counters.Counters[c1zstore.LedgerActionRestarts])
}

// Eight writers restart the same item at the same revision at once: exactly
// one commits, the rest are fenced with ErrStaleLedgerWork, and the item
// ends at revision+1 with one invalidation.
func TestPendingWorkRestartRaceOnOneItem(t *testing.T) {
	e, _ := newTestEngine(t)
	work := pendingTestSeed(t, e)
	caps := c1zstore.RestartLimits{PerAction: 100, PerSync: 100}
	const racers = 8
	writers := make([]c1zstore.PageWriter, racers)
	for i := range writers {
		writers[i] = e.Ledger().BeginPage()
		require.NoError(t, writers[i].SetPendingWork(work))
		require.NoError(t, writers[i].SetRestart(caps))
		bucket := c1zstore.LedgerCounters{Counters: map[string]uint64{c1zstore.LedgerActionRestarts: 1}}
		require.NoError(t, writers[i].SetCounterBucket("run", uint32(i), bucket))
	}
	var wg sync.WaitGroup
	results := make([]error, racers)
	start := make(chan struct{})
	for i := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results[i] = writers[i].Commit(context.Background(), work.Action.Identity, &c1zstore.LedgerRow{NextPageToken: "R", Restarted: true})
			writers[i].Discard()
		}()
	}
	close(start)
	wg.Wait()
	committed := 0
	for _, err := range results {
		if err == nil {
			committed++
			continue
		}
		require.ErrorIs(t, err, ErrStaleLedgerWork)
	}
	require.Equal(t, 1, committed)
	items := pendingItems(t, e)
	require.Len(t, items, 1)
	require.Equal(t, work.Revision+1, items[0].Revision)
	require.Equal(t, "R", items[0].Action.Identity.PageToken)
	require.Equal(t, []uint64{work.Revision}, items[0].Restarts)
	counters, err := e.Ledger().Counters(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 1, counters.Counters[c1zstore.LedgerActionRestarts])
}

// D10 concurrent cell: workers restarting at once under the budget; exactly
// PerSync restart pages commit, never PerSync+1.
func TestPendingWorkRestartBudgetUnderConcurrentWorkers(t *testing.T) {
	e, _ := newTestEngine(t)
	_, err := e.StartNewSync(t.Context(), connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	const workers = 4
	seeds := make([]c1zstore.LedgerWork, workers)
	for i := range seeds {
		seeds[i].Action.Identity = c1zstore.LedgerActionIdentity{Op: "list-grants", ResourceTypeID: "group", ResourceID: fmt.Sprint(i), PageToken: "A"}
	}
	require.NoError(t, e.Ledger().BeginCollecting(t.Context(), seeds, nil))
	caps := c1zstore.RestartLimits{PerAction: 100, PerSync: 6}
	items := pendingItems(t, e)
	var wg sync.WaitGroup
	var mu sync.Mutex
	committed, refused := 0, 0
	for w := range workers {
		item := items[len(items)-1-w]
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := uint64(0); i < 5; i++ {
				writer := e.Ledger().BeginPage()
				if err := writer.SetPendingWork(item); err != nil {
					t.Error(err)
					return
				}
				if err := writer.SetRestart(caps); err != nil {
					t.Error(err)
					return
				}
				bucket := c1zstore.LedgerCounters{Counters: map[string]uint64{c1zstore.LedgerActionRestarts: i + 1}}
				if err := writer.SetCounterBucket("run", uint32(w), bucket); err != nil { //nolint:gosec // small loop index
					t.Error(err)
					return
				}
				err := writer.Commit(context.Background(), item.Action.Identity, &c1zstore.LedgerRow{Restarted: true})
				writer.Discard()
				mu.Lock()
				switch {
				case err == nil:
					committed++
				case errors.Is(err, c1zstore.ErrRestartLimitExceeded):
					refused++
				default:
					t.Error(err)
				}
				mu.Unlock()
				if err != nil {
					return
				}
				item.Revision++
				item.Action.Identity.PageToken = ""
				item.Restarts = append(item.Restarts, item.Revision-1)
			}
		}()
	}
	wg.Wait()
	require.EqualValues(t, caps.PerSync, committed)
	require.Positive(t, refused)
	counters, err := e.Ledger().Counters(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, caps.PerSync, counters.Counters[c1zstore.LedgerActionRestarts])
}

// D7 stale side: after a restart to "", the item's identity equals a
// first-page identity, so only the revision fences a commit from the
// attempt that still holds the expired token. It gets ErrStaleLedgerWork
// and writes nothing.
func TestPendingWorkRestartFencesStaleAttempt(t *testing.T) {
	e, _ := newTestEngine(t)
	work := pendingTestSeed(t, e)
	require.NoError(t, restartTestCommit(t, e, work, "", restartTestCaps, "run", 0, 0, nil))
	for _, stale := range []struct {
		name   string
		commit func() error
	}{
		{"continuation", func() error { return pendingTestCommit(t, e, work, "T2") }},
		{"finish", func() error { return pendingTestCommit(t, e, work, "") }},
		{"second-restart", func() error { return restartTestCommit(t, e, work, "", restartTestCaps, "run-2", 1, 0, nil) }},
	} {
		t.Run(stale.name, func(t *testing.T) {
			before := pendingItems(t, e)
			require.ErrorIs(t, stale.commit(), ErrStaleLedgerWork)
			require.Equal(t, before, pendingItems(t, e))
			rows, err := e.Ledger().rowCount(t.Context())
			require.NoError(t, err)
			require.EqualValues(t, 1, rows, "only the restart row exists")
			require.Zero(t, countRecordKeys(t, e))
		})
	}
}

// Restart pages are collection-phase pages: the expansion entry cannot
// restart.
func TestPendingWorkRestartRefusedOutsideCollecting(t *testing.T) {
	e, _ := newTestEngine(t)
	_, err := e.StartNewSync(t.Context(), connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	seed := c1zstore.LedgerWork{Action: c1zstore.LedgerChild{Identity: c1zstore.LedgerActionIdentity{Op: ledgerExpansionOp}}}
	require.NoError(t, e.Ledger().BeginCollecting(t.Context(), []c1zstore.LedgerWork{seed}, nil))
	require.NoError(t, e.Ledger().BeginExpanding(t.Context()))
	item := pendingItems(t, e)[0]
	require.ErrorIs(t, restartTestCommit(t, e, item, "", restartTestCaps, "run", 0, 0, nil), ErrLedgerQueuePhase)
}

// C10 at the engine: F1 (batch error), F2 (crash before sync), F3/F4 (after
// flush). After reopen the image is one of exactly two: nothing, or the
// row, the moved item, and the counter together.
func TestPendingWorkRestartCrashDichotomy(t *testing.T) {
	skipOnWindowsMemFS(t)
	for _, restartToken := range []string{"", "B"} {
		for _, cut := range []string{"batch-error", "before", "after-nosync", "after-flush"} {
			t.Run(fmt.Sprintf("restart=%q/%s", restartToken, cut), func(t *testing.T) {
				fs := vfs.NewCrashableMem()
				cache := pebble.NewCache(8 << 20)
				defer cache.Unref()
				e, err := Open(t.Context(), "restart", WithVFS(fs), WithSharedCache(cache), withPanicOnFatalLogger())
				require.NoError(t, err)
				defer e.Close()
				work := pendingTestSeed(t, e)
				require.NoError(t, pendingTestCommit(t, e, work, "T"))
				require.NoError(t, e.db.FlushMemtables())
				work = pendingItems(t, e)[0]
				switch cut {
				case "batch-error":
					injected := errors.New("restart batch failure")
					e.db.SetRecordCommitTestHook(func() error { return injected })
					require.ErrorIs(t, restartTestCommit(t, e, work, restartToken, restartTestCaps, "run", 0, 0, nil), injected)
					e.db.SetRecordCommitTestHook(nil)
				case "before":
				default:
					require.NoError(t, restartTestCommit(t, e, work, restartToken, restartTestCaps, "run", 0, 0, nil))
				}
				if cut == "after-flush" {
					require.NoError(t, e.db.FlushMemtables())
				}
				image := fs.CrashClone(vfs.CrashCloneCfg{})
				recovered, err := Open(t.Context(), "restart", WithVFS(image), WithSharedCache(cache), withPanicOnFatalLogger())
				require.NoError(t, err)
				defer recovered.Close()
				items := pendingItems(t, recovered)
				require.Len(t, items, 1)
				row, rowFound := readHistoryRow(t, recovered, work.Action.Identity, work.ID, work.Revision)
				counters, err := recovered.Ledger().Counters(t.Context())
				require.NoError(t, err)
				committed := items[0].Revision == work.Revision+1
				switch cut {
				case "batch-error", "before":
					require.False(t, committed)
				case "after-flush":
					require.True(t, committed)
				}
				if committed {
					require.True(t, rowFound)
					require.True(t, row.GetRestarted())
					require.Equal(t, restartToken, items[0].Action.Identity.PageToken)
					require.Equal(t, []uint64{work.Revision}, items[0].Restarts)
					require.EqualValues(t, 1, counters.Counters[c1zstore.LedgerActionRestarts])
				} else {
					require.False(t, rowFound)
					require.Equal(t, work, items[0])
					require.Zero(t, counters.Counters[c1zstore.LedgerActionRestarts])
				}
				require.Zero(t, countRecordKeys(t, recovered))
			})
		}
	}
}

// I8: the report counts a restarted row as a restart, never a terminal
// page or a continuation, before and after the scrub.
func TestLedgerReportCountsRestartPages(t *testing.T) {
	for _, retain := range []bool{false, true} {
		t.Run(map[bool]string{false: "scrubbed", true: "retained"}[retain], func(t *testing.T) {
			e, _ := newTestEngine(t)
			work := pendingTestSeed(t, e)
			e.ledger.SetRetainTokens(retain)
			require.NoError(t, pendingTestCommit(t, e, work, "T"))
			work = pendingItems(t, e)[0]
			require.NoError(t, restartTestCommit(t, e, work, "", restartTestCaps, "run", 0, 0, nil))
			work = pendingItems(t, e)[0]
			require.NoError(t, pendingTestCommit(t, e, work, ""))
			require.Empty(t, pendingItems(t, e))

			assertCounts := func(restarts, terminal, continuations uint64) {
				t.Helper()
				var collections []ledgerReportCollection
				summary, err := ledgerReport(t.Context(), e, func(c ledgerReportCollection) { collections = append(collections, c) })
				require.NoError(t, err)
				require.EqualValues(t, restarts, summary.Restarts)
				var gotRestarts, gotTerminal, gotContinuations uint64
				for _, c := range collections {
					gotRestarts += c.Restarts
					gotTerminal += c.TerminalPages
					gotContinuations += c.Continuations
				}
				require.EqualValues(t, restarts, gotRestarts)
				require.EqualValues(t, terminal, gotTerminal)
				require.EqualValues(t, continuations, gotContinuations)
			}
			assertCounts(1, 1, 1)
			require.NoError(t, sealWithStats(t, e, t.Context(), c1zstore.SyncStats{}))
			assertCounts(1, 2, 1)
		})
	}
}

// S8: the restart token rides field 2, so the scrub that empties next
// page tokens empties it and keeps the hash; retain keeps it verbatim. The
// row keeps Restarted either way.
func TestLedgerScrubRestartToken(t *testing.T) {
	const restartToken = "https://x/?sig=RESTART-SECRET" //nolint:gosec // a page token shape, not a credential
	for _, retain := range []bool{false, true} {
		t.Run(map[bool]string{false: "scrubbed", true: "retained"}[retain], func(t *testing.T) {
			e, _ := newTestEngine(t)
			work := pendingTestSeed(t, e)
			e.ledger.SetRetainTokens(retain)
			require.NoError(t, restartTestCommit(t, e, work, restartToken, restartTestCaps, "run", 0, 0, nil))
			work = pendingItems(t, e)[0]
			require.Equal(t, restartToken, work.Action.Identity.PageToken)
			require.NoError(t, pendingTestCommit(t, e, work, ""))
			require.NoError(t, e.Flush(t.Context()))
			require.Positive(t, checkpointNeedleHits(t, e, []byte("RESTART-SECRET")))
			require.NoError(t, sealWithStats(t, e, t.Context(), c1zstore.SyncStats{}))

			seen := 0
			require.NoError(t, e.ledger.iterate(t.Context(), func(r *v3.LedgerRow) bool {
				if !r.GetRestarted() {
					return true
				}
				seen++
				require.Equal(t, ledgerTokenHash(restartToken), r.GetNextPageTokenHash())
				require.Equal(t, !retain, r.GetScrubbed())
				if retain {
					require.Equal(t, restartToken, r.GetNextPageToken())
				} else {
					require.Empty(t, r.GetNextPageToken())
				}
				return true
			}))
			require.Equal(t, 1, seen)
			hits := checkpointNeedleHits(t, e, []byte("RESTART-SECRET"))
			if retain {
				require.Positive(t, hits)
			} else {
				require.Zero(t, hits)
			}
		})
	}
}

// A restarted row whose restart token is empty projects terminal=false,
// verbatim and scrubbed; the same bytes without Restarted project a
// terminal page.
func TestLedgerReportProjectRestartedNotTerminal(t *testing.T) {
	for _, scrubbed := range []bool{false, true} {
		for _, restarted := range []bool{false, true} {
			t.Run(fmt.Sprintf("scrubbed=%t/restarted=%t", scrubbed, restarted), func(t *testing.T) {
				row := &v3.LedgerRow{}
				row.SetIdentity(ledgerIdentityToProto(grantsPageIdentity("group", "A")))
				row.SetRestarted(restarted)
				row.SetScrubbed(scrubbed)
				if scrubbed {
					row.SetNextPageTokenHash(ledgerTokenHash(""))
				}
				data, err := marshalRecord(row)
				require.NoError(t, err)
				projected, err := ledgerReportProject(data)
				require.NoError(t, err)
				require.True(t, projected.paginationKnown)
				require.Equal(t, restarted, projected.restarted)
				require.Equal(t, !restarted, projected.terminal)
			})
		}
	}
}
