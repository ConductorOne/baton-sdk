package pebble

// Regression tests for six lifecycle defects found reviewing the ledger
// storage port. Each fails on the code as it stood before its fix; the
// comment on each names the mechanism, so a future change that
// reintroduces one gets told which contract it broke rather than just
// which assertion moved.

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// The seal must not leave a verbatim page token in the frontier.
//
// takeoverToken stores the taken-over sync token JSON verbatim, and every
// Action in that JSON carries a page_token. ScrubLedgerTokens iterates
// LedgerRowBounds, which is kind 0x00 only; the frontier is kind 0x03, so
// it was never rewritten and the sealed artifact shipped tokens the seal
// is supposed to have removed.
func TestLedgerScrubReachesTheTakeoverFrontier(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	_, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	// Stands in for a page token that carries a credential, which is the
	// case the seal's scrub exists for.
	const marker = "opaque-cursor-9f3a"
	state := `{"v":1,"actions":[{"op":"list-grants","page_token":"` + marker + `"}]}`
	require.NoError(t, e.CheckpointSync(ctx, state))

	moved, err := e.TakeoverToken(ctx, "run-1", nil, c1zstore.LedgerCounters{
		Counters: map[string]uint64{"completed_actions": 1},
	})
	require.NoError(t, err)
	require.Equal(t, state, moved, "the resume gets the stack verbatim; that part is intended")

	f, found, err := e.GetLedgerFrontier(ctx)
	require.NoError(t, err)
	require.True(t, found)
	require.Contains(t, f.GetState(), marker, "before the seal the frontier holds the stack")

	require.NoError(t, e.ScrubLedgerTokens(ctx))

	f, found, err = e.GetLedgerFrontier(ctx)
	require.NoError(t, err)
	require.True(t, found, "the takeover record survives as an audit trail")
	require.Empty(t, f.GetState(), "the verbatim stack does not")
	require.NotEmpty(t, f.GetAttempt(), "attempt and taken_over_at are the audit fact and stay")
	require.False(t, strings.Contains(f.GetState(), marker))
}

// A sync with ledger rows must be refused a checkpoint token even when
// the in-flight stamp has been cleared.
//
// clearLedgerInFlight drops the durable stamp and the in-memory flag
// before endSyncFinalize writes ended_at. If the write then fails, or the
// process crashes and reopens, the flag reads false over rows that are
// still there, and gating on the flag alone let CheckpointSync write a
// token beside a live ledger.
func TestCheckpointRefusedWhileLedgerRowsExistWithoutTheStamp(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	_, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	u := e.NewPageUnit()
	require.NoError(t, u.Commit(ctx, grantsPageIdentity("github", "p1"), nil))
	require.True(t, e.ledgerInFlight.Load(), "committing a page stamps in flight")
	require.ErrorIs(t, e.CheckpointSync(ctx, "tok"), ErrLedgeredSyncWritesNoToken)

	// Exactly the state the seal's clear leaves behind, and the state a
	// reopen after a crash in that window reconstructs: stamp gone, rows
	// still there.
	require.NoError(t, e.clearLedgerInFlight())
	require.False(t, e.ledgerInFlight.Load())

	require.ErrorIs(t, e.CheckpointSync(ctx, "tok"), ErrLedgeredSyncWritesNoToken,
		"rows outlive the stamp, so rows are what the gate asks about")
	require.ErrorIs(t, e.EndSync(ctx), ErrLedgeredSyncNeedsStats,
		"the same applies to sealing without stats")
}

// StartNewSync must not leave a stamp describing a ledger it deleted.
//
// ResetForNewSync excises the ledger family but the keyspace stamp lives
// in the preserved engine-meta range, so the in-flight classification
// outlived the rows. The replacement sync was then refused a token by
// CheckpointSync and refused a plain seal by EndSync, on a file with no
// ledger at all: neither protocol could finish it.
// The stamp only outlives its rows when the ledgered sync never sealed,
// so the setup has to abandon one: commit a page, then reopen without
// EndSync. A seal would call clearLedgerInFlight itself and the reset
// would have nothing left to clear.
func TestResetForNewSyncClearsTheInFlightStamp(t *testing.T) {
	ctx := context.Background()
	e, dir := newTestEngine(t)
	_, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	u := e.NewPageUnit()
	require.NoError(t, u.Commit(ctx, grantsPageIdentity("github", "p1"), nil))
	require.True(t, e.ledgerInFlight.Load(), "committing a page stamps in flight")

	// The crash: no seal, so the stamp stays on disk.
	e = reopenEngine(t, e, dir)
	require.True(t, e.ledgerInFlight.Load(), "reopen reads the stamp back")

	// StartNewSync wipes the ledger family. The stamp has to go with it.
	_, err = e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	rows, err := e.LedgerRowCount(ctx)
	require.NoError(t, err)
	require.Zero(t, rows, "the wipe took the rows")
	require.False(t, e.ledgerInFlight.Load(), "and the stamp that described them")

	// Both protocols work again on a file with no ledger. Before the fix
	// the replacement sync could use neither.
	require.NoError(t, e.CheckpointSync(ctx, "tok"))
	require.NoError(t, e.EndSync(ctx))
}

// A takeover carrying only run-level stats must still write its bucket.
//
// The gate tested Counters and Flags but LedgerCounters has five fields.
// A token whose phases had run without completing a page produced no
// bucket, and takeover clears that token in the same batch, so the
// timings and call stats had no second copy to fold from.
func TestTakeoverPersistsStatsOnlyCounters(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	_, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, e.CheckpointSync(ctx, `{"v":1,"actions":[{"op":"list-grants"}]}`))

	// No Counters, no Flags: only the three fields the old gate ignored.
	_, err = e.TakeoverToken(ctx, "run-1", nil, c1zstore.LedgerCounters{
		ConnectorCalls:  map[string]c1zstore.CallStat{"ListGrants": {Count: 2, TotalMs: 40, MaxMs: 30}},
		StepDurationsMs: map[string]int64{"list-grants": 100},
		SessionCalls:    map[string]c1zstore.CallStat{"Get": {Count: 5, TotalMs: 5, MaxMs: 2}},
	})
	require.NoError(t, err)

	n, err := e.LedgerCounterBucketCount(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, n, "the takeover's stats survive it")

	sum, err := e.SumLedgerCounters(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(100), sum.GetStepDurationsMs()["list-grants"])
	require.Equal(t, int64(2), sum.GetConnectorCalls()["ListGrants"].GetCount())
	require.Equal(t, int64(5), sum.GetSessionCalls()["Get"].GetCount())

	// An entirely empty struct still writes nothing. Its own engine: the
	// takeover above left a frontier, and CheckpointSync is refused once
	// the ledger holds anything, so this cannot be set up on that file.
	e2, _ := newTestEngine(t)
	_, err = e2.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, e2.CheckpointSync(ctx, `{"v":1,"actions":[{"op":"list-grants"}]}`))
	_, err = e2.TakeoverToken(ctx, "run-1", nil, c1zstore.LedgerCounters{})
	require.NoError(t, err)
	n, err = e2.LedgerCounterBucketCount(ctx)
	require.NoError(t, err)
	require.Zero(t, n, "nothing to record, so no bucket")
}

// A spent PageUnit must answer both page-scoped reads the same way.
//
// release nilled resourceIdx but not entitlementIdx, and neither getter
// checked done. GetResourceRecord therefore fell through to the DB while
// GetEntitlementRecord indexed a nil slice and panicked, so the two
// disagreed about what a committed unit does.
func TestPageUnitReadsAfterCommitAreRefusedNotPanics(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	_, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	for _, spend := range []string{"commit", "discard"} {
		t.Run(spend, func(t *testing.T) {
			u := e.NewPageUnit()
			require.NoError(t, u.StageEntitlements(v3.EntitlementRecord_builder{
				ExternalId: "ent-1",
				Resource:   v3.ResourceRef_builder{ResourceTypeId: "group", ResourceId: "g1"}.Build(),
			}.Build()))
			require.NoError(t, u.StageResources(v3.ResourceRecord_builder{
				ResourceTypeId: "user", ResourceId: "u1",
			}.Build()))

			// Staged, so both reads come from the buffer.
			gotEnt, err := u.GetEntitlementRecord(ctx, "ent-1")
			require.NoError(t, err)
			require.Equal(t, "ent-1", gotEnt.GetExternalId())
			gotRes, err := u.GetResourceRecord(ctx, "user", "u1")
			require.NoError(t, err)
			require.Equal(t, "u1", gotRes.GetResourceId())

			if spend == "commit" {
				require.NoError(t, u.Commit(ctx, grantsPageIdentity("github", spend), nil))
			} else {
				u.Discard()
			}

			// The staged ids are the dangerous ones: they are what the
			// surviving index still pointed at.
			_, err = u.GetEntitlementRecord(ctx, "ent-1")
			require.ErrorIs(t, err, ErrPageUnitCommitted)
			_, err = u.GetResourceRecord(ctx, "user", "u1")
			require.ErrorIs(t, err, ErrPageUnitCommitted)
		})
	}
}

// The retain opt-out must survive a crash, and its absence must scrub.
//
// The declaration is made by the process that starts the sync while the
// seal runs wherever the sync finishes, which after a crash is a
// different process holding a zero-valued flag. An in-memory flag alone
// cannot cross that boundary, so the declaration is also a durable ledger
// fact written into the page's own batch.
//
// Both directions matter, and they are not symmetric. Losing the fact
// costs a debugging aid; the default it falls back to is the safe one.
// That asymmetry is why the flag records the exception rather than the
// rule — see LedgerFactRetainTokens.
func TestRetainDeclarationSurvivesCrashAndItsAbsenceScrubs(t *testing.T) {
	ctx := context.Background()
	const marker = "opaque-cursor-7c1b"

	// A page, a crash, and a resume in a process that never declared
	// anything. Returns the sealed engine's rows.
	sealAfterCrash := func(t *testing.T, declare func(e *Engine)) []*v3.LedgerRow {
		t.Helper()
		e, dir := newTestEngine(t)
		syncID, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)
		declare(e)
		u := e.NewPageUnit()
		require.NoError(t, u.Commit(ctx, grantsPageIdentity("github", "p1"),
			v3.LedgerRow_builder{NextPageToken: marker}.Build()))

		e = reopenEngine(t, e, dir)
		require.False(t, e.RetainLedgerTokens(), "no in-memory declaration survives a reopen")
		resumed, err := NewAdapter(e).ResumeSync(ctx, connectorstore.SyncTypeFull, syncID)
		require.NoError(t, err)
		require.Equal(t, syncID, resumed)
		require.NoError(t, e.EndSyncWithStats(ctx, c1zstore.SyncStats{}))

		var rows []*v3.LedgerRow
		require.NoError(t, e.IterateLedger(ctx, func(r *v3.LedgerRow) bool {
			rows = append(rows, r)
			return true
		}))
		require.Len(t, rows, 1)
		return rows
	}

	t.Run("retain declared: the fact carries it across the crash", func(t *testing.T) {
		rows := sealAfterCrash(t, func(e *Engine) { e.SetRetainLedgerTokens(true) })
		require.False(t, rows[0].GetScrubbed(), "the sealing process honored a fact it never set")
		require.Equal(t, marker, rows[0].GetNextPageToken())
	})

	t.Run("nothing declared: the seal scrubs", func(t *testing.T) {
		rows := sealAfterCrash(t, func(*Engine) {})
		require.True(t, rows[0].GetScrubbed())
		require.Empty(t, rows[0].GetNextPageToken())
		require.Len(t, rows[0].GetNextPageTokenHash(), ledgerTokenHashLen,
			"the hash is kept, so the ledger can still match the page")
	})
}

// A takeover's migrated counters must survive worker 0's first page.
//
// Buckets are blind-written whole totals keyed by (run, worker) and the
// fold sums across them, so the takeover writing at (runID, 0) put it on
// a key a real page worker also owns: worker 0's first commit in the
// same run replaced it and the pre-takeover counters left the fold with
// nothing to detect the loss.
func TestTakeoverBucketSurvivesWorkerZerosPage(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	_, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, e.CheckpointSync(ctx, `{"v":1,"actions":[{"op":"list-grants"}]}`))

	_, err = e.TakeoverToken(ctx, "run-1", nil, c1zstore.LedgerCounters{
		Counters: map[string]uint64{"completed_actions": 5},
	})
	require.NoError(t, err)

	// Worker 0's page, same run, staging its own whole total.
	u := e.NewPageUnit()
	require.NoError(t, u.StageCounterBucket("run-1", 0, bucket(0, "completed_actions", uint64(2))))
	require.NoError(t, u.Commit(ctx, grantsPageIdentity("github", "p1"), nil))

	sum, err := e.SumLedgerCounters(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(7), sum.GetCounters()["completed_actions"],
		"5 migrated by the takeover plus 2 from worker 0; a shared key would report only 2")

	n, err := e.LedgerCounterBucketCount(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, n, "two distinct buckets, not one overwritten")
}

// Dropping the ledger must drop the stamp that describes it.
//
// DropLedger removed the rows and left both the on-disk stamp and the
// in-memory flag set, so a drop before the seal left a file with no
// ledger that still refused CheckpointSync and still refused a plain
// EndSync — and still read as an unsupported layout to a token-only SDK.
// Same defect ResetForNewSync had, in the other place that deletes these
// rows.
func TestDropLedgerClearsTheInFlightStamp(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	_, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	u := e.NewPageUnit()
	require.NoError(t, u.Commit(ctx, grantsPageIdentity("github", "p1"), nil))
	require.True(t, e.ledgerInFlight.Load())
	require.ErrorIs(t, e.CheckpointSync(ctx, "tok"), ErrLedgeredSyncWritesNoToken)

	require.NoError(t, e.ResetLedger(ctx))
	require.False(t, e.ledgerInFlight.Load(), "the stamp goes with the rows")

	stamp, err := e.keyspaceVersionStamp()
	require.NoError(t, err)
	require.Equal(t, keyspaceVersion, stamp, "and on disk, so an older SDK can read the file")

	// The sync is token-only again, which is what the drop made true.
	require.NoError(t, e.CheckpointSync(ctx, "tok"))
	require.NoError(t, e.EndSync(ctx))
}

// A failed seal must not leave its stats overlay behind.
//
// endSync stashes the overlay before GetSyncRunRecord and endSyncFinalize
// can fail, and only PersistSyncStats consumes it. A failed seal leaves
// the sync bound for a retry with the entry still keyed by syncID, where
// a later PersistSyncStats for that id would apply it — contradicting
// setSyncStatsOverlay's own contract that the value never outlives the
// EndSync that supplied it.
func TestFailedSealDropsItsStatsOverlay(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	syncID, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	// Fail the ended_at stamp, which is before the stats sidecar write,
	// so the overlay is stashed but never consumed.
	boom := errors.New("injected")
	e.test.endSyncStampHook = func() error { return boom }
	require.ErrorIs(t, e.EndSyncWithStats(ctx, c1zstore.SyncStats{
		Run: c1zstore.RunStats{StepDurationsMs: map[string]int64{"list-grants": 3}},
	}), boom)
	e.test.endSyncStampHook = nil

	require.NotContains(t, e.syncStatsOverlay, syncID,
		"a failed seal's stats must not be waiting for the next seal of this id")

	// The retry supplies its own stats, and those are what get persisted.
	require.NoError(t, e.EndSyncWithStats(ctx, c1zstore.SyncStats{
		Run: c1zstore.RunStats{StepDurationsMs: map[string]int64{"list-grants": 9}},
	}))
	stats, err := e.readSyncStats(ctx, syncID)
	require.NoError(t, err)
	require.Equal(t, int64(9), stats.GetStepDurationsMs()["list-grants"],
		"the retry's stats, not the failed attempt's")
}

// ResetLedger must mark the store dirty; see the store-level test in
// pkg/dotc1z. Here: the engine-level wipe itself is complete, so the
// store wrapper is the only thing between it and the saved file.
func TestResetLedgerWipesEveryLedgerSubFamily(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	_, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, e.CheckpointSync(ctx, `{"v":1,"actions":[{"op":"list-grants"}]}`))
	_, err = e.TakeoverToken(ctx, "run-1", []string{"needs_expansion"}, c1zstore.LedgerCounters{
		Counters: map[string]uint64{"completed_actions": 1},
	})
	require.NoError(t, err)
	u := e.NewPageUnit()
	require.NoError(t, u.StageFact("has_external_resource_grants"))
	require.NoError(t, u.Commit(ctx, grantsPageIdentity("github", "p1"), nil))

	require.NoError(t, e.ResetLedger(ctx))

	rows, err := e.LedgerRowCount(ctx)
	require.NoError(t, err)
	require.Zero(t, rows, "rows")
	facts, err := e.LedgerFacts(ctx)
	require.NoError(t, err)
	require.Empty(t, facts, "facts")
	n, err := e.LedgerCounterBucketCount(ctx)
	require.NoError(t, err)
	require.Zero(t, n, "buckets")
	_, found, err := e.GetLedgerFrontier(ctx)
	require.NoError(t, err)
	require.False(t, found, "frontier")
}
