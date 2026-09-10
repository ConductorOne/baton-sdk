package pebble

// Regression tests for six lifecycle defects found reviewing the ledger
// storage port. Each fails on the code as it stood before its fix; the
// comment on each names the mechanism, so a future change that
// reintroduces one gets told which contract it broke rather than just
// which assertion moved.

import (
	"context"
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
// it was never rewritten and the sealed artifact shipped the tokens that
// SetLedgerTokensSensitive promises are never in a sealed artifact.
func TestLedgerScrubReachesTheTakeoverFrontier(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	_, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	const secret = "tok_live_CREDENTIAL"
	state := `{"v":1,"actions":[{"op":"list-grants","page_token":"` + secret + `"}]}`
	require.NoError(t, e.CheckpointSync(ctx, state))

	moved, err := e.TakeoverToken(ctx, "run-1", nil, c1zstore.LedgerCounters{
		Counters: map[string]uint64{"completed_actions": 1},
	})
	require.NoError(t, err)
	require.Equal(t, state, moved, "the resume gets the stack verbatim; that part is intended")

	f, found, err := e.GetLedgerFrontier(ctx)
	require.NoError(t, err)
	require.True(t, found)
	require.Contains(t, f.GetState(), secret, "before the seal the frontier holds the stack")

	e.SetLedgerTokensSensitive(true)
	require.NoError(t, e.ScrubLedgerTokens(ctx))

	f, found, err = e.GetLedgerFrontier(ctx)
	require.NoError(t, err)
	require.True(t, found, "the takeover record survives as an audit trail")
	require.Empty(t, f.GetState(), "the verbatim stack does not")
	require.NotEmpty(t, f.GetAttempt(), "attempt and taken_over_at are the audit fact and stay")
	require.False(t, strings.Contains(f.GetState(), secret))
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
