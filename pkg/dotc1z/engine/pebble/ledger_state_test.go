package pebble

// Tests for the ledger family's sync-level sub-families (brief §3.6,
// §3.8): fact keys, counter buckets and the token-only takeover. The
// row tests are in ledger_test.go.

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

func bucket(flags uint64, kv ...any) *v3.LedgerCounterBucket {
	m := map[string]uint64{}
	for i := 0; i+1 < len(kv); i += 2 {
		m[kv[i].(string)] = kv[i+1].(uint64)
	}
	return v3.LedgerCounterBucket_builder{Counters: m, Flags: flags}.Build()
}

// Facts and the bucket land in the page's batch: a failed commit lands
// neither; a successful one lands both with the row. Facts are
// idempotent; buckets are keyed per (run, worker) and the fold sums
// across them, so a resume under a different worker count adds buckets
// rather than clobbering any.
func TestLedgerFactsAndBucketsRideThePageUnit(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	_, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	// Failed commit: nothing.
	u := e.NewPageUnit()
	require.NoError(t, u.StageFact("needs_expansion"))
	require.NoError(t, u.StageCounterBucket("run-1", 0, bucket(0b1, "grants_dropped", uint64(3))))
	boom := errors.New("injected")
	e.db.SetRecordCommitTestHook(func() error { return boom })
	require.ErrorIs(t, u.Commit(ctx, grantsPageIdentity("github", "p1"), nil), boom)
	e.db.SetRecordCommitTestHook(nil)
	facts, err := e.LedgerFacts(ctx)
	require.NoError(t, err)
	require.Empty(t, facts, "a fact from a failed page was never established")
	sum, err := e.SumLedgerCounters(ctx)
	require.NoError(t, err)
	require.Empty(t, sum.GetCounters())

	// Retry: both land with the row.
	require.NoError(t, u.Commit(ctx, grantsPageIdentity("github", "p1"), nil))
	facts, err = e.LedgerFacts(ctx)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"needs_expansion": ""}, facts)

	// Same fact again, another worker's bucket in the same run, then a
	// second run with one worker (fewer workers on resume).
	u2 := e.NewPageUnit()
	require.NoError(t, u2.StageFact("needs_expansion"))
	require.NoError(t, u2.StageFact("has_external_resource_grants"))
	require.NoError(t, u2.StageCounterBucket("run-1", 3, bucket(0b10, "grants_dropped", uint64(2), "entitlements_dropped", uint64(1))))
	require.NoError(t, u2.Commit(ctx, grantsPageIdentity("github", "p2"), nil))
	u3 := e.NewPageUnit()
	// The worker's cumulative total (5), not a delta: blind overwrite.
	require.NoError(t, u3.StageCounterBucket("run-1", 0, bucket(0b1, "grants_dropped", uint64(5))))
	require.NoError(t, u3.Commit(ctx, grantsPageIdentity("github", "p3"), nil))
	u4 := e.NewPageUnit()
	require.NoError(t, u4.StageCounterBucket("run-2", 0, bucket(0, "grants_dropped", uint64(1))))
	require.NoError(t, u4.Commit(ctx, grantsPageIdentity("github", "p4"), nil))

	facts, err = e.LedgerFacts(ctx)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"needs_expansion": "", "has_external_resource_grants": ""}, facts)
	n, err := e.LedgerCounterBucketCount(ctx)
	require.NoError(t, err)
	require.Equal(t, 3, n, "(run-1,0) (run-1,3) (run-2,0)")
	sum, err = e.SumLedgerCounters(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 5+2+1, sum.GetCounters()["grants_dropped"])
	require.EqualValues(t, 1, sum.GetCounters()["entitlements_dropped"])
	require.EqualValues(t, 0b11, sum.GetFlags())

	// Rows are unaffected by the siblings: the row iterators see rows only.
	rows, err := e.LedgerRowCount(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 4, rows)

	// The whole family goes with the sync.
	require.NoError(t, e.EndSyncWithStats(ctx, c1zstore.SyncStats{}))
	_, err = e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	facts, err = e.LedgerFacts(ctx)
	require.NoError(t, err)
	require.Empty(t, facts)
	n, err = e.LedgerCounterBucketCount(ctx)
	require.NoError(t, err)
	require.Zero(t, n)
	_, found, err := e.GetLedgerFrontier(ctx)
	require.NoError(t, err)
	require.False(t, found)
}

// The takeover is one unit: frontier written, facts and bucket written,
// token cleared — or none of it. After it, the token is empty and the
// frontier holds the moved state; a second takeover is a no-op.
func TestLedgerTakeoverIsOneUnit(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	_, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	const legacyState = `{"v":1,"actions":[{"op":"list-grants"}]}`
	require.NoError(t, e.CheckpointSync(ctx, legacyState))

	counters := c1zstore.LedgerCounters{Counters: map[string]uint64{"grants_dropped": 7, "known": 1}, Flags: 0b100}

	// Fault before the batch lands: legacyState intact, frontier absent, no
	// facts, no bucket. The resumed sync will take over again.
	boom := errors.New("injected")
	e.db.SetRecordCommitTestHook(func() error { return boom })
	_, err = e.TakeoverToken(ctx, "run-1", []string{"needs_expansion"}, counters)
	require.ErrorIs(t, err, boom)
	e.db.SetRecordCommitTestHook(nil)
	step, err := e.CurrentSyncStep(ctx)
	require.NoError(t, err)
	require.Equal(t, legacyState, step, "legacyState survives a failed takeover")
	_, found, err := e.GetLedgerFrontier(ctx)
	require.NoError(t, err)
	require.False(t, found)
	facts, err := e.LedgerFacts(ctx)
	require.NoError(t, err)
	require.Empty(t, facts)
	n, err := e.LedgerCounterBucketCount(ctx)
	require.NoError(t, err)
	require.Zero(t, n)

	// Success: all four.
	moved, err := e.TakeoverToken(ctx, "run-1", []string{"needs_expansion"}, counters)
	require.NoError(t, err)
	require.Equal(t, legacyState, moved)
	step, err = e.CurrentSyncStep(ctx)
	require.NoError(t, err)
	require.Empty(t, step, "token cleared")
	f, found, err := e.GetLedgerFrontier(ctx)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, legacyState, f.GetState())
	require.Equal(t, e.CurrentSyncID(), f.GetAttempt())
	require.NotNil(t, f.GetTakenOverAt())
	facts, err = e.LedgerFacts(ctx)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"needs_expansion": ""}, facts)
	got, err := e.LedgerCounters(ctx)
	require.NoError(t, err)
	require.Equal(t, counters, got)
	// The takeover marks the file in flight like a page commit would.
	v, err := e.keyspaceVersionStamp()
	require.NoError(t, err)
	require.Equal(t, keyspaceVersionLedgerInFlight, v)

	// Nothing to take over now: a no-op that leaves the frontier alone.
	moved, err = e.TakeoverToken(ctx, "run-2", nil, c1zstore.LedgerCounters{})
	require.NoError(t, err)
	require.Empty(t, moved)
	n, err = e.LedgerCounterBucketCount(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, n)

	// Sync-run metadata other than the token is preserved by the takeover.
	rec, err := e.GetSyncRunRecord(ctx, e.CurrentSyncID())
	require.NoError(t, err)
	require.Equal(t, v3.SyncType_SYNC_TYPE_FULL, rec.GetType())
	require.NotNil(t, rec.GetStartedAt())
	require.Nil(t, rec.GetEndedAt())

	// The frontier is readable through the store-facing interface too.
	sf, found, err := e.LedgerFrontier(ctx)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, legacyState, sf.State)
	require.False(t, sf.TakenOverAt.IsZero())
}

func TestLedgerTakeoverRequiresOpenSync(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	_, err := e.TakeoverToken(ctx, "run", nil, c1zstore.LedgerCounters{})
	require.Error(t, err)
	// And with a sync but no token: no-op, no frontier.
	_, err = e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	moved, err := e.TakeoverToken(ctx, "run", nil, c1zstore.LedgerCounters{})
	require.NoError(t, err)
	require.Empty(t, moved)
	_, found, err := e.GetLedgerFrontier(ctx)
	require.NoError(t, err)
	require.False(t, found)
}

// A ledgered sync seals only through EndSyncWithStats (brief §3.13):
// plain EndSync refuses while the ledger is in flight, so a sync can
// never seal with its timing / call stats and ingest quality silently
// absent. The stats given to the seal land on the sidecar; they come from
// the counter-bucket fold across runs. A sync that never touched the
// ledger (token-only) seals through plain EndSync as before.
func TestLedgeredSyncSealsOnlyWithStats(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	syncID, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	// Two runs' buckets (a page bucket and a run-level bucket each); the
	// fold adds counts and durations and takes the max latency.
	require.NoError(t, e.PutCounterBucket(ctx, "run-1", 0, c1zstore.LedgerCounters{
		Counters:       map[string]uint64{"completed_actions": 3},
		ConnectorCalls: map[string]c1zstore.CallStat{"ListGrants": {Count: 2, TotalMs: 40, MaxMs: 30}},
	}))
	require.NoError(t, e.PutCounterBucket(ctx, "run-1", c1zstore.RunBucketWorker, c1zstore.LedgerCounters{
		StepDurationsMs: map[string]int64{"list-grants": 100},
	}))
	require.NoError(t, e.PutCounterBucket(ctx, "run-2", 0, c1zstore.LedgerCounters{
		Counters:       map[string]uint64{"completed_actions": 1},
		ConnectorCalls: map[string]c1zstore.CallStat{"ListGrants": {Count: 1, TotalMs: 10, MaxMs: 10}},
	}))
	require.NoError(t, e.PutCounterBucket(ctx, "run-2", c1zstore.RunBucketWorker, c1zstore.LedgerCounters{
		StepDurationsMs: map[string]int64{"list-grants": 50},
	}))
	// Rewriting a bucket supersedes it (a total, not a delta).
	require.NoError(t, e.PutCounterBucket(ctx, "run-2", 0, c1zstore.LedgerCounters{
		Counters:       map[string]uint64{"completed_actions": 2},
		ConnectorCalls: map[string]c1zstore.CallStat{"ListGrants": {Count: 2, TotalMs: 20, MaxMs: 12}},
	}))
	require.NoError(t, e.PutCounterBucket(ctx, "run-2", c1zstore.RunBucketWorker, c1zstore.LedgerCounters{
		StepDurationsMs: map[string]int64{"list-grants": 60},
	}))
	n, err := e.LedgerCounterBucketCount(ctx)
	require.NoError(t, err)
	require.Equal(t, 4, n)
	foldC, err := e.LedgerCounters(ctx)
	require.NoError(t, err)
	fold := c1zstore.RunStats{
		StepDurationsMs:    foldC.StepDurationsMs,
		ConnectorCallStats: foldC.ConnectorCalls,
		SessionStoreStats:  foldC.SessionCalls,
		CompletedActions:   foldC.Counters["completed_actions"],
	}
	require.EqualValues(t, 160, fold.StepDurationsMs["list-grants"])
	require.Equal(t, c1zstore.CallStat{Count: 4, TotalMs: 60, MaxMs: 30}, fold.ConnectorCallStats["ListGrants"])
	require.EqualValues(t, 5, fold.CompletedActions)

	require.True(t, e.ledgerInFlight.Load(), "bucket writes mark the ledger in flight")
	require.ErrorIs(t, e.EndSync(ctx), ErrLedgeredSyncNeedsStats, "plain EndSync must refuse a ledgered sync")
	still, err := e.GetSyncRunRecord(ctx, syncID)
	require.NoError(t, err)
	require.Nil(t, still.GetEndedAt(), "the refusal happens before anything is sealed")

	require.NoError(t, e.EndSyncWithStats(ctx, c1zstore.SyncStats{
		Run:           fold,
		IngestQuality: &c1zstore.IngestQuality{GrantsDropped: 1, SourceCacheReplayBlocked: true, ReasonFlags: 2},
	}))
	stats, err := e.readSyncStats(ctx, syncID)
	require.NoError(t, err)
	require.NotNil(t, stats)
	require.EqualValues(t, 160, stats.GetStepDurationsMs()["list-grants"])
	require.EqualValues(t, 4, stats.GetConnectorCallStats()["ListGrants"].GetCount())
	require.EqualValues(t, 30, stats.GetConnectorCallStats()["ListGrants"].GetMaxMs())
	require.EqualValues(t, 1, stats.GetIngestQuality().GetGrantsDropped())
	require.True(t, stats.GetIngestQuality().GetSourceCacheReplayBlocked())
	sealed, err := e.GetSyncRunRecord(ctx, syncID)
	require.NoError(t, err)
	require.Empty(t, sealed.GetSyncToken())
	require.NotNil(t, sealed.GetEndedAt())

	// Token-only: no ledger writes, plain EndSync still seals.
	e2, _ := newTestEngine(t)
	_, err = e2.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, e2.EndSync(ctx))
}
