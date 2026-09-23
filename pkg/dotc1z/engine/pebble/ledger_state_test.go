package pebble

// Tests for the ledger family's sync-level sub-families (brief §3.6,
// §3.8): fact keys, counter buckets and the token-only takeover. The
// row tests are in ledger_test.go.

import (
	"context"
	"errors"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
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
	require.NoError(t, e.Ledger().InitializePendingWork(t.Context(), nil))

	u := e.ledger.newPageUnit()
	require.NoError(t, u.StageFact("needs_expansion"))
	require.NoError(t, u.StageCounterBucket("run-1", 0, bucket(0b1, "grants_dropped", uint64(3))))
	boom := errors.New("injected")
	e.db.SetRecordCommitTestHook(func() error { return boom })
	require.ErrorIs(t, u.Commit(ctx, grantsPageIdentity("github", "p1"), nil), boom)
	e.db.SetRecordCommitTestHook(nil)
	facts, err := e.ledger.Facts(ctx)
	require.NoError(t, err)
	require.Empty(t, facts, "a fact from a failed page was never established")
	sum, err := e.ledger.Counters(ctx)
	require.NoError(t, err)
	require.Empty(t, sum.Counters)

	require.NoError(t, u.Commit(ctx, grantsPageIdentity("github", "p1"), nil))
	facts, err = e.ledger.Facts(ctx)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"needs_expansion": ""}, facts)

	u2 := e.ledger.newPageUnit()
	require.NoError(t, u2.StageFact("needs_expansion"))
	require.NoError(t, u2.StageFact("has_external_resource_grants"))
	require.NoError(t, u2.StageCounterBucket("run-1", 3, bucket(0b10, "grants_dropped", uint64(2), "entitlements_dropped", uint64(1))))
	require.NoError(t, u2.Commit(ctx, grantsPageIdentity("github", "p2"), nil))
	u3 := e.ledger.newPageUnit()
	require.NoError(t, u3.StageCounterBucket("run-1", 0, bucket(0b1, "grants_dropped", uint64(5))))
	require.NoError(t, u3.Commit(ctx, grantsPageIdentity("github", "p3"), nil))
	u4 := e.ledger.newPageUnit()
	require.NoError(t, u4.StageCounterBucket("run-2", 0, bucket(0, "grants_dropped", uint64(1))))
	require.NoError(t, u4.Commit(ctx, grantsPageIdentity("github", "p4"), nil))

	facts, err = e.ledger.Facts(ctx)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"needs_expansion": "", "has_external_resource_grants": ""}, facts)
	n, err := e.ledger.counterBucketCount(ctx)
	require.NoError(t, err)
	require.Equal(t, 3, n, "(run-1,0) (run-1,3) (run-2,0)")
	sum, err = e.ledger.Counters(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 5+2+1, sum.Counters["grants_dropped"])
	require.EqualValues(t, 1, sum.Counters["entitlements_dropped"])
	require.EqualValues(t, 0b11, sum.Flags)

	rows, err := e.ledger.rowCount(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 4, rows)

	require.NoError(t, e.EndSyncWithStats(ctx, c1zstore.SyncStats{}))
	_, err = e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	facts, err = e.ledger.Facts(ctx)
	require.NoError(t, err)
	require.Empty(t, facts)
	n, err = e.ledger.counterBucketCount(ctx)
	require.NoError(t, err)
	require.Zero(t, n)
	_, found, err := e.ledger.Frontier(ctx)
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
	_, err = e.ledger.Takeover(ctx, "run-1", []string{"needs_expansion"}, counters)
	require.ErrorIs(t, err, boom)
	e.db.SetRecordCommitTestHook(nil)
	step, err := e.CurrentSyncStep(ctx)
	require.NoError(t, err)
	require.Equal(t, legacyState, step, "legacyState survives a failed takeover")
	_, found, err := e.ledger.Frontier(ctx)
	require.NoError(t, err)
	require.False(t, found)
	facts, err := e.ledger.Facts(ctx)
	require.NoError(t, err)
	require.Empty(t, facts)
	n, err := e.ledger.counterBucketCount(ctx)
	require.NoError(t, err)
	require.Zero(t, n)

	moved, err := e.ledger.Takeover(ctx, "run-1", []string{"needs_expansion"}, counters)
	require.NoError(t, err)
	require.Equal(t, legacyState, moved)
	step, err = e.CurrentSyncStep(ctx)
	require.NoError(t, err)
	require.Empty(t, step, "token cleared")
	f, found, err := e.ledger.Frontier(ctx)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, legacyState, f.State)
	require.Equal(t, e.CurrentSyncID(), f.Attempt)
	require.False(t, f.TakenOverAt.IsZero())
	facts, err = e.ledger.Facts(ctx)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"needs_expansion": ""}, facts)
	got, err := e.ledger.Counters(ctx)
	require.NoError(t, err)
	require.Equal(t, counters, got)
	v, err := e.keyspaceVersionStamp()
	require.NoError(t, err)
	require.Equal(t, keyspaceVersionLedgerInFlight, v)

	moved, err = e.ledger.Takeover(ctx, "run-2", nil, c1zstore.LedgerCounters{})
	require.NoError(t, err)
	require.Empty(t, moved)
	n, err = e.ledger.counterBucketCount(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, n)

	rec, err := e.GetSyncRunRecord(ctx, e.CurrentSyncID())
	require.NoError(t, err)
	require.Equal(t, v3.SyncType_SYNC_TYPE_FULL, rec.GetType())
	require.NotNil(t, rec.GetStartedAt())
	require.Nil(t, rec.GetEndedAt())

	sf, found, err := e.ledger.Frontier(ctx)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, legacyState, sf.State)
	require.False(t, sf.TakenOverAt.IsZero())
}

// The same unit on crash images instead of injected errors. Three cuts:
// before the call, between the in-flight stamp and the batch (the record
// commit hook fires before the WAL write, so a clone taken in it is that
// image), and after the call returns with every unsynced byte dropped.
// Retention is on, so the post image is also where a process that never
// set the flag reads the takeover's retain fact.
func TestLedgerTakeoverCrashImages(t *testing.T) {
	skipOnWindowsMemFS(t)
	ctx := context.Background()
	fs := vfs.NewCrashableMem()
	e, err := Open(ctx, "takeover-crash-db", WithVFS(fs), withPanicOnFatalLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = e.Close() })
	syncID, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	const legacyState = `{"v":1,"actions":[{"op":"list-grants"}]}`
	require.NoError(t, e.CheckpointSync(ctx, legacyState))
	e.ledger.SetRetainTokens(true)
	counters := c1zstore.LedgerCounters{Counters: map[string]uint64{"grants_dropped": 7}, Flags: 0b100}

	open := func(image *vfs.MemFS, label string) *Engine {
		re, err := Open(ctx, "takeover-crash-db", WithVFS(image), withPanicOnFatalLogger())
		require.NoError(t, err, label)
		t.Cleanup(func() { _ = re.Close() })
		require.NoError(t, re.SetCurrentSync(ctx, syncID), label)
		return re
	}
	stamp := func(re *Engine) uint32 {
		v, err := re.keyspaceVersionStamp()
		require.NoError(t, err)
		return v
	}
	tokenOnly := func(re *Engine, label string) {
		step, err := re.CurrentSyncStep(ctx)
		require.NoError(t, err, label)
		require.Equal(t, legacyState, step, "%s: token intact", label)
		_, found, err := re.ledger.Frontier(ctx)
		require.NoError(t, err, label)
		require.False(t, found, "%s: no frontier", label)
		facts, err := re.ledger.Facts(ctx)
		require.NoError(t, err, label)
		require.Empty(t, facts, "%s: no facts", label)
		n, err := re.ledger.counterBucketCount(ctx)
		require.NoError(t, err, label)
		require.Zero(t, n, "%s: no bucket", label)
	}

	pre := open(fs.CrashClone(vfs.CrashCloneCfg{}), "pre")
	tokenOnly(pre, "pre")
	require.Equal(t, keyspaceVersion, stamp(pre), "pre: a plain v2 file")

	var mid *vfs.MemFS
	e.db.SetRecordCommitTestHook(func() error {
		mid = fs.CrashClone(vfs.CrashCloneCfg{})
		return nil
	})
	moved, err := e.ledger.Takeover(ctx, "run-1", []string{"needs_expansion"}, counters)
	e.db.SetRecordCommitTestHook(nil)
	require.NoError(t, err)
	require.Equal(t, legacyState, moved)
	require.NotNil(t, mid, "the hook ran")

	withTokenOnlySDK(func() {
		_, err := Open(ctx, "takeover-crash-db", WithVFS(mid), WithReadOnly(true))
		require.Error(t, err, "mid: token-only SDK refuses the in-flight stamp")
		require.Contains(t, err.Error(), "unsupported keyspace layout v3")
	})
	m := open(mid, "mid")
	tokenOnly(m, "mid")
	require.Equal(t, keyspaceVersionLedgerInFlight, stamp(m), "mid: stamp landed before the batch")
	again, err := m.ledger.Takeover(ctx, "run-1", nil, c1zstore.LedgerCounters{})
	require.NoError(t, err)
	require.Equal(t, legacyState, again, "mid: the resumed sync takes over again")

	post := open(fs.CrashClone(vfs.CrashCloneCfg{}), "post")
	step, err := post.CurrentSyncStep(ctx)
	require.NoError(t, err)
	require.Empty(t, step, "post: token cleared")
	f, found, err := post.ledger.Frontier(ctx)
	require.NoError(t, err)
	require.True(t, found, "post: frontier durable with no unsynced bytes kept")
	require.Equal(t, legacyState, f.State)
	facts, err := post.ledger.Facts(ctx)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"needs_expansion": "", c1zstore.LedgerFactRetainTokens: ""}, facts)
	got, err := post.ledger.Counters(ctx)
	require.NoError(t, err)
	require.Equal(t, counters, got)
	require.Equal(t, keyspaceVersionLedgerInFlight, stamp(post))
	require.False(t, post.ledger.retainTokensFlag(), "premise: this process never set the flag")
	scrub, err := post.ledger.sealScrubsTokens()
	require.NoError(t, err)
	require.False(t, scrub, "post: the takeover's fact alone keeps the seal from scrubbing")
}

func TestLedgerTakeoverRequiresOpenSync(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	_, err := e.ledger.Takeover(ctx, "run", nil, c1zstore.LedgerCounters{})
	require.Error(t, err)
	_, err = e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	moved, err := e.ledger.Takeover(ctx, "run", nil, c1zstore.LedgerCounters{})
	require.NoError(t, err)
	require.Empty(t, moved)
	_, found, err := e.ledger.Frontier(ctx)
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
	require.NoError(t, e.Ledger().InitializePendingWork(t.Context(), nil))

	require.NoError(t, e.ledger.PutCounterBucket(ctx, "run-1", 0, c1zstore.LedgerCounters{
		Counters:       map[string]uint64{"completed_actions": 3},
		ConnectorCalls: map[string]c1zstore.CallStat{"ListGrants": {Count: 2, TotalMs: 40, MaxMs: 30}},
	}))
	require.NoError(t, e.ledger.PutCounterBucket(ctx, "run-1", c1zstore.RunBucketWorker, c1zstore.LedgerCounters{
		StepDurationsMs: map[string]int64{"list-grants": 100},
	}))
	require.NoError(t, e.ledger.PutCounterBucket(ctx, "run-2", 0, c1zstore.LedgerCounters{
		Counters:       map[string]uint64{"completed_actions": 1},
		ConnectorCalls: map[string]c1zstore.CallStat{"ListGrants": {Count: 1, TotalMs: 10, MaxMs: 10}},
	}))
	require.NoError(t, e.ledger.PutCounterBucket(ctx, "run-2", c1zstore.RunBucketWorker, c1zstore.LedgerCounters{
		StepDurationsMs: map[string]int64{"list-grants": 50},
	}))
	require.NoError(t, e.ledger.PutCounterBucket(ctx, "run-2", 0, c1zstore.LedgerCounters{
		Counters:       map[string]uint64{"completed_actions": 2},
		ConnectorCalls: map[string]c1zstore.CallStat{"ListGrants": {Count: 2, TotalMs: 20, MaxMs: 12}},
	}))
	require.NoError(t, e.ledger.PutCounterBucket(ctx, "run-2", c1zstore.RunBucketWorker, c1zstore.LedgerCounters{
		StepDurationsMs: map[string]int64{"list-grants": 60},
	}))
	n, err := e.ledger.counterBucketCount(ctx)
	require.NoError(t, err)
	require.Equal(t, 4, n)
	foldC, err := e.ledger.Counters(ctx)
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

	require.True(t, e.ledger.inFlight.Load(), "bucket writes mark the ledger in flight")
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

	e2, _ := newTestEngine(t)
	_, err = e2.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, e2.EndSync(ctx))
}
