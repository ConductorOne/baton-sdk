package sync //nolint:revive,nolintlint // backwards-compatible package name

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/internal/chaosconnector"
)

// The sync-trace oracle bridge (formal/occult/TRACE_BRIDGE.md, mapping 2:
// real sync executions). These tests record canonical sync traces
// (sync_trace_audit.go) from the source-cache chaos scenarios —
// single-attempt cold/warm syncs and crash/resume multi-attempt syncs —
// sanity-check their shape in-process, and export them as JSONL
// fixtures for the Occult trace-policy oracle when
// BATON_SYNC_TRACE_FIXTURE_DIR is set. The committed fixtures live at
// formal/occult/host/testdata/realtraces/ and are verified against the
// five deliverable-7 policies by
// formal/occult/host/real_trace_oracle_test.go; regenerate them with:
//
//	BATON_SYNC_TRACE_FIXTURE_DIR=$(pwd)/formal/occult/host/testdata/realtraces \
//	  go test -run 'TestChaosSyncTraceOracle' ./pkg/sync/
//
// The recorder is observational only (commit-order events); rendering
// conventions live on the oracle side. Every fixture starts at sync
// birth (resumed=false in the header — the precondition for the
// renderer's structural-clear convention); crash/resume fixtures carry
// explicit "resume" marker lines between attempt segments.

// syncTraceFixtureHeader is the first JSONL line of an exported trace.
type syncTraceFixtureHeader struct {
	Name    string `json:"name"`
	Resumed bool   `json:"resumed"`
}

func TestChaosSyncTraceOracleFixtures(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 120*time.Second)
	defer cancel()
	tmpDir, paths := sourceCachePaths(t, 2)

	fixture := newSourceCacheFixture(t)
	scenario := newSourceCacheScenario(t, fixture)
	run, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
	require.NoError(t, err)
	run.SetSourceCacheCapability(sourceCacheCapabilityRW("gen-1", "cfg-1"))

	// Generation A: cold record sync (no previous artifact; the lookup is
	// NoopLookup, so the trace carries no consult events).
	coldTrace := runTracedSourceCacheSync(t, ctx, run, paths[0], tmpDir, "")

	// Generation B: warm replay sync against A's artifact.
	warmTrace := runTracedSourceCacheSync(t, ctx, run, paths[1], tmpDir, paths[0])

	rowKind := "grants"

	// In-process sanity only — the real verdicts are the Occult policy
	// oracle's job. Cold: the scope's page rows landed and its validator
	// published before the seal; no replay machinery ran.
	require.NotEmpty(t, coldTrace)
	requireTraceOrder(t, coldTrace,
		syncTraceEvent{Kind: syncTraceUpsert, RowKind: rowKind, ScopeKey: scGrantsScopeKey},
		syncTraceEvent{Kind: syncTracePublish, RowKind: rowKind, ScopeKey: scGrantsScopeKey},
		syncTraceEvent{Kind: syncTraceSeal},
	)
	require.NotContains(t, kinds(coldTrace), syncTraceReplay)
	require.NotContains(t, kinds(coldTrace), syncTraceConsult)

	// Warm: consult precedes the replay unit's clear+copy legs, exactly
	// one replay ran, the validator republished, and the seal closed it.
	requireTraceOrder(t, warmTrace,
		syncTraceEvent{Kind: syncTraceConsult, RowKind: rowKind, ScopeKey: scGrantsScopeKey},
		syncTraceEvent{Kind: syncTraceClear, RowKind: rowKind, ScopeKey: scGrantsScopeKey},
		syncTraceEvent{Kind: syncTraceReplay, RowKind: rowKind, ScopeKey: scGrantsScopeKey},
		syncTraceEvent{Kind: syncTracePublish, RowKind: rowKind, ScopeKey: scGrantsScopeKey},
		syncTraceEvent{Kind: syncTraceSeal},
	)
	require.Equal(t, 1, countKind(warmTrace, syncTraceReplay))

	// Both traces end at their seal (the recorder stops with the sync).
	require.Equal(t, syncTraceSeal, coldTrace[len(coldTrace)-1].Kind)
	require.Equal(t, syncTraceSeal, warmTrace[len(warmTrace)-1].Kind)

	exportTraceFixture(t, "cold_record_sync", coldTrace)
	exportTraceFixture(t, "warm_replay_sync", warmTrace)
}

// runTracedSourceCacheSync mirrors runSourceCacheSync but installs the
// sync-trace audit on the concrete syncer before running.
func runTracedSourceCacheSync(
	t *testing.T,
	ctx context.Context,
	run *chaosconnector.Run,
	c1zPath string,
	tmpDir string,
	prevPath string,
) []syncTraceEvent {
	t.Helper()
	var opts []SyncOpt
	if prevPath != "" {
		opts = append(opts, WithPreviousSyncC1ZPath(prevPath))
	}
	h := newChaosHarness(t, ctx, run, c1zPath, tmpDir, chaosTransportDirect, opts...)
	concrete, ok := h.Syncer.(*syncer)
	require.True(t, ok, "chaos harness syncer is not the concrete *syncer")
	audit := &syncTraceAudit{}
	concrete.testSyncTraceAudit = audit
	h.SyncAndClose(t, ctx)
	return audit.snapshot()
}

// requireTraceOrder asserts the expected events appear in the trace in
// the given relative order (other events may interleave).
func requireTraceOrder(t *testing.T, trace []syncTraceEvent, expected ...syncTraceEvent) {
	t.Helper()
	i := 0
	for _, ev := range trace {
		if i < len(expected) && ev == expected[i] {
			i++
		}
	}
	require.Equalf(t, len(expected), i,
		"trace missing expected event order (matched %d of %d): trace=%v expected=%v", i, len(expected), trace, expected)
}

func kinds(trace []syncTraceEvent) []syncTraceKind {
	out := make([]syncTraceKind, 0, len(trace))
	for _, ev := range trace {
		out = append(out, ev.Kind)
	}
	return out
}

func countKind(trace []syncTraceEvent, kind syncTraceKind) int {
	n := 0
	for _, ev := range trace {
		if ev.Kind == kind {
			n++
		}
	}
	return n
}

// exportTraceFixture writes one sync's trace as a JSONL fixture (header
// line, then one event per line, with a {"kind":"resume"} marker line
// between attempt segments) when BATON_SYNC_TRACE_FIXTURE_DIR is set.
func exportTraceFixture(t *testing.T, name string, attempts ...[]syncTraceEvent) {
	t.Helper()
	dir := os.Getenv("BATON_SYNC_TRACE_FIXTURE_DIR")
	if dir == "" {
		return
	}
	// The path is the developer's own updater flag, not external input.
	require.NoError(t, os.MkdirAll(dir, 0o755)) //nolint:gosec // test-only fixture export
	var buf []byte
	header, err := json.Marshal(syncTraceFixtureHeader{Name: name, Resumed: false})
	require.NoError(t, err)
	buf = append(buf, header...)
	buf = append(buf, '\n')
	total := 0
	for i, trace := range attempts {
		if i > 0 {
			buf = append(buf, []byte(`{"kind":"resume"}`)...)
			buf = append(buf, '\n')
		}
		for _, ev := range trace {
			line, err := json.Marshal(ev)
			require.NoError(t, err)
			buf = append(buf, line...)
			buf = append(buf, '\n')
		}
		total += len(trace)
	}
	path := filepath.Join(dir, name+".jsonl")
	require.NoError(t, os.WriteFile(path, buf, 0o600)) //nolint:gosec // test-only fixture export
	t.Logf("wrote trace fixture %s (%d attempts, %d events)", path, len(attempts), total)
}

// TestChaosSyncTraceOracleInterruptedFixtures records a MULTI-ATTEMPT
// sync trace from the crash/resume scenario pinned by
// chaos_source_cache_resume_test.go: a warm two-page delta round is cut
// by an EffectCrash before its second page ("warm-2" — i.e. AFTER the
// replay copy and the overlay upsert committed), then resumed to seal
// by a NEW syncer over the same artifact.
//
// PINNED DISCOVERY (this recorder is the first instrument that can
// distinguish a skipped copy from an idempotent re-copy): the resume
// RE-RUNS the replay copy even when attempt 1 checkpoints at every
// batch boundary (checkpointInterval=0, the strongest cadence). The
// main loop checkpoints between BATCHES, and a paginated action's page
// chain runs inside one batch, so a mid-chain cut leaves
// MarkSourceCacheReplayed un-checkpointed no matter the interval — the
// resume restarts the action from its root and re-copies. The
// across-attempt re-copy is B5-legal at-least-once idempotence, which
// is exactly what the policy oracle's once-per-scope-resets-at-resume
// semantics legalize. The replayed-set's skip role is WITHIN-attempt:
// warm-2's second replay annotation skips because page "" marked the
// scope replayed in the same attempt — visible below as exactly one
// replay per attempt.
func TestChaosSyncTraceOracleInterruptedFixtures(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	tmpDir, paths := sourceCachePaths(t, 2)
	seedPath, warmPath := paths[0], paths[1]

	fx := newSCCollectionFixture(t)
	scenario := scResumeScenario(t, fx)
	capability := sourceCacheCapabilityRW("gen-1", "cfg-1")

	// Generation A: cold seed (not exported; the sync under trace is
	// generation B).
	seedRun, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
	require.NoError(t, err)
	seedRun.SetSourceCacheCapability(capability)
	runSourceCacheSync(t, ctx, seedRun, chaosTransportDirect, seedPath, tmpDir, "", WithWorkerCount(1))

	// Generation B, attempt 1: warm sync cut before "warm-2".
	interruptedRun, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule(chaosconnector.Rule{
		ID: "cut",
		Match: chaosconnector.Matcher{
			Service:   chaosconnector.ExactString("GrantsService"),
			Method:    chaosconnector.ExactString("ListGrants"),
			PageToken: chaosconnector.ExactString("warm-2"),
			Attempt:   1,
			Phase:     chaosconnector.PhaseBeforeCall,
		},
		Effects:  []chaosconnector.Effect{{Kind: chaosconnector.EffectCrash}},
		MinFires: 1,
		MaxFires: 1,
	}))
	require.NoError(t, err)
	require.NoError(t, interruptedRun.SetEpoch("second"))
	interruptedRun.SetSourceCacheCapability(capability)
	interruptedHarness := newChaosHarness(t, ctx, interruptedRun, warmPath, tmpDir, chaosTransportDirect,
		WithPreviousSyncC1ZPath(seedPath), WithWorkerCount(1))
	interruptedConcrete, ok := interruptedHarness.Syncer.(*syncer)
	require.True(t, ok)
	// The strongest cadence: even per-batch checkpoints cannot make the
	// mid-chain cut resumable past the copy (see the pin above).
	interruptedConcrete.checkpointInterval = 0
	attempt1Audit := &syncTraceAudit{}
	interruptedConcrete.testSyncTraceAudit = attempt1Audit
	require.ErrorIs(t, interruptedHarness.Syncer.Sync(ctx), chaosconnector.ErrInterruptRequested)
	require.NoError(t, interruptedHarness.Close(t.Context()))
	attempt1 := attempt1Audit.snapshot()

	// Generation B, resume: a new syncer over the same artifact.
	resumeRun, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
	require.NoError(t, err)
	require.NoError(t, resumeRun.SetEpoch("second"))
	resumeRun.SetSourceCacheCapability(capability)
	resumeHarness := newChaosHarness(t, ctx, resumeRun, warmPath, tmpDir, chaosTransportDirect,
		WithPreviousSyncC1ZPath(seedPath), WithWorkerCount(1))
	resumeConcrete, ok := resumeHarness.Syncer.(*syncer)
	require.True(t, ok)
	attempt2Audit := &syncTraceAudit{}
	resumeConcrete.testSyncTraceAudit = attempt2Audit
	resumeHarness.SyncAndClose(t, ctx)
	attempt2 := attempt2Audit.snapshot()

	rowKind := "grants"
	// Attempt 1 committed the replay unit and the overlay upsert, never
	// published, never sealed (the cut hit before warm-2).
	requireTraceOrder(t, attempt1,
		syncTraceEvent{Kind: syncTraceConsult, RowKind: rowKind, ScopeKey: scGrantsScopeKey},
		syncTraceEvent{Kind: syncTraceClear, RowKind: rowKind, ScopeKey: scGrantsScopeKey},
		syncTraceEvent{Kind: syncTraceReplay, RowKind: rowKind, ScopeKey: scGrantsScopeKey},
		syncTraceEvent{Kind: syncTraceUpsert, RowKind: rowKind, ScopeKey: scGrantsScopeKey},
	)
	require.NotContains(t, kinds(attempt1), syncTracePublish)
	require.NotContains(t, kinds(attempt1), syncTraceSeal)
	require.Equal(t, 1, countKind(attempt1, syncTraceReplay))

	// Attempt 2 restarts the action from its root: re-consult, re-clear,
	// re-copy (the pin), the re-applied overlay upsert, warm-2's skip
	// (one replay only), the validator publish, and the seal.
	requireTraceOrder(t, attempt2,
		syncTraceEvent{Kind: syncTraceConsult, RowKind: rowKind, ScopeKey: scGrantsScopeKey},
		syncTraceEvent{Kind: syncTraceClear, RowKind: rowKind, ScopeKey: scGrantsScopeKey},
		syncTraceEvent{Kind: syncTraceReplay, RowKind: rowKind, ScopeKey: scGrantsScopeKey},
		syncTraceEvent{Kind: syncTraceUpsert, RowKind: rowKind, ScopeKey: scGrantsScopeKey},
		syncTraceEvent{Kind: syncTracePublish, RowKind: rowKind, ScopeKey: scGrantsScopeKey},
		syncTraceEvent{Kind: syncTraceSeal},
	)
	require.Equal(t, 1, countKind(attempt2, syncTraceReplay),
		"mid-chain cut resume must re-run the replay copy exactly once (root restart + within-attempt skip on warm-2)")
	require.Equal(t, syncTraceSeal, attempt2[len(attempt2)-1].Kind)

	exportTraceFixture(t, "warm_replay_sync_interrupted", attempt1, attempt2)
}
