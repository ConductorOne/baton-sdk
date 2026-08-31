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
// real sync executions). This test records canonical sync traces
// (sync_trace_audit.go) from the reference source-cache scenario — one
// cold record sync, one warm replay sync — sanity-checks their shape
// in-process, and exports them as JSONL fixtures for the Occult
// trace-policy oracle when BATON_SYNC_TRACE_FIXTURE_DIR is set. The
// committed fixtures live at formal/occult/host/testdata/realtraces/ and
// are verified against the five deliverable-7 policies by
// formal/occult/host/real_trace_oracle_test.go; regenerate them with:
//
//	BATON_SYNC_TRACE_FIXTURE_DIR=$(pwd)/../../formal/occult/host/testdata/realtraces \
//	  go test -run TestChaosSyncTraceOracleFixtures ./pkg/sync/
//
// The recorder is observational only (commit-order events); rendering
// conventions live on the oracle side. Both fixtures are single-attempt
// traces of NEW syncs (resumed=false in the header), which is the
// precondition for the renderer's structural-clear convention.

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

// exportTraceFixture writes the trace as a JSONL fixture (header line,
// then one event per line) when BATON_SYNC_TRACE_FIXTURE_DIR is set.
func exportTraceFixture(t *testing.T, name string, trace []syncTraceEvent) {
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
	for _, ev := range trace {
		line, err := json.Marshal(ev)
		require.NoError(t, err)
		buf = append(buf, line...)
		buf = append(buf, '\n')
	}
	path := filepath.Join(dir, name+".jsonl")
	require.NoError(t, os.WriteFile(path, buf, 0o600)) //nolint:gosec // test-only fixture export
	t.Logf("wrote trace fixture %s (%d events)", path, len(trace))
}
