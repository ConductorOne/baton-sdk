package synccompactor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

var allIncrementalFaultStages = []string{
	"mid_expand_write",
	"after_walk",
	"after_sidecar",
	"before_end_sync",
	"after_end_sync",
	"after_marker",
	"before_close",
	"before_publish",
	"after_publish",
}

func TestIncrementalFaultInventoryIsComplete(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	require.True(t, ok)
	source, err := os.ReadFile(filepath.Join(filepath.Dir(thisFile), "compactor.go"))
	require.NoError(t, err)
	re := regexp.MustCompile(`(?:runIncrementalTestHook|hook)\("([a-z_]+)"`)
	matches := re.FindAllSubmatch(source, -1)
	actualSet := make(map[string]struct{}, len(matches))
	for _, match := range matches {
		actualSet[string(match[1])] = struct{}{}
	}
	actual := make([]string, 0, len(actualSet))
	for stage := range actualSet {
		actual = append(actual, stage)
	}
	sort.Strings(actual)
	want := append([]string(nil), allIncrementalFaultStages...)
	sort.Strings(want)
	require.Equal(t, want, actual,
		"every production fault cut must be present in the exhaustive crash/retry sweep")
}

func TestIncrementalExpansionOutcomeLogging(t *testing.T) {
	tests := []struct {
		name        string
		build       func(*testing.T, context.Context, string) []*CompactableSync
		options     []Option
		wantOutcome string
		wantReason  string
	}{
		{
			name: "success", build: func(t *testing.T, ctx context.Context, dir string) []*CompactableSync {
				return buildIncrementalFixtures(t, ctx, dir)
			},
			options:     []Option{WithEngine(c1zstore.EnginePebble), WithIncrementalExpansion()},
			wantOutcome: "succeeded", wantReason: "none",
		},
		{
			name: "revocation decline",
			build: func(t *testing.T, ctx context.Context, dir string) []*CompactableSync {
				return buildSpecChangeFixtures(t, ctx, dir, false, true)
			},
			options:     []Option{WithEngine(c1zstore.EnginePebble), WithIncrementalExpansion()},
			wantOutcome: "declined", wantReason: "revocation",
		},
		{
			name: "dropped edge decline", build: buildDroppedEdgeFixtures,
			options:     []Option{WithEngine(c1zstore.EnginePebble), WithIncrementalExpansion()},
			wantOutcome: "declined", wantReason: "dropped_edge",
		},
		{
			name: "cycle decline", build: buildCycleLoggingFixtures,
			options:     []Option{WithEngine(c1zstore.EnginePebble), WithIncrementalExpansion()},
			wantOutcome: "declined", wantReason: "cycle",
		},
		{
			name: "invalid graph counters",
			build: func(t *testing.T, ctx context.Context, dir string) []*CompactableSync {
				entries := buildIncrementalFixtures(t, ctx, dir)
				graph := baseGraphForFixtures(t, ctx)
				graph.NextNodeID = 0
				store, err := dotc1z.NewStore(ctx, entries[0].FilePath, dotc1z.WithTmpDir(t.TempDir()))
				require.NoError(t, err)
				persistFixtureGraph(t, ctx, store, entries[0].SyncID, graph)
				require.NoError(t, store.Close(ctx))
				return entries
			},
			options:     []Option{WithEngine(c1zstore.EnginePebble), WithIncrementalExpansion()},
			wantOutcome: "fell_back", wantReason: "base_graph_error",
		},
		{
			// Overflow means the recorded ids no longer describe what was
			// skipped, so seeding them cannot make this agree with full
			// expansion. Only this case declines.
			name: "dangling overflow decline",
			build: func(t *testing.T, ctx context.Context, dir string) []*CompactableSync {
				entries := buildIncrementalFixtures(t, ctx, dir)
				graph := baseGraphForFixtures(t, ctx)
				graph.NoteUnrecoverableDangling()
				store, err := dotc1z.NewStore(ctx, entries[0].FilePath, dotc1z.WithTmpDir(t.TempDir()))
				require.NoError(t, err)
				persistFixtureGraph(t, ctx, store, entries[0].SyncID, graph)
				require.NoError(t, store.Close(ctx))
				return entries
			},
			options:     []Option{WithEngine(c1zstore.EnginePebble), WithIncrementalExpansion()},
			wantOutcome: "declined", wantReason: "dangling_overflow",
		},
		{
			// Ordinary dangling ids must NOT decline. The expander prechecks them:
			// still-missing ids remain recorded without seeding a walk, while ids
			// that now resolve seed their affected closure.
			name: "recorded dangling ids still take the fast path",
			build: func(t *testing.T, ctx context.Context, dir string) []*CompactableSync {
				entries := buildIncrementalFixtures(t, ctx, dir)
				graph := baseGraphForFixtures(t, ctx)
				graph.NoteDanglingReference("never:resolves")
				store, err := dotc1z.NewStore(ctx, entries[0].FilePath, dotc1z.WithTmpDir(t.TempDir()))
				require.NoError(t, err)
				persistFixtureGraph(t, ctx, store, entries[0].SyncID, graph)
				require.NoError(t, store.Close(ctx))
				return entries
			},
			options:     []Option{WithEngine(c1zstore.EnginePebble), WithIncrementalExpansion()},
			wantOutcome: "succeeded", wantReason: "none",
		},
		{
			name: "unsupported engine", build: func(t *testing.T, ctx context.Context, dir string) []*CompactableSync {
				return buildIncrementalFixturesEngine(t, ctx, dir, c1zstore.EngineSQLite)
			},
			options:     []Option{WithIncrementalExpansion()},
			wantOutcome: "not_attempted", wantReason: "unsupported_engine",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var logs bytes.Buffer
			core := zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
				zapcore.AddSync(&logs), zap.InfoLevel)
			ctx := ctxzap.ToContext(context.Background(), zap.New(core))
			entries := tc.build(t, ctx, t.TempDir())
			options := append([]Option{WithTmpDir(t.TempDir())}, tc.options...)
			compactor, cleanup, err := NewCompactor(ctx, t.TempDir(), entries, options...)
			require.NoError(t, err)
			defer func() { require.NoError(t, cleanup()) }()
			_, err = compactor.Compact(ctx)
			require.NoError(t, err)

			found := false
			for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
				var fields map[string]any
				require.NoError(t, json.Unmarshal([]byte(line), &fields))
				if fields["msg"] == "incremental grant expansion outcome" &&
					fields["incremental_expansion_outcome"] == tc.wantOutcome &&
					fields["incremental_expansion_reason"] == tc.wantReason {
					found = true
				}
			}
			require.True(t, found, "missing stable outcome=%s reason=%s in logs:\n%s",
				tc.wantOutcome, tc.wantReason, logs.String())
		})
	}
}

func buildCycleLoggingFixtures(t *testing.T, ctx context.Context, dir string) []*CompactableSync {
	t.Helper()
	entries := buildIncrementalFixtures(t, ctx, dir)
	groupB, groupC := grp("grpB"), grp("grpC")
	entB := ent("ent-b", groupB)
	path := filepath.Join(dir, "cycle.c1z")
	store, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	syncID, err := store.StartNewSync(ctx, connectorstore.SyncTypePartial, "")
	require.NoError(t, err)
	require.NoError(t, store.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()))
	require.NoError(t, store.PutResources(ctx, groupB, groupC))
	require.NoError(t, store.PutEntitlements(ctx, entB))
	require.NoError(t, store.PutGrants(ctx, ruleGrant(entB, groupC, "ent-c")))
	require.NoError(t, store.EndSync(ctx))
	require.NoError(t, store.Close(ctx))
	return append(entries, &CompactableSync{FilePath: path, SyncID: syncID})
}

func TestIncrementalExpansionProcessKillRetry(t *testing.T) {
	for _, stage := range []string{"after_sidecar", "before_end_sync"} {
		t.Run(stage, func(t *testing.T) {
			ctx := context.Background()
			entries := buildIncrementalFixtures(t, ctx, t.TempDir())
			failedDest := t.TempDir()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestIncrementalExpansionProcessKillHelper$") //nolint:gosec // os.Args[0] is the current test binary.
			cmd.Env = append(os.Environ(),
				"BATON_INCREMENTAL_KILL_HELPER=1",
				"BATON_INCREMENTAL_KILL_STAGE="+stage,
				"BATON_INCREMENTAL_BASE_PATH="+entries[0].FilePath,
				"BATON_INCREMENTAL_BASE_SYNC="+entries[0].SyncID,
				"BATON_INCREMENTAL_INC_PATH="+entries[1].FilePath,
				"BATON_INCREMENTAL_INC_SYNC="+entries[1].SyncID,
				"BATON_INCREMENTAL_DEST="+failedDest,
				"BATON_INCREMENTAL_TMP="+t.TempDir(),
			)
			err := cmd.Run()
			require.Error(t, err, "helper must be killed at %s", stage)
			published, readErr := os.ReadDir(failedDest)
			require.NoError(t, readErr)
			require.Empty(t, published, "killed attempt must publish nothing")

			var outcomes [][]string
			for retryNumber := 0; retryNumber < 2; retryNumber++ {
				retry, cleanup, newErr := NewCompactor(ctx, t.TempDir(), entries,
					WithTmpDir(t.TempDir()), WithEngine(c1zstore.EnginePebble), WithIncrementalExpansion())
				require.NoError(t, newErr)
				out, compactErr := retry.Compact(ctx)
				require.NoError(t, compactErr)
				require.True(t, retry.incrementalExpansionRan)
				outcomes = append(outcomes, grantOutcome(t, ctx, out.FilePath, out.SyncID))
				assertSealedCompactionArtifact(t, ctx, out, true)
				require.NoError(t, cleanup())
			}
			require.Equal(t, outcomes[0], outcomes[1], "two fresh retries must converge identically")
		})
	}
}

func TestIncrementalExpansionProcessKillHelper(t *testing.T) {
	if os.Getenv("BATON_INCREMENTAL_KILL_HELPER") != "1" {
		t.Skip("subprocess helper")
	}
	ctx := context.Background()
	entries := []*CompactableSync{
		{FilePath: os.Getenv("BATON_INCREMENTAL_BASE_PATH"), SyncID: os.Getenv("BATON_INCREMENTAL_BASE_SYNC")},
		{FilePath: os.Getenv("BATON_INCREMENTAL_INC_PATH"), SyncID: os.Getenv("BATON_INCREMENTAL_INC_SYNC")},
	}
	compactor, _, err := NewCompactor(ctx, os.Getenv("BATON_INCREMENTAL_DEST"), entries,
		WithTmpDir(os.Getenv("BATON_INCREMENTAL_TMP")),
		WithEngine(c1zstore.EnginePebble),
		WithIncrementalExpansion())
	require.NoError(t, err)
	compactor.incrementalTestHook = func(stage string) error {
		if stage == os.Getenv("BATON_INCREMENTAL_KILL_STAGE") {
			process, findErr := os.FindProcess(os.Getpid())
			if findErr == nil {
				_ = process.Kill()
			}
			os.Exit(91)
		}
		return nil
	}
	_, _ = compactor.Compact(ctx)
	os.Exit(92)
}

// TestIncrementalExpansionCrashRetry injects failures at the feature's three
// commit cuts. A failed attempt must publish nothing; a fresh retry from the
// same immutable inputs must converge to the full-expansion oracle.
func TestIncrementalExpansionCrashRetry(t *testing.T) {
	for _, stage := range allIncrementalFaultStages {
		stage := stage
		t.Run(stage, func(t *testing.T) {
			ctx := context.Background()
			entries := buildIncrementalFixtures(t, ctx, t.TempDir())
			failedDest := t.TempDir()
			failed, failedCleanup, err := NewCompactor(ctx, failedDest, entries,
				WithTmpDir(t.TempDir()),
				WithEngine(c1zstore.EnginePebble),
				WithIncrementalExpansion(),
			)
			require.NoError(t, err)
			failed.incrementalTestHook = func(at string) error {
				if at == "after_end_sync" {
					run, runErr := failed.compactedC1z.SyncMeta().LatestFinishedSyncOfAnyType(ctx)
					require.NoError(t, runErr)
					require.Empty(t, run.Generation, "verification must be absent immediately after seal")
				}
				if at == "after_marker" {
					run, runErr := failed.compactedC1z.SyncMeta().LatestFinishedSyncOfAnyType(ctx)
					require.NoError(t, runErr)
					require.True(t, run.IsVerified(), "marker must be present only after seal")
				}
				if at == stage {
					return errors.New("injected crash")
				}
				return nil
			}
			_, err = failed.Compact(ctx)
			require.Error(t, err)
			require.NoError(t, failedCleanup())
			published, err := os.ReadDir(failedDest)
			require.NoError(t, err)
			if stage == "after_publish" {
				require.Len(t, published, 1, "post-publish failure must leave one complete artifact")
				path := filepath.Join(failedDest, published[0].Name())
				store, openErr := dotc1z.NewStore(ctx, path, dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(t.TempDir()))
				require.NoError(t, openErr)
				run, runErr := store.SyncMeta().LatestFinishedSyncOfAnyType(ctx)
				require.NoError(t, runErr)
				require.NoError(t, store.Close(ctx))
				assertSealedCompactionArtifact(t, ctx, &CompactableSync{FilePath: path, SyncID: run.ID}, true)
			} else {
				require.Empty(t, published, "failed attempt must not publish an artifact")
			}

			retry, retryCleanup, err := NewCompactor(ctx, t.TempDir(), entries,
				WithTmpDir(t.TempDir()),
				WithEngine(c1zstore.EnginePebble),
				WithIncrementalExpansion(),
			)
			require.NoError(t, err)
			defer func() { require.NoError(t, retryCleanup()) }()
			retryOut, err := retry.Compact(ctx)
			require.NoError(t, err)
			require.True(t, retry.incrementalExpansionRan)

			fullEntries := buildIncrementalFixtures(t, ctx, t.TempDir())
			full, fullCleanup, err := NewCompactor(ctx, t.TempDir(), fullEntries,
				WithTmpDir(t.TempDir()), WithEngine(c1zstore.EnginePebble))
			require.NoError(t, err)
			defer func() { require.NoError(t, fullCleanup()) }()
			fullOut, err := full.Compact(ctx)
			require.NoError(t, err)

			require.Equal(t,
				grantOutcome(t, ctx, fullOut.FilePath, fullOut.SyncID),
				grantOutcome(t, ctx, retryOut.FilePath, retryOut.SyncID))
			assertSealedCompactionArtifact(t, ctx, retryOut, true)
		})
	}
}
