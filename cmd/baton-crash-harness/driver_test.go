// Driver for the real-binary interruption instrument (main.go).
//
// TestCrashHarnessBuildsAgainstHead always runs: it is the compile gate that
// keeps the tag-gated harness binary from rotting against HEAD.
//
// TestCrashResumeRealConnector is the instrument itself, gated behind
// BATON_DEMO_CRASH=1 (run via `make crash-check`). It composes real
// processes into a production-shaped sync history:
//
//   - budget-bounded sessions (-run-duration-ms): the session
//     force-checkpoints on expiry, durably saves the c1z, and exits; the
//     next session — a fresh process, like the next task in production —
//     resumes from the artifact.
//   - SIGKILLs armed at varying offsets: some land mid-sync, some inside
//     the end-of-session save itself. Nothing gets to react; the next
//     session must fall back to whatever artifact survived, uncorrupted.
//
// The in-repo suites (checkpoint cut enumeration, resume adversary,
// scheduler soak) interrupt the syncer in-process at points the harness can
// name; none of them exercise a real process boundary. Per
// docs/BUG_CATCHING.md §1 the invariant under interruption is
// content-completeness of the surviving artifact, not any particular resume
// behavior — so the oracle is ID sets against an uninterrupted baseline of
// the same deterministic connector, not counts or error text.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// Dataset flags passed to the binary and the totals they imply (the
// connector is deterministic, so the totals are exact). usersPerGroup is
// restated from main.go, which is excluded from this compilation by its
// build tag.
const (
	harnessUsers         = 3000
	harnessGroups        = 150
	harnessUsersPerGroup = 20
	// Stretches every paginated connector call so sessions are
	// interruptible mid-action and the sequential cells outlive
	// minCheckpointInterval, exercising the production checkpoint cadence.
	harnessPageDelayMs = 25

	wantResources = harnessUsers + harnessGroups
	wantEnts      = harnessGroups
	wantGrants    = harnessGroups * harnessUsersPerGroup

	// maxSessions bounds the crash loop. The slowest cell runs ~15s of
	// connector time — call it eight budget sessions — and at most half the
	// sessions are killed, so 40 leaves generous headroom.
	maxSessions = 40
)

// killFractions are the SIGKILL arming delays as fractions of the cell's
// session budget, cycled over killed sessions. They land early-sync,
// mid-sync, and in or after the end-of-session checkpoint save — the last is
// the window where a hard death races the artifact rewrite itself.
var killFractions = []float64{0.35, 0.75, 1.05, 1.3}

type cellConfig struct {
	engine  string
	workers int
	mode    string
	// budgetMs bounds each crash-loop session, mirroring production's
	// budget-bounded connector tasks. Sized per cell so every history
	// spans enough sessions to satisfy the vacuity quotas below: parallel
	// cells sync in a few seconds, the sequential cell in ~15s.
	budgetMs int
}

func repoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	require.NoError(t, err)
	return root
}

func buildHarness(t *testing.T, out string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", "build", "-tags", "crashharness", "-o", out, "./cmd/baton-crash-harness")
	cmd.Dir = repoRoot(t)
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "build harness:\n%s", output)
}

// TestCrashHarnessBuildsAgainstHead is the ungated compile gate: the harness
// binary must always build against the current tree.
func TestCrashHarnessBuildsAgainstHead(t *testing.T) {
	buildHarness(t, filepath.Join(t.TempDir(), "baton-crash-harness"))
}

func TestCrashResumeRealConnector(t *testing.T) {
	if os.Getenv("BATON_DEMO_CRASH") == "" {
		t.Skip("real-binary crash/resume instrument; set BATON_DEMO_CRASH=1 (or run `make crash-check`)")
	}
	if runtime.GOOS == "windows" {
		t.Skip("the interruption schedule relies on unix signal semantics")
	}
	tmp := t.TempDir()
	bin := filepath.Join(tmp, "baton-crash-harness")
	buildHarness(t, bin)

	cells := []cellConfig{
		{engine: "sqlite", workers: 4, budgetMs: 1000},
		{engine: "pebble", workers: 4, budgetMs: 500},
		{engine: "pebble", workers: 0, budgetMs: 2000},
	}
	for _, cell := range cells {
		t.Run(fmt.Sprintf("%s_workers_%d", cell.engine, cell.workers), func(t *testing.T) {
			cellTmp := t.TempDir()

			// Uninterrupted baseline contents. The connector is
			// deterministic, so the baseline is exact — this doubles as the
			// vacuity guard on the dataset flags.
			baselinePath := filepath.Join(cellTmp, "baseline.c1z")
			res := runSession(t, bin, cell, cellTmp, baselinePath, harnessPageDelayMs, 0, 0)
			require.NoError(t, res.err, "baseline sync failed:\n%s", res.output)
			require.True(t, res.result.Complete, "baseline must complete:\n%s", res.output)
			baseline := snapshotStore(t, baselinePath, cellTmp)
			require.Zero(t, baseline.unfinishedRuns, "baseline must seal its sync")
			require.Len(t, baseline.resources, wantResources)
			require.Len(t, baseline.entitlements, wantEnts)
			require.Len(t, baseline.grants, wantGrants)

			// Interruption loop: budget-bounded sessions, with a SIGKILL
			// armed on every second session.
			crashPath := filepath.Join(cellTmp, "crash.c1z")
			completed := false
			budgetSessions := 0
			killedSessions := 0
			sawUnfinished := false
			sawCheckpointToken := false
			for i := 0; i < maxSessions; i++ {
				killAfter := time.Duration(0)
				if i%2 == 1 {
					frac := killFractions[(i/2)%len(killFractions)]
					killAfter = time.Duration(frac * float64(cell.budgetMs) * float64(time.Millisecond))
				}
				res := runSession(t, bin, cell, cellTmp, crashPath, harnessPageDelayMs, cell.budgetMs, killAfter)
				switch {
				case res.killed:
					killedSessions++
				case res.err != nil:
					t.Fatalf("session %d failed:\n%s", i, res.output)
				case res.result.Complete:
					completed = true
				case res.result.NotComplete:
					budgetSessions++
				default:
					t.Fatalf("session %d reported neither complete nor not_complete:\n%s", i, res.output)
				}
				if completed {
					break
				}
				if unfinished, token, ok := tryInspect(crashPath, cellTmp); ok && unfinished > 0 {
					sawUnfinished = true
					if token {
						sawCheckpointToken = true
					}
				}
			}
			require.True(t, completed, "sync did not complete within %d sessions", maxSessions)
			// Meta-assertions against a vacuous harness: the history must
			// actually contain budget expiries, hard kills, and durable
			// mid-flight checkpoints for resumes to pick up. If these fire,
			// grow the dataset, the page delay, or the schedule.
			require.GreaterOrEqual(t, budgetSessions, 2, "history contains too few budget-expired sessions")
			require.GreaterOrEqual(t, killedSessions, 2, "history contains too few killed sessions")
			require.True(t, sawUnfinished, "no session left a durable unfinished sync; the harness never exercised resume")
			require.True(t, sawCheckpointToken, "no durable unfinished sync carried a checkpoint token; resumes always restarted from scratch")

			final := snapshotStore(t, crashPath, cellTmp)
			require.Zero(t, final.unfinishedRuns, "resumed store must seal its sync")
			requireSameSet(t, "resources", baseline.resources, final.resources)
			requireSameSet(t, "entitlements", baseline.entitlements, final.entitlements)
			requireSameSet(t, "grants", baseline.grants, final.grants)
		})
	}
}

func TestChaosLifecycleRealProcessCalibration(t *testing.T) {
	if os.Getenv("BATON_DEMO_CRASH") == "" {
		t.Skip("real-binary chaos calibration; set BATON_DEMO_CRASH=1 (or run `make crash-check`)")
	}
	if runtime.GOOS == "windows" {
		t.Skip("the interruption schedule relies on unix signal semantics")
	}
	tmp := t.TempDir()
	bin := filepath.Join(tmp, "baton-crash-harness")
	buildHarness(t, bin)
	cell := cellConfig{
		engine:  "pebble",
		workers: 1,
		mode:    "chaos-lifecycle-retain",
	}
	cellTmp := t.TempDir()

	baselinePath := filepath.Join(cellTmp, "chaos-baseline.c1z")
	baselineResult := runSession(t, bin, cell, cellTmp, baselinePath, 0, 0, 0)
	require.NoError(t, baselineResult.err, "baseline chaos sync failed:\n%s", baselineResult.output)
	require.True(t, baselineResult.result.Complete)
	baseline := snapshotStore(t, baselinePath, cellTmp)
	require.Contains(t, baseline.entitlements, "lifecycle-dangling")

	crashPath := filepath.Join(cellTmp, "chaos-crash.c1z")
	budgetResult := runSession(t, bin, cell, cellTmp, crashPath, 2000, 750, 0)
	require.NoError(t, budgetResult.err, "budget session failed:\n%s", budgetResult.output)
	require.True(t, budgetResult.result.NotComplete)
	unfinished := snapshotStore(t, crashPath, cellTmp)
	require.Positive(t, unfinished.unfinishedRuns)
	require.Contains(t, unfinished.entitlements, "lifecycle-dangling",
		"retained row did not land before the interrupted page")

	killResult := runSession(t, bin, cell, cellTmp, crashPath, 2000, 0, 500*time.Millisecond)
	require.True(t, killResult.killed, "SIGKILL did not land:\n%s", killResult.output)

	finalResult := runSession(t, bin, cell, cellTmp, crashPath, 0, 0, 0)
	require.NoError(t, finalResult.err, "final chaos resume failed:\n%s", finalResult.output)
	require.True(t, finalResult.result.Complete)
	final := snapshotStore(t, crashPath, cellTmp)
	require.Zero(t, final.unfinishedRuns)
	requireSameSet(t, "resources", baseline.resources, final.resources)
	requireSameSet(t, "entitlements", baseline.entitlements, final.entitlements)
	requireSameSet(t, "grants", baseline.grants, final.grants)
}

// sessionOutcome mirrors main.go's HARNESS_RESULT json (a distinct type so
// the package also compiles when the crashharness build tag pulls in both
// files, e.g. under the linter).
type sessionOutcome struct {
	Complete    bool   `json:"complete"`
	NotComplete bool   `json:"not_complete"`
	SyncErr     string `json:"sync_err"`
}

type runResult struct {
	result sessionOutcome
	killed bool
	err    error
	output string
}

// runSession executes one real sync session of the harness binary.
// killAfter > 0 arms a SIGKILL; runDurationMs > 0 bounds the session budget.
func runSession(
	t *testing.T,
	bin string,
	cell cellConfig,
	cellTmp string,
	c1zPath string,
	pageDelayMs int,
	runDurationMs int,
	killAfter time.Duration,
) runResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	args := []string{
		"-c1z", c1zPath,
		"-users", strconv.Itoa(harnessUsers),
		"-groups", strconv.Itoa(harnessGroups),
		"-page-delay-ms", strconv.Itoa(pageDelayMs),
		"-run-duration-ms", strconv.Itoa(runDurationMs),
	}
	if cell.mode != "" {
		args = append(args, "-mode", cell.mode)
	}
	cmd := exec.CommandContext(ctx, bin, args...) // #nosec G204 -- bin is built by this test into its private TempDir.
	cmd.Dir = cellTmp
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	cmd.Env = append(os.Environ(),
		"TMPDIR="+cellTmp,
		"BATON_STORAGE_ENGINE="+cell.engine,
		"BATON_WORKERS="+strconv.Itoa(cell.workers),
	)
	require.NoError(t, cmd.Start())
	var fired atomic.Bool
	var timer *time.Timer
	if killAfter > 0 {
		timer = time.AfterFunc(killAfter, func() {
			fired.Store(true)
			_ = cmd.Process.Kill() // ignore error: the process may already have exited
		})
	}
	err := cmd.Wait()
	if timer != nil {
		timer.Stop()
	}
	res := runResult{err: err, output: out.String()}
	if err != nil && fired.Load() {
		// Only a signaled death counts as killed: a session that finished
		// before the kill landed completed on its own.
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			if ws, ok := exitErr.Sys().(syscall.WaitStatus); ok && ws.Signaled() && ws.Signal() == syscall.SIGKILL {
				res.killed = true
				res.err = nil
			}
		}
	}
	if !res.killed && res.err == nil {
		for _, line := range strings.Split(res.output, "\n") {
			if strings.HasPrefix(line, "HARNESS_RESULT ") {
				res.err = json.Unmarshal([]byte(strings.TrimPrefix(line, "HARNESS_RESULT ")), &res.result)
			}
		}
	}
	return res
}

type storeSnapshot struct {
	resources      []string
	entitlements   []string
	grants         []string
	unfinishedRuns int
}

// syncRunLister is the store surface run inspection needs.
type syncRunLister interface {
	ListSyncRuns(ctx context.Context, pageToken string, pageSize uint32) ([]*c1zstore.SyncRun, string, error)
}

func snapshotStore(t *testing.T, c1zPath, tmpDir string) storeSnapshot {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	store, err := dotc1z.NewStore(ctx, c1zPath, dotc1z.WithTmpDir(tmpDir))
	require.NoError(t, err, "open %s", c1zPath)
	defer func() { _ = store.Close(ctx) }()

	snap := storeSnapshot{}
	unfinished, _, err := inspectRuns(ctx, store)
	require.NoError(t, err)
	snap.unfinishedRuns = unfinished

	pageToken := ""
	for {
		resp, err := store.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{PageToken: pageToken}.Build())
		require.NoError(t, err)
		for _, r := range resp.GetList() {
			snap.resources = append(snap.resources, r.GetId().GetResourceType()+"|"+r.GetId().GetResource())
		}
		if pageToken = resp.GetNextPageToken(); pageToken == "" {
			break
		}
	}
	for {
		resp, err := store.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{PageToken: pageToken}.Build())
		require.NoError(t, err)
		for _, e := range resp.GetList() {
			snap.entitlements = append(snap.entitlements, e.GetId())
		}
		if pageToken = resp.GetNextPageToken(); pageToken == "" {
			break
		}
	}
	for {
		resp, err := store.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{PageToken: pageToken}.Build())
		require.NoError(t, err)
		for _, g := range resp.GetList() {
			snap.grants = append(snap.grants, g.GetId())
		}
		if pageToken = resp.GetNextPageToken(); pageToken == "" {
			break
		}
	}
	slices.Sort(snap.resources)
	slices.Sort(snap.entitlements)
	slices.Sort(snap.grants)
	return snap
}

// inspectRuns reports the number of unfinished sync runs and whether any of
// them carries a non-empty checkpoint token.
func inspectRuns(ctx context.Context, store c1zstore.Store) (int, bool, error) {
	lister, ok := store.(syncRunLister)
	if !ok {
		return 0, false, errors.New("store does not implement ListSyncRuns")
	}
	unfinished := 0
	tokenSeen := false
	pageToken := ""
	for {
		runs, next, err := lister.ListSyncRuns(ctx, pageToken, 100)
		if err != nil {
			return 0, false, err
		}
		for _, run := range runs {
			if run.EndedAt == nil {
				unfinished++
				if run.SyncToken != "" {
					tokenSeen = true
				}
			}
		}
		if next == "" {
			return unfinished, tokenSeen, nil
		}
		pageToken = next
	}
}

// tryInspect opens a possibly-partial artifact between sessions. Failure to
// open is not an error here — the artifact may not have been saved yet — it
// just contributes no evidence; the resuming binary and the final
// snapshotStore are the authorities on openability.
func tryInspect(c1zPath, tmpDir string) (int, bool, bool) {
	if _, err := os.Stat(c1zPath); err != nil {
		return 0, false, false
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	store, err := dotc1z.NewStore(ctx, c1zPath, dotc1z.WithTmpDir(tmpDir))
	if err != nil {
		return 0, false, false
	}
	defer func() { _ = store.Close(ctx) }()
	unfinished, tokenSeen, err := inspectRuns(ctx, store)
	if err != nil {
		return 0, false, false
	}
	return unfinished, tokenSeen, true
}

// requireSameSet fails with a bounded diff instead of dumping two multi-
// thousand-element slices.
func requireSameSet(t *testing.T, kind string, baseline, final []string) {
	t.Helper()
	if slices.Equal(baseline, final) {
		return
	}
	baseSet := make(map[string]struct{}, len(baseline))
	for _, v := range baseline {
		baseSet[v] = struct{}{}
	}
	finalSet := make(map[string]struct{}, len(final))
	for _, v := range final {
		finalSet[v] = struct{}{}
	}
	var missing, extra []string
	for _, v := range baseline {
		if _, ok := finalSet[v]; !ok {
			missing = append(missing, v)
		}
	}
	for _, v := range final {
		if _, ok := baseSet[v]; !ok {
			extra = append(extra, v)
		}
	}
	const maxShown = 10
	if len(missing) > maxShown {
		missing = append(missing[:maxShown], fmt.Sprintf("... and %d more", len(missing)-maxShown))
	}
	if len(extra) > maxShown {
		extra = append(extra[:maxShown], fmt.Sprintf("... and %d more", len(extra)-maxShown))
	}
	t.Fatalf("%s diverged after interrupted resume: baseline=%d final=%d\nmissing from final: %v\nunexpected in final: %v",
		kind, len(baseline), len(final), missing, extra)
}
