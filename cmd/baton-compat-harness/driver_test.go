// Driver for the two-artifact checkpoint compatibility harness (main.go).
//
// TestCompatHarnessBuildsAgainstHead always runs: it is the compile gate
// that keeps the tag-gated harness from rotting against HEAD.
//
// TestCheckpointCompatAcrossSDKVersions is the instrument itself and needs
// a second SDK checkout, so it is gated behind BATON_COMPAT=1 (run it via
// `make compat-check`). It materializes a pinned past release with `git
// worktree`, compiles the SAME harness source against both trees, and runs
// every (gen version × resume version) cell:
//
//	new→new  self-equivalence baseline
//	old→old  old-binary baseline (also validates the harness under old)
//	old→new  backward: HEAD must resume an old checkpoint to a complete store
//	new→old  forward: the old release meets a version-stamped type-scoped
//	         checkpoint; whatever it does (refuse, restart), the sealed
//	         store must be content-complete — silently sealing the
//	         incomplete resume is the worst bug on this repo's record
//
// The oracle is counted rows against the connector's known topology, in
// every cell — not error strings.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The connector topology in main.go: numGroups groups + 1 user, one
// entitlement and one grant per group.
const (
	wantResources = numGroupsForDriver + 1
	wantEnts      = numGroupsForDriver
	wantGrants    = numGroupsForDriver
	// Kept in lockstep with numGroups in main.go; main.go is excluded from
	// this compilation by its build tag, so the constant is restated here.
	numGroupsForDriver = 12
)

func repoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	require.NoError(t, err)
	return root
}

func buildHarness(t *testing.T, tree, out string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", "build", "-tags", "compatharness", "-o", out, "./cmd/baton-compat-harness")
	cmd.Dir = tree
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "build harness in %s:\n%s", tree, output)
}

// TestCompatHarnessBuildsAgainstHead is the ungated compile gate: the
// harness source must always build against the current tree.
func TestCompatHarnessBuildsAgainstHead(t *testing.T) {
	buildHarness(t, repoRoot(t), filepath.Join(t.TempDir(), "harness"))
}

func runHarness(t *testing.T, bin, mode, c1zPath string) compatDriverResult {
	return runHarnessOut(t, bin, mode, c1zPath, "")
}

func runHarnessOut(t *testing.T, bin, mode, c1zPath, outPath string) compatDriverResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	args := []string{"-mode", mode, "-c1z", c1zPath}
	if outPath != "" {
		args = append(args, "-out", outPath)
	}
	cmd := exec.CommandContext(ctx, bin, args...)
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s -mode %s:\n%s", bin, mode, output)

	var line string
	for _, l := range strings.Split(string(output), "\n") {
		if strings.HasPrefix(l, "COMPAT_RESULT ") {
			line = strings.TrimPrefix(l, "COMPAT_RESULT ")
		}
	}
	require.NotEmpty(t, line, "no COMPAT_RESULT line in output:\n%s", output)
	var result compatDriverResult
	require.NoError(t, json.Unmarshal([]byte(line), &result))
	return result
}

// compatDriverResult mirrors compatResult in main.go (excluded from this
// compilation by its build tag).
type compatDriverResult struct {
	Mode               string `json:"mode"`
	SyncErr            string `json:"sync_err"`
	NotComplete        bool   `json:"not_complete"`
	Resources          int    `json:"resources"`
	Ents               int    `json:"entitlements"`
	Grants             int    `json:"grants"`
	CountErr           string `json:"count_err"`
	UnfinishedRuns     int    `json:"unfinished_runs"`
	TokenLen           int    `json:"token_len"`
	TokenSpawned       bool   `json:"token_spawned"`
	TokenTypeScoped    bool   `json:"token_type_scoped"`
	GraphPresent       bool   `json:"graph_present"`
	GraphReusable      bool   `json:"graph_reusable"`
	GraphErr           string `json:"graph_err"`
	IncrementalRan     bool   `json:"incremental_ran"`
	IncrementalOutcome string `json:"incremental_outcome"`
	IncrementalReason  string `json:"incremental_reason"`
	IncrementalError   string `json:"incremental_error"`
	ArtifactPath       string `json:"artifact_path"`
	AllocatedBytes     uint64 `json:"allocated_bytes"`
	LogicalDigest      string `json:"logical_digest"`
}

func TestDefaultPathPerformanceAgainstPinnedMain(t *testing.T) {
	if os.Getenv("BATON_GRAPH_COMPAT") == "" {
		t.Skip("pinned-main performance ratchet; set BATON_GRAPH_COMPAT=1")
	}
	mainRef := os.Getenv("BATON_GRAPH_COMPAT_MAIN_REF")
	if mainRef == "" {
		mainRef = "10a6da053799febd092bfffeb7c5c6ec195dca0c"
	}
	root := repoRoot(t)
	tmp := t.TempDir()
	mainTree := filepath.Join(tmp, "main-tree")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	output, err := exec.CommandContext(ctx, "git", "-C", root, "worktree", "add", "--detach", mainTree, mainRef).CombinedOutput() // #nosec G702 -- local test ref and TempDir paths only.
	require.NoError(t, err, "git worktree add %s:\n%s", mainRef, output)
	t.Cleanup(func() {
		rmCtx, rmCancel := context.WithTimeout(context.Background(), time.Minute)
		defer rmCancel()
		_, _ = exec.CommandContext(rmCtx, "git", "-C", root, "worktree", "remove", "--force", mainTree).CombinedOutput()
	})
	src, err := os.ReadFile(filepath.Join(root, "cmd", "baton-compat-harness", "main.go"))
	require.NoError(t, err)
	mainHarnessDir := filepath.Join(mainTree, "cmd", "baton-compat-harness")
	require.NoError(t, os.MkdirAll(mainHarnessDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(mainHarnessDir, "main.go"), src, 0o600)) // #nosec G703 -- destination is inside the test TempDir worktree.

	candidateBin := filepath.Join(tmp, "candidate")
	mainBin := filepath.Join(tmp, "main")
	buildHarness(t, root, candidateBin)
	buildHarness(t, mainTree, mainBin)
	base := filepath.Join(tmp, "base.c1z")
	baseResult := runHarness(t, mainBin, "resume", base)
	require.Empty(t, baseResult.SyncErr)

	measure := func(t *testing.T, bin, label string) (uint64, string) {
		t.Helper()
		values := make([]uint64, 0, 5)
		var logicalDigest string
		for i := 0; i < 5; i++ {
			input := filepath.Join(tmp, fmt.Sprintf("%s-input-%d.c1z", label, i))
			copyCompatArtifact(t, base, input)
			out := filepath.Join(tmp, fmt.Sprintf("%s-output-%d.c1z", label, i))
			result := runHarnessOut(t, bin, "graph-default-compact", input, out)
			require.Equal(t, wantResources, result.Resources)
			require.Equal(t, wantEnts, result.Ents)
			require.Equal(t, wantGrants, result.Grants)
			require.Positive(t, result.AllocatedBytes)
			inspection := runHarness(t, candidateBin, "graph-inspect", out)
			require.False(t, inspection.GraphPresent,
				"default flag-off compaction must not write a graph sidecar (%s run %d)", label, i)
			require.False(t, inspection.GraphReusable)
			require.NotEmpty(t, inspection.LogicalDigest)
			if logicalDigest == "" {
				logicalDigest = inspection.LogicalDigest
			} else {
				require.Equal(t, logicalDigest, inspection.LogicalDigest,
					"logical output changed across identical %s runs", label)
			}
			values = append(values, result.AllocatedBytes)
		}
		sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
		return values[len(values)/2], logicalDigest
	}
	mainAlloc, mainDigest := measure(t, mainBin, "main")
	candidateAlloc, candidateDigest := measure(t, candidateBin, "candidate")
	require.Equal(t, mainDigest, candidateDigest,
		"flag-off compaction must preserve exact logical resources, entitlements, and grants")
	require.LessOrEqual(t, candidateAlloc, mainAlloc*110/100,
		"default compaction allocation regression: candidate=%d main=%d", candidateAlloc, mainAlloc)
}

func copyCompatArtifact(t *testing.T, src, dst string) {
	t.Helper()
	data, err := os.ReadFile(src)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(dst, data, 0o600)) // #nosec G703 -- callers provide paths inside their test TempDir.
}

func compatArtifactDigest(t *testing.T, path string) [sha256.Size]byte {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return sha256.Sum256(data)
}

func TestCheckpointCompatAcrossSDKVersions(t *testing.T) {
	if os.Getenv("BATON_COMPAT") == "" {
		t.Skip("two-artifact compatibility matrix; set BATON_COMPAT=1 (or run `make compat-check`)")
	}
	oldRef := os.Getenv("BATON_COMPAT_OLD_REF")
	if oldRef == "" {
		// The last release before the type-scoped/fan-out branch.
		oldRef = "v0.20.2"
	}
	root := repoRoot(t)
	tmp := t.TempDir()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Materialize the old tree. worktree (vs. clone) shares objects and
	// works offline; --detach avoids claiming the ref.
	oldTree := filepath.Join(tmp, "old-tree")
	wtAdd := exec.CommandContext(ctx, "git", "-C", root, "worktree", "add", "--detach", oldTree, oldRef) // #nosec G702 -- local test ref and TempDir paths only.
	output, err := wtAdd.CombinedOutput()
	require.NoError(t, err, "git worktree add %s:\n%s", oldRef, output)
	t.Cleanup(func() {
		rmCtx, rmCancel := context.WithTimeout(context.Background(), time.Minute)
		defer rmCancel()
		out, rmErr := exec.CommandContext(rmCtx, "git", "-C", root, "worktree", "remove", "--force", oldTree).CombinedOutput()
		if rmErr != nil {
			t.Logf("git worktree remove: %v\n%s", rmErr, out)
		}
	})

	// The same source compiles against both trees: copy it into the old
	// checkout (the old release predates the harness).
	src, err := os.ReadFile(filepath.Join(root, "cmd", "baton-compat-harness", "main.go"))
	require.NoError(t, err)
	oldHarnessDir := filepath.Join(oldTree, "cmd", "baton-compat-harness")
	require.NoError(t, os.MkdirAll(oldHarnessDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(oldHarnessDir, "main.go"), src, 0o600)) // #nosec G703 -- destination is inside the test TempDir worktree.

	newBin := filepath.Join(tmp, "harness-new")
	oldBin := filepath.Join(tmp, "harness-old")
	buildHarness(t, root, newBin)
	buildHarness(t, oldTree, oldBin)

	cells := []struct {
		name       string
		gen        string
		resumeWith string
	}{
		{"new_gen_new_resume", newBin, newBin},
		{"old_gen_old_resume", oldBin, oldBin},
		{"old_gen_new_resume", oldBin, newBin},
		{"new_gen_old_resume", newBin, oldBin},
	}
	for _, cell := range cells {
		t.Run(cell.name, func(t *testing.T) {
			c1zPath := filepath.Join(t.TempDir(), "compat.c1z")

			gen := runHarness(t, cell.gen, "gen", c1zPath)
			require.True(t, gen.NotComplete,
				"gen must be interrupted mid-flight (got sync_err=%q)", gen.SyncErr)
			require.GreaterOrEqual(t, gen.UnfinishedRuns, 1,
				"gen must leave an unfinished sync run")
			require.Positive(t, gen.TokenLen, "gen checkpoint token must be non-empty")
			if cell.gen == newBin {
				// Meta-assertion against a vacuous harness: the new
				// binary's checkpoint must actually carry the adversarial
				// state the exchange exists to test.
				require.True(t, gen.TokenSpawned,
					"new-binary gen checkpoint must hold spawned cursors")
				require.True(t, gen.TokenTypeScoped,
					"new-binary gen checkpoint must hold type-scoped actions")
			}

			resume := runHarness(t, cell.resumeWith, "resume", c1zPath)
			require.Empty(t, resume.SyncErr, "resume must complete")
			require.Empty(t, resume.CountErr)
			require.Zero(t, resume.UnfinishedRuns, "resume must seal the sync")
			require.Equal(t, wantResources, resume.Resources, "resource count")
			require.Equal(t, wantEnts, resume.Ents, "entitlement count")
			require.Equal(t, wantGrants, resume.Grants, "grant count")
		})
	}
}

func TestGraphReuseCompatAcrossSDKVersions(t *testing.T) {
	if os.Getenv("BATON_GRAPH_COMPAT") == "" {
		t.Skip("graph-sidecar compatibility matrix; set BATON_GRAPH_COMPAT=1")
	}
	oldRef := os.Getenv("BATON_GRAPH_COMPAT_OLD_REF")
	if oldRef == "" {
		oldRef = "v0.20.6"
	}
	root := repoRoot(t)
	tmp := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	oldTree := filepath.Join(tmp, "old-tree")
	wtAdd := exec.CommandContext(ctx, "git", "-C", root, "worktree", "add", "--detach", oldTree, oldRef) // #nosec G702 -- local test ref and TempDir paths only.
	output, err := wtAdd.CombinedOutput()
	require.NoError(t, err, "git worktree add %s:\n%s", oldRef, output)
	t.Cleanup(func() {
		rmCtx, rmCancel := context.WithTimeout(context.Background(), time.Minute)
		defer rmCancel()
		_, _ = exec.CommandContext(rmCtx, "git", "-C", root, "worktree", "remove", "--force", oldTree).CombinedOutput()
	})

	src, err := os.ReadFile(filepath.Join(root, "cmd", "baton-compat-harness", "main.go"))
	require.NoError(t, err)
	oldHarnessDir := filepath.Join(oldTree, "cmd", "baton-compat-harness")
	require.NoError(t, os.MkdirAll(oldHarnessDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(oldHarnessDir, "main.go"), src, 0o600)) // #nosec G703 -- destination is inside the test TempDir worktree.

	newBin := filepath.Join(tmp, "graph-harness-new")
	oldBin := filepath.Join(tmp, "graph-harness-old")
	buildHarness(t, root, newBin)
	buildHarness(t, oldTree, oldBin)

	requireComplete := func(t *testing.T, result compatDriverResult) {
		t.Helper()
		require.Empty(t, result.CountErr)
		require.Equal(t, wantResources, result.Resources)
		require.Equal(t, wantEnts, result.Ents)
		require.Equal(t, wantGrants, result.Grants)
	}

	// M2: old -> old baseline.
	oldBase := filepath.Join(tmp, "old-base.c1z")
	requireComplete(t, runHarness(t, oldBin, "resume", oldBase))

	// M3: old -> new. No graph exists, so the candidate must full-fallback.
	newFromOld := filepath.Join(tmp, "new-from-old.c1z")
	oldToNew := runHarnessOut(t, newBin, "graph-compact", oldBase, newFromOld)
	requireComplete(t, oldToNew)
	require.False(t, oldToNew.IncrementalRan)

	// M6: an old reader can read the candidate output.
	requireComplete(t, runHarness(t, oldBin, "graph-inspect", newFromOld))

	// Candidate seed proves the new sidecar premise before any cross-version row.
	newSeed := filepath.Join(tmp, "new-seed.c1z")
	seed := runHarness(t, newBin, "graph-seed", newSeed)
	requireComplete(t, seed)
	require.True(t, seed.GraphPresent)
	require.True(t, seed.GraphReusable)

	// M1: new -> new admits reuse and remains complete.
	newFromNew := filepath.Join(tmp, "new-from-new.c1z")
	newToNew := runHarnessOut(t, newBin, "graph-compact", newSeed, newFromNew)
	requireComplete(t, newToNew)
	require.True(t, newToNew.IncrementalRan, "valid candidate sidecar must not vacuously full-fallback: outcome=%s reason=%s error=%s",
		newToNew.IncrementalOutcome, newToNew.IncrementalReason, newToNew.IncrementalError)

	// M4: new -> old read. The unknown sidecar must not break or mutate the
	// old-visible artifact.
	before := compatArtifactDigest(t, newSeed)
	requireComplete(t, runHarness(t, oldBin, "graph-inspect", newSeed))
	require.Equal(t, before, compatArtifactDigest(t, newSeed))

	// M5/M9: new -> old fold-style full compaction -> new. The candidate
	// must not trust metadata copied or transformed by the old binary.
	roundTripInput := filepath.Join(tmp, "roundtrip-input.c1z")
	copyCompatArtifact(t, newSeed, roundTripInput)
	oldTransformed := filepath.Join(tmp, "old-transformed.c1z")
	requireComplete(t, runHarnessOut(t, oldBin, "graph-old-compact", roundTripInput, oldTransformed))
	newAfterOld := filepath.Join(tmp, "new-after-old.c1z")
	afterOld := runHarnessOut(t, newBin, "graph-compact", oldTransformed, newAfterOld)
	requireComplete(t, afterOld)
	require.False(t, afterOld.IncrementalRan, "old transformation cannot certify the candidate graph generation")

	// M7/M8: malformed, unknown-version, and foreign-sync sidecars all
	// fail closed to one complete full expansion.
	for _, mutation := range []string{"graph-corrupt", "graph-unknown-version", "graph-foreign-sync"} {
		t.Run(mutation, func(t *testing.T) {
			mutated := filepath.Join(t.TempDir(), "mutated.c1z")
			copyCompatArtifact(t, newSeed, mutated)
			mutatedState := runHarness(t, newBin, mutation, mutated)
			require.True(t, mutatedState.GraphPresent)
			require.False(t, mutatedState.GraphReusable)
			out := filepath.Join(t.TempDir(), "repaired.c1z")
			repaired := runHarnessOut(t, newBin, "graph-compact", mutated, out)
			requireComplete(t, repaired)
			require.False(t, repaired.IncrementalRan)
		})
	}
}
