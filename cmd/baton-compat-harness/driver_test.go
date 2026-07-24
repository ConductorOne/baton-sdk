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
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
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
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, "-mode", mode, "-c1z", c1zPath)
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
	Mode            string `json:"mode"`
	SyncErr         string `json:"sync_err"`
	NotComplete     bool   `json:"not_complete"`
	Resources       int    `json:"resources"`
	Ents            int    `json:"entitlements"`
	Grants          int    `json:"grants"`
	CountErr        string `json:"count_err"`
	UnfinishedRuns  int    `json:"unfinished_runs"`
	TokenLen        int    `json:"token_len"`
	TokenSpawned    bool   `json:"token_spawned"`
	TokenTypeScoped bool   `json:"token_type_scoped"`
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
	//nolint:gosec // the ref comes from the developer's own environment; this is a local test driver
	wtAdd := exec.CommandContext(ctx, "git", "-C", root, "worktree", "add", "--detach", oldTree, oldRef)
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
	//nolint:gosec // the path is rooted in this test's own TempDir worktree
	require.NoError(t, os.WriteFile(filepath.Join(oldHarnessDir, "main.go"), src, 0o600))

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
