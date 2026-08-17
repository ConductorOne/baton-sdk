package pebble

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// TestLockChecksCompiledIn fails any test run that was built without the
// deadlock-shape checks. The checks are a compile-time constant
// (lock_checks_enabled.go), so nothing at runtime can report that a build
// silently dropped them — except a test that is itself compiled either
// way and looks. A plain `go test ./...` failing here is working as
// intended: the write_barrier_reentry tests don't exist in that build,
// and this failure is the only sign coverage was lost.
//
// Benchmarks are the one legitimate unarmed run — the checks would land
// on the Pebble side only of every Pebble-vs-SQLite comparison — and
// `go test -bench=. -run='^$'` selects no tests, so it never trips this.
func TestLockChecksCompiledIn(t *testing.T) {
	if !writeBarrierOwnerChecks {
		t.Fatal("this binary was built without the pebble deadlock-shape checks, so the tests that " +
			"assert them were excluded too. Run tests with -tags=baton_lockchecks (what `make test` and CI do) " +
			"or -race, which arms them for free. Only benchmarks should run unarmed: use -bench with -run='^$'.")
	}
}

// TestLockChecksSuppliedByTestInvocations is the config-level half of the
// tripwire above: it fails when a whole-tree `go test ./...` invocation
// in the Makefile or a CI workflow stops supplying -tags=baton_lockchecks
// (or -race, which arms the checks by itself). The runtime tripwire makes
// a de-armed CI run fail; this one makes the de-arming visible at the
// diff that does it, in whichever armed environment still runs.
//
// The match is line-based and deliberately dumb: a line containing both
// `go test` and `./...` must also contain the tag or -race. If an
// invocation gets split across lines or moved into a variable, update
// this to follow it — the floor assertions below fail loudly if the
// pattern stops matching anything, so a restructure cannot quietly
// retire the check.
func TestLockChecksSuppliedByTestInvocations(t *testing.T) {
	root := repoRoot(t)

	var configs []string
	for _, pattern := range []string{".github/workflows/*.yaml", ".github/workflows/*.yml"} {
		matches, err := filepath.Glob(filepath.Join(root, pattern))
		if err != nil {
			t.Fatalf("globbing workflows: %v", err)
		}
		configs = append(configs, matches...)
	}
	configs = append(configs, filepath.Join(root, "Makefile"))

	race := regexp.MustCompile(`(^|[\s=])-race([\s,]|$)`)
	var violations []string
	wholeTreeInvocations := map[string]int{}
	for _, path := range configs {
		f, err := os.Open(path)
		if err != nil {
			t.Fatalf("opening %s: %v", path, err)
		}
		// Slash-normalized, because the ".github/" prefix test below is
		// the only thing keeping the workflow floor honest, and on Windows
		// filepath.Rel hands back backslashes — which would leave the
		// floor at zero and fail a run that has nothing wrong with it.
		rel, err := filepath.Rel(root, path)
		if err != nil {
			t.Fatalf("relativizing %s against %s: %v", path, root, err)
		}
		rel = filepath.ToSlash(rel)

		scanner := bufio.NewScanner(f)
		lineNo := 0
		for scanner.Scan() {
			lineNo++
			line := scanner.Text()
			if !strings.Contains(line, "go test") || !strings.Contains(line, "./...") {
				continue
			}
			wholeTreeInvocations[rel]++
			if !strings.Contains(line, "baton_lockchecks") && !race.MatchString(line) {
				violations = append(violations, fmt.Sprintf("%s:%d: %s", rel, lineNo, strings.TrimSpace(line)))
			}
		}
		if err := scanner.Err(); err != nil {
			t.Fatalf("reading %s: %v", path, err)
		}
		_ = f.Close()
	}

	if len(violations) > 0 {
		t.Fatalf("whole-tree `go test ./...` invocations missing -tags=baton_lockchecks (and not using -race), "+
			"so the pebble deadlock-shape checks and their tests are silently excluded there:\n  %s",
			strings.Join(violations, "\n  "))
	}
	// Floors: if a restructure moves the invocations out of reach of the
	// line matcher, fail here rather than pass with nothing checked.
	if wholeTreeInvocations["Makefile"] == 0 {
		t.Fatal("no whole-tree `go test ./...` line found in the Makefile — if the test target changed shape, " +
			"update this tripwire to follow it")
	}
	workflowHits := 0
	for path, n := range wholeTreeInvocations {
		if strings.HasPrefix(path, ".github/") {
			workflowHits += n
		}
	}
	if workflowHits == 0 {
		t.Fatal("no whole-tree `go test ./...` line found in any CI workflow — if the workflows changed shape, " +
			"update this tripwire to follow it")
	}
}

// repoRoot walks up from the package directory to the module root, which
// is where the Makefile and workflows live.
func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("no go.mod found walking up from the test's working directory")
		}
		dir = parent
	}
}
