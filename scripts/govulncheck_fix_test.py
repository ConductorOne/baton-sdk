"""Tests for scripts/govulncheck-fix.sh.

Runs the script with stubbed `govulncheck`, `go` and `git` on PATH, so each
branch is exercised without a scan or a module download: a clean scan,
report-only findings, several fixes resolved in one `go get`, semver ordering
of fixed versions, standard-library findings raising the go directive, the
companion retry after a build failure, the downgrade guard, and the
workflow's checkout and close-out conditions. Standard library
only; run with `python3 scripts/govulncheck_fix_test.py`. The govulncheck
workflow runs it before each scan; it is not part of the pull request CI.
"""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


SCRIPT = Path(__file__).with_name("govulncheck-fix.sh").resolve()
WORKFLOW = SCRIPT.parents[1] / ".github/workflows/govulncheck.yaml"


def finding(module, fixed="v1.3.0", function="Vulnerable"):
    frame = {"module": module, "version": "v1.0.0"}
    if function is not None:
        frame["function"] = function
    return {"finding": {"trace": [frame], "fixed_version": fixed, "osv": "GO-TEST"}}


class GovulncheckFixTest(unittest.TestCase):
    def run_fix(self, findings, fail_scan=False, fail_build=False, companion=False, downgrade=False, companion_module="example.com/bridge", go_line="1.27.1", dependency_go=""):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        root = Path(directory.name)
        binary = root / "bin"
        binary.mkdir()
        (root / "gomod.json").write_text(json.dumps({"Go": go_line}))
        (root / "scan.json").write_text("\n".join(map(json.dumps, findings)))
        (binary / "govulncheck").write_text(
            '#!/bin/sh\ncat "$FIX_TEST_ROOT/scan.json"\nexit "${FAIL_SCAN:-0}"\n'
        )
        (binary / "git").write_text(
            '#!/bin/sh\nif [ -f "$FIX_TEST_ROOT/changed" ]; then echo " M go.mod"; fi\n'
        )
        (binary / "go").write_text('''#!/bin/sh
printf '%s\n' "$*" >> "$FIX_TEST_ROOT/go-calls"
case "$1" in
  list)
    case "$*" in
      *Module*) echo "$COMPANION_MODULE" ;;
      'list -m') echo example.com/sdk ;;
      *)
        if [ -f "$FIX_TEST_ROOT/resolved" ]; then
          cat "$FIX_TEST_ROOT/resolved"
        else
          echo v1.0.0
        fi ;;
    esac ;;
  mod)
    case "$2" in
      edit)
        case "$3" in
          -json) cat "$FIX_TEST_ROOT/gomod.json" ;;
          -go=*)
            printf '{"Go":"%s"}\n' "${3#-go=}" > "$FIX_TEST_ROOT/gomod.json"
            touch "$FIX_TEST_ROOT/changed" ;;
        esac ;;

    esac ;;
  get)
    [ "$#" -gt 1 ] || exit 1
    if [ -n "$DEPENDENCY_GO" ]; then
      printf '{"Go":"%s"}\n' "$DEPENDENCY_GO" > "$FIX_TEST_ROOT/gomod.json"
    fi
    touch "$FIX_TEST_ROOT/changed"
    case "$*" in
      *@latest*)
        case "$*" in *a@v*) echo 'conflicting version constraints' >&2; exit 1 ;; esac
        echo "$COMPANION_VERSION" > "$FIX_TEST_ROOT/resolved" ;;
      *)
        for target in "$@"; do
          case "$target" in
            go@*) echo "explicit go version rejected" >&2; exit 1 ;;
            *@*) printf '%s\n' "${target##*@}" > "$FIX_TEST_ROOT/resolved" ;;
          esac
        done ;;
    esac ;;
  build)
    if [ "$COMPANION" = 1 ] && [ ! -f "$FIX_TEST_ROOT/built" ]; then
      touch "$FIX_TEST_ROOT/built"
      echo '# example.com/bridge/pkg' >&2
      exit 1
    fi
    exit "${FAIL_BUILD:-0}" ;;
esac
''')
        for path in binary.iterdir():
            path.chmod(0o700)
        environment = dict(os.environ, PATH=str(binary) + os.pathsep + os.environ["PATH"],
                           FIX_TEST_ROOT=str(root), GOVULN_OUT=str(root / "out"),
                           GITHUB_OUTPUT=str(root / "outputs"), GITHUB_STEP_SUMMARY=str(root / "summary"),
                           FAIL_SCAN="1" if fail_scan else "0", FAIL_BUILD="1" if fail_build else "0",
                           COMPANION="1" if companion else "0",
                           COMPANION_VERSION="v1.0.0" if downgrade else "v1.4.0",
                           COMPANION_MODULE=companion_module,
                           DEPENDENCY_GO=dependency_go)
        result = subprocess.run(["bash", str(SCRIPT)], cwd=root, env=environment,
                                text=True, capture_output=True)
        return root, result

    def test_clean_scan(self):
        root, result = self.run_fix([])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("needed=false", (root / "outputs").read_text())
        self.assertIn("changed=false", (root / "outputs").read_text())

    def test_report_only_findings_still_release_a_stale_pr(self):
        # Nothing to bump means an open fix pull request has nothing left to
        # deliver, even though the scan is not clean.
        root, result = self.run_fix([finding("example.com/unfixed", "")])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("needed=false", (root / "outputs").read_text())
        self.assertIn("changed=false", (root / "outputs").read_text())

    def test_stdlib_finding_raises_the_go_directive(self):
        root, result = self.run_fix([finding("stdlib", "v1.27.2"), finding("stdlib", "v1.27.3")])
        self.assertEqual(result.returncode, 0, result.stderr)
        gets = [line for line in (root / "go-calls").read_text().splitlines() if line.startswith("get ")]
        self.assertEqual(gets, [])
        self.assertEqual(json.loads((root / "gomod.json").read_text())["Go"], "1.27.3")
        self.assertIn("changed=true", (root / "outputs").read_text())
        # A run that opens or refreshes the fix PR must not also close it.
        self.assertIn("needed=true", (root / "outputs").read_text())
        self.assertIn("| `go` directive (standard library) | 1.27.1 | 1.27.3 |", (root / "summary").read_text())

    def test_stdlib_fix_already_covered_by_go_directive_is_reported_only(self):
        root, result = self.run_fix([finding("stdlib", "v1.27.1")])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn("get ", (root / "go-calls").read_text())
        self.assertIn("changed=false", (root / "outputs").read_text())
        self.assertIn("needed=false", (root / "outputs").read_text())
        self.assertIn("already covered by the go directive", (root / "summary").read_text())

    def test_stdlib_fix_compares_against_the_go_directive(self):
        for go_line, changed in [("1.27.1", True), ("1.27", True), ("1.27.4", False), ("1.28.0", False)]:
            with self.subTest(go_line=go_line):
                root, result = self.run_fix([finding("stdlib", "v1.27.3")], go_line=go_line)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertNotIn("jq: error", result.stderr)
                self.assertIn("changed=" + str(changed).lower(), (root / "outputs").read_text())

    def test_stdlib_fix_forms_normalize_to_a_go_line(self):
        for fixed in ["v1.27.3", "go1.27.3", "1.27.3"]:
            with self.subTest(fixed=fixed):
                root, result = self.run_fix([finding("stdlib", fixed)])
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(json.loads((root / "gomod.json").read_text())["Go"], "1.27.3")

    def test_stdlib_fix_preserves_a_higher_dependency_go_floor(self):
        root, result = self.run_fix([finding("stdlib", "v1.27.3"), finding("example.com/a")],
                                    dependency_go="1.28.0")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads((root / "gomod.json").read_text())["Go"], "1.28.0")
        self.assertIn("| `go` directive (standard library) | 1.27.1 | 1.28.0 |", (root / "summary").read_text())

    def test_stdlib_without_a_fix_is_not_reported_as_fixed(self):
        for fixed in [None, ""]:
            with self.subTest(fixed=fixed):
                root, result = self.run_fix([finding("stdlib", fixed)])
                self.assertEqual(result.returncode, 0, result.stderr)
                body = (root / "summary").read_text()
                self.assertIn("no fixed version yet", body)
                self.assertNotIn("already covered", body)
                self.assertIn("[GO-TEST]", body)

    def test_fixed_targets_are_resolved_together(self):
        root, result = self.run_fix([finding("example.com/a"), finding("example.com/b")])
        self.assertEqual(result.returncode, 0, result.stderr)
        gets = [line for line in (root / "go-calls").read_text().splitlines() if line.startswith("get ")]
        self.assertEqual(gets, ["get example.com/a@v1.3.0 example.com/b@v1.3.0"])
        self.assertIn("changed=true", (root / "outputs").read_text())
        self.assertIn("needed=true", (root / "outputs").read_text())

    def test_highest_fix_uses_semver_order(self):
        for versions, expected in [
            (["v1.2.3", "v1.2.3-rc.1"], "v1.2.3"),
            (["v1.2.3", "v1.2.3-0.20260101000000-abcdef"], "v1.2.3"),
            (["v1.2.3-rc.2", "v1.2.3-rc.10"], "v1.2.3-rc.10"),
            (["v1.9.0", "v1.10.0"], "v1.10.0"),
        ]:
            with self.subTest(versions=versions):
                root, result = self.run_fix([finding("example.com/a", version) for version in versions])
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn("get example.com/a@" + expected + "\n", (root / "go-calls").read_text())

    def test_scan_failure_does_not_emit_success_outputs(self):
        root, result = self.run_fix([], fail_scan=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse((root / "outputs").exists())

    def test_companion_retry_preserves_fixed_targets(self):
        root, result = self.run_fix([finding("example.com/a")], companion=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        calls = (root / "go-calls").read_text()
        self.assertIn("get example.com/bridge@latest\n", calls)
        self.assertEqual(calls.count("build ./...\n"), 2)
        self.assertIn("changed=true", (root / "outputs").read_text())

    def test_companion_can_be_an_original_fix_target(self):
        root, result = self.run_fix([finding("example.com/a")], companion=True,
                                    companion_module="example.com/a")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("get example.com/a@latest\n", (root / "go-calls").read_text())
        self.assertIn("changed=true", (root / "outputs").read_text())

    def test_companion_cannot_downgrade_a_fixed_module(self):
        root, result = self.run_fix([finding("example.com/a")], companion=True, downgrade=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("below required fix", result.stderr)
        self.assertFalse((root / "outputs").exists())

    def test_build_failure_does_not_offer_pr(self):
        root, result = self.run_fix([finding("example.com/a")], fail_build=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("changed=false", (root / "outputs").read_text())

    def test_workflow_scans_main_and_closes_when_nothing_is_needed(self):
        workflow = WORKFLOW.read_text()
        checkout = workflow.split("- name: Checkout code", 1)[1].split("- name:", 1)[0]
        close = workflow.split("- name: Close a stale fix pull request", 1)[1]
        self.assertIn("ref: main", checkout)
        self.assertIn("if: steps.fix.outputs.needed == 'false'", close)


if __name__ == "__main__":
    unittest.main()
