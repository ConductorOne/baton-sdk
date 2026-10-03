import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class WindowsTestShardTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        (self.root / "go.mod").write_text("module shard-smoke\n\ngo 1.27.1\n", encoding="utf-8")
        (self.root / "coverage_test.go").write_text(
            '''package smoke
import ("flag"; "fmt"; "os"; "testing")
func TestMain(m *testing.M) {
    flag.Parse()
    if os.Getenv("DROP_TESTS") == "1" && flag.Lookup("test.list").Value.String() == "" { os.Exit(0) }
    os.Exit(m.Run())
}
func TestPass(t *testing.T) {}
func TestSkipped(t *testing.T) { t.Skip("intentional skip") }
func Example() {
    fmt.Println("example exercised")
    // Output: example exercised
}
func FuzzSeed(f *testing.F) {
    f.Add("seed")
    f.Fuzz(func(t *testing.T, s string) { if s != "seed" { t.Fatal(s) } })
}
''',
            encoding="utf-8",
        )

    def run_shard(self, drop_tests=False):
        environment = {**os.environ, "DROP_TESTS": "1" if drop_tests else "0", "GOWORK": "off"}
        return subprocess.run(
            [sys.executable, str(Path(__file__).with_name("windows-test-shard.py").resolve()), "--shard", "0", "--count", "1"],
            cwd=self.root,
            env=environment,
            capture_output=True,
            text=True,
            check=False,
        )

    def test_examples_fuzz_seeds_and_skips_are_executed(self):
        result = self.run_shard()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        events = [json.loads(line) for line in (self.root / "test.json").read_text(encoding="utf-8").splitlines()]
        outcomes = {
            e["Test"]: e["Action"]
            for e in events
            if e.get("Test") and "/" not in e["Test"] and e["Action"] in ("pass", "skip")
        }
        self.assertEqual(outcomes, {"TestPass": "pass", "TestSkipped": "skip", "Example": "pass", "FuzzSeed": "pass"})

    def test_successful_binary_that_omits_tests_fails_the_shard(self):
        result = self.run_shard(drop_tests=True)
        self.assertNotEqual(result.returncode, 0)
        manifest = json.loads((self.root / "test-shard.json").read_text(encoding="utf-8"))
        self.assertEqual({e["test"] for e in manifest["inventory"]}, {"TestPass", "TestSkipped", "Example", "FuzzSeed"})
        events = [json.loads(line) for line in (self.root / "test.json").read_text(encoding="utf-8").splitlines()]
        self.assertTrue(any(e["Action"] == "pass" and not e.get("Test") for e in events))
        self.assertFalse(any(e.get("Test") for e in events))


if __name__ == "__main__":
    unittest.main()
