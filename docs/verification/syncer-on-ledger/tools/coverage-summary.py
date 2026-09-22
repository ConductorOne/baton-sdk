#!/usr/bin/env python3
"""Summarize executed tests and changed-file statement coverage, without inferring product coverage."""

import argparse
import json
from pathlib import Path
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--baseline", default="eb63f1b5")
    parser.add_argument("--events", type=Path, nargs="+", required=True)
    parser.add_argument("--profiles", type=Path, nargs="+", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    changed = set(subprocess.check_output(
        ["git", "diff", "--name-only", args.baseline, args.revision, "--", "*.go"], text=True).splitlines())
    changed = {name for name in changed if not name.endswith("_test.go")}
    tests = []
    packages = []
    for path in args.events:
        started, finished = set(), set()
        for line in path.read_text().splitlines():
            if not line.startswith("{"):
                continue
            event = json.loads(line)
            if event.get("Action") == "start":
                started.add(event.get("Package"))
            if event.get("Action") not in ("pass", "fail", "skip"):
                continue
            result = {"package": event.get("Package"), "status": event["Action"], "elapsed_seconds": event.get("Elapsed")}
            if "Test" in event:
                result["test"] = event["Test"]
                tests.append(result)
            else:
                packages.append(result)
                finished.add(event.get("Package"))
        if not finished or started - finished:
            raise ValueError(f"{path}: incomplete package result stream")
    files = {}
    prefix = "github.com/conductorone/baton-sdk/"
    profiled_files = set()
    for path in args.profiles:
        current_files = set()
        lines = path.read_text().splitlines()
        if not lines or lines[0] not in ("mode: set", "mode: count", "mode: atomic"):
            raise ValueError(f"{path}: invalid coverage header")
        for line in lines[1:]:
            location, statements, hits = line.rsplit(maxsplit=2)
            name, span = location.rsplit(":", 1)
            name = name.removeprefix(prefix)
            if name not in changed:
                continue
            if name in profiled_files:
                raise ValueError(f"{name}: overlapping coverage profiles")
            current_files.add(name)
            entry = files.setdefault(name, {"statements": 0, "covered_statements": 0, "uncovered_ranges": []})
            count = int(statements)
            entry["statements"] += count
            if int(hits) > 0:
                entry["covered_statements"] += count
            else:
                entry["uncovered_ranges"].append(span)
        profiled_files.update(current_files)
    result = {
        "source_revision": args.revision,
        "baseline": args.baseline,
        "scope": "Executed tests and statement coverage only; not branch coverage or a mapping to P1-P10 cells.",
        "packages": packages,
        "test_outcomes": {status: sum(test["status"] == status for test in tests) for status in ("pass", "fail", "skip")},
        "tests": sorted(tests, key=lambda item: (item["package"], item["test"])),
        "changed_files": dict(sorted(files.items())),
        "changed_go_files_without_profile": sorted(changed - files.keys()),
    }
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    if any(package["status"] == "fail" for package in packages) or result["test_outcomes"]["fail"]:
        raise SystemExit("Tests failed; manifest preserves the failures")


if __name__ == "__main__":
    main()
