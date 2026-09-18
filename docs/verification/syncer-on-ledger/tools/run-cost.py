#!/usr/bin/env python3
"""Interleave isolated cost processes; incomplete instrumentation is always labeled smoke."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import statistics
import subprocess
import time


def digest(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def run_arm(binary, arm, pages, records, workers, output, timeout):
    result_path = output.with_suffix(".json")
    env = dict(os.environ, BATON_LEDGER_COST="1", BATON_LEDGER_COST_PAGES=str(pages),
               BATON_LEDGER_COST_RECORDS=str(records), BATON_LEDGER_COST_WORKERS=str(workers),
               BATON_LEDGER_COST_OUTPUT=str(result_path), BATON_LEDGER_COST_ARM=arm)
    test = "TestLedgerCostBaseline" if arm == "token" else "TestLedgerCostRuntime"
    start = time.monotonic()
    with output.open("wb") as log:
        process = subprocess.Popen([str(binary), f"-test.run=^{test}$", "-test.v", f"-test.timeout={timeout}s"],
                                   env=env, stdout=log, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + timeout + 30
        while True:
            waited, status, usage = os.wait4(process.pid, os.WNOHANG)
            if waited:
                break
            if time.monotonic() >= deadline:
                process.kill()
                _, status, usage = os.wait4(process.pid, 0)
                process.returncode = os.waitstatus_to_exitcode(status)
                raise TimeoutError(f"{arm} exceeded process deadline; see {output}")
            time.sleep(0.1)
        process.returncode = os.waitstatus_to_exitcode(status)
    if process.returncode:
        raise RuntimeError(f"{arm} exited {process.returncode}; see {output}")
    result = json.loads(result_path.read_text())
    expected = "token-path" if arm == "token" else f"ledger-scheduler-{arm}-no-sync"
    if result["arm"] != expected:
        raise ValueError(f"expected {expected}, got {result['arm']}")
    for key, value in (("pages", pages), ("records_per_page", records), ("workers", workers),
                       ("resources_verified", pages * records)):
        if result[key] != value:
            raise ValueError(f"{arm}: {key}={result[key]}, expected {value}")
    result["pebble_bytes_written"] = sum(result[key] for key in ("wal_bytes_before_close", "flush_bytes_before_close", "compaction_bytes_before_close"))
    result.update(process_wall_seconds=time.monotonic() - start,
                  peak_rss_kib=usage.ru_maxrss, binary_sha256=digest(binary),
                  classification="smoke; C49 metrics and boundary dispositions incomplete")
    result_path.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", required=True, type=Path)
    parser.add_argument("--ledger", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--pages", type=int, nargs="+", default=[1000])
    parser.add_argument("--records", type=int, nargs="+", default=[100])
    parser.add_argument("--workers", type=int, nargs="+", choices=[1, 4], default=[1, 4])
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--timeout", type=int, default=3600)
    args = parser.parse_args()
    if args.repetitions < 1 or args.timeout < 1 or min(args.pages + args.records) < 1:
        parser.error("counts and timeout must be positive")
    if min(args.pages) <= max(args.workers):
        parser.error("page count must exceed worker count for the resumed smoke arm")
    if not args.baseline.is_file() or not args.ledger.is_file():
        parser.error("both compiled test executables must exist")
    if args.output.exists():
        parser.error("output directory must not already exist; preserve earlier samples")
    args.output.mkdir(parents=True)
    machine = subprocess.run(["python3", str(Path(__file__).with_name("machine.py"))],
                             capture_output=True, text=True, check=True)
    (args.output / "machine.json").write_text(machine.stdout)
    binaries = {"token": args.baseline.resolve(), "fresh": args.ledger.resolve(), "resume": args.ledger.resolve()}
    samples = []
    rows = []
    metrics = ["sync_wall_ns", "pebble_bytes_written", "wal_bytes_before_close", "flush_bytes_before_close",
               "compaction_bytes_before_close", "c1z_bytes", "peak_rss_kib",
               "page_commit_ns", "handler_ns", "seal_ns", "seal_fold_ns", "seal_scrub_ns", "seal_purge_ns", "resume_walk_ns"]
    for pages in args.pages:
        for records in args.records:
            for workers in args.workers:
                cell = {arm: [] for arm in binaries}
                for repetition in range(args.repetitions):
                    arms = ["token", "fresh", "resume"]
                    offset = repetition % len(arms)
                    for arm in arms[offset:] + arms[:offset]:
                        name = f"p{pages}-r{records}-w{workers}-rep{repetition}-{arm}"
                        sample = run_arm(binaries[arm], arm, pages, records, workers,
                                         args.output / (name + ".log"), args.timeout)
                        cell[arm].append(sample)
                        samples.append(sample)
                        (args.output / "samples.json").write_text(json.dumps(samples, indent=2) + "\n")
                        print(name, "verified", flush=True)
                for metric in metrics:
                    medians = {}
                    for arm, arm_samples in cell.items():
                        values = [sample.get(metric) for sample in arm_samples]
                        medians[arm] = statistics.median(values) if all(value is not None for value in values) else None
                    baseline = medians["token"]
                    ratios = {arm: medians[arm] / baseline if baseline and medians[arm] is not None else None
                              for arm in ("fresh", "resume")}
                    rows.append(dict(pages=pages, records=records, workers=workers, metric=metric,
                                     medians=medians, ratios=ratios))
    (args.output / "table.json").write_text(json.dumps(rows, indent=2) + "\n")
    lines = ["# Interleaved cost smoke", "",
             "Existing scheduler with synthetic page handlers; actual resumed NoSync. Not the CO-009 Sync arm or an acceptance table.", "",
             "| Pages | Records/page | Workers | Metric | Token | Fresh | Resumed | Fresh/token | Resumed/token |",
             "| --- | --- | --- | --- | --- | --- | --- | --- | --- |"]
    render = lambda value: "N/A" if value is None else f"{value:.4g}"
    for row in rows:
        values = [row["pages"], row["records"], row["workers"], row["metric"],
                  *(render(row["medians"][arm]) for arm in ("token", "fresh", "resume")),
                  *(render(row["ratios"][arm]) for arm in ("fresh", "resume"))]
        lines.append("| " + " | ".join(map(str, values)) + " |")
    (args.output / "table.md").write_text("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
