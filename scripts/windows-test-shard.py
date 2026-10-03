#!/usr/bin/env python3
"""Partition the native Go test inventory without changing storage durability."""

import argparse
import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path


def shard_for(name: str, count: int) -> int:
    return int.from_bytes(hashlib.sha256(name.encode()).digest()[:8], "big") % count


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--shard", type=int, required=True)
    parser.add_argument("--count", type=int, required=True)
    args = parser.parse_args()
    if args.count < 1 or not 0 <= args.shard < args.count:
        parser.error("shard must be in [0, count), with count positive")

    flags = ["-tags=baton_lambda_support", "-short", "-timeout=30m"]
    listing = subprocess.run(
        ["go", "test", *flags, "-list=.", "-json", "./..."],
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=False,
    )
    if listing.returncode != 0:
        print(listing.stdout, file=sys.stderr)
        print(listing.stderr, file=sys.stderr)
        return listing.returncode
    inventory: set[tuple[str, str]] = set()
    for line in listing.stdout.splitlines():
        event = json.loads(line)
        if event.get("Action") != "output":
            continue
        name = event.get("Output", "").strip()
        if re.fullmatch(r"(?:Test|Example|Fuzz)\w*", name):
            inventory.add((event["Package"], name))
    if not inventory:
        raise RuntimeError("go test returned an empty test inventory")

    entries = [
        {"package": package, "test": name, "shard": shard_for(name, args.count)}
        for package, name in sorted(inventory)
    ]
    selected = sorted({e["test"] for e in entries if e["shard"] == args.shard})
    if not selected:
        raise RuntimeError(f"shard {args.shard} has no tests")
    pattern = "^(" + "|".join(re.escape(name) for name in selected) + ")$"
    # Windows limits the entire process command line to 32767 characters.
    if len(pattern) > 30000:
        raise RuntimeError("test selector exceeds Windows command-line budget; increase shard count")

    manifest = {"shard": args.shard, "count": args.count, "inventory": entries}
    Path("test-shard.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    sizes = [sum(e["shard"] == i for e in entries) for i in range(args.count)]
    print(f"Native test inventory: {len(entries)} package/test pairs; shard sizes: {sizes}", flush=True)
    print(f"Running shard {args.shard}/{args.count} ({len(selected)} unique test names)", flush=True)
    with Path("test.json").open("w", encoding="utf-8") as output:
        result = subprocess.run(
            ["go", "test", *flags, "-count=1", "-json", f"-run={pattern}", "./..."],
            stdout=output,
            check=False,
        )
    if result.returncode != 0:
        return result.returncode
    expected = {(e["package"], e["test"]) for e in entries if e["shard"] == args.shard}
    completed: set[tuple[str, str]] = set()
    for line in Path("test.json").read_text(encoding="utf-8").splitlines():
        event = json.loads(line)
        if event.get("Action") in ("pass", "skip") and event.get("Test"):
            completed.add((event["Package"], event["Test"]))
    missing = expected - completed
    unexpected = {pair for pair in completed if "/" not in pair[1]} - expected
    if missing or unexpected:
        raise RuntimeError(f"shard coverage mismatch: missing={sorted(missing)}, unexpected={sorted(unexpected)}")
    print(f"Verified {len(expected)} selected package/test pairs completed", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
