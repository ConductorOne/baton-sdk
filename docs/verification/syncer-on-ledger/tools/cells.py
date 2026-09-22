#!/usr/bin/env python3
"""Emit finite coverage products with accepted change orders (CO-021 excludes expansion pages)."""

import argparse
import itertools
import json
import sys

ACTIONS = (
    "init", "resource-types", "resources", "resources-for-entitlements",
    "entitlements", "grants", "external-resources", "assets",
    "targeted-resource", "static-entitlements",
)
REOPENS = ("new-syncer", "new-process-c1z", "cold-crash-image")
WORKERS = ("1-to-1", "1-to-4", "4-to-1", "4-to-4")
FORMATS = ("v0", "v1", "v2")
BUCKETS = ("worker-0", "worker-n", "run", "takeover")
CUTS = (
    "before-connector", "response-before-stage", "partial-stage",
    "all-staged", "stamp-before-batch", "batch-failure",
    "durable-before-transition", "transition-before-dispatch", "after-dispatch",
)
PRODUCTS = {
    "P1": {"action": ACTIONS, "transition": ("finish", "next", "children", "next-and-children", "warning"),
           "cut": CUTS, "reopen": REOPENS, "workers": WORKERS},
    "P2": {"action": ACTIONS, "row": ("exact", "absent", "wrong-op", "wrong-type", "wrong-resource",
           "wrong-parent-type", "wrong-parent", "wrong-token", "wrong-type-scoped", "unreadable", "scrubbed"), "reopen": REOPENS},
    "P3": {"engine": ("pebble", "sqlite", "empty", "unknown"), "ledger": ("present", "absent"), "attach": ("injected", "path")},
    "P4": {"format": FORMATS, "image": ("token", "stamp-token", "frontier", "frontier-rows", "neither", "conflict"),
           "reopen": REOPENS, "counters": ("absent", "existing")},
    "P5": {"bucket": BUCKETS, "payload": ("zero", "counters", "flags", "calls", "step-session", "all"),
           "workers": WORKERS, "attempts": (1, 2, 3)},
    "P6-put": {"kind": ("resource-types", "resources", "entitlements", "grants"),
               "form": ("new", "same-value", "changed-value", "duplicate", "empty"),
               "result": ("commit", "fail", "crash"), "reopen": REOPENS},
    "P6-delete": {"target": ("staged", "existing", "absent"), "result": ("commit", "fail", "crash"), "reopen": REOPENS},
    "P7": {"cut": ("drained", "invariants", "cleanup", "seal-ready", "partial-scrub", "scrub-before-ended",
           "ended-before-stats", "stats-failure"), "reopen": REOPENS, "retain": (False, True)},
    "P8": {"state": ("fresh", "token", "stamp", "frontier", "rows", "rows-no-stamp", "partial-scrub", "scrubbed", "sealed"),
           "entry": ("automatic", "explicit-id", "new-targeted"), "reopen": REOPENS},
    "P9": {"fact": ("needs-expansion", "external-grants", "fetch-related", "skip-entitlements-grants", "skip-grants", "ingest-known", "ingest-blocked"),
           "result": ("commit", "discard", "fail"), "workers": ("single", "overlapping")},
    "P10": {"graph": ("chain", "tree", "diamond", "duplicate-spawn", "mutual-cycle"), "workers": WORKERS,
            "cut": ("before-parent", "after-parent", "after-child")},
    "CO-002": {"target": ("stored-and-staged-same-external-id",), "delete": ("full-identity",)},
    "C49": {"pages": (1000, 10000, 100000), "records": (100, 1000, 10000), "workers": (1, 4),
            "arm": ("baseline", "ledger-fresh", "ledger-resumed")},
}
EXPECTED = {"P1": 5400, "P2": 330, "P3": 16, "P4": 108, "P5": 288, "P6-put": 180,
            "P6-delete": 27, "P7": 48, "P8": 81, "P9": 42, "P10": 60, "CO-002": 1, "C49": 54}


def cells(product):
    axes = PRODUCTS[product]
    for values in itertools.product(*axes.values()):
        fields = dict(zip(axes, values))
        identity = product + "/" + "/".join(f"{key}={value}" for key, value in fields.items())
        cell = {"id": identity, "product": product, "axes": fields, "status": "not assessed"}
        if product == "P3":
            cell["expected"] = "accepted" if (fields["engine"], fields["ledger"]) in (("pebble", "present"), ("sqlite", "absent")) else "attachment error"
        if product == "P4":
            cell["required_resume_counts"] = [1, 2, 3]
        yield cell


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--product", choices=PRODUCTS)
    parser.add_argument("--summary", action="store_true")
    args = parser.parse_args()
    totals = {}
    seen = set()
    for product in ([args.product] if args.product else PRODUCTS):
        count = 0
        for cell in cells(product):
            if cell["id"] in seen:
                raise ValueError("duplicate cell: " + cell["id"])
            seen.add(cell["id"])
            count += 1
            if not args.summary:
                print(json.dumps(cell, sort_keys=True))
        if count != EXPECTED[product]:
            raise ValueError(f"{product}: expected {EXPECTED[product]}, generated {count}")
        totals[product] = count
    if args.summary:
        json.dump(totals, sys.stdout, indent=2, sort_keys=True)
        print()


if __name__ == "__main__":
    main()
