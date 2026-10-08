#!/usr/bin/env bash
# Trust gate for formal/c1z. Fails on:
#   1. any compiler warning or error (a `sorry` is only a warning);
#   2. any theorem whose axiom set differs from AXIOMS.golden;
#   3. a stale generated/cases.json (the oracle output is checked in
#      because the Go CI has no Lean toolchain; this step keeps it honest).
# Run from anywhere; needs `lake` on PATH (elan installs it).
set -euo pipefail

cd "$(dirname "$0")/.."

if ! command -v lake >/dev/null 2>&1; then
  echo "formal-c1z-check: 'lake' is not on PATH; install elan (https://github.com/leanprover/elan) and run 'elan toolchain install \$(cat lean-toolchain)'" >&2
  exit 2
fi

echo "== build (warnings are errors)"
build_log="$(mktemp)"
trap 'rm -f "$build_log"' EXIT
# `lake build` prints progress lines; only `warning:`/`error:` diagnostics
# count. The library and the oracle executable are both built.
if ! lake build C1z c1z-oracle 2>&1 | tee "$build_log" >/dev/null; then
  cat "$build_log" >&2
  echo "formal-c1z-check: build failed" >&2
  exit 1
fi
if grep -E '^(warning|error):' "$build_log" >/dev/null; then
  grep -E '^(warning|error):' "$build_log" >&2
  echo "formal-c1z-check: compiler diagnostics present (a 'sorry' is a warning); see above" >&2
  exit 1
fi

echo "== axiom audit"
axioms_out="$(lake env lean scripts/Axioms.lean 2>&1)"
if [[ "${UPDATE_GOLDEN:-}" == "1" ]]; then
  printf '%s\n' "$axioms_out" > AXIOMS.golden
  echo "formal-c1z-check: wrote AXIOMS.golden"
fi
if ! diff -u AXIOMS.golden <(printf '%s\n' "$axioms_out"); then
  echo "formal-c1z-check: axiom set drifted from AXIOMS.golden; review the diff, then rerun with UPDATE_GOLDEN=1 if intended" >&2
  exit 1
fi
if grep -Ev '^.* depends on axioms: \[(propext|Classical\.choice|Quot\.sound)(, (propext|Classical\.choice|Quot\.sound))*\]$|^.* does not depend on any axioms$' AXIOMS.golden; then
  echo "formal-c1z-check: AXIOMS.golden lists a non-standard axiom (sorryAx, native_decide, or a custom axiom)" >&2
  exit 1
fi

echo "== oracle freshness"
if ! lake exe c1z-oracle | diff -u generated/cases.json -; then
  echo "formal-c1z-check: generated/cases.json is stale; run 'make formal-c1z-oracle'" >&2
  exit 1
fi

echo "formal-c1z-check: ok"
