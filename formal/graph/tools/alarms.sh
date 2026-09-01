#!/usr/bin/env bash
# Shared alarm-tag extraction for the graph model's verdict scripts
# (sweep.sh and bakeoff.sh source this). ONE definition of the
# monitor-name alternation: when the two scripts each carried a copy,
# a monitor added to only one would silently emit an empty tag in the
# other's committed evidence, so there are no copies. SEAL-WORLD is
# asserted only by the bake-off's tcG5d* cells but stays in the shared
# alternation (output-neutral for the frozen 66 sweep cells; a
# G5d-shaped cell landing in the sweep is not silently untagged).
#
# The tag pipeline needs rg. Without this guard a missing rg is
# swallowed into an empty command substitution — the same empty tag a
# red cell with an unrecognized monitor produces — so sourcing this
# file fails loudly instead.
command -v rg >/dev/null 2>&1 || {
  echo "graph tools: rg (ripgrep) is required for alarm-tag extraction and is not on PATH" >&2
  exit 2
}

MONITOR_ALTERNATION="P-GEN|P-MARK|P-ADOPT|P1-[A-Z-]+[A-Z]|P2-[A-Z]+|P3'-[A-Z]+|P4-STUCK|P5-UNDER|P5-OVER|P6-[GES]|SEAL-EXPECT|SEAL-WORLD|REDO-PROBE|PURGE-PROBE|POISON-PROBE|DEAD-DISPATCH|PASS-BUDGET|EXEC-BOUND|Deadlock detected|liveness"

# alarm_tag <counterexample-file> — comma-joined sorted set of the
# firing monitor names found in the trace. Empty when nothing in the
# alternation matches; callers treat an empty tag on a RED cell as a
# MISMATCH (an untagged red is unauditable).
alarm_tag() {
  rg -o "($MONITOR_ALTERNATION)" "$1" | sort -u | paste -sd, -
}
