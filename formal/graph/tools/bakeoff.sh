#!/usr/bin/env bash
# Bake-off phase cells (GS-CO-005 declared verdicts), kept OUT of the
# calibration sweep: sweep.sh reproduces the frozen 66-cell matrix
# (PCheckerOutput/sweep/summary.txt) and this script reproduces the
# 12-cell bake-off run of record (PCheckerOutput/bakeoff/summary.txt)
# without either overwriting the other's evidence.
#
# Usage: tools/bakeoff.sh [schedules]   (run from formal/graph)
set -u
S="${1:-10000}"
OUT="PCheckerOutput/bakeoff"
SUMMARY="$OUT/summary.txt"
mkdir -p "$OUT"
: > "$SUMMARY"

# cell:expected  (RED = counterexample expected, GREEN = none).
CELLS="
tcG6aE_Ctl:GREEN
tcG6aS_Ctl:GREEN
tcG6cE_Ctl:GREEN
tcG6cS_Ctl:GREEN
tcG6bE_Redo:RED
tcG6bS_Redo:RED
tcG5dE_W1:RED
tcG5dS_W1:RED
tcG5dE_W2:RED
tcG5dS_W2:RED
tcG5dE_W3:GREEN
tcG5dS_W3:GREEN
"

mismatches=0
total=0
for entry in $CELLS; do
  rest="${entry#*:}"
  cell="${entry%%:*}"
  expected="${rest%%:*}"
  strategy=""
  case "$rest" in *:*) strategy="${rest#*:}";; esac
  total=$((total + 1))
  rm -rf "$OUT/$cell"
  # shellcheck disable=SC2086
  p check -tc "$cell" -s "$S" ${strategy/=/ } -o "$OUT/$cell" > "$OUT/$cell.log" 2>&1
  ce=$(ls "$OUT/$cell"/BugFinding/graph_[0-9]*_[0-9]*.txt 2>/dev/null | head -1)
  if [ -n "$ce" ]; then observed="RED"; else observed="GREEN"; fi
  if [ "$observed" = "$expected" ]; then mark="ok"; else mark="MISMATCH"; mismatches=$((mismatches + 1)); fi
  # The alarm tag is what makes an expected-RED cell auditable:
  # counterexample PRESENCE alone matches expected=RED even when the
  # cell redded for a different reason than its calibrated monitor (P
  # also emits a counterexample for deadlock and liveness), so the
  # firing monitor name is extracted and recorded. Kept in sync with
  # sweep.sh's alternation so a cell moved between the scripts keeps
  # its tagging; SEAL-WORLD is the G5d bake-off monitor.
  detail=""
  if [ "$observed" = "RED" ]; then
    detail=" [$(rg -o "(P-GEN|P-MARK|P-ADOPT|P1-[A-Z-]+[A-Z]|P2-[A-Z]+|P3'-[A-Z]+|P4-STUCK|P5-UNDER|P5-OVER|P6-[GES]|SEAL-EXPECT|SEAL-WORLD|REDO-PROBE|PURGE-PROBE|POISON-PROBE|DEAD-DISPATCH|PASS-BUDGET|EXEC-BOUND|Deadlock detected|liveness)" "$ce" 2>/dev/null | sort -u | paste -sd, -)]"
  fi
  line="$cell expected=$expected observed=$observed $mark$detail"
  echo "$line" | tee -a "$SUMMARY"
done
echo "BAKEOFF-DONE cells=$total mismatches=$mismatches" | tee -a "$SUMMARY"
