#!/usr/bin/env bash
# Full-cell regression sweep. Verdicts are audited by counterexample
# trace-file presence (NOT the "Found N bugs" tail — see the NOTE in
# CALIBRATION.md: the strategy portfolio's last block can report 0
# bugs after an earlier block found one).
#
# Usage: tools/sweep.sh [schedules]   (run from formal/walker)
set -u
S="${1:-10000}"
OUT="PCheckerOutput/sweep"
SUMMARY="$OUT/summary.txt"
mkdir -p "$OUT"
: > "$SUMMARY"

# cell:expected  (RED = counterexample expected, GREEN = none)
CELLS="
tc1a1b_P1:RED
tc1a1b_P2:RED
tc1a1b_P3:RED
tc1bii_P1:RED
tc1c_P1:RED
tc1c_P1_probe:RED
tc1c_P2:RED
tc1c_P2_honest:GREEN
tcGreen_All:GREEN
tc2stop_P6A:RED
tc2crash_P6A:RED
tc2green_P6A:GREEN
tc3a_P1:RED
tc3a_P2:GREEN
tc3b_P1:RED
tc3bBindingOn_All:GREEN
tc3atomic_All:GREEN
tc4shipped_All:GREEN
tc4atomic_All:GREEN
tc4noOnce_P1:RED
tc4noLocks_P1:RED
tc5a_P1:RED
tc5a_Gate_All:GREEN
tc5b_Dropout_All:GREEN
tc5b_CrashWindow:RED
tc5c_C1Probe:RED
tc6naive_P1:RED
tc6atomic_All:GREEN
tc6atomicStop_All:GREEN
tc6overlayNaive_P1:RED
tc6overlayNaive_P2:RED
tc6overlayLast_P1:RED
tc6overlayMutO4_P1:RED
tc6overlay_All:GREEN
tc6overlayStop_All:GREEN
tc7a_P6R:RED
tc7a_P1P2:GREEN
tc7b_P6R:RED
tc7c_All:GREEN
tc7aTaintW_P6R:GREEN
tc7bTaintW_P6R:RED
tc7aTaintAll_P6R:GREEN
tc7bTaintAll_P6R:GREEN
tcP4stuck_P4:RED
tcP4ladder_All:GREEN
tcP4leak_P1:RED
tcP4release_All:GREEN
"

mismatches=0
total=0
for entry in $CELLS; do
  cell="${entry%%:*}"
  expected="${entry##*:}"
  total=$((total + 1))
  rm -rf "$OUT/$cell"
  p check -tc "$cell" -s "$S" -o "$OUT/$cell" > "$OUT/$cell.log" 2>&1
  ce=$(ls "$OUT/$cell"/BugFinding/walker_[0-9]*_[0-9]*.txt 2>/dev/null | head -1)
  if [ -n "$ce" ]; then observed="RED"; else observed="GREEN"; fi
  if [ "$observed" = "$expected" ]; then mark="ok"; else mark="MISMATCH"; mismatches=$((mismatches + 1)); fi
  detail=""
  if [ "$observed" = "RED" ]; then
    detail=" [$(rg -o "(P[0-9][0-9A-Z'-]*[A-Z]|SEAL-EXPECT|C1-PROBE|P4-STUCK|Deadlock detected|liveness)" "$ce" 2>/dev/null | sort -u | paste -sd, -)]"
  fi
  line="$cell expected=$expected observed=$observed $mark$detail"
  echo "$line" | tee -a "$SUMMARY"
done
echo "SWEEP-DONE cells=$total mismatches=$mismatches" | tee -a "$SUMMARY"
