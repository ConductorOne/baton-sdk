#!/usr/bin/env bash
# Full-cell regression sweep. Verdicts are audited by counterexample
# trace-file presence (NOT the "Found N bugs" tail — see the NOTE in
# CALIBRATION.md: the strategy portfolio's last block can report 0
# bugs after an earlier block found one). p check's exit status gates
# only the GREEN side (a counterexample-free nonzero exit is
# CHECKER-ERROR — absence of a find from a checker that died proves
# nothing — while a found counterexample stands regardless of exit
# status).
#
# Usage: tools/sweep.sh [schedules]   (run from formal/walker)
set -u
# The alarm-tag pipeline needs rg; without this guard a missing rg is
# swallowed into an empty tag, indistinguishable from an unrecognized
# monitor. (The graph scripts share tools/alarms.sh; this alternation
# is walker-specific and single-consumer, so it stays inline.)
command -v rg >/dev/null 2>&1 || {
  echo "walker tools: rg (ripgrep) is required for alarm-tag extraction and is not on PATH" >&2
  exit 2
}
MONITOR_ALTERNATION="P[0-9][0-9A-Z'-]*[A-Z]|SEAL-EXPECT|C1-PROBE|P4-STUCK|Deadlock detected|liveness"
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
tc2crash_P6C:RED
tc2clear_P6C:RED
tc2consistent_P6C:GREEN
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
tc8green_P8:GREEN
tc8crash_P8:GREEN
tc8stop_P8:GREEN
tc8reconOff_P8:RED
tc8staleList_P8:RED
tc8overDelete_P8:RED
"

mismatches=0
total=0
for entry in $CELLS; do
  cell="${entry%%:*}"
  expected="${entry##*:}"
  total=$((total + 1))
  rm -rf "$OUT/$cell"
  p check -tc "$cell" -s "$S" -o "$OUT/$cell" > "$OUT/$cell.log" 2>&1
  pstatus=$?
  ce=$(ls "$OUT/$cell"/BugFinding/walker_[0-9]*_[0-9]*.txt 2>/dev/null | head -1)
  # Verdict precedence: a counterexample is RED even if the checker
  # then exited nonzero (the find stands); a counterexample-free
  # nonzero exit is CHECKER-ERROR, not GREEN — "no bug found" from a
  # checker that died is not evidence of anything.
  if [ -n "$ce" ]; then observed="RED"
  elif [ "$pstatus" -ne 0 ]; then observed="CHECKER-ERROR"
  else observed="GREEN"; fi
  mark="ok"
  [ "$observed" = "$expected" ] || mark="MISMATCH"
  detail=""
  if [ "$observed" = "RED" ]; then
    tag=$(rg -o "($MONITOR_ALTERNATION)" "$ce" | sort -u | paste -sd, -)
    # An empty tag means the firing monitor is outside the alternation
    # above — an untagged red is unauditable, so it is a mismatch even
    # when RED was expected. Per-cell alarm enforcement stays with the
    # graph bake-off (bakeoff.sh): walker sweep cells can legitimately
    # red on more than one calibrated shape (tc3a_P1's two P1 clauses),
    # so the comparison surface for WHICH monitor fired is
    # CALIBRATION.md, not this script.
    [ -n "$tag" ] || mark="MISMATCH"
    detail=" [$tag]"
  elif [ "$observed" = "CHECKER-ERROR" ]; then
    detail=" (p exit $pstatus, see $OUT/$cell.log)"
  fi
  [ "$mark" = "ok" ] || mismatches=$((mismatches + 1))
  line="$cell expected=$expected observed=$observed $mark$detail"
  echo "$line" | tee -a "$SUMMARY"
done
echo "SWEEP-DONE cells=$total mismatches=$mismatches" | tee -a "$SUMMARY"
# The exit status carries the verdict (the Makefile's formal targets
# rely on it): a drifted sweep must not read as a green make.
[ "$mismatches" -eq 0 ]
