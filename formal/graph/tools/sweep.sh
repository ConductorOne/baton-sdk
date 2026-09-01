#!/usr/bin/env bash
# Graph-model cell regression sweep (walker tools/sweep.sh parity).
# Verdicts are audited by counterexample trace-file presence, not the
# "Found N bugs" tail; p check's exit status gates only the GREEN side
# (a counterexample-free nonzero exit is CHECKER-ERROR — absence of a
# find from a checker that died proves nothing — while a found
# counterexample stands regardless of exit status).
#
# Usage: tools/sweep.sh [schedules]   (run from formal/graph)
set -u
. "$(dirname "$0")/alarms.sh"
S="${1:-10000}"
OUT="PCheckerOutput/sweep"
SUMMARY="$OUT/summary.txt"
mkdir -p "$OUT"
: > "$SUMMARY"

# cell:expected[:strategy]  (RED = counterexample expected, GREEN =
# none). Optional third field = extra p-check strategy flag for cells
# whose target is too deep for uniform random search (G1D-REACH).
CELLS="
tcG1i_All:GREEN
tcG1ii_All:GREEN
tcG1iii_E:GREEN
tcG1iii_S:GREEN
tcG1b_All:GREEN
tcG1cMut_P3:GREEN
tcG1cMut_Seal:GREEN
tcG1sup_P1:RED
tcG1bMut_PGEN:RED
tcG1cMut_Adopt:RED
tcG2ea_Core:GREEN
tcG2eb_All:GREEN
tcG2s_All:GREEN
tcG2awE_All:GREEN
tcG2awS_All:GREEN
tcG2eb2c_All:GREEN
tcG2fbE_All:GREEN
tcG2fbS_All:GREEN
tcG2ea_P6G:RED
tcG2ebRetrOff_P6G:RED
tcG2sStampOff_P6G:RED
tcG2awE_Redo:RED
tcG2awS_Redo:RED
tcG2fbE_Redo:RED
tcG2ebPend_Redo:RED
tcG2awWA_E:RED:--sch-feedbackpct=20
tcG2awWA_S:RED
tcG1dProbe_Redo:RED
tcG1d_P6G:RED:--sch-feedbackpct=20
tcG1e_PGEN:RED
tcG5aE_All:GREEN
tcG5aS_All:GREEN
tcG5f_All:GREEN
tcG5bE_P5:RED
tcG5bS_P5:RED
tcG5c_P5:RED
tcG5e_Probe:RED
tcG5e_PurgeOff:RED
tcG5f_Drop:RED
tcG3_E:GREEN
tcG3_S:GREEN
tcG6aE_All:GREEN
tcG6aS_All:GREEN
tcG6bE_All:GREEN
tcG6bS_All:GREEN
tcG6cE_All:GREEN
tcG6cS_All:GREEN
tcG6aE_Redo:RED
tcG6aS_Redo:RED
tcG6cE_Redo:RED
tcG6cS_Redo:RED
tcG7_Ladder:GREEN
tcG7_Stuck:RED
tcG8b_All:GREEN
tcG8bMut_Ctl:GREEN
tcG8bMut_P1:RED
tcG8c_All:GREEN
tcG8c_Poison:RED
tcG8d_All:GREEN
tcG8dMut_PMARK:RED:--sch-feedbackpct=20
tcG9s_All:GREEN
tcG9awS_All:GREEN
tcG9G5aS_All:GREEN
tcG9cBase_All:GREEN
tcG9c_All:GREEN
tcG9c_Redo:RED
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
  pstatus=$?
  ce=$(ls "$OUT/$cell"/BugFinding/graph_[0-9]*_[0-9]*.txt 2>/dev/null | head -1)
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
    tag=$(alarm_tag "$ce")
    # An empty tag means the firing monitor is outside the shared
    # alternation (tools/alarms.sh) — an untagged red is unauditable,
    # so it is a mismatch even when RED was expected. Per-cell alarm
    # ENFORCEMENT is deliberately bake-off-only (see bakeoff.sh):
    # sweep cells can legitimately red on more than one calibrated
    # shape (e.g. the walker's tc3a_P1), so the sweep's comparison
    # surface for WHICH monitor fired is CALIBRATION.md, not this
    # script.
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
