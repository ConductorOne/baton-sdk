#!/usr/bin/env bash
# Graph-model cell regression sweep (walker tools/sweep.sh parity).
# Verdicts are audited by counterexample trace-file presence, not the
# "Found N bugs" tail.
#
# Usage: tools/sweep.sh [schedules]   (run from formal/graph)
set -u
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
  ce=$(ls "$OUT/$cell"/BugFinding/graph_[0-9]*_[0-9]*.txt 2>/dev/null | head -1)
  if [ -n "$ce" ]; then observed="RED"; else observed="GREEN"; fi
  if [ "$observed" = "$expected" ]; then mark="ok"; else mark="MISMATCH"; mismatches=$((mismatches + 1)); fi
  detail=""
  if [ "$observed" = "RED" ]; then
    detail=" [$(rg -o "(P-GEN|P-MARK|P-ADOPT|P1-[A-Z-]+[A-Z]|P2-[A-Z]+|P3'-[A-Z]+|P4-STUCK|P5-UNDER|P5-OVER|P6-[GES]|SEAL-EXPECT|REDO-PROBE|PURGE-PROBE|POISON-PROBE|DEAD-DISPATCH|PASS-BUDGET|EXEC-BOUND|Deadlock detected|liveness)" "$ce" 2>/dev/null | sort -u | paste -sd, -)]"
  fi
  line="$cell expected=$expected observed=$observed $mark$detail"
  echo "$line" | tee -a "$SUMMARY"
done
echo "SWEEP-DONE cells=$total mismatches=$mismatches" | tee -a "$SUMMARY"
