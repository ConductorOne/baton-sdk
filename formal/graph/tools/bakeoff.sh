#!/usr/bin/env bash
# Bake-off phase cells (GS-CO-005 declared verdicts), kept OUT of the
# calibration sweep: sweep.sh reproduces the frozen 66-cell matrix
# (PCheckerOutput/sweep/summary.txt) and this script reproduces the
# 12-cell bake-off run of record (PCheckerOutput/bakeoff/summary.txt)
# without either overwriting the other's evidence.
#
# Usage: tools/bakeoff.sh [schedules]   (run from formal/graph)
set -u
. "$(dirname "$0")/alarms.sh"
S="${1:-10000}"
OUT="PCheckerOutput/bakeoff"
SUMMARY="$OUT/summary.txt"
mkdir -p "$OUT"
: > "$SUMMARY"

# cell:expected[:alarm[:strategy]]  (RED = counterexample expected,
# GREEN = none). The third field is the cell's calibrated monitor and
# it is ENFORCED: counterexample presence alone matches expected=RED
# even for a cell that redded on a deadlock instead of its declared
# property, so a RED whose extracted tag does not contain the declared
# alarm is a MISMATCH. Enforcement is sound here because every
# bake-off red has exactly one pre-registered monitor (unlike sweep
# cells, which can legitimately red on more than one calibrated shape
# — see sweep.sh). The optional fourth field is an extra p-check
# strategy flag for cells whose target is too narrow for uniform
# random search to find reliably at the default budget — same
# mechanism as the sweep's third field.
CELLS="
tcG6aE_Ctl:GREEN
tcG6aS_Ctl:GREEN
tcG6cE_Ctl:GREEN
tcG6cS_Ctl:GREEN
tcG6bE_Redo:RED:EXEC-BOUND
tcG6bS_Redo:RED:EXEC-BOUND
tcG5dE_W1:RED:SEAL-WORLD
tcG5dS_W1:RED:SEAL-WORLD
tcG5dE_W2:RED:SEAL-WORLD
tcG5dS_W2:RED:SEAL-WORLD:--sch-feedbackpct=20
tcG5dE_W3:GREEN
tcG5dS_W3:GREEN
"

mismatches=0
total=0
for entry in $CELLS; do
  rest="${entry#*:}"
  cell="${entry%%:*}"
  expected="${rest%%:*}"
  alarm=""
  strategy=""
  case "$rest" in *:*)
    rest="${rest#*:}"
    alarm="${rest%%:*}"
    case "$rest" in *:*) strategy="${rest#*:}";; esac
  ;; esac
  # Grammar guard: the fields are positional and the alarm is only
  # consulted on the RED branch, so a strategy flag written in the
  # alarm slot (cell:GREEN:--flag) would be dropped SILENTLY — the
  # cell would run without its intended search and still report ok.
  # A strategy with no alarm takes an explicit empty third field
  # (cell:expected::--flag).
  case "$alarm" in --*)
    echo "bakeoff.sh: $cell: third field must be the calibrated alarm, not '$alarm' — grammar is cell:expected[:alarm[:strategy]]; for strategy-only write $cell:$expected::$alarm" >&2
    exit 2
  ;; esac
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
    # Empty tag = firing monitor outside the shared alternation
    # (tools/alarms.sh): unauditable, so a mismatch even when RED was
    # expected. Declared-alarm check is a substring match against the
    # comma-joined tag set — sound while no declared alarm is a
    # substring of a different monitor's name (EXEC-BOUND and
    # SEAL-WORLD are not).
    [ -n "$tag" ] || mark="MISMATCH"
    if [ -n "$alarm" ]; then
      case "$tag" in *"$alarm"*) ;; *) mark="MISMATCH";; esac
    fi
    detail=" [$tag]"
  elif [ "$observed" = "CHECKER-ERROR" ]; then
    detail=" (p exit $pstatus, see $OUT/$cell.log)"
  fi
  [ "$mark" = "ok" ] || mismatches=$((mismatches + 1))
  line="$cell expected=$expected observed=$observed $mark$detail"
  echo "$line" | tee -a "$SUMMARY"
done
echo "BAKEOFF-DONE cells=$total mismatches=$mismatches" | tee -a "$SUMMARY"
# The exit status carries the verdict (the Makefile's formal targets
# rely on it): a drifted run must not read as a green make.
[ "$mismatches" -eq 0 ]
