#!/usr/bin/env bash
# Soak the live property test across many seeds. One `go test` per seed
# keeps a failure cheap to replay and lets an interrupted run resume
# from the next seed. Seeds run -j at a time; inside each run the cases
# are parallel subtests, so -j above 1 mostly helps on machines with
# more cores than one run saturates.
#
#   formal/c1z/scripts/soak.sh [-n CASES_PER_FAMILY] [-s FIRST_SEED] [-e LAST_SEED] [-j JOBS] [-t GO_TEST_TIMEOUT]
#
# Defaults: -n 1000, -s 1, -e 50, -j 1, -t 2h. Runs every seed, then
# exits non-zero if any failed, printing each failing seed's replay
# command. Per-seed logs: formal/c1z/.lake/soak-<seed>.log. Needs
# `lake` on PATH (elan).
#
# C1Z_FORMAL_CONTAINER_N and C1Z_FORMAL_TMPDIR pass through from the
# environment (see ORACLE_SCHEMA.md, "Go-side environment").
set -uo pipefail

n=1000
first=1
last=50
jobs=1
timeout=2h
while getopts "n:s:e:j:t:h" opt; do
  case "$opt" in
    n) n="$OPTARG" ;;
    s) first="$OPTARG" ;;
    e) last="$OPTARG" ;;
    j) jobs="$OPTARG" ;;
    t) timeout="$OPTARG" ;;
    h|*)
      sed -n '2,16p' "$0" | sed 's/^# \{0,1\}//'
      exit 2
      ;;
  esac
done

root="$(cd "$(dirname "$0")/../../.." && pwd)"
oracle="$root/formal/c1z/.lake/build/bin/c1z-oracle"
logdir="$root/formal/c1z/.lake"

if ! command -v lake >/dev/null 2>&1; then
  echo "soak: 'lake' is not on PATH; install elan and run 'elan toolchain install \$(cat formal/c1z/lean-toolchain)'" >&2
  exit 2
fi

echo "soak: building oracle"
(cd "$root/formal/c1z" && lake build c1z-oracle >/dev/null) || { echo "soak: oracle build failed" >&2; exit 1; }

# run_seed is invoked once per seed, possibly concurrently. It writes
# the seed's log and prints one status line; the exit status marks the
# result for the summary below.
run_seed() {
  local seed="$1"
  local start=$SECONDS
  if C1Z_FORMAL_ORACLE="$oracle" \
     C1Z_FORMAL_PROPERTY_N="$n" \
     C1Z_FORMAL_PROPERTY_SEED="$seed" \
     go test -count=1 -timeout "$timeout" -run 'TestFormalProperty|TestFormalContainerProperty' "$root/pkg/dotc1z/engine/pebble/" "$root/pkg/dotc1z/" >"$logdir/soak-$seed.log" 2>&1; then
    echo "soak: seed $seed ok ($((SECONDS - start))s)"
    rm -f "$logdir/soak-$seed.failed"
  else
    echo "soak: seed $seed FAILED ($((SECONDS - start))s); log: formal/c1z/.lake/soak-$seed.log" >&2
    : >"$logdir/soak-$seed.failed"
  fi
}
export -f run_seed
export oracle n timeout root logdir

echo "soak: seeds $first..$last, $n cases per family, $jobs seed(s) at a time, go test timeout $timeout"
rm -f "$logdir"/soak-*.failed
seq "$first" "$last" | xargs -P "$jobs" -I{} bash -c 'run_seed "$@"' _ {}

failed=("$logdir"/soak-*.failed)
if [[ -e "${failed[0]}" ]]; then
  echo "soak: FAILED seeds:" >&2
  for f in "${failed[@]}"; do
    seed="${f##*/soak-}"; seed="${seed%.failed}"
    echo "  C1Z_FORMAL_ORACLE=\"$oracle\" C1Z_FORMAL_PROPERTY_N=$n C1Z_FORMAL_PROPERTY_SEED=$seed go test -count=1 -timeout $timeout -v -run 'TestFormalProperty|TestFormalContainerProperty' ./pkg/dotc1z/engine/pebble/ ./pkg/dotc1z/" >&2
  done
  exit 1
fi
echo "soak: all seeds passed"
