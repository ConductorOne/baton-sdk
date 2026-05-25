#!/usr/bin/env bash
# autoresearch.sh — Pebble engine READ perf benchmark driver.
# Primary metric: pebble_readpaginated_1m_ms.
# Sentinels: pebble_writepack_1m_ms (no write regression),
#            sqlite_readpaginated_1k_ms (no SQLite regression).
set -uo pipefail

export GOCACHE="${GOCACHE:-$HOME/.cache/go-build}"
export CGO_ENABLED=0

BENCH_DIR="./pkg/dotc1z/engine/pebble"
COMMON_FLAGS=(-tags=batonsdkv2 -run='^$' -benchmem -benchtime=2x -timeout=30m)

# Read sweep scales. 1M is the primary target. Override via env for
# fast inner-loop iteration on a single idea (default-include the
# small scales so each kept change has cross-scale directional
# confirmation).
READ_SCALES="${BATONSDK_READ_SCALES:-100,1000,10000,100000,1000000}"

# Write sentinel: just the 1M WritePack scale to verify the WritePack
# session's wins haven't regressed.
WRITE_SENTINEL_SCALES="${BATONSDK_WRITE_SENTINEL_SCALES:-1000000}"

# SQLite read sentinel: 1k scale (cheap, catches if Pebble changes
# leak into SQLite somehow).
SQLITE_SENTINEL_SCALES="${BATONSDK_SQLITE_SENTINEL_SCALES:-1000}"

OUT_PREAD=$(mktemp -t ar-read.XXXXXX)
OUT_SREAD=$(mktemp -t ar-sread.XXXXXX)
OUT_WSENT=$(mktemp -t ar-wsent.XXXXXX)

# 1) Pebble paginated read sweep — primary metric here.
BATONSDK_BENCH_SCALES="$READ_SCALES" \
  go test "${COMMON_FLAGS[@]}" \
    -bench 'BenchmarkRegisteredPebbleUnpackReadGrants$' \
    "$BENCH_DIR" >"$OUT_PREAD" 2>&1 || true

# 2) SQLite paginated read regression sentinel.
BATONSDK_BENCH_SCALES="$SQLITE_SENTINEL_SCALES" \
  go test "${COMMON_FLAGS[@]}" \
    -bench 'BenchmarkRegisteredSQLiteUnpackReadGrants$' \
    "$BENCH_DIR" >"$OUT_SREAD" 2>&1 || true

# 3) WritePack regression sentinel — keep the WritePack session's win.
BATONSDK_BENCH_SCALES="$WRITE_SENTINEL_SCALES" \
  go test "${COMMON_FLAGS[@]}" \
    -bench 'BenchmarkRegisteredPebbleWritePack$' \
    "$BENCH_DIR" >"$OUT_WSENT" 2>&1 || true

# bench_value <file> <bench-line-prefix-regex> <column>
# col 1=name 2=iters 3=ns/op 5=B/op 7=allocs/op
bench_value() {
  local file=$1 prefix=$2 col=$3
  awk -v p="^${prefix}$" -v c="$col" '$1 ~ p { print $c; exit }' "$file"
}
nz() { [ -n "${1:-}" ] && echo "$1" || echo 0; }
ns_to_ms() {
  awk -v v="$1" 'BEGIN { if (v=="" || v==0) { print 0; exit } printf "%.3f", v / 1000000 }'
}

pread_100=$(nz   "$(bench_value "$OUT_PREAD" 'BenchmarkRegisteredPebbleUnpackReadGrants/grants=100-[0-9]+'     3)")
pread_1k=$(nz    "$(bench_value "$OUT_PREAD" 'BenchmarkRegisteredPebbleUnpackReadGrants/grants=1000-[0-9]+'    3)")
pread_10k=$(nz   "$(bench_value "$OUT_PREAD" 'BenchmarkRegisteredPebbleUnpackReadGrants/grants=10000-[0-9]+'   3)")
pread_100k=$(nz  "$(bench_value "$OUT_PREAD" 'BenchmarkRegisteredPebbleUnpackReadGrants/grants=100000-[0-9]+'  3)")
pread_1m=$(nz    "$(bench_value "$OUT_PREAD" 'BenchmarkRegisteredPebbleUnpackReadGrants/grants=1000000-[0-9]+' 3)")
pread_1m_bytes=$(nz  "$(bench_value "$OUT_PREAD" 'BenchmarkRegisteredPebbleUnpackReadGrants/grants=1000000-[0-9]+' 5)")
pread_1m_allocs=$(nz "$(bench_value "$OUT_PREAD" 'BenchmarkRegisteredPebbleUnpackReadGrants/grants=1000000-[0-9]+' 7)")

sread_1k=$(nz "$(bench_value "$OUT_SREAD" 'BenchmarkRegisteredSQLiteUnpackReadGrants/grants=1000-[0-9]+' 3)")
pwrite_1m=$(nz "$(bench_value "$OUT_WSENT" 'BenchmarkRegisteredPebbleWritePack/grants=1000000-[0-9]+' 3)")

# --- Emit METRIC lines (consumed by run_experiment) ---
echo "METRIC pebble_readpaginated_1m_ms=$(ns_to_ms "$pread_1m")"
echo "METRIC pebble_readpaginated_100k_ms=$(ns_to_ms "$pread_100k")"
echo "METRIC pebble_readpaginated_10k_ms=$(ns_to_ms "$pread_10k")"
echo "METRIC pebble_readpaginated_1k_ms=$(ns_to_ms "$pread_1k")"
echo "METRIC pebble_readpaginated_100_ms=$(ns_to_ms "$pread_100")"
echo "METRIC pebble_readpaginated_1m_bytes_op=$pread_1m_bytes"
echo "METRIC pebble_readpaginated_1m_allocs_op=$pread_1m_allocs"
echo "METRIC sqlite_readpaginated_1k_ms=$(ns_to_ms "$sread_1k")"
echo "METRIC pebble_writepack_1m_ms=$(ns_to_ms "$pwrite_1m")"

# --- Diagnostic output ---
echo
echo "=== Pebble paginated reads ==="
grep 'BenchmarkRegisteredPebbleUnpackReadGrants' "$OUT_PREAD" || echo "(no rows)"
if grep -qE '^(FAIL|--- FAIL|panic:|build failed)' "$OUT_PREAD"; then
  echo "--- pebble read bench errors (tail) ---"
  tail -40 "$OUT_PREAD"
fi

echo
echo "=== SQLite paginated read sentinel ==="
grep 'BenchmarkRegisteredSQLiteUnpackReadGrants' "$OUT_SREAD" || echo "(no rows)"

echo
echo "=== WritePack 1M sentinel ==="
grep 'BenchmarkRegisteredPebbleWritePack' "$OUT_WSENT" || echo "(no rows)"

rm -f "$OUT_PREAD" "$OUT_SREAD" "$OUT_WSENT"
exit 0
