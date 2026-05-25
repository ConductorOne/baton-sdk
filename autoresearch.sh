#!/usr/bin/env bash
# autoresearch.sh — Pebble engine perf benchmark driver.
# Emits METRIC name=value lines for the autoresearch loop, plus diagnostic
# bench output for the agent to localize regressions. Always exits 0;
# missing/failed bench rows surface as zero-valued metrics.
set -uo pipefail

export GOCACHE="${GOCACHE:-$HOME/.cache/go-build}"
export CGO_ENABLED=0

BENCH_DIR="./pkg/dotc1z/engine/pebble"
CODEC_DIR="./pkg/dotc1z/engine/pebble/microtests"
COMMON_FLAGS=(-tags=batonsdkv2 -run='^$' -benchmem -benchtime=2x -timeout=20m)

# Allow caller to override scales for fast iteration mode.
WRITE_SCALES="${BATONSDK_BENCH_SCALES:-100,1000,10000,100000,1000000}"
READ_SCALES="${BATONSDK_READ_SCALES:-100,1000,10000,100000}"

OUT_WRITE=$(mktemp -t ar-write.XXXXXX)
OUT_READ=$(mktemp -t ar-read.XXXXXX)
OUT_CODEC=$(mktemp -t ar-codec.XXXXXX)
OUT_SOLO=$(mktemp -t ar-solo.XXXXXX)

# 1) Pebble + SQLite WritePack sweep.
BATONSDK_BENCH_SCALES="$WRITE_SCALES" \
  go test "${COMMON_FLAGS[@]}" \
    -bench 'BenchmarkRegistered(Pebble|SQLite)WritePack$' \
    "$BENCH_DIR" >"$OUT_WRITE" 2>&1 || true

# 2) Pebble paginated read sweep.
BATONSDK_BENCH_SCALES="$READ_SCALES" \
  go test "${COMMON_FLAGS[@]}" \
    -bench 'BenchmarkRegisteredPebbleUnpackReadGrants$' \
    "$BENCH_DIR" >"$OUT_READ" 2>&1 || true

# 3) Codec hot-path microbench.
go test "${COMMON_FLAGS[@]}" \
  -bench='BenchmarkCodec(Direct|Reflect)$' \
  "$CODEC_DIR" >"$OUT_CODEC" 2>&1 || true

# 4) Solo write (cold-start cost).
go test "${COMMON_FLAGS[@]}" \
  -bench='BenchmarkRegisteredPebbleWriteGrant$' \
  "$BENCH_DIR" >"$OUT_SOLO" 2>&1 || true

# bench_value <file> <bench-line-prefix> <column>
# `go test -benchmem` rows look like:
#   BenchmarkX-8      2   12345 ns/op   678 B/op   9 allocs/op
# col 1=name 2=iters 3=ns/op 5=B/op 7=allocs/op
bench_value() {
  local file=$1 prefix=$2 col=$3
  awk -v p="^${2}$" -v c="$col" '$1 ~ p { print $c; exit }' "$file"
}

# Some scales the bench may skip if not configured. Default to 0 if empty.
nz() { [ -n "${1:-}" ] && echo "$1" || echo 0; }

# --- Pebble WritePack at each scale ---
pwrite_100=$(nz "$(bench_value "$OUT_WRITE"   'BenchmarkRegisteredPebbleWritePack/grants=100-[0-9]+'     3)")
pwrite_1k=$(nz "$(bench_value  "$OUT_WRITE"   'BenchmarkRegisteredPebbleWritePack/grants=1000-[0-9]+'    3)")
pwrite_10k=$(nz "$(bench_value "$OUT_WRITE"   'BenchmarkRegisteredPebbleWritePack/grants=10000-[0-9]+'   3)")
pwrite_100k=$(nz "$(bench_value "$OUT_WRITE"  'BenchmarkRegisteredPebbleWritePack/grants=100000-[0-9]+'  3)")
pwrite_1m=$(nz "$(bench_value  "$OUT_WRITE"   'BenchmarkRegisteredPebbleWritePack/grants=1000000-[0-9]+' 3)")
pwrite_1m_bytes=$(nz "$(bench_value "$OUT_WRITE" 'BenchmarkRegisteredPebbleWritePack/grants=1000000-[0-9]+' 5)")
pwrite_1m_allocs=$(nz "$(bench_value "$OUT_WRITE" 'BenchmarkRegisteredPebbleWritePack/grants=1000000-[0-9]+' 7)")

swrite_1k=$(nz "$(bench_value  "$OUT_WRITE"   'BenchmarkRegisteredSQLiteWritePack/grants=1000-[0-9]+'    3)")

pread_1k=$(nz "$(bench_value   "$OUT_READ"    'BenchmarkRegisteredPebbleUnpackReadGrants/grants=1000-[0-9]+'   3)")
pread_100k=$(nz "$(bench_value "$OUT_READ"    'BenchmarkRegisteredPebbleUnpackReadGrants/grants=100000-[0-9]+' 3)")

codec_direct=$(nz "$(bench_value  "$OUT_CODEC" 'BenchmarkCodecDirect-[0-9]+'  3)")
codec_reflect=$(nz "$(bench_value "$OUT_CODEC" 'BenchmarkCodecReflect-[0-9]+' 3)")

solo_write=$(nz "$(bench_value "$OUT_SOLO" 'BenchmarkRegisteredPebbleWriteGrant-[0-9]+' 3)")

ns_to_ms() { awk -v v="$1" 'BEGIN { if (v=="" || v==0) { print 0; exit } printf "%.3f", v / 1000000 }'; }

# --- Emit METRIC lines (consumed by run_experiment) ---
echo "METRIC pebble_writepack_1m_ms=$(ns_to_ms "$pwrite_1m")"
echo "METRIC pebble_writepack_100k_ms=$(ns_to_ms "$pwrite_100k")"
echo "METRIC pebble_writepack_10k_ms=$(ns_to_ms "$pwrite_10k")"
echo "METRIC pebble_writepack_1k_ms=$(ns_to_ms "$pwrite_1k")"
echo "METRIC pebble_writepack_100_ms=$(ns_to_ms "$pwrite_100")"
echo "METRIC pebble_writepack_1m_bytes_op=$pwrite_1m_bytes"
echo "METRIC pebble_writepack_1m_allocs_op=$pwrite_1m_allocs"
echo "METRIC pebble_readpaginated_100k_ms=$(ns_to_ms "$pread_100k")"
echo "METRIC pebble_readpaginated_1k_ms=$(ns_to_ms "$pread_1k")"
echo "METRIC pebble_writegrant_solo_ns_op=$solo_write"
echo "METRIC codec_direct_ns_op=$codec_direct"
echo "METRIC codec_reflect_ns_op=$codec_reflect"
echo "METRIC sqlite_writepack_1k_ms=$(ns_to_ms "$swrite_1k")"

# --- Diagnostic output ---
echo
echo "=== Write+Pack details ==="
grep -E 'BenchmarkRegistered(Pebble|SQLite)WritePack' "$OUT_WRITE" || echo "(no rows — check $OUT_WRITE)"
if grep -qE '^(FAIL|--- FAIL|panic:|build failed)' "$OUT_WRITE"; then
  echo "--- write bench errors (tail) ---"
  tail -40 "$OUT_WRITE"
fi

echo
echo "=== Read details ==="
grep 'BenchmarkRegisteredPebbleUnpackReadGrants' "$OUT_READ" || echo "(no rows — check $OUT_READ)"
if grep -qE '^(FAIL|--- FAIL|panic:|build failed)' "$OUT_READ"; then
  echo "--- read bench errors (tail) ---"
  tail -40 "$OUT_READ"
fi

echo
echo "=== Codec ==="
grep 'BenchmarkCodec' "$OUT_CODEC" || echo "(no rows — check $OUT_CODEC)"

echo
echo "=== Solo write ==="
grep 'BenchmarkRegisteredPebbleWriteGrant' "$OUT_SOLO" || echo "(no rows — check $OUT_SOLO)"

rm -f "$OUT_WRITE" "$OUT_READ" "$OUT_CODEC" "$OUT_SOLO"
exit 0
