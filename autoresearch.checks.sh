#!/usr/bin/env bash
# autoresearch.checks.sh — correctness gate. Runs after each successful bench.
# Suppresses verbose progress; only failures bubble up to the agent.
set -euo pipefail

export CGO_ENABLED=0

echo ">> engine + adapter + compactor + equivalence + envelope tests"
go test -tags=batonsdkv2 -count=1 -timeout=5m \
  ./pkg/dotc1z/engine/pebble/... \
  ./pkg/dotc1z/engine/equivalence/... \
  ./pkg/synccompactor/pebble/... \
  ./pkg/dotc1z/format/v3/... 2>&1 | tail -60

echo ">> SQLite engine regression guard"
go test -tags=baton_lambda_support -short -count=1 -timeout=5m \
  ./pkg/dotc1z/ 2>&1 | tail -40

echo ">> golangci-lint"
golangci-lint run --timeout=3m --build-tags=batonsdkv2 \
  ./pkg/dotc1z/engine/... \
  ./pkg/synccompactor/pebble/... 2>&1 | tail -40

echo ">> go.mod / go.sum drift"
if ! git diff --quiet -- go.mod go.sum; then
  echo "FAIL: go.mod or go.sum modified — new dependency introduced"
  git diff --stat -- go.mod go.sum
  exit 1
fi

echo ">> proto wire format drift"
if ! git diff --quiet -- proto/c1/storage/v3/; then
  echo "FAIL: proto wire format changed — out of scope for perf loop"
  git diff --stat -- proto/c1/storage/v3/
  exit 1
fi

echo "OK"
