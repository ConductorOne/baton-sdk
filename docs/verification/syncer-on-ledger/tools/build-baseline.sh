#!/usr/bin/env bash
set -euo pipefail

if [[ $# != 1 ]]; then
  echo 'usage: build-baseline.sh /absolute/path/to/baseline.test' >&2
  exit 2
fi
case "$1" in
  /*) ;;
  *) echo 'output path must be absolute' >&2; exit 2 ;;
esac
repo_root=$(git rev-parse --show-toplevel)
fixture="$repo_root/pkg/sync/ledger_cost_test.go"
baseline_root=$(mktemp -d /tmp/syncer-ledger-baseline.XXXXXX)
cleanup() {
  git -C "$repo_root" worktree remove --force "$baseline_root" >/dev/null 2>&1 || true
  rmdir "$baseline_root" 2>/dev/null || true
}
trap cleanup EXIT
git -C "$repo_root" worktree add --detach "$baseline_root" eb63f1b5
cp "$fixture" "$baseline_root/pkg/sync/ledger_cost_test.go"
(
  cd "$baseline_root"
  GOTOOLCHAIN=go1.26.0 go test -mod=vendor -c -o "$1" ./pkg/sync
)
