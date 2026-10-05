#!/usr/bin/env bash
set -euo pipefail
if [[ $# != 2 || "$1" != /* ]]; then
  echo 'usage: build-service-rollback.sh /absolute/path/to/new-test-binary SDK_COMMIT' >&2
  exit 2
fi
if [[ -e "$1" ]]; then
  echo 'output already exists' >&2
  exit 2
fi
repo_root=$(git rev-parse --show-toplevel)
probe_root=$(mktemp -d /tmp/ledger-service-rollback.XXXXXX)
cleanup() {
  git -C "$repo_root" worktree remove --force "$probe_root" >/dev/null 2>&1 || true
  rmdir "$probe_root" 2>/dev/null || true
}
trap cleanup EXIT
git worktree add --detach "$probe_root" "$2"
cp "$repo_root/pkg/connectorrunner/ledger_rollback_child_test.go" "$probe_root/pkg/connectorrunner/ledger_rollback_child_test.go"
(cd "$probe_root" && GOTOOLCHAIN=go1.26.0 go test -mod=vendor -c ./pkg/connectorrunner -o "$1")
