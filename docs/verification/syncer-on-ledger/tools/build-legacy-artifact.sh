#!/usr/bin/env bash
set -euo pipefail

if [[ $# != 1 ]]; then
  echo 'usage: build-legacy-artifact.sh /absolute/path/to/new-artifact.c1z' >&2
  exit 2
fi
case "$1" in
  /*) ;;
  *) echo 'output path must be absolute' >&2; exit 2 ;;
esac
repo_root=$(git rev-parse --show-toplevel)
baseline_root=$(mktemp -d /tmp/syncer-ledger-compat.XXXXXX)
cleanup() {
  git -C "$repo_root" worktree remove --force "$baseline_root" >/dev/null 2>&1 || true
  rmdir "$baseline_root" 2>/dev/null || true
}
trap cleanup EXIT
git -C "$repo_root" worktree add --detach "$baseline_root" eb63f1b5
cp "$repo_root/pkg/sync/ledger_cost_test.go" "$baseline_root/pkg/sync/ledger_cost_test.go"
cp "$repo_root/docs/verification/syncer-on-ledger/tools/legacy-artifact-producer.go.txt" "$baseline_root/pkg/sync/legacy_artifact_producer_test.go"
(
  cd "$baseline_root"
  BATON_LEGACY_ARTIFACT_OUTPUT="$1" GOTOOLCHAIN=go1.26.0 go test -mod=vendor ./pkg/sync -run '^TestProduceLegacyCheckpointArtifact$' -count=1
)
