#!/usr/bin/env bash
# update <multipass-checkout>: vendor the contract bundle from a clean Multipass
#   checkout at its current HEAD and record that commit in UPSTREAM.json.
# check <multipass-checkout>: the checkout is at the pinned commit, regenerates
#   the same bundle, and matches the vendored copy byte for byte.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
dest=pkg/secretcontracts
mode=${1:-}; upstream=${2:-}
[[ -n $upstream && -d $upstream/.git ]] || { echo "usage: $0 update|check <multipass-checkout>" >&2; exit 2; }
git -C "$upstream" diff --quiet HEAD || { echo "multipass checkout has uncommitted changes" >&2; exit 1; }
head=$(git -C "$upstream" rev-parse HEAD)
(cd "$upstream" && scripts/secret-contracts.sh check)
case $mode in
  update)
    rm -rf "$dest/bundle"
    cp -r "$upstream/secret-contracts" "$dest/bundle"
    jq -n --arg commit "$head" \
      --arg revision "$(jq -r .source.revision "$dest/bundle/manifest.json")" \
      --arg bundle "$(jq -r .bundle_sha256 "$dest/bundle/manifest.json")" \
      --arg manifest "$(sha256sum "$dest/bundle/manifest.json" | cut -d' ' -f1)" \
      '{repository: "https://github.com/ductone/multipass", commit: $commit, source_revision: $revision, bundle_sha256: $bundle, manifest_sha256: $manifest}' \
      > "$dest/UPSTREAM.json"
    ;;
  check)
    pinned=$(jq -r .commit "$dest/UPSTREAM.json")
    [[ $head == "$pinned" ]] || { echo "multipass is at $head, pinned $pinned" >&2; exit 1; }
    diff -r "$upstream/secret-contracts" "$dest/bundle"
    ;;
  *) echo "usage: $0 update|check <multipass-checkout>" >&2; exit 2 ;;
esac
