#!/usr/bin/env bash
# Copy the hand-authored contracts and generated fixtures from a clean checkout.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
dest=pkg/secretcontracts
mode=${1:-}; upstream=${2:-}
[[ -n $upstream ]] && git -C "$upstream" rev-parse --git-dir >/dev/null 2>&1 || { echo "usage: $0 update|check <multipass-checkout>" >&2; exit 2; }
git -C "$upstream" diff --quiet HEAD || { echo "multipass checkout has uncommitted changes" >&2; exit 1; }
head=$(git -C "$upstream" rev-parse HEAD)
case $mode in
 update)
    find "$dest/bundle" -depth -mindepth 1 -delete
    cp -a "$upstream/secret-contracts/." "$dest/bundle/"
    python3 - "$dest" "$head" <<'PYCODE'
import hashlib, json, pathlib, sys
root = pathlib.Path(sys.argv[1])
files = {str(p.relative_to(root / "bundle")): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted((root / "bundle").rglob("*")) if p.is_file()}
(root / "UPSTREAM.json").write_text(json.dumps({"repository": "https://github.com/ductone/multipass", "commit": sys.argv[2], "files_sha256": files}, indent=2) + "\n")
PYCODE
 ;;
 check)
    pinned=$(jq -r .commit "$dest/UPSTREAM.json")
    [[ $head == "$pinned" ]] || { echo "multipass is at $head, pinned $pinned" >&2; exit 1; }
    diff -r "$upstream/secret-contracts" "$dest/bundle"
 ;;
 *) echo "usage: $0 update|check <multipass-checkout>" >&2; exit 2 ;;
esac
