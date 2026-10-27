#!/usr/bin/env bash
# Copy the hand-authored contracts and generated fixtures from a clean checkout.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
dest=pkg/secretcontracts
mode=${1:-}; upstream=${2:-}
[[ -n $upstream ]] && git -C "$upstream" rev-parse --git-dir >/dev/null 2>&1 || { echo "usage: $0 update|check <multipass-checkout>" >&2; exit 2; }
head=$(git -C "$upstream" rev-parse --verify 'HEAD^{commit}')
git -C "$upstream" diff --quiet "$head" || { echo "multipass checkout has uncommitted changes" >&2; exit 1; }
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
python3 - "$upstream" "$head" "$tmp" <<'PYCODE'
import io, os, pathlib, subprocess, sys
upstream, head, destination = sys.argv[1:]
git = ["git", "-C", upstream]
listing = subprocess.run(git + ["ls-tree", "-r", "-z", head, "--", "secret-contracts"], check=True, stdout=subprocess.PIPE).stdout
entries = []
for entry in listing.split(b"\0"):
    if not entry:
        continue
    metadata, path = entry.split(b"\t", 1)
    mode, kind, oid = metadata.split(b" ")
    if kind != b"blob" or mode not in (b"100644", b"100755") or not path.startswith(b"secret-contracts/"):
        raise SystemExit("unsupported contract tree entry")
    entries.append((mode, oid, path))
if not entries:
    raise SystemExit("contract tree is empty")
objects = subprocess.run(git + ["cat-file", "--batch"], input=b"".join(oid + b"\n" for _, oid, _ in entries), check=True, stdout=subprocess.PIPE).stdout
stream = io.BytesIO(objects)
for mode, oid, path in entries:
    object_id, kind, size = stream.readline().rstrip(b"\n").split(b" ")
    if object_id != oid or kind != b"blob":
        raise SystemExit("unexpected contract object")
    size = int(size)
    data = stream.read(size)
    if len(data) != size or stream.read(1) != b"\n":
        raise SystemExit("truncated contract object")
    target = pathlib.Path(destination) / os.fsdecode(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(data)
    target.chmod(0o755 if mode == b"100755" else 0o644)
PYCODE
case $mode in
 update)
    find "$dest/bundle" -depth -mindepth 1 -delete
    cp -a "$tmp/secret-contracts/." "$dest/bundle/"
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
    diff -r "$tmp/secret-contracts" "$dest/bundle"
    python3 - "$dest" <<'PYCODE'
import hashlib, json, pathlib, sys
root = pathlib.Path(sys.argv[1])
pin = json.loads((root / "UPSTREAM.json").read_text())
files = {str(p.relative_to(root / "bundle")): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted((root / "bundle").rglob("*")) if p.is_file()}
if pin["files_sha256"] != files:
    raise SystemExit("contract file hashes do not match UPSTREAM.json")
PYCODE
 ;;
 *) echo "usage: $0 update|check <multipass-checkout>" >&2; exit 2 ;;
esac
