#!/usr/bin/env bash
# Exercise the copy/check boundary using independent scratch repositories.
set -euo pipefail
script=$(cd "$(dirname "$0")" && pwd)/update-secret-contracts.sh
real_git=$(command -v git)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
upstream=$tmp/upstream
consumer=$tmp/consumer
mkdir -p "$upstream/secret-contracts" "$consumer/scripts" "$consumer/pkg/secretcontracts/bundle"
git -C "$upstream" init -q
git -C "$consumer" init -q
cp "$script" "$consumer/scripts/update-secret-contracts.sh"
printf 'committed bytes\n' > "$upstream/secret-contracts/contract.txt"
printf 'ignored.txt\n' > "$upstream/.gitignore"
printf 'secret-contracts/contract.txt export-ignore\n' > "$upstream/.gitattributes"
git -C "$upstream" add .
GIT_AUTHOR_NAME=Test GIT_AUTHOR_EMAIL=test@example.invalid GIT_COMMITTER_NAME=Test GIT_COMMITTER_EMAIL=test@example.invalid git -C "$upstream" commit -qm 'Add contract'
run() { (cd "$consumer" && bash scripts/update-secret-contracts.sh "$1" "$upstream"); }
fail() { echo "$1" >&2; exit 1; }
run update
run check
cmp "$upstream/secret-contracts/contract.txt" "$consumer/pkg/secretcontracts/bundle/contract.txt"
printf 'untracked\n' > "$upstream/secret-contracts/untracked.txt"
printf 'ignored\n' > "$upstream/secret-contracts/ignored.txt"
run update
run check
[[ ! -e "$consumer/pkg/secretcontracts/bundle/untracked.txt" && ! -e "$consumer/pkg/secretcontracts/bundle/ignored.txt" ]] || fail 'untracked file copied'
printf 'tracked mutation\n' > "$upstream/secret-contracts/contract.txt"
if run update 2>/dev/null; then fail 'modified tracked file accepted'; fi
[[ $(cat "$consumer/pkg/secretcontracts/bundle/contract.txt") == 'committed bytes' ]] || fail 'tracked mutation copied'
git -C "$upstream" restore secret-contracts/contract.txt
# Mutate the tracked working file after the initial diff, at tree export time.
# The exported bytes must still come from the already captured commit.
mkdir -p "$tmp/bin"
cat > "$tmp/bin/git" <<'WRAPPER'
#!/usr/bin/env bash
if [[ ${3:-} == ls-tree ]]; then printf 'raced bytes\n' > "$TEST_UPSTREAM/secret-contracts/contract.txt"; fi
exec "$TEST_REAL_GIT" "$@"
WRAPPER
chmod +x "$tmp/bin/git"
(cd "$consumer" && TEST_UPSTREAM="$upstream" TEST_REAL_GIT="$real_git" PATH="$tmp/bin:$PATH" bash scripts/update-secret-contracts.sh update "$upstream")
[[ $(cat "$consumer/pkg/secretcontracts/bundle/contract.txt") == 'committed bytes' ]] || fail 'raced working bytes copied'
git -C "$upstream" restore secret-contracts/contract.txt
run check
printf 'tampered\n' > "$consumer/pkg/secretcontracts/bundle/extra.txt"
if run check >/dev/null 2>&1; then fail 'extra destination file accepted'; fi
rm "$consumer/pkg/secretcontracts/bundle/extra.txt"
python3 - "$consumer/pkg/secretcontracts/UPSTREAM.json" <<'PYCODE'
import json, pathlib, sys
p = pathlib.Path(sys.argv[1]); pin = json.loads(p.read_text()); pin['files_sha256']['contract.txt'] = '0' * 64; p.write_text(json.dumps(pin))
PYCODE
if run check >/dev/null 2>&1; then fail 'tampered hash inventory accepted'; fi
run update
python3 - "$consumer/pkg/secretcontracts/UPSTREAM.json" <<'PYCODE'
import json, pathlib, sys
p = pathlib.Path(sys.argv[1]); pin = json.loads(p.read_text()); pin['commit'] = '0' * 40; p.write_text(json.dumps(pin))
PYCODE
if run check >/dev/null 2>&1; then fail 'wrong pin accepted'; fi
printf 'PASS: clean, export-ignore, untracked, ignored, tracked mutation, tree export race, destination inventory, hash, pin\n'
