#!/usr/bin/env bash
# Test the baton CLI against checked-in expanded .c1z fixtures.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
GOOS="${GOOS:-$(go env GOOS)}"
GOARCH="${GOARCH:-$(go env GOARCH)}"
BATON_BIN="${BATON_BIN:-$ROOT/dist/${GOOS}_${GOARCH}/baton}"
FIXTURES_FILE="${FIXTURES_FILE:-$ROOT/scripts/baton-fixtures.json}"

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "required command not found: $1" >&2
    exit 1
  fi
}

assert_eq() {
  local label="$1"
  local expected="$2"
  local actual="$3"
  if [[ "$expected" != "$actual" ]]; then
    echo "FAIL [$label]: expected '$expected', got '$actual'" >&2
    exit 1
  fi
}

json_count() {
  local file="$1"
  local command="$2"
  local field="$3"
  "$BATON_BIN" "$command" -f "$file" --output-format=json 2>/dev/null | jq -r "$field | length"
}

verify_syncs() {
  local file="$1"
  local sync_id="$2"
  local sync_type="$3"
  local label="$4"
  local output
  output=$("$BATON_BIN" syncs -f "$file" --output-format=json 2>/dev/null)

  jq -e --arg id "$sync_id" --arg type "$sync_type" '
    (.syncs | length) == 1 and
    .syncs[0].id == $id and
    .syncs[0].syncType == $type and
    (.syncs[0].startedAt | length) > 0 and
    (.syncs[0].endedAt | length) > 0 and
    (.syncs[0].syncToken | length) > 0
  ' <<<"$output" >/dev/null

  echo "  ok syncs ($label)"
}

verify_fixture() {
  local name path sync_id sync_type verify_grants_json
  name=$(jq -r '.name' <<<"$1")
  path=$(jq -r '.path' <<<"$1")
  sync_id=$(jq -r '.syncId' <<<"$1")
  sync_type=$(jq -r '.syncType' <<<"$1")
  verify_grants_json=$(jq -r '.verifyGrantsJson' <<<"$1")

  local file="$ROOT/$path"
  if [[ ! -f "$file" ]]; then
    echo "FAIL [$name]: fixture not found: $file" >&2
    exit 1
  fi

  echo "== $name ($path) =="

  verify_syncs "$file" "$sync_id" "$sync_type" "$name"

  local expected actual json_key
  for json_key in resources entitlements resourceTypes; do
    expected=$(jq -r ".stats.$json_key" <<<"$1")
    case "$json_key" in
      resources) actual=$(json_count "$file" resources '.resources') ;;
      entitlements) actual=$(json_count "$file" entitlements '.entitlements') ;;
      resourceTypes) actual=$(json_count "$file" resource-types '.resourceTypes') ;;
    esac
    assert_eq "$name:$json_key" "$expected" "$actual"
    echo "  ok $json_key count=$actual"
  done

  echo "  c1z: $file"
  local stats_json
  if ! stats_json=$("$BATON_BIN" stats -f "$file" --output-format=json 2>/dev/null); then
    echo "FAIL [$name]: baton stats failed for $file" >&2
    exit 1
  fi
  echo "  stats: $(jq -c . <<<"$stats_json")"

  local stat_key
  for stat_key in group user; do
    expected=$(jq -r ".stats.$stat_key" <<<"$1")
    actual=$(jq -r --arg k "$stat_key" '.stats.resourcesByResourceType.[$k] // empty' <<<"$stats_json")
    assert_eq "$name:stats.resourcesByResourceType:$stat_key" "$expected" "$actual"
    echo "  ok stats $stat_key count=$actual"
  done

  if [[ "$verify_grants_json" == "true" ]]; then
    expected=$(jq -r '.stats.grants' <<<"$1")
    actual=$(json_count "$file" grants '.grants')
    assert_eq "$name:grants-json" "$expected" "$actual"
    echo "  ok grants json count=$actual"
  fi
}

main() {
  require_cmd go
  require_cmd jq

  if [[ ! -x "$BATON_BIN" ]]; then
    echo "baton binary not found at $BATON_BIN (run make build first)" >&2
    exit 1
  fi

  if [[ ! -f "$FIXTURES_FILE" ]]; then
    echo "fixtures file not found: $FIXTURES_FILE" >&2
    exit 1
  fi

  local count
  count=$(jq '.fixtures | length' "$FIXTURES_FILE")
  echo "running baton tests against $count fixture(s)"

  local i fixture
  for ((i = 0; i < count; i++)); do
    fixture=$(jq -c ".fixtures[$i]" "$FIXTURES_FILE")
    verify_fixture "$fixture"
  done

  echo "all baton tests passed"
}

main "$@"
