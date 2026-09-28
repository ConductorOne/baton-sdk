#!/usr/bin/env bash
# Scan this module for vulnerabilities reachable from its own code and bump the
# affected dependencies to their fixed versions.
#
# Only symbol-level findings count: govulncheck also reports vulnerabilities in
# modules and packages the code imports but never calls, and those cannot
# affect anything built on this SDK. Standard-library findings raise the `go`
# directive to the fixed Go release: that is the floor every dependent
# inherits when it takes the new SDK, and the binaries that matter are theirs.
# This repository's own builds follow, since the go line selects the
# toolchain. Findings with no fixed version yet are reported only.
#
# Edits go.mod, go.sum and vendor/ in place, the same way `make update-deps`
# does. Writes, under $GOVULN_OUT (default $RUNNER_TEMP/govulncheck, or
# /tmp/govulncheck outside GitHub Actions):
#   govulncheck.json  the raw scan
#   findings.tsv      kind, module, current, fixed, advisory — one row each
#   pr-body.md        the pull request body / step summary
# and, when GITHUB_OUTPUT is set, `changed` and `clean` for the workflow.
#
# Run it locally from the repository root: scripts/govulncheck-fix.sh
set -euo pipefail

out="${GOVULN_OUT:-${RUNNER_TEMP:-/tmp}/govulncheck}"
mkdir -p "$out"
report="$out/govulncheck.json"
findings="$out/findings.tsv"
body="$out/pr-body.md"

# Exit status 3 means "vulnerabilities found" in text mode; JSON mode returns
# 0 either way, but tolerate 3 so a change there does not break the run.
status=0
govulncheck -json ./... > "$report" || status=$?
if [ "$status" -ne 0 ] && [ "$status" -ne 3 ]; then
  echo "govulncheck failed with status $status" >&2
  exit "$status"
fi

# One row per reachable finding. trace[0] is the vulnerable symbol's own
# frame, so its module is the one to bump; a finding without a function there
# is module- or package-level, which means imported but not called.
jq -r '
  select(.finding != null) | .finding
  | select(.trace[0].function != null)
  | (if (.fixed_version // "") == "" then "unfixed"
     elif .trace[0].module == "stdlib" then "stdlib"
     else "module" end) as $kind
  | [$kind, .trace[0].module, .trace[0].version, (if (.fixed_version // "") == "" then "-" else .fixed_version end), .osv]
  | @tsv' "$report" | sort -u > "$findings"

highest_version() {
  jq -Rrs '
    def semver_key:
      ltrimstr("v") | split("+")[0]
      | capture("^(?<core>[^-]+)(?:-(?<pre>.*))?$")
      | [(.core | split(".") | map(tonumber)), (.pre == null),
         ((.pre // "") | split(".")
          | map(if test("^[0-9]+$") then [0, tonumber] else [1, .] end))];
    split("\n") | map(select(length > 0)) | max_by(semver_key) // empty'
}

# The same module can carry several advisories with different fixed versions;
# the highest wins, which satisfies all of them.
bumped=()
targets=()
while IFS= read -r mod; do
  [ -n "$mod" ] || continue
  fixed=$(awk -F'\t' -v m="$mod" '$1 == "module" && $2 == m {print $4}' "$findings" | highest_version)
  current=$(go list -m -mod=mod -f '{{.Version}}' "$mod")
  echo "bumping $mod $current -> $fixed"
  targets+=("$mod@$fixed")
  bumped+=("$mod"$'\t'"$current"$'\t'"$fixed")
done < <(awk -F'\t' '$1 == "module" {print $2}' "$findings" | sort -u)

# Standard-library findings: the highest fixed Go release becomes the `go`
# directive, when it is newer than the one go.mod already has. govulncheck
# reports the fix as "v1.27.3" or "go1.27.3"; go.mod wants "1.27.3".
go_from=""
go_to=""
stdlib_fixed=""
stdlib_rows=$(awk -F'\t' '$1 == "stdlib" && $4 != "-" {print $4}' "$findings")
if [ -n "$stdlib_rows" ]; then
  stdlib_fixed=$(printf '%s\n' "$stdlib_rows" | sed -e 's/^go//' -e 's/^v//' | highest_version)
fi
if [ -n "$stdlib_fixed" ]; then
  go_from=$(go mod edit -json | jq -r .Go)
  if [ "$(printf '%s\n' "$stdlib_fixed" "$go_from" | highest_version)" != "$go_from" ]; then
    go_to="$stdlib_fixed"
    echo "raising the go directive $go_from -> $go_to"
  fi
fi

# A bump can leave the tree unbuildable when a sibling module has to move
# with it: a new API in the bumped module, an old caller vendored from
# another (bumping the otel log exporter without the otelzap bridge does
# this). One retry moves the modules whose vendored packages failed to
# compile to their latest release. If that does not build either, this is
# not a pull request to open: report it, leave the edited tree in place for
# whoever looks, and fail the run.
build_log="$out/build.log"
build_ok=true
companions=()
if [ "${#targets[@]}" -gt 0 ] || [ -n "$go_to" ]; then
  if [ "${#targets[@]}" -gt 0 ]; then
    go get "${targets[@]}"
  fi
  if [ -n "$go_to" ]; then
    selected_go=$(go mod edit -json | jq -r .Go)
    if [ "$(printf '%s\n' "$go_to" "$selected_go" | highest_version)" != "$selected_go" ]; then
      go mod edit -go="$go_to"
    fi
  fi
  go mod tidy
  go mod vendor
  if ! go build ./... > "$build_log" 2>&1; then
    while IFS= read -r pkg; do
      mod=$(go list -mod=mod -f '{{with .Module}}{{.Path}}{{end}}' "$pkg" 2>/dev/null || true)
      if [ -n "$mod" ] && [ "$mod" != "$(go list -m)" ]; then
        companions+=("$mod")
      fi
    done < <(sed -n 's/^# \([^ ]*\)$/\1/p' "$build_log" | sort -u)
    if [ "${#companions[@]}" -gt 0 ]; then
      echo "build failed; retrying with the failing packages' modules at latest: ${companions[*]}" >&2
      go get "${companions[@]/%/@latest}"
      go mod tidy
      go mod vendor
    fi
    if [ "${#companions[@]}" -eq 0 ] || ! go build ./... > "$build_log" 2>&1; then
      build_ok=false
      echo "go build ./... failed after the bumps; see $build_log" >&2
    fi
  fi
fi

if [ -n "$go_to" ]; then
  go_to=$(go mod edit -json | jq -r .Go)
fi

if [ "$build_ok" = true ] && [ "${#targets[@]}" -gt 0 ]; then
  for target in "${targets[@]}"; do
    mod="${target%@*}"
    fixed="${target##*@}"
    selected=$(go list -m -mod=mod -f '{{.Version}}' "$mod")
    if [ "$(printf '%s\n' "$fixed" "$selected" | highest_version)" != "$selected" ]; then
      echo "$mod resolved to $selected, below required fix $fixed; no pull request will be opened" >&2
      exit 1
    fi
  done
fi

changed=false
clean=false
if [ ! -s "$findings" ]; then
  clean=true
fi
if [ "$build_ok" = true ] && [ -n "$(git status --porcelain -- go.mod go.sum vendor)" ]; then
  changed=true
fi
if [ -n "${GITHUB_OUTPUT:-}" ]; then
  echo "changed=$changed" >> "$GITHUB_OUTPUT"
  echo "clean=$clean" >> "$GITHUB_OUTPUT"
fi

# The body doubles as the step summary. Advisories link to the Go
# vulnerability database entry, which carries the description and the
# affected symbols.
advisories() {
  awk -F'\t' -v k="$1" -v m="$2" '$1 == k && $2 == m {print $5}' "$findings" | sort -u \
    | sed 's#^\(.*\)$#[\1](https://pkg.go.dev/vuln/\1)#' | paste -sd ',' - | sed 's/,/, /g'
}

{
  total=$(wc -l < "$findings" | tr -d ' ')
  if [ "$total" -eq 0 ]; then
    echo "govulncheck found no vulnerability reachable from this module's code."
  else
    echo "govulncheck found $total reachable finding(s)."
  fi
  echo
  if [ "${#targets[@]}" -gt 0 ] || [ -n "$go_to" ]; then
    echo "## Bumps"
    echo
    echo "| Module | Current | Fixed | Advisories |"
    echo "|---|---|---|---|"
    # Guarded: expanding an empty array trips `set -u` on bash 3.2 (macOS).
    if [ "${#bumped[@]}" -gt 0 ]; then
      for row in "${bumped[@]}"; do
        IFS=$'\t' read -r mod current fixed <<<"$row"
        echo "| \`$mod\` | $current | $fixed | $(advisories module "$mod") |"
      done
    fi
    if [ -n "$go_to" ]; then
      echo "| \`go\` directive (standard library) | $go_from | $go_to | $(advisories stdlib stdlib) |"
    fi
    echo
    if [ -n "$go_to" ]; then
      echo "The \`go\` directive is the floor every dependent inherits when it takes this SDK; a dependent still on an older Go picks up the newer toolchain on its next build."
      echo
    fi
    if [ "$build_ok" = true ]; then
      echo "go.mod, go.sum and vendor/ are updated; \`go build ./...\` passes."
      if [ "${#companions[@]}" -gt 0 ]; then
        # shellcheck disable=SC2016 # the backticks are Markdown, not a command substitution
        list=$(printf '`%s`, ' "${companions[@]}" | sed 's/, $//')
        echo "The bumps alone did not build; these modules moved to their latest release with them: $list."
      fi
    else
      echo "**The bumped tree does not build**, so no pull request was opened. A sibling module probably has to move with the bump; the last lines of \`go build ./...\`:"
      echo
      echo '```'
      tail -n 20 "$build_log"
      echo '```'
    fi
    echo
  fi
  if [ -n "$(awk -F'\t' '$1 == "stdlib"' "$findings")" ] && [ -z "$go_to" ]; then
    echo "## Standard library findings already covered by the go directive"
    echo
    echo "go.mod already requires Go $go_from, at or above every fixed release below; builds of this module carry the fix."
    echo
    while IFS=$'\t' read -r _ _ current fixed osv; do
      echo "* [$osv](https://pkg.go.dev/vuln/$osv) — $current, fixed in $fixed"
    done < <(awk -F'\t' '$1 == "stdlib"' "$findings")
    echo
  fi
  if [ -n "$(awk -F'\t' '$1 == "unfixed"' "$findings")" ]; then
    echo "## Reachable findings with no fixed version yet"
    echo
    while IFS=$'\t' read -r _ mod current _ osv; do
      echo "* [$osv](https://pkg.go.dev/vuln/$osv) — \`$mod\` $current"
    done < <(awk -F'\t' '$1 == "unfixed"' "$findings")
    echo
  fi
  echo "---"
  echo "Produced by \`scripts/govulncheck-fix.sh\` (symbol-level findings only). The weekly run refreshes this pull request while findings remain and closes it when the scan comes back clean."
} > "$body"

if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
  cat "$body" >> "$GITHUB_STEP_SUMMARY"
fi
cat "$body"

if [ "$build_ok" != true ]; then
  exit 1
fi
