#!/usr/bin/env bash

# Run the race-enabled test suite for one shard of the module's packages.
# Trusted master splits `go test -race ./...` across parallel jobs (#1570);
# together the shards run every package that `go list ./...` reports exactly
# once. Work items are balanced by their measured durations in
# race-weights.tsv (see scripts/ci/balance-shards.py), and a package without a
# weight (for example a new one) costs the default below, so it still runs. A
# weight for a package that no longer exists fails the shard until
# race-weights.tsv is updated.
#
# A single slow test would otherwise bound whichever shard runs its package,
# so race-weights.tsv may carve such a test out as its own work item, written
# "<package> <TopLevelTest>". That item runs only the test (-run); the
# package's own item runs every other test, example and fuzz seed corpus in
# the package (-skip). A carved test that no longer exists fails the shard.
#
# Set RACE_TIMINGS_OUTPUT to append each work item's measured duration in the
# same "<item><TAB><seconds>" format, so the weights can be refreshed from
# recent master runs. --list prints the shard's work items without testing.
set -euo pipefail

GO="${GO:-go}"
list_only=false
if [[ "${1:-}" == --list ]]; then
  list_only=true
  shift
fi
shard="${1:-}"
count="${2:-}"
if [[ $# -ne 2 || ! "$shard" =~ ^[0-9]+$ || ! "$count" =~ ^[1-9][0-9]*$ ]] || (( shard >= count )); then
  echo "usage: scripts/run-race-shard.sh [--list] <index> <count> (0 <= index < count)" >&2
  exit 64
fi

script_dir="$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
weights="${RACE_WEIGHTS:-$script_dir/race-weights.tsv}"
balancer="$script_dir/../../scripts/ci/balance-shards.py"
cd -- "$(dirname -- "$script_dir")" || exit 1

# carved: "<package> <Test>" lines for every carved-out test.
carved="$(awk -F '\t' '$0 !~ /^#/ && $1 ~ /^[^ ]+ [A-Za-z_][A-Za-z0-9_]*$/ { print $1 }' "$weights")"
packages="$("$GO" list ./...)"
stale="$(printf '%s\n' "$carved" | awk 'NR == FNR { listed[$0] = 1; next } NF && !($1 in listed) { print $1 }' <(printf '%s\n' "$packages") -)"
if [[ -n "$stale" ]]; then
  echo "race shard: race-weights.tsv carves tests out of missing packages: $stale" >&2
  exit 1
fi
assigned="$(printf '%s\n%s\n' "$packages" "$carved" | python3 "$balancer" --default 5 "$weights" "$shard" "$count")"
if [[ "$list_only" == true ]]; then
  printf '%s\n' "$assigned"
  exit 0
fi

# carved_pattern PACKAGE: the ^(A|B)$ pattern of PACKAGE's carved tests, or
# nothing when it has none.
carved_pattern() {
  printf '%s\n' "$carved" | awk -v package="$1" '
    $1 == package { names = names (names == "" ? "" : "|") $2 }
    END { if (names != "") printf "^(%s)$", names }
  '
}

whole=()
separate=()
while IFS= read -r item; do
  if [[ "$item" == *" "* || -n "$(carved_pattern "$item")" ]]; then
    separate+=("$item")
  else
    whole+=("$item")
  fi
done <<<"$assigned"

output="$(mktemp "${TMPDIR:-/tmp}/llmtw-race-shard.XXXXXX")"
trap 'rm -f -- "$output"' EXIT

# record_timings ITEM: append each package result in $output, named ITEM when
# given. Only work item names and elapsed seconds leave the raw output, and a
# near-instant package is recorded as 0.1 seconds because weights must be
# positive.
record_timings() {
  [[ -n "${RACE_TIMINGS_OUTPUT:-}" ]] || return 0
  awk -v item="$1" '($1 == "ok" || $1 == "FAIL") && $3 ~ /^[0-9]+(\.[0-9]+)?s$/ {
    seconds = substr($3, 1, length($3) - 1) + 0
    printf "%s\t%g\n", (item == "" ? $2 : item), (seconds < 0.1 ? 0.1 : seconds)
  }' "$output" >>"$RACE_TIMINGS_OUTPUT"
}

status=0
if (( ${#whole[@]} > 0 )); then
  "$GO" test -race "${whole[@]}" -timeout 30m 2>&1 | tee "$output" || status=$?
  record_timings ""
fi
for item in ${separate[@]+"${separate[@]}"}; do
  package="${item% *}"
  if [[ "$item" == *" "* ]]; then
    test_name="${item##* }"
    # -run matching nothing passes silently, so prove the test still exists.
    listing="$("$GO" test -race -list "^${test_name}\$" "$package")"
    if ! grep -qxF -- "$test_name" <<<"$listing"; then
      echo "race shard: carved test $test_name no longer exists in $package; update race-weights.tsv" >&2
      exit 1
    fi
    selection=(-run "^${test_name}\$")
  else
    selection=(-skip "$(carved_pattern "$package")")
  fi
  "$GO" test -race "${selection[@]}" "$package" -timeout 30m 2>&1 | tee "$output" || status=$?
  record_timings "$item"
done
exit "$status"
