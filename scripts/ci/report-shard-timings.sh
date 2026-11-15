#!/usr/bin/env bash

# Print a shard's "<item><TAB><seconds>" timing records to the job log and,
# under GitHub Actions, the step summary (#1570). The records name only fuzz
# targets or Go packages and their elapsed seconds, never test output. A shard
# that stopped before recording anything prints nothing.
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: report-shard-timings.sh LABEL TIMINGS" >&2
  exit 64
fi
label="$1"
timings="$2"
[[ -f "$timings" && ! -L "$timings" ]] || exit 0

total="$(awk -F '\t' '{ total += $2 } END { printf "%g", total }' "$timings")"
printf '%s durations (seconds, %s in total):\n' "$label" "$total"
cat -- "$timings"
if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
  {
    printf '| %s | Seconds |\n| --- | ---: |\n' "$label"
    # Literal Markdown backticks are intentional.
    # shellcheck disable=SC2016
    awk -F '\t' '{ printf "| `%s` | %s |\n", $1, $2 }' "$timings"
    printf '| Total | %s |\n' "$total"
  } >>"$GITHUB_STEP_SUMMARY"
fi
