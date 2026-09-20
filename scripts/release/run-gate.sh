#!/usr/bin/env bash
set -euo pipefail
kind="${1:?summary kind required}"
output="${2:?summary path required}"
shift 2
mkdir -p -- "$(dirname -- "$output")"
root="$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
temporary="$(mktemp)"
trap 'rm -f -- "$temporary"' EXIT
printf 'release evidence: %s started\n' "$kind"
if "$@" >"$temporary" 2>&1; then
  python3 "$root/scripts/release/collect.py" gate-summary --kind "$kind" --input "$temporary" --output "$output"
  printf 'release evidence: %s passed (%ss)\n' "$kind" "$SECONDS"
else
  status=$?
  printf 'release evidence: %s failed (%ss); raw output discarded\n' "$kind" "$SECONDS" >&2
  exit "$status"
fi
