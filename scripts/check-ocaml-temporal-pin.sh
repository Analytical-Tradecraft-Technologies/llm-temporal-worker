#!/usr/bin/env bash
set -euo pipefail

# Keep the package's immutable SDK source and both CI install paths aligned.
# The source and hash are intentionally repeated in package metadata and
# workflows, while the Cargo prefetch helper records the approved checkout.
# This check prevents any copy from silently drifting.
canonical_source='git+https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal.git'
source_reference_pattern='git\+https://github\.com/[^/]+/ocaml-temporal\.git#[0-9a-f]{40}'
prefetch_helper='scripts/ci/prefetch-temporal-sdk-cargo.sh'
files=(
  ocaml/llm_temporal_worker/llm-temporal-ocaml.opam
  .github/workflows/master.yml
  .github/workflows/pull-request.yml
)

source_references="$(
  { grep -hEo "$source_reference_pattern" "${files[@]}" || true; } \
    | sort
)"
source_count="$(printf '%s\n' "$source_references" | awk 'NF {count++} END {print count + 0}')"
pins="$(
  printf '%s\n' "$source_references" \
    | sed 's/.*#//' \
    | sort -u
)"
pin_count="$(printf '%s\n' "$pins" | awk 'NF {count++} END {print count + 0}')"

if [[ "$source_count" != "${#files[@]}" ]]; then
  printf 'Expected exactly %s OCaml Temporal Git sources across package metadata and CI; found %s.\n' \
    "${#files[@]}" "$source_count" >&2
  exit 1
fi

if [[ "$pin_count" != 1 ]]; then
  printf 'Expected one OCaml Temporal commit across package metadata and CI; found %s.\n' "$pin_count" >&2
  printf '%s\n' "$pins" >&2
  exit 1
fi

canonical_reference="${canonical_source}#${pins}"
for file in "${files[@]}"; do
  source_reference="$(
    { grep -Eo "$source_reference_pattern" "$file" || true; } \
      | sort
  )"
  if [[ "$source_reference" != "$canonical_reference" ]]; then
    printf 'Expected canonical OCaml Temporal source %s in %s; found %s.\n' \
      "$canonical_reference" "$file" "${source_reference:-none}" >&2
    exit 1
  fi
done

helper_pins="$(
  sed -nE 's/^readonly temporal_sdk_commit="([0-9a-f]{40})"$/\1/p' "$prefetch_helper"
)"
helper_pin_count="$(printf '%s\n' "$helper_pins" | awk 'NF {count++} END {print count + 0}')"

if [[ "$helper_pin_count" != 1 ]]; then
  printf 'Expected exactly one approved OCaml Temporal SDK commit in %s; found %s.\n' \
    "$prefetch_helper" "$helper_pin_count" >&2
  exit 1
fi

if [[ "$helper_pins" != "$pins" ]]; then
  printf 'Expected OCaml Temporal SDK prefetch pin %s to match source pin %s; found %s.\n' \
    "$prefetch_helper" "$pins" "$helper_pins" >&2
  exit 1
fi

printf 'OCaml Temporal dependency source: %s\n' "$canonical_source"
printf 'OCaml Temporal dependency pin: %s\n' "$pins"
