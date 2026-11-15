#!/usr/bin/env bash

# Run all checked-in fuzz seeds deterministically for pull requests, or split
# longer Go fuzzing across trusted-master shards. Any saved corpus input is
# retained below testdata/fuzz and is replayed by the smoke mode.
set -euo pipefail

GO="${GO:-go}"
mode="${1:-smoke}"
script_dir="$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

created_cache=""
if [[ -z "${GOCACHE:-}" ]]; then
  created_cache="$(mktemp -d "${TMPDIR:-/tmp}/llmtw-task18-fuzz-gocache.XXXXXX")"
  export GOCACHE="$created_cache"
fi
cleanup() {
  if [[ -n "$created_cache" ]]; then
    rm -rf "$created_cache"
  fi
}
trap cleanup EXIT HUP INT TERM

targets=(
  "./budget FuzzSlidingWindowBoundaries"
  "./llm FuzzCanonicalJSONIdempotent"
  "./llm FuzzFastJSONDecodingMatchesReference"
  "./llm FuzzItemEncodingMatchesLegacyMapEncoding"
  "./llm FuzzMarshaledRawJSON"
  "./llm FuzzRequestCanonicalizesClosedServiceClasses"
  "./llm FuzzRequestDigestLegacyCompatibility"
  "./llm/provider FuzzAssemblerEventSequences"
  "./llm/provider/anthropicmessages FuzzDecodeStream"
  "./llm/provider/bedrockmessages FuzzDecodeStream"
  "./llm/provider/openaichat FuzzDecodeStream"
  "./llm/provider/openairesponses FuzzDecodeStream"
  "./llm/schema FuzzSchemaCanonicalAndBounded"
  "./pricing FuzzParseDecimalAndCeil"
  "./pricing FuzzNanoUSDMaterialization"
  "./pricing FuzzUSDParseRoundTrip"
  "./routing FuzzPlannerCanonicalizesServiceClassFallbacks"
  "./routing FuzzPlannerNeverSelectsUnauthorizedClasses"
  "./routing FuzzProviderCacheKeyForkIdentity"
  "./state FuzzVerifyHandleNeverPanics"
  "./state FuzzCanonicalTranscriptNeverPanics"
  "./state FuzzCheckpointBlobCodecDecodeDeltaNeverPanics"
  "./storage/redis FuzzBudgetManifestMemberOrder"
  "./storage/redis FuzzParseBudgetStatusNano"
  "./storage/redis FuzzOperationCodecRoundTrip"
  "./storage/redis FuzzProviderStatusDecode"
)

# Shards are balanced by each target's measured duration in fuzz-weights.tsv
# (#1570): the heaviest remaining target goes to the lightest shard. Every
# target needs a weight and every weight needs a target, so adding or renaming
# a target fails here until its measured duration is recorded. Master records
# each target's duration in the fuzz evidence (FUZZ_TIMINGS_OUTPUT) in the same
# format, so the weights can be refreshed from recent runs.
fuzz_weights="$script_dir/fuzz-weights.tsv"
balancer="$script_dir/../../scripts/ci/balance-shards.py"

run_seed_replay() {
  local package="$1"
  local target="$2"
  echo "replay $package $target"
  "$GO" test "$package" -run "^${target}$" -count=1
}

run_fuzz() {
  local package="$1"
  local target="$2"
  local duration="$3"
  local started elapsed
  echo "fuzz $package $target for $duration"
  started="$(date +%s)"
  "$GO" test "$package" -run '^$' -fuzz "^${target}$" -fuzztime "$duration" -parallel=1
  elapsed=$(($(date +%s) - started))
  if [[ -n "${FUZZ_TIMINGS_OUTPUT:-}" ]]; then
    # Weights must be positive; a sub-second target still costs a second.
    printf '%s %s\t%s\n' "$package" "$target" "$((elapsed > 0 ? elapsed : 1))" >>"$FUZZ_TIMINGS_OUTPUT"
  fi
}

case "$mode" in
  smoke)
    for entry in "${targets[@]}"; do
      read -r package target <<<"$entry"
      run_seed_replay "$package" "$target"
    done
    ;;
  shard | list)
    shard="${2:-}"
    count="${3:-}"
    if [[ ! "$shard" =~ ^[0-9]+$ || ! "$count" =~ ^[1-9][0-9]*$ ]] || (( shard >= count )); then
      echo "usage: scripts/run-fuzz.sh $mode <index> <count> (0 <= index < count)" >&2
      exit 64
    fi
    assigned="$(printf '%s\n' "${targets[@]}" | python3 "$balancer" "$fuzz_weights" "$shard" "$count")"
    if [[ "$mode" == list ]]; then
      # Print the shard's targets without fuzzing, for planning and tests.
      printf '%s\n' "$assigned"
      exit 0
    fi
    # Go accepts a duration or an Nx execution budget for -fuzztime. Trusted
    # master supplies 250000x so each target has a fixed workload; the local
    # default remains a short duration for ad-hoc shard runs.
    duration="${FUZZ_TIME:-45s}"
    while IFS= read -r entry; do
      read -r package target <<<"$entry"
      run_fuzz "$package" "$target" "$duration"
    done <<<"$assigned"
    ;;
  *)
    echo "usage: scripts/run-fuzz.sh [smoke|shard <index> <count>|list <index> <count>]" >&2
    exit 64
    ;;
esac

# Use this command to reproduce a saved corpus input by its corpus path:
#   go test <package> -run '^<FuzzTarget>/<corpus-entry>$' -count=1
