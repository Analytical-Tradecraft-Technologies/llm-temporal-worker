#!/usr/bin/env bash

# Run all checked-in fuzz seeds deterministically for pull requests, or split
# longer Go fuzzing across trusted-master shards. Any saved corpus input is
# retained below testdata/fuzz and is replayed by the smoke mode.
set -euo pipefail

GO="${GO:-go}"
mode="${1:-smoke}"

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
  "./llm FuzzRequestCanonicalizesClosedServiceClasses"
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
)

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
  echo "fuzz $package $target for $duration"
  "$GO" test "$package" -run '^$' -fuzz "^${target}$" -fuzztime "$duration" -parallel=1
}

case "$mode" in
  smoke)
    for entry in "${targets[@]}"; do
      read -r package target <<<"$entry"
      run_seed_replay "$package" "$target"
    done
    ;;
  shard)
    shard="${2:?usage: scripts/run-fuzz.sh shard <0|1|2>}"
    if [[ ! "$shard" =~ ^[0-2]$ ]]; then
      echo "fuzz shard must be 0, 1, or 2" >&2
      exit 64
    fi
    # Go accepts a duration or an Nx execution budget for -fuzztime. Trusted
    # master supplies 250000x so each target has a fixed workload; the local
    # default remains a short duration for ad-hoc shard runs.
    duration="${FUZZ_TIME:-45s}"
    for index in "${!targets[@]}"; do
      if (( index % 3 != shard )); then
        continue
      fi
      read -r package target <<<"${targets[$index]}"
      run_fuzz "$package" "$target" "$duration"
    done
    ;;
  *)
    echo "usage: scripts/run-fuzz.sh [smoke|shard <0|1|2>]" >&2
    exit 64
    ;;
esac

# Use this command to reproduce a saved corpus input by its corpus path:
#   go test <package> -run '^<FuzzTarget>/<corpus-entry>$' -count=1
