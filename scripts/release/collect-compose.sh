#!/usr/bin/env bash
# Capture only allowlisted summaries from the already-tested Compose project.
set -euo pipefail
compose_project="${1:?Compose project required}"
artifact_dir="${2:?Artifact directory required}"
root="$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
collector="$root/scripts/release/collect.py"
mkdir -p "$artifact_dir"
temporary="$(mktemp -d)"
trap 'rm -rf -- "$temporary"' EXIT
fail() { printf '%s\n' "$1" >&2; exit 1; }
collect_service_summary() {
  local service="$1"
  local kind="$2"
  local container
  container="$(docker ps -q --filter "label=com.docker.compose.project=$compose_project" --filter "label=com.docker.compose.service=$service")"
  [[ -n "$container" && "$container" != *$'\n'* ]] || fail "$service did not have exactly one Compose container"
  local state health
  IFS='|' read -r state health < <(docker inspect --format '{{.State.Status}}|{{if .State.Health}}{{.State.Health.Status}}{{end}}' "$container")
  python3 "$collector" service-summary \
    --kind "$kind" \
    --state "$state" \
    --health "$health" \
    --output "$artifact_dir/${kind//_/-}.json"
}

printf 'release evidence: capturing tested Compose services\n'
collect_service_summary redis redis_summary
collect_service_summary temporal temporal_summary
python3 "$collector" compose-summary --output "$artifact_dir/compose-summary.json"

collect_redacted_log() {
  local kind="$1"
  local output=""
  shift
  case "$kind" in
    redis_log) output="$artifact_dir/redis-log.json" ;;
    temporal_log) output="$artifact_dir/temporal-log.json" ;;
    compose_log) output="$artifact_dir/compose-log.json" ;;
    *) fail "unsupported redacted log kind: $kind" ;;
  esac
  local input="$temporary/$kind.raw.log"
  local service container
  : > "$input"
  for service in "$@"; do
    container="$(docker ps -q --filter "label=com.docker.compose.project=$compose_project" --filter "label=com.docker.compose.service=$service")"
    if ! docker logs --timestamps "$container" >>"$input" 2>&1; then
      fail "$kind log collection failed"
    fi
  done
  python3 "$collector" redacted-log \
    --kind "$kind" \
    --input "$input" \
    --output "$output"
}

# Keep the actual Compose output only in $temporary. The retained files contain
# fixed allowlisted event counts, never raw service text or credentials.
collect_redacted_log redis_log redis
collect_redacted_log temporal_log temporal
collect_redacted_log compose_log redis temporal

printf 'release evidence: Compose summaries complete (%ss)\n' "$SECONDS"
