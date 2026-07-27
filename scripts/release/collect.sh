#!/usr/bin/env bash

# Collect only allowlisted, redacted release-evidence inputs. Command output,
# service output, and scanner diagnostics remain in a private temporary
# directory and are never copied to the retained artifact directory.
set -euo pipefail

root="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
module_root="$root/golang"
collector="$root/scripts/release/collect.py"
artifact_dir=""
image_oci_layout=""

fail() {
  printf 'release evidence collection failed: %s\n' "$1" >&2
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --artifact-dir)
      [[ $# -ge 2 ]] || fail "--artifact-dir requires a directory"
      artifact_dir="$2"
      shift 2
      ;;
    --image-oci-layout)
      [[ $# -ge 2 ]] || fail "--image-oci-layout requires a path"
      image_oci_layout="$2"
      shift 2
      ;;
    *)
      fail "unknown argument: $1"
      ;;
  esac
done

[[ -n "$artifact_dir" ]] || fail "--artifact-dir is required"
[[ -n "$image_oci_layout" ]] || fail "--image-oci-layout is required"
if [[ "$artifact_dir" != /* ]]; then
  artifact_dir="$root/$artifact_dir"
fi
if [[ -e "$artifact_dir" ]]; then
  [[ -d "$artifact_dir" && ! -L "$artifact_dir" ]] || fail "artifact directory must be a real directory"
  [[ -z "$(find "$artifact_dir" -mindepth 1 -maxdepth 1 -print -quit)" ]] || fail "artifact directory must be empty"
else
  mkdir -p -- "$artifact_dir"
fi
artifact_dir="$(CDPATH= cd -- "$artifact_dir" && pwd -P)"

if [[ "$image_oci_layout" != /* ]]; then
  image_oci_layout="$root/$image_oci_layout"
fi
image_oci_parent="$(dirname -- "$image_oci_layout")"
[[ -d "$image_oci_parent" && ! -L "$image_oci_parent" ]] || fail "temporary OCI directory parent must be a real directory"
image_oci_layout="$(CDPATH= cd -- "$image_oci_parent" && pwd -P)/$(basename -- "$image_oci_layout")"
[[ "$(basename -- "$image_oci_layout")" == "image.oci" ]] || fail "temporary OCI directory must use the image.oci filename"
case "$image_oci_layout" in
  "$artifact_dir"|"$artifact_dir"/*) fail "temporary OCI directory must be outside the artifact directory" ;;
esac
[[ ! -e "$image_oci_layout" && ! -L "$image_oci_layout" ]] || fail "temporary OCI directory path must not already exist"

temporary="$(mktemp -d "${TMPDIR:-/tmp}/llmtw-release-evidence.XXXXXX")"
compose_project="llmtw-release-evidence-${GITHUB_RUN_ID:-local}-$$"
compose_started=0

cleanup() {
  if [[ "$compose_started" == 1 ]]; then
    docker compose -p "$compose_project" -f "$module_root/compose.yaml" down --volumes --remove-orphans >/dev/null 2>&1 || true
  fi
  rm -rf -- "$temporary"
}
trap cleanup EXIT HUP INT TERM

run_gate() {
  local kind="$1"
  shift
  if ! "$@" >"$temporary/$kind.output" 2>&1; then
    fail "$kind gate failed; inspect the trusted CI step output"
  fi
  python3 "$collector" gate-summary \
    --kind "$kind" \
    --input "$temporary/$kind.output" \
    --output "$artifact_dir/${kind//_/-}.json"
}

run_gate test_summary bash -c "cd \"$module_root\" && go test ./..."
run_gate race_summary bash -c "cd \"$module_root\" && go test -race ./..."
run_gate fuzz_summary bash -c "cd \"$module_root\" && bash \"$module_root/scripts/run-fuzz.sh\" smoke"

if ! bash -c "cd \"$module_root\" && go test ./engine -run '^$' -bench '^BenchmarkGenerateMemoryAdmissionAndCompile$' -benchmem -count=1" >"$temporary/benchmark.output" 2>&1; then
  fail "memory admission benchmark failed; inspect the trusted CI step output"
fi
python3 "$collector" benchmark-summary \
  --kind benchmark_summary \
  --input "$temporary/benchmark.output" \
  --output "$artifact_dir/benchmark-summary.json"

python3 "$collector" fixture-manifest \
  --root "$module_root" \
  --output "$artifact_dir/fixture-manifest.json"

if ! docker compose -p "$compose_project" -f "$module_root/compose.yaml" up --wait --wait-timeout 180 -d redis temporal >"$temporary/compose-up.output" 2>&1; then
  fail "Redis and Temporal did not become healthy; inspect the trusted CI step output"
fi
compose_started=1

collect_service_summary() {
  local service="$1"
  local kind="$2"
  local -a containers=()
  mapfile -t containers < <(docker compose -p "$compose_project" -f "$module_root/compose.yaml" ps -q "$service")
  [[ ${#containers[@]} -eq 1 && -n "${containers[0]}" ]] || fail "$service did not have exactly one Compose container"
  local state health
  IFS='|' read -r state health < <(docker inspect --format '{{.State.Status}}|{{if .State.Health}}{{.State.Health.Status}}{{end}}' "${containers[0]}")
  python3 "$collector" service-summary \
    --kind "$kind" \
    --state "$state" \
    --health "$health" \
    --output "$artifact_dir/${kind//_/-}.json"
}

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
  if ! docker compose -p "$compose_project" -f "$module_root/compose.yaml" logs --no-color --timestamps "$@" >"$input" 2>&1; then
    fail "$kind collection failed; inspect the trusted CI step output"
  fi
  python3 "$collector" redacted-log \
    --kind "$kind" \
    --input "$input" \
    --output "$output"
}

# Keep the actual Compose output only in $temporary. The retained files contain
# fixed allowlisted event counts, never raw service text or credentials.
collect_redacted_log redis_log redis
collect_redacted_log temporal_log temporal
collect_redacted_log compose_log

command -v kubectl >/dev/null 2>&1 || fail "kubectl is required to render Kubernetes manifests"
expected_kubectl_version="${RELEASE_EVIDENCE_KUBECTL_VERSION:-}"
if [[ -n "$expected_kubectl_version" ]]; then
  actual_kubectl_version="$(kubectl version --client --output=json | python3 -c 'import json, sys; print(json.load(sys.stdin)["clientVersion"]["gitVersion"])')"
  [[ "$actual_kubectl_version" == "$expected_kubectl_version" ]] || fail "kubectl client version $actual_kubectl_version does not match required $expected_kubectl_version"
fi
manifest_entries=()
for source in \
  "$module_root/deploy/kubernetes/base" \
  "$module_root/deploy/kubernetes/examples/aws-workload-identity" \
  "$module_root/deploy/kubernetes/examples/azure-workload-identity" \
  "$module_root/deploy/kubernetes/examples/redis-tls"; do
  rendered="$temporary/$(basename "$source").yaml"
  kubectl kustomize "$source" >"$rendered"
  manifest_entries+=(--entry "${source#"$module_root/"}=$rendered")
done
python3 "$collector" rendered-manifests \
  "${manifest_entries[@]}" \
  --output "$artifact_dir/rendered-manifests.json"

python3 "$collector" dependency-license \
  --baseline "$module_root/tools/supplychainverify/baseline.json" \
  --output "$artifact_dir/dependencies.json"

if ! SECURITY_REPORT="$temporary/security-verify.json" make -C "$module_root" security-verify >"$temporary/security-verify.output" 2>&1; then
  fail "security verification failed; inspect the trusted CI step output"
fi
python3 "$collector" vulnerability-results \
  --input "$temporary/security-verify.json" \
  --output "$artifact_dir/vulnerabilities.json"

if ! IMAGE_VERIFY_OCI_LAYOUT="$image_oci_layout" make -C "$module_root" image-verify >"$temporary/image-verify.output" 2>&1; then
  fail "temporary OCI image verification failed; inspect the trusted CI step output"
fi
[[ -d "$image_oci_layout" && ! -L "$image_oci_layout" && -f "$image_oci_layout/oci-layout" && -f "$image_oci_layout/index.json" ]] || fail "image verification did not create a temporary OCI directory"
