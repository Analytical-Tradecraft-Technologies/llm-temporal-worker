#!/usr/bin/env bash

# Collect only allowlisted, redacted release-evidence inputs. Command output,
# service output, and scanner diagnostics remain in a private temporary
# directory and are never copied to the retained artifact directory.
set -euo pipefail

root="$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
module_root="$root/golang"
collector="$root/scripts/release/collect.py"
artifact_dir=""
image_oci_layout=""
worker_error_metrics=""
verified_inputs=""

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
    --verified-inputs)
      [[ $# -ge 2 ]] || fail "--verified-inputs requires a directory"
      verified_inputs="$2"
      shift 2
      ;;
    --worker-error-metrics)
      [[ $# -ge 2 ]] || fail "--worker-error-metrics requires a Prometheus text snapshot"
      worker_error_metrics="$2"
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
artifact_dir="$(CDPATH='' cd -- "$artifact_dir" && pwd -P)"

if [[ "$image_oci_layout" != /* ]]; then
  image_oci_layout="$root/$image_oci_layout"
fi
image_oci_parent="$(dirname -- "$image_oci_layout")"
[[ -d "$image_oci_parent" && ! -L "$image_oci_parent" ]] || fail "temporary OCI directory parent must be a real directory"
image_oci_layout="$(CDPATH='' cd -- "$image_oci_parent" && pwd -P)/$(basename -- "$image_oci_layout")"
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

if [[ -n "$verified_inputs" ]]; then
  printf 'release evidence: reusing successful test, race, fuzz and Compose inputs\n'
  for name in test-summary race-summary fuzz-summary redis-summary temporal-summary compose-summary redis-log temporal-log compose-log; do
    [[ -f "$verified_inputs/$name.json" && ! -L "$verified_inputs/$name.json" ]] || fail "missing verified input: $name"
    cp -- "$verified_inputs/$name.json" "$artifact_dir/$name.json"
  done
else
  run_gate test_summary bash -c "cd \"$module_root\" && go test ./..."
  run_gate race_summary bash -c "cd \"$module_root\" && go test -race ./..."
  run_gate fuzz_summary bash -c "cd \"$module_root\" && bash \"$module_root/scripts/run-fuzz.sh\" smoke"
fi

printf "release evidence: memory benchmark (elapsed %ss)\n" "$SECONDS"
if ! bash -c "cd \"$module_root\" && go test ./engine -run '^$' -bench '^BenchmarkGenerateMemoryAdmissionAndCompile$' -benchmem -count=1" >"$temporary/benchmark.output" 2>&1; then
  fail "memory admission benchmark failed; inspect the trusted CI step output"
fi
python3 "$collector" benchmark-summary \
  --kind benchmark_summary \
  --input "$temporary/benchmark.output" \
  --output "$artifact_dir/benchmark-summary.json"

# Production worker metrics are never available to the trusted release job by
# default. A protected operator may supply a bounded Prometheus text snapshot
# explicitly; only its redacted count summary is retained in the bundle.
if [[ -n "$worker_error_metrics" ]]; then
  python3 "$root/scripts/release/slo-evidence.py" worker-error-summary \
    --input "$worker_error_metrics" \
    --output "$artifact_dir/worker-error-summary.json"
fi

python3 "$collector" fixture-manifest \
  --root "$module_root" \
  --output "$artifact_dir/fixture-manifest.json"

if [[ -z "$verified_inputs" ]]; then
  if ! docker compose -p "$compose_project" -f "$module_root/compose.yaml" up --wait --wait-timeout 180 -d redis temporal >"$temporary/compose-up.output" 2>&1; then
    fail "Redis and Temporal did not become healthy; inspect the trusted CI step output"
  fi
  compose_started=1

  bash "$root/scripts/release/collect-compose.sh" "$compose_project" "$artifact_dir"
fi

printf "release evidence: manifest and dependency metadata (elapsed %ss)\n" "$SECONDS"
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

go -C "$module_root" mod edit -json >"$temporary/go-mod.json"
python3 "$collector" dependency-license \
  --baseline "$module_root/tools/supplychainverify/baseline.json" \
  --go-mod "$temporary/go-mod.json" \
  --output "$artifact_dir/dependencies.json"

if [[ -z "$verified_inputs" ]]; then
  if ! IMAGE_VERIFY_OCI_LAYOUT="$image_oci_layout" make -C "$module_root" image-verify >"$temporary/image-verify.output" 2>&1; then
    # Print only fixed stage names, never raw build/runtime output or credentials.
    for stage in "OCI export" "OCI import (requires the containerd image store)" \
      "OCI extraction" "imported image lookup" "runtime checks"; do
      if grep -Fqx -- "image-verify: failed at $stage" "$temporary/image-verify.output"; then
        printf 'image-verify: failed at %s\n' "$stage" >&2
      fi
    done
    fail "temporary OCI image verification failed; raw command output was discarded"
  fi
  [[ -d "$image_oci_layout" && ! -L "$image_oci_layout" && -f "$image_oci_layout/oci-layout" && -f "$image_oci_layout/index.json" ]] || fail "image verification did not create a temporary OCI directory"
fi
printf "release evidence: collection complete (elapsed %ss)\n" "$SECONDS"
