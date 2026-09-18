#!/bin/sh
set -eu

fail() { printf 'joined-btf25-temporal-smoke: %s\n' "$*" >&2; exit 2; }
command -v docker >/dev/null 2>&1 || fail "missing prerequisite: docker"
docker compose version >/dev/null 2>&1 || fail "missing prerequisite: docker compose v2"
command -v openssl >/dev/null 2>&1 || fail "missing prerequisite: openssl"
command -v timeout >/dev/null 2>&1 || fail "missing prerequisite: timeout"
command -v python3 >/dev/null 2>&1 || fail "missing prerequisite: python3"

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
victoria=${VICTORIA_ROOT:-$(CDPATH= cd -- "$root/../../../victoria-hft/victoria-hft" 2>/dev/null && pwd || true)}
[ -n "$victoria" ] && [ -f "$victoria/research/ai_ach/go.mod" ] || fail "set VICTORIA_ROOT to the victoria-hft repository root"
input_mode=${BTF25_INPUT_MODE:-synthetic}
case "$input_mode" in
  synthetic)
    [ -z "${BTF25_DATASET_FILE:-}" ] || fail "synthetic mode refuses BTF25_DATASET_FILE"
    [ -z "${BTF25_DATASET_LICENSE_ACK_FILE:-}" ] || fail "synthetic mode refuses BTF25_DATASET_LICENSE_ACK_FILE"
    engine_input_args="--input-mode synthetic"
    ;;
  licensed)
    [ "${BTF25_LICENSED_CORPUS_OPT_IN:-}" = "I_ACKNOWLEDGE_BTF2_V1_NONCOMMERCIAL_LICENSE" ] || fail "licensed mode requires BTF25_LICENSED_CORPUS_OPT_IN=I_ACKNOWLEDGE_BTF2_V1_NONCOMMERCIAL_LICENSE"
    [ -f "${BTF25_DATASET_FILE:-}" ] || fail "licensed mode requires BTF25_DATASET_FILE regular file"
    [ -f "${BTF25_DATASET_LICENSE_ACK_FILE:-}" ] || fail "licensed mode requires BTF25_DATASET_LICENSE_ACK_FILE regular file"
    BTF25_CORPUS_FILE=$(CDPATH= cd -- "$(dirname -- "$BTF25_DATASET_FILE")" && printf '%s/%s\n' "$(pwd -P)" "$(basename -- "$BTF25_DATASET_FILE")")
    BTF25_LICENSE_ACK_FILE=$(CDPATH= cd -- "$(dirname -- "$BTF25_DATASET_LICENSE_ACK_FILE")" && printf '%s/%s\n' "$(pwd -P)" "$(basename -- "$BTF25_DATASET_LICENSE_ACK_FILE")")
    export BTF25_CORPUS_FILE BTF25_LICENSE_ACK_FILE
    engine_input_args="--input-mode licensed --dataset /run/btf25-input/corpus.parquet --license-ack /run/btf25-input/license-ack.json"
    ;;
  *) fail "BTF25_INPUT_MODE must be synthetic or licensed" ;;
esac
project="joined-btf25-temporal-smoke-$$"
local_dir="$root/.local/joined-smoke"
secret_dir="$root/.local/joined-secrets"
rm -rf "$local_dir" "$secret_dir"
mkdir -p "$local_dir" "$secret_dir"

output_root=${BTF25_OUTPUT_ROOT:-$root/.local/btf25-temporal-smoke}
mkdir -p "$output_root"
output_root=$(CDPATH= cd -- "$output_root" && pwd -P)
case "$output_root" in "$root"/*) ;; *) fail "BTF25_OUTPUT_ROOT must resolve beneath the llm-temporal-worker Go root" ;; esac
output_parent=$(dirname -- "$output_root")
output_name=$(basename -- "$output_root")
timeout 60 docker run --rm --volume "$output_parent:/output-parent:z" docker.io/library/alpine:3.22.1@sha256:4bcff63911fcb4448bd4fdacec207030997caf25e9bea4045fa6c8c44de311d1 rm -rf "/output-parent/$output_name"
mkdir -p "$output_root"
: > "$output_root/timeout-plan.json"
: > "$output_root/engine-result.json"
: > "$output_root/engine-error.log"
: > "$output_root/verification-result.json"
: > "$output_root/joined-evidence.json"
: > "$output_root/proof.json"
: > "$output_root/worker.log"
: > "$output_root/victoria-worker.log"
: > "$output_root/temporal.log"
: > "$output_root/joined-smoke.log"

export VICTORIA_ROOT="$victoria"
export BTF25_OUTPUT_ROOT="$output_root"
export JOINED_FIXTURE_ROOT="$root/integration/joined"
export LLMTW_CONTINUATION_KEY_FILE="$local_dir/continuation-hmac"
export LLMTW_WORKER_POSTGRES_PASSWORD="joined-smoke-postgres"
export LLMTW_COMPOSE_WORKER_CONFIG_FILE="$root/integration/joined/llm-config.yaml"
export LLMTW_COMPOSE_WORKER_CAPABILITIES_FILE="$root/integration/joined/capabilities.yaml"
export LLMTW_COMPOSE_WORKER_PRICES_FILE="$root/integration/joined/prices.yaml"
export BTF25_PROFILE_FILE="$victoria/research/ai_ach/competition/smoke/btf25-profile.json"
if [ "$input_mode" = synthetic ]; then
  export BTF25_CORPUS_FILE="$root/integration/joined/synthetic-no-btf2-corpus"
  export BTF25_LICENSE_ACK_FILE="$root/integration/joined/synthetic-no-btf2-license-ack.json"
fi


provider_mode=${BTF25_PROVIDER_MODE:-fake}
case "$provider_mode" in
  fake)
    [ -z "${BTF25_REAL_PROVIDER_API_KEY:-}" ] || fail "fake mode refuses BTF25_REAL_PROVIDER_API_KEY"
    provider_mock_service="provider-mock"
    ;;
  real)
    [ "${BTF25_REAL_PROVIDER_OPT_IN:-}" = "I_ACKNOWLEDGE_REAL_PROVIDER_COSTS_NO_PUBLICATION" ] || fail "real mode requires BTF25_REAL_PROVIDER_OPT_IN=I_ACKNOWLEDGE_REAL_PROVIDER_COSTS_NO_PUBLICATION"
    [ -n "${BTF25_REAL_PROVIDER_API_KEY:-}" ] || fail "real mode requires BTF25_REAL_PROVIDER_API_KEY"
    [ -f "${BTF25_REAL_PROVIDER_CONFIG_FILE:-}" ] || fail "real mode requires BTF25_REAL_PROVIDER_CONFIG_FILE"
    [ -f "${BTF25_REAL_PROVIDER_CAPABILITIES_FILE:-}" ] || fail "real mode requires BTF25_REAL_PROVIDER_CAPABILITIES_FILE"
    [ -f "${BTF25_REAL_PROVIDER_PRICES_FILE:-}" ] || fail "real mode requires BTF25_REAL_PROVIDER_PRICES_FILE"
    [ -f "${BTF25_REAL_PROVIDER_PROFILE_FILE:-}" ] || fail "real mode requires BTF25_REAL_PROVIDER_PROFILE_FILE"
    LLMTW_COMPOSE_WORKER_CONFIG_FILE=$(CDPATH= cd -- "$(dirname -- "$BTF25_REAL_PROVIDER_CONFIG_FILE")" && printf '%s/%s\n' "$(pwd -P)" "$(basename -- "$BTF25_REAL_PROVIDER_CONFIG_FILE")")
    LLMTW_COMPOSE_WORKER_CAPABILITIES_FILE=$(CDPATH= cd -- "$(dirname -- "$BTF25_REAL_PROVIDER_CAPABILITIES_FILE")" && printf '%s/%s\n' "$(pwd -P)" "$(basename -- "$BTF25_REAL_PROVIDER_CAPABILITIES_FILE")")
    LLMTW_COMPOSE_WORKER_PRICES_FILE=$(CDPATH= cd -- "$(dirname -- "$BTF25_REAL_PROVIDER_PRICES_FILE")" && printf '%s/%s\n' "$(pwd -P)" "$(basename -- "$BTF25_REAL_PROVIDER_PRICES_FILE")")
    BTF25_PROFILE_FILE=$(CDPATH= cd -- "$(dirname -- "$BTF25_REAL_PROVIDER_PROFILE_FILE")" && printf '%s/%s\n' "$(pwd -P)" "$(basename -- "$BTF25_REAL_PROVIDER_PROFILE_FILE")")
    export LLMTW_COMPOSE_WORKER_CONFIG_FILE LLMTW_COMPOSE_WORKER_CAPABILITIES_FILE LLMTW_COMPOSE_WORKER_PRICES_FILE BTF25_PROFILE_FILE
    provider_mock_service=""
    ;;
  *) fail "BTF25_PROVIDER_MODE must be fake or real" ;;
esac

smoke_port_base=$((20000 + ($$ % 20000)))
export LLMTW_COMPOSE_TEMPORAL_PORT="$smoke_port_base"
export LLMTW_COMPOSE_TEMPORAL_UI_PORT=$((smoke_port_base + 1))
export LLMTW_COMPOSE_REDIS_PORT=$((smoke_port_base + 2))
export LLMTW_COMPOSE_HEALTH_PORT=$((smoke_port_base + 3))
export LLMTW_COMPOSE_METRICS_PORT=$((smoke_port_base + 4))
runtime_containers=""
cleanup() {
  status=$?
  if [ "$status" -ne 0 ]; then
    for container_id in $runtime_containers; do
      service=$(docker inspect --format '{{ index .Config.Labels "com.docker.compose.service" }}' "$container_id" 2>/dev/null || true)
      [ -n "$service" ] || service="runtime-$container_id"
      docker logs "$container_id" > "$output_root/$service.log" 2>&1 || true
      docker logs "$container_id" >&2 || true
    done
    docker compose -p "$project" -f "$root/compose.yaml" -f "$root/integration/joined/compose.yaml" --profile worker --profile durable --profile btf25 logs --no-color temporal joined-smoke worker victoria-worker schema-install budget-bootstrap >&2 || true
  fi
  for container_id in $runtime_containers; do docker rm -f "$container_id" >/dev/null 2>&1 || true; done
  docker compose -p "$project" -f "$root/compose.yaml" -f "$root/integration/joined/compose.yaml" --profile worker --profile durable --profile btf25 down --volumes --remove-orphans >/dev/null 2>&1 || true
  rm -rf "$local_dir" "$secret_dir"
  exit "$status"
}
trap cleanup EXIT HUP INT TERM

openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=joined-smoke-ca -keyout "$local_dir/ca-key.pem" -out "$local_dir/ca.pem" >/dev/null 2>&1
openssl req -newkey rsa:2048 -nodes -subj /CN=joined-smoke -addext subjectAltName=DNS:joined-smoke -keyout "$local_dir/server-key.pem" -out "$local_dir/server.csr" >/dev/null 2>&1
openssl x509 -req -days 1 -in "$local_dir/server.csr" -CA "$local_dir/ca.pem" -CAkey "$local_dir/ca-key.pem" -CAcreateserial -copy_extensions copy -out "$local_dir/server.pem" >/dev/null 2>&1
printf '%s' 'eyJhbGciOiJub25lIn0.eyJzdWIiOiJqb2luZWQtc21va2UifQ.c2ln' > "$secret_dir/temporal-api-key"
printf '%s\n' 'joined-smoke-att-token' > "$secret_dir/att-token"
printf '%s\n' 'joined-smoke-search-token' > "$secret_dir/search-token"
printf '%s\n' 'joined-smoke-metaculus-token' > "$secret_dir/metaculus-token"
printf '%s\n' 'joined-smoke-credential-token' > "$secret_dir/aws-token"
printf '%s\n' 'joined-smoke-continuation-key-01' > "$local_dir/continuation-hmac"
chmod 0400 "$local_dir/ca-key.pem" "$local_dir/server-key.pem" "$local_dir/continuation-hmac" "$secret_dir"/*
chmod 0444 "$local_dir/ca.pem" "$local_dir/server.pem"

compose="docker compose -p $project -f $root/compose.yaml -f $root/integration/joined/compose.yaml"
wait_for_healthy_container() {
  service=$1
  container_id=$2
  attempts=0
  while [ "$attempts" -lt 90 ]; do
    state=$(docker inspect --format '{{if ne .State.Status "running"}}{{.State.Status}}{{else if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$container_id" 2>/dev/null || true)
    case "$state" in
      healthy) return ;;
      exited|dead) docker inspect --format 'status={{.State.Status}} exit_code={{.State.ExitCode}} oom_killed={{.State.OOMKilled}} error={{printf "%q" .State.Error}} finished_at={{.State.FinishedAt}}' "$container_id" >&2 || true; docker logs "$container_id" >&2 || true; fail "joined smoke service stopped before readiness: $service" ;;
    esac
    attempts=$((attempts + 1))
    sleep 2
  done
  fail "joined smoke service did not become healthy: $service"
}
start_service() {
  service=$1
  container_id=$(timeout 120 sh -c "$compose --profile worker --profile durable run -d --no-deps '$service'") || fail "failed to start joined smoke service: $service"
  [ -n "$container_id" ] || fail "joined smoke service returned no container ID: $service"
  runtime_containers="$runtime_containers $container_id"
  wait_for_healthy_container "$service" "$container_id"
}
start_compose_service() {
  service=$1
  timeout 120 sh -c "$compose --profile worker --profile durable up -d --no-deps '$service'" || fail "failed to create joined smoke service: $service"
  container_id=$(docker ps -aq --filter "label=com.docker.compose.project=$project" --filter "label=com.docker.compose.service=$service")
  [ -n "$container_id" ] || fail "joined smoke service was not created: $service"
  wait_for_healthy_container "$service" "$container_id"
}

for service in worker victoria-worker schema-install budget-bootstrap joined-smoke $provider_mock_service victoria-btf25-engine; do
  [ -n "$service" ] || continue
  timeout 600 sh -c "$compose --profile worker --profile durable --profile btf25 build '$service'"
done
if [ "$provider_mode" = real ]; then
  effective="$local_dir/real-effective-config.json"
  timeout 60 sh -c "$compose --profile worker --profile durable run --rm -T --no-deps worker print-effective-config --config /etc/llmtw/config.yaml" > "$effective"
  python3 -c 'import json,sys,urllib.parse; c=json.load(open(sys.argv[1], encoding="utf-8")); assert c["continuation"]["allow_provider_hosted_state"] is False; assert "demo-model" in c["models"]; assert c["endpoints"] and all(e["provider_storage"]["permitted"] is False and e["base_url"].startswith("https://") and e["auth"].get("name") == "BTF25_REAL_PROVIDER_API_KEY" and urllib.parse.urlparse(e["base_url"]).hostname not in {"joined-smoke","provider-mock","localhost","127.0.0.1","::1"} for e in c["endpoints"].values())' "$effective" || fail "real-provider config must route demo-model over HTTPS to a non-local provider through BTF25_REAL_PROVIDER_API_KEY with provider storage and hosted state disabled"
fi

timeout 60 docker run --rm --volume "$local_dir:/smoke:z" --volume "$secret_dir:/secrets:z" --volume "$output_root:/output:z" docker.io/library/alpine:3.22.1@sha256:4bcff63911fcb4448bd4fdacec207030997caf25e9bea4045fa6c8c44de311d1 sh -c 'mkdir -p /output/model /output/evaluator /output/validation /output/first /output/second /output/verification /output/control && chown 65532:65532 /secrets/temporal-api-key /smoke/continuation-hmac /secrets/att-token /secrets/search-token /secrets/metaculus-token /secrets/aws-token && chown -R 65532:65532 /output/model /output/evaluator /output/validation /output/first /output/second /output/verification /output/control'
for service in postgres worker-postgres redis temporal $provider_mock_service joined-smoke; do
  [ -n "$service" ] || continue
  start_compose_service "$service"
done
for service in schema-install redis-function-provisioner blob-volume-provisioner budget-bootstrap; do
  timeout 120 sh -c "$compose --profile worker --profile durable run --rm -T --no-deps '$service'" ||
    fail "joined smoke bootstrap service failed: $service"
done
[ -s "$local_dir/pricing-identity.json" ] || fail "budget bootstrap emitted no compiled pricing identity"
start_service worker
start_service victoria-worker

max_concurrency=${BTF25_MAX_CONCURRENCY:-4}
case "$max_concurrency" in ''|*[!0-9]*) fail "BTF25_MAX_CONCURRENCY must be an integer in [1,8]" ;; esac
[ "$max_concurrency" -ge 1 ] && [ "$max_concurrency" -le 8 ] || fail "BTF25_MAX_CONCURRENCY must be an integer in [1,8]"
set -- --profile "$BTF25_PROFILE_FILE" --max-concurrency "$max_concurrency"
if [ -n "${BTF25_OVERALL_TIMEOUT:-}" ]; then
  set -- "$@" --overall-timeout "$BTF25_OVERALL_TIMEOUT"
fi
if [ -n "${BTF25_COMMAND_TIMEOUT_SECONDS:-}" ]; then
  set -- "$@" --command-timeout-seconds "$BTF25_COMMAND_TIMEOUT_SECONDS"
fi
timeout_plan=$(python3 "$root/scripts/btf25_timeout_plan.py" "$@") || fail "invalid BTF25 frozen timeout plan"
printf '%s\n' "$timeout_plan" > "$output_root/timeout-plan.json"
overall_timeout=$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["overall_timeout"])' "$timeout_plan")
command_timeout=$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["command_timeout_seconds"])' "$timeout_plan")
if ! timeout "$command_timeout" sh -c "$compose --profile worker --profile durable --profile btf25 run --rm -T --no-deps victoria-btf25-engine --output-root /run/btf25-output --profile /run/btf25-input/profile.json --pricing-identity /run/smoke/pricing-identity.json --max-concurrency '$max_concurrency' --overall-timeout '$overall_timeout' $engine_input_args" > "$output_root/engine-result.json" 2> "$output_root/engine-error.log"; then
  cat "$output_root/engine-error.log" >&2
  fail "BTF25 Temporal engine failed; diagnostics preserved at $output_root/engine-error.log"
fi
[ -s "$output_root/control/references.env" ] || fail "BTF25 Temporal engine emitted no references"
. "$output_root/control/references.env"
for value in "$BTF25_MODEL_SHA256" "$BTF25_EVALUATOR_SHA256" "$BTF25_VALIDATION_SHA256" "$BTF25_FIRST_FORECAST_SHA256" "$BTF25_SECOND_FORECAST_SHA256" "$BTF25_VERIFICATION_SHA256"; do
  [ "${#value}" -eq 64 ] || fail "engine emitted malformed artifact SHA-256"
  case "$value" in *[!0-9a-f]*) fail "engine emitted malformed artifact SHA-256" ;; esac
done

timeout 300 sh -c "$compose --profile worker --profile durable --profile btf25 run --rm -T --no-deps --entrypoint /usr/local/bin/competition_runner victoria-btf25-engine smoke --first-forecast-artifacts /run/btf25-output/first --first-forecast-sha256 '$BTF25_FIRST_FORECAST_SHA256' --second-forecast-artifacts /run/btf25-output/second --second-forecast-sha256 '$BTF25_SECOND_FORECAST_SHA256' --model-artifacts /run/btf25-output/model --model-view-sha256 '$BTF25_MODEL_SHA256' --evaluator-artifacts /run/btf25-output/evaluator --evaluator-view-sha256 '$BTF25_EVALUATOR_SHA256' --validation-artifacts /run/btf25-output/validation --validation-sha256 '$BTF25_VALIDATION_SHA256' --output-artifacts /run/btf25-output/verification" > "$output_root/verification-result.json"
[ -s "$output_root/verification-result.json" ] || fail "strict verifier emitted no result"
timeout 30 sh -c "$compose exec -T joined-smoke wget --no-check-certificate -qO- https://127.0.0.1/evidence" > "$output_root/joined-evidence.json"
proof=$(python3 -c 'import json,sys
engine=json.load(open(sys.argv[1], encoding="utf-8"))
evidence=json.load(open(sys.argv[2], encoding="utf-8"))
timeout_plan=json.load(open(sys.argv[3], encoding="utf-8"))
input_mode=sys.argv[4]
assert engine["schema_version"] == "ai_ach.btf25_temporal_smoke_result/v3"
assert engine["purpose"] == "integration_only_not_performance"
assert engine["input_mode"] == input_mode
assert engine["selection_size"] == 25
assert engine["temporal_forecast_workflows"] == 50
assert engine["forecast_continuations"] >= 50
assert engine["returned_forecasts"] == 50
assert engine["platform_submit_enabled"] is False
assert 0 < engine["max_cost_microunits_per_forecast"] <= 5000000
assert engine["max_runtime_seconds_per_forecast"] == timeout_plan["profile_max_runtime_seconds"]
assert evidence["schema_version"] == "ai_ach.joined_smoke_evidence/v2"
assert evidence["att_counts"].get("question_revision") == 25
assert evidence["att_counts"].get("sealed_forecast") == 50
assert evidence["point_in_time_graph_forecasts"] == 50
assert evidence["returned_forecasts"] == 50
assert evidence["att_replay_requests"] >= 25
assert evidence["metaculus_submissions"] == 0
assert evidence["metaculus_comments"] == 0
assert evidence["search_requests"] == 0
proof={
 "schema_version":"ai_ach.btf25_joined_temporal_proof/v1",
 "purpose":engine["purpose"],
 "input_mode":input_mode,
 "license_gate":"synthetic_no_dataset" if input_mode == "synthetic" else "licensed_explicit_opt_in",
 "selection_size":engine["selection_size"],
 "temporal_forecast_workflows":engine["temporal_forecast_workflows"],
 "inference_continuations":engine["forecast_continuations"],
 "point_in_time_graph_forecasts":evidence["point_in_time_graph_forecasts"],
 "att_question_revision_ingress":evidence["att_counts"]["question_revision"],
 "att_sealed_forecast_ingress":evidence["att_counts"]["sealed_forecast"],
 "att_replay_requests":evidence["att_replay_requests"],
 "returned_forecasts":engine["returned_forecasts"],
 "platform_submit_enabled":engine["platform_submit_enabled"],
 "search_requests":evidence["search_requests"],
 "metaculus_submissions":evidence["metaculus_submissions"],
 "metaculus_comments":evidence["metaculus_comments"],
 "max_cost_microunits_per_forecast":engine["max_cost_microunits_per_forecast"],
 "max_runtime_seconds_per_forecast":engine["max_runtime_seconds_per_forecast"],
 "overall_timeout_seconds":timeout_plan["overall_timeout_seconds"],
}
print(json.dumps(proof,sort_keys=True,separators=(",",":")))' "$output_root/engine-result.json" "$output_root/joined-evidence.json" "$output_root/timeout-plan.json" "$input_mode") ||
  fail "BTF25 smoke did not prove the real Temporal workflow, point-in-time graph, continuation, ATT ingress replay idempotency, and return-only output"
printf '%s\n' "$proof" > "$output_root/proof.json"
printf 'BTF25_TEMPORAL_SMOKE=passed\nBTF25_INPUT_MODE=%s\nBTF25_PROVIDER_MODE=%s\nBTF25_ARTIFACT_ROOT=%s\nBTF25_PROOF=%s\nBTF25_NOTICE=integration-only synthetic/contract smoke; no benchmark claim and no platform publication\n' "$input_mode" "$provider_mode" "$output_root" "$proof"
