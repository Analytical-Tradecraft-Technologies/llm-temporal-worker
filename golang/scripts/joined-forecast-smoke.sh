#!/bin/sh
set -eu

fail() { printf 'joined-forecast-smoke: %s\n' "$*" >&2; exit 2; }
command -v docker >/dev/null 2>&1 || fail "missing prerequisite: docker"
docker compose version >/dev/null 2>&1 || fail "missing prerequisite: docker compose v2"
command -v openssl >/dev/null 2>&1 || fail "missing prerequisite: openssl"
command -v timeout >/dev/null 2>&1 || fail "missing prerequisite: timeout"

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
victoria=${VICTORIA_ROOT:-$(CDPATH= cd -- "$root/../../../victoria-hft/victoria-hft" 2>/dev/null && pwd || true)}
[ -n "$victoria" ] && [ -f "$victoria/research/ai_ach/go.mod" ] || fail "missing prerequisite: set VICTORIA_ROOT to the victoria-hft repository root"
project="joined-forecast-smoke-$$"
local_dir="$root/.local/joined-smoke"
secret_dir="$root/.local/joined-secrets"
rm -rf "$local_dir" "$secret_dir"
mkdir -p "$local_dir" "$secret_dir"
export VICTORIA_ROOT="$victoria"
export JOINED_FIXTURE_ROOT="$root/integration/joined"
export LLMTW_CONTINUATION_KEY_FILE="$local_dir/continuation-hmac"
export LLMTW_WORKER_POSTGRES_PASSWORD="joined-smoke-postgres"
export LLMTW_COMPOSE_WORKER_CONFIG_FILE="$root/integration/joined/llm-config.yaml"
export LLMTW_COMPOSE_WORKER_CAPABILITIES_FILE="$root/integration/joined/capabilities.yaml"
export LLMTW_COMPOSE_WORKER_PRICES_FILE="$root/integration/joined/prices.yaml"
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
    [ ! -s "$local_dir/forecast-result.json" ] || cat "$local_dir/forecast-result.json" >&2
    [ ! -s "$local_dir/forecast-error.log" ] || cat "$local_dir/forecast-error.log" >&2
    cp "$local_dir/forecast-result.json" "$root/.local/forecast-result.diagnostic.json" 2>/dev/null || true
    cp "$local_dir/forecast-error.log" "$root/.local/forecast-error.diagnostic.log" 2>/dev/null || true
    sh -c "$compose --profile worker --profile durable logs --no-color joined-smoke" > "$root/.local/joined-smoke.diagnostic.log" 2>&1 || true
    sh -c "$compose --profile worker --profile durable logs --no-color provider-mock" > "$root/.local/provider-mock.diagnostic.log" 2>&1 || true
    for container_id in $runtime_containers; do
      service_name=$(docker inspect --format '{{ index .Config.Labels "com.docker.compose.service" }}' "$container_id" 2>/dev/null || printf unknown)
      docker logs "$container_id" > "$root/.local/$service_name.diagnostic.log" 2>&1 || true
      docker logs "$container_id" >&2 || true
    done
  fi
  if [ "$status" -ne 0 ]; then
    docker compose -p "$project" -f "$root/compose.yaml" -f "$root/integration/joined/compose.yaml" --profile worker --profile durable --profile engine logs --no-color temporal joined-smoke worker victoria-worker schema-install budget-bootstrap >&2 || true
  fi
  for container_id in $runtime_containers; do
    docker rm -f "$container_id" >/dev/null 2>&1 || true
  done
  docker compose -p "$project" -f "$root/compose.yaml" -f "$root/integration/joined/compose.yaml" --profile worker --profile durable --profile engine down --volumes --remove-orphans >/dev/null 2>&1 || true
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
start_service() {
  service=$1
  container_id=$(timeout 120 sh -c "$compose --profile worker --profile durable run --rm -d --no-deps $service") ||
    fail "failed to start joined smoke service: $service"
  [ -n "$container_id" ] || fail "joined smoke service returned no container ID: $service"
  runtime_containers="$runtime_containers $container_id"
  attempts=0
  while [ "$attempts" -lt 90 ]; do
    state=$(docker inspect --format '{{if ne .State.Status "running"}}{{.State.Status}}{{else if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$container_id" 2>/dev/null || true)
    case "$state" in
      healthy) return ;;
      exited | dead)
        docker logs "$container_id" >&2 || true
        fail "joined smoke service stopped before readiness: $service"
        ;;
    esac
    attempts=$((attempts + 1))
    sleep 2
  done
  fail "joined smoke service did not become healthy: $service"
}
timeout 600 sh -c "$compose --profile worker --profile durable build worker victoria-worker schema-install budget-bootstrap joined-smoke provider-mock"
timeout 60 docker run --rm \
  --volume "$local_dir:/smoke:z" \
  --volume "$secret_dir:/secrets:z" \
  docker.io/library/alpine:3.22.1@sha256:4bcff63911fcb4448bd4fdacec207030997caf25e9bea4045fa6c8c44de311d1 \
  chown 65532:65532 /secrets/temporal-api-key /smoke/continuation-hmac /secrets/att-token /secrets/search-token /secrets/metaculus-token /secrets/aws-token
timeout 600 sh -c "$compose --profile worker --profile durable up -d --wait temporal worker-postgres redis joined-smoke provider-mock"
timeout 120 sh -c "$compose --profile worker --profile durable up -d schema-install redis-function-provisioner blob-volume-provisioner budget-bootstrap"
for service in schema-install redis-function-provisioner blob-volume-provisioner budget-bootstrap; do
  container_id=""
  for candidate in $(sh -c "$compose --profile worker --profile durable ps -q"); do
    candidate_service=$(docker inspect --format '{{ index .Config.Labels "com.docker.compose.service" }}' "$candidate")
    if [ "$candidate_service" = "$service" ]; then
      container_id=$candidate
      break
    fi
  done
  [ -n "$container_id" ] || fail "joined smoke service was not created: $service"
  exit_code=$(timeout 120 docker wait "$container_id") || fail "joined smoke service did not finish: $service"
  [ "$exit_code" = 0 ] || fail "joined smoke service failed: $service (exit $exit_code)"
done
start_service worker
start_service victoria-worker
result="$local_dir/forecast-result.json"
timeout 300 sh -c "$compose --profile worker --profile durable --profile engine run --rm -T --no-deps victoria-engine < '$victoria/research/ai_ach/competition/smoke/prophet-request.json'" > "$result" 2> "$local_dir/forecast-error.log"
[ -s "$result" ] || fail "Prophet engine returned no forecast"
proof=$(timeout 30 sh -c "$compose exec -T joined-smoke wget --no-check-certificate -qO- https://127.0.0.1/proof") || fail "joined lifecycle evidence is incomplete"
printf 'JOINED_FORECAST_RESULT=%s\n' "$(cat "$result")"
printf 'JOINED_FORECAST_EVIDENCE=%s\n' "$proof"
