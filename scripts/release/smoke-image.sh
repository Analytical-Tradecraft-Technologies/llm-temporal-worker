#!/usr/bin/env bash

set -euo pipefail

fail() {
  echo "release image smoke: $*" >&2
  exit 1
}

: "${LLMTW_SMOKE_IMAGE:?LLMTW_SMOKE_IMAGE is required}"

root="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd -P)"
module_root="${root}/golang"
project="llmtw-image-smoke-${GITHUB_RUN_ID:-local}-$$"
temporary="$(mktemp -d "${TMPDIR:-/tmp}/llmtw-image-smoke.XXXXXX")"
key="${temporary}/continuation-hmac"
provider_image="llmtw/provider-mock:image-smoke-$$"

cleanup() {
  COMPOSE_PROJECT_NAME="${project}" \
    LLMTW_CONTINUATION_KEY_FILE="${key}" \
    LLMTW_WORKER_IMAGE="${LLMTW_SMOKE_IMAGE}" \
    LLMTW_PROVIDER_MOCK_IMAGE="${provider_image}" \
    docker compose -f "${module_root}/compose.yaml" --profile worker down \
      --volumes --remove-orphans --timeout 10 >/dev/null 2>&1 || true
  docker image rm --force "${provider_image}" >/dev/null 2>&1 || true
  rm -rf -- "${temporary}"
}
trap cleanup EXIT HUP INT TERM

command -v docker >/dev/null 2>&1 || fail "Docker is required"
docker info >/dev/null 2>&1 || fail "a running Docker daemon is required"
docker image inspect "${LLMTW_SMOKE_IMAGE}" >/dev/null 2>&1 || fail "the requested image is not loaded"

architecture="$(docker image inspect --format '{{.Architecture}}/{{.Os}}' "${LLMTW_SMOKE_IMAGE}")"
[[ "${architecture}" == "amd64/linux" ]] || fail "image platform is ${architecture}, want amd64/linux"

umask 077
od -An -N32 -tx1 /dev/urandom | tr -d '[:space:]' > "${key}"
[[ "$(wc -c < "${key}")" == 64 ]] || fail "could not create an ephemeral continuation key"
chmod 0444 "${key}"

export COMPOSE_PROJECT_NAME="${project}"
export LLMTW_CONTINUATION_KEY_FILE="${key}"
export LLMTW_WORKER_IMAGE="${LLMTW_SMOKE_IMAGE}"
export LLMTW_PROVIDER_MOCK_IMAGE="${provider_image}"
export LLMTW_COMPOSE_TEMPORAL_PORT=0
export LLMTW_COMPOSE_TEMPORAL_UI_PORT=0
export LLMTW_COMPOSE_REDIS_PORT=0
export LLMTW_COMPOSE_HEALTH_PORT=0
export LLMTW_COMPOSE_METRICS_PORT=0

# Build only the local, credential-free provider fixture. --no-build below is
# the guard that makes Compose run the caller-supplied production image rather
# than rebuilding the worker service from the checkout.
docker compose -f "${module_root}/compose.yaml" --profile worker build provider-mock
docker compose -f "${module_root}/compose.yaml" --profile worker up \
  --detach --wait --wait-timeout 300 --no-build

worker_container="$(docker compose -f "${module_root}/compose.yaml" ps -q worker)"
[[ -n "${worker_container}" ]] || fail "Compose did not create the worker container"
expected_image_id="$(docker image inspect --format '{{.Id}}' "${LLMTW_SMOKE_IMAGE}")"
actual_image_id="$(docker inspect --format '{{.Image}}' "${worker_container}")"
[[ "${actual_image_id}" == "${expected_image_id}" ]] || fail "Compose did not run the requested image"

entrypoint="$(docker inspect --format '{{.Path}}' "${worker_container}")"
arguments="$(docker inspect --format '{{json .Args}}' "${worker_container}")"
[[ "${entrypoint}" == "/usr/local/bin/llm-temporal-worker" ]] || fail "worker did not use the native production entrypoint"
[[ "${arguments}" == '["worker","--config","/etc/llmtw/config.yaml"]' ]] || fail "worker did not use the production command"

docker exec "${worker_container}" /usr/local/bin/llm-temporal-worker healthcheck \
  --url http://127.0.0.1:8080/health/live \
  --url http://127.0.0.1:8080/health/ready >/dev/null

printf 'production image startup/readiness smoke passed for %s\n' "${LLMTW_SMOKE_IMAGE}"
