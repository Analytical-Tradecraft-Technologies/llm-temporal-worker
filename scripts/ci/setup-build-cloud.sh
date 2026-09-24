#!/usr/bin/env bash

# Master-only remote builder. Never fall back to a runner-local build.
set -euo pipefail

[[ "${GITHUB_REPOSITORY:-}" == "Analytical-Tradecraft-Technologies/llm-temporal-worker" && "${GITHUB_REF:-}" == "refs/heads/master" ]] || {
  echo "Docker Build Cloud credentials may only be used on the canonical master branch" >&2
  exit 1
}
case "${GITHUB_EVENT_NAME:-}" in
  push|schedule|workflow_dispatch) ;;
  *) echo "Unsupported Docker Build Cloud event" >&2; exit 1 ;;
esac
: "${RUNNER_TEMP:?RUNNER_TEMP is required}"
: "${GITHUB_ENV:?GITHUB_ENV is required}"
: "${DOCKER_ACCOUNT:?Set the docker_push DOCKER_ACCOUNT variable to the token owner}"
: "${DOCKER_CLOUD_BUILDER:?Set the docker_push DOCKER_CLOUD_BUILDER variable to organization/builder}"
: "${DOCKER_ACCESS_TOKEN:?The docker_push DOCKER_ACCESS_TOKEN secret is required}"
[[ "$DOCKER_ACCOUNT" =~ ^[a-z0-9][a-z0-9_-]*$ && "$DOCKER_CLOUD_BUILDER" =~ ^analyticaltradecraft/[a-z0-9][a-z0-9_-]*$ ]] || {
  echo "Invalid Docker account or analyticaltradecraft cloud builder endpoint" >&2
  exit 1
}

readonly buildx_version="v0.37.1"
readonly buildx_sha256="9447199cdb435f25880548343c128a4b6650e8891ee598905d8d29d39a8e359b"
export DOCKER_CONFIG="${RUNNER_TEMP}/llmtw-cloud-docker"
umask 077
mkdir -p -- "${DOCKER_CONFIG}/cli-plugins"
# Remove credentials if setup fails before the workflow's always() cleanup.
trap 'rm -rf -- "${DOCKER_CONFIG}"' ERR
binary="${DOCKER_CONFIG}/cli-plugins/docker-buildx"
download="${DOCKER_CONFIG}/buildx-download"
curl --fail --location --retry 3 --silent --show-error \
  "https://github.com/docker/buildx/releases/download/${buildx_version}/buildx-${buildx_version}.linux-amd64" \
  --output "${download}"
printf '%s  %s\n' "${buildx_sha256}" "${download}" | sha256sum --check --status
install -m 0755 "${download}" "${binary}"
rm -f -- "${download}"

printf '%s' "$DOCKER_ACCESS_TOKEN" | docker login --username "$DOCKER_ACCOUNT" --password-stdin docker.io
unset DOCKER_ACCESS_TOKEN
builder="$(docker buildx create --driver cloud --use "$DOCKER_CLOUD_BUILDER")"
[[ "$builder" =~ ^[a-zA-Z0-9_.-]+$ ]]
docker buildx inspect "$builder" --bootstrap
# BUILDX_BUILDER also routes existing docker build and Compose builds remotely.
{
  printf 'DOCKER_CONFIG=%s\n' "$DOCKER_CONFIG"
  printf 'BUILDX_BUILDER=%s\n' "$builder"
} >> "$GITHUB_ENV"
trap - ERR
