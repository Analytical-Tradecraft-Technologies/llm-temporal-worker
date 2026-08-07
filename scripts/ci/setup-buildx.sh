#!/usr/bin/env bash

set -euo pipefail

readonly buildx_version="v0.16.2"
readonly buildx_sha256="43e4c928a0be38ab34e206c82957edfdd54f3e7124f1dadd7779591c3acf77ea"
readonly buildkit_image="moby/buildkit:v0.16.0@sha256:bc1fe18224dbcb92599139db0c745696c48ba9fd4ac24038d1fa81fdd7dcac27"

: "${RUNNER_TEMP:?RUNNER_TEMP must identify a runner-temporary directory}"
: "${GITHUB_ENV:?GITHUB_ENV must be available in GitHub Actions}"

tool_root="${RUNNER_TEMP}/llmtw-ci-tools"
docker_config="${tool_root}/docker"
plugin_dir="${docker_config}/cli-plugins"
binary="${plugin_dir}/docker-buildx"
download="${tool_root}/buildx-${buildx_version}-linux-amd64"

mkdir -p -- "${plugin_dir}"
curl --fail --location --retry 3 --silent --show-error \
  "https://github.com/docker/buildx/releases/download/${buildx_version}/buildx-${buildx_version}.linux-amd64" \
  --output "${download}"
printf '%s  %s\n' "${buildx_sha256}" "${download}" | sha256sum --check --status
install -m 0755 "${download}" "${binary}"
rm -f -- "${download}"

export DOCKER_CONFIG="${docker_config}"
builder="llmtw-${GITHUB_JOB:-ci}-${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-1}"
builder="${builder//[^a-zA-Z0-9_.-]/-}"
docker buildx create --name "${builder}" --driver docker-container \
  --driver-opt "image=${buildkit_image}" --use
docker buildx inspect --bootstrap
printf 'DOCKER_CONFIG=%s\n' "${docker_config}" >> "${GITHUB_ENV}"
