#!/usr/bin/env bash

set -euo pipefail

readonly buildx_version="v0.21.2"
readonly buildx_sha256="b13bee81c3db12a4be7d0b9d042b64d0dd9ed116f7674dfac0ffdf2a71acfe3d"
readonly buildkit_image="moby/buildkit:v0.20.2@sha256:c457984bd29f04d6acc90c8d9e717afe3922ae14665f3187e0096976fe37b1c8"

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
