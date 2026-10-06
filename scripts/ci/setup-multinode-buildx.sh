#!/usr/bin/env bash

# Uncredentialed stand-in for Docker Build Cloud in the merge queue.
#
# Build Cloud is a multi-node builder: one BuildKit node per platform. Buildx
# behaves differently on such builders (for example, it cannot export a
# multi-platform OCI archive from one, #971), so a runner-local single-node
# builder cannot catch those failures. This creates a local builder with the
# same shape (one linux/amd64 node and one linux/arm64 node) using the same
# pinned Buildx release as scripts/ci/setup-build-cloud.sh, then proves the
# builder really is multi-node by checking that Buildx rejects a
# multi-platform OCI export from it. No registry credentials are involved.
set -euo pipefail

if [[ -n "${DOCKER_ACCESS_TOKEN:-}" ]]; then
  echo "the local multi-node builder must not receive registry credentials" >&2
  exit 1
fi
: "${RUNNER_TEMP:?RUNNER_TEMP must identify a runner-temporary directory}"
: "${GITHUB_ENV:?GITHUB_ENV must be available in GitHub Actions}"

# Keep in step with scripts/ci/setup-build-cloud.sh.
readonly buildx_version="v0.37.1"
readonly buildx_sha256="9447199cdb435f25880548343c128a4b6650e8891ee598905d8d29d39a8e359b"
readonly buildkit_image="moby/buildkit:v0.33.1@sha256:cec9f139f45e93c5c69c60f8b07cfad9f43f4ef6b6a6cd917527fea5ff2e3dea"

tool_root="${RUNNER_TEMP}/llmtw-multinode"
export DOCKER_CONFIG="${tool_root}/docker"
mkdir -p -- "${DOCKER_CONFIG}/cli-plugins"
download="${tool_root}/buildx-download"
curl --fail --location --retry 3 --silent --show-error \
  "https://github.com/docker/buildx/releases/download/${buildx_version}/buildx-${buildx_version}.linux-amd64" \
  --output "${download}"
printf '%s  %s\n' "${buildx_sha256}" "${download}" | sha256sum --check --status
install -m 0755 "${download}" "${DOCKER_CONFIG}/cli-plugins/docker-buildx"
rm -f -- "${download}"

# The arm64 node runs on this amd64 runner under QEMU user emulation from
# Ubuntu's archive; its fix-binary registration works inside BuildKit.
sudo apt-get update
sudo apt-get install --yes qemu-user-static
binfmt_misc="${LLMTW_BINFMT_MISC_DIR:-/proc/sys/fs/binfmt_misc}"
if [[ ! -e "${binfmt_misc}/qemu-aarch64" ]]; then
  echo "arm64 emulation is not registered with binfmt_misc" >&2
  exit 1
fi

builder="llmtw-multinode-${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-1}"
builder="${builder//[^a-zA-Z0-9_.-]/-}"
# Buildx needs a distinct Docker endpoint per node; a second context for the
# same daemon provides one.
docker context create "${builder}-arm64" --docker "host=${DOCKER_HOST:-unix:///var/run/docker.sock}"
docker buildx create --name "${builder}" --driver docker-container \
  --driver-opt "image=${buildkit_image}" \
  --platform linux/amd64 --node "${builder}-amd64"
docker buildx create --name "${builder}" --append --driver docker-container \
  --driver-opt "image=${buildkit_image}" \
  --platform linux/arm64 --node "${builder}-arm64" "${builder}-arm64"
docker buildx inspect "${builder}" --bootstrap

probe="$(mktemp -d "${tool_root}/probe.XXXXXX")"
trap 'rm -rf -- "${probe}"' EXIT
printf 'FROM scratch\n' > "${probe}/Dockerfile"
if docker buildx build --builder "${builder}" --platform linux/amd64,linux/arm64 \
  --output "type=oci,dest=${probe}/probe.tar" "${probe}" 2> "${probe}/error"; then
  echo "builder ${builder} accepted a multi-platform OCI export, so it is not multi-node like Docker Build Cloud" >&2
  exit 1
fi
if ! grep -q 'multi-node' "${probe}/error"; then
  echo "multi-node probe failed for an unexpected reason:" >&2
  cat "${probe}/error" >&2
  exit 1
fi

{
  printf 'DOCKER_CONFIG=%s\n' "${DOCKER_CONFIG}"
  printf 'BUILDX_BUILDER=%s\n' "${builder}"
} >> "${GITHUB_ENV}"
