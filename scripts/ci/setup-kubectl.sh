#!/usr/bin/env bash

set -euo pipefail

readonly kubectl_version="v1.32.6"
readonly kubectl_sha256="0e31ebf882578b50e50fe6c43e3a0e3db61f6a41c9cded46485bc74d03d576eb"

: "${RUNNER_TEMP:?RUNNER_TEMP must identify a runner-temporary directory}"
: "${GITHUB_PATH:?GITHUB_PATH must be available in GitHub Actions}"

tool_root="${RUNNER_TEMP}/llmtw-ci-tools"
bin_dir="${tool_root}/bin"
download="${tool_root}/kubectl-${kubectl_version}-linux-amd64"

mkdir -p -- "${bin_dir}"
curl --fail --location --retry 3 --silent --show-error \
  "https://dl.k8s.io/release/${kubectl_version}/bin/linux/amd64/kubectl" \
  --output "${download}"
printf '%s  %s\n' "${kubectl_sha256}" "${download}" | sha256sum --check --status
install -m 0755 "${download}" "${bin_dir}/kubectl"
rm -f -- "${download}"
printf '%s\n' "${bin_dir}" >> "${GITHUB_PATH}"
