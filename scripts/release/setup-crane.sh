#!/usr/bin/env bash

set -euo pipefail

readonly crane_version="v0.20.3"
readonly crane_archive_sha256="36c67a932f489b3f2724b64af90b599a8ef2aa7b004872597373c0ad694dc059"

: "${RUNNER_TEMP:?RUNNER_TEMP must identify a runner-temporary directory}"
: "${GITHUB_PATH:?GITHUB_PATH must be available in GitHub Actions}"

tool_root="${RUNNER_TEMP}/llmtw-release-tools"
bin_dir="${tool_root}/bin"
archive="${tool_root}/go-containerregistry-${crane_version}-Linux-x86_64.tar.gz"
mkdir -p -- "${bin_dir}"

curl --fail --location --retry 3 --silent --show-error \
  "https://github.com/google/go-containerregistry/releases/download/${crane_version}/go-containerregistry_Linux_x86_64.tar.gz" \
  --output "${archive}"
printf '%s  %s\n' "${crane_archive_sha256}" "${archive}" | sha256sum --check --status
tar --extract --gzip --file "${archive}" --directory "${bin_dir}" crane
chmod 0755 "${bin_dir}/crane"
rm -f -- "${archive}"
printf '%s\n' "${bin_dir}" >> "${GITHUB_PATH}"
