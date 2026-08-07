#!/usr/bin/env bash

set -euo pipefail

readonly syft_version="v1.44.0"
readonly syft_sha256="0e91737aee2b5baf1d255b959630194a302335d848ff97bb07921eb6205b5f5a"

: "${RUNNER_TEMP:?RUNNER_TEMP must identify a runner-temporary directory}"
: "${GITHUB_PATH:?GITHUB_PATH must be available in GitHub Actions}"

tool_root="${RUNNER_TEMP}/llmtw-ci-tools"
bin_dir="${tool_root}/bin"
archive="${tool_root}/syft_${syft_version#v}_linux_amd64.tar.gz"
mkdir -p -- "${bin_dir}"
extract_dir="$(mktemp -d "${tool_root}/syft.XXXXXX")"
trap 'rm -rf -- "${extract_dir}" "${archive}"' EXIT
curl --fail --location --retry 3 --silent --show-error \
  "https://github.com/anchore/syft/releases/download/${syft_version}/syft_${syft_version#v}_linux_amd64.tar.gz" \
  --output "${archive}"
printf '%s  %s\n' "${syft_sha256}" "${archive}" | sha256sum --check --status
tar -xzf "${archive}" -C "${extract_dir}"
install -m 0755 "${extract_dir}/syft" "${bin_dir}/syft"
printf '%s\n' "${bin_dir}" >> "${GITHUB_PATH}"
