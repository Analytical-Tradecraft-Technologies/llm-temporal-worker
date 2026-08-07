#!/usr/bin/env bash

set -euo pipefail

readonly trivy_version="v0.72.0"
readonly trivy_sha256="bbb64b9695866ce4a7a8f5c9592002c5961cab378577fa3f8a040df362b9b2ea"

: "${RUNNER_TEMP:?RUNNER_TEMP must identify a runner-temporary directory}"
: "${GITHUB_ENV:?GITHUB_ENV must be available in GitHub Actions}"
: "${GITHUB_PATH:?GITHUB_PATH must be available in GitHub Actions}"

tool_root="${RUNNER_TEMP}/llmtw-ci-tools"
bin_dir="${tool_root}/bin"
archive="${tool_root}/trivy_${trivy_version#v}_Linux-64bit.tar.gz"
cache_dir="${RUNNER_TEMP}/llmtw-trivy-cache"
mkdir -p -- "${bin_dir}" "${cache_dir}"
extract_dir="$(mktemp -d "${tool_root}/trivy.XXXXXX")"
trap 'rm -rf -- "${extract_dir}" "${archive}"' EXIT
curl --fail --location --retry 3 --silent --show-error \
  "https://github.com/aquasecurity/trivy/releases/download/${trivy_version}/trivy_${trivy_version#v}_Linux-64bit.tar.gz" \
  --output "${archive}"
printf '%s  %s\n' "${trivy_sha256}" "${archive}" | sha256sum --check --status
tar -xzf "${archive}" -C "${extract_dir}"
install -m 0755 "${extract_dir}/trivy" "${bin_dir}/trivy"
printf '%s\n' "${bin_dir}" >> "${GITHUB_PATH}"
printf 'TRIVY_CACHE_DIR=%s\n' "${cache_dir}" >> "${GITHUB_ENV}"
