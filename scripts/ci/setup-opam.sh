#!/usr/bin/env bash

set -euo pipefail

readonly opam_version="2.3.0"
readonly opam_sha256="324e78e3f33efeba279aacf9f9610cfec7b2df7d7e0e1640f75f09de85f96cc9"
readonly ocaml_version="5.2.0"

: "${RUNNER_TEMP:?RUNNER_TEMP must identify a runner-temporary directory}"
: "${GITHUB_ENV:?GITHUB_ENV must be available in GitHub Actions}"
: "${GITHUB_PATH:?GITHUB_PATH must be available in GitHub Actions}"

if ! bwrap --version > /dev/null 2>&1; then
  printf '%s\n' 'setup-opam requires a usable Bubblewrap (bwrap) runtime to preserve OPAM sandboxing' >&2
  exit 1
fi
if ! bwrap --unshare-user --unshare-net --ro-bind / / true > /dev/null 2>&1; then
  printf '%s\n' 'setup-opam requires Bubblewrap user and network namespace support to preserve OPAM sandboxing' >&2
  exit 1
fi

tool_root="${RUNNER_TEMP}/llmtw-ci-tools"
bin_dir="${tool_root}/bin"
download="${tool_root}/opam-${opam_version}-x86_64-linux"
opam_root="${RUNNER_TEMP}/llmtw-opam-root"

mkdir -p -- "${bin_dir}"
curl --fail --location --retry 3 --silent --show-error \
  "https://github.com/ocaml/opam/releases/download/${opam_version}/opam-${opam_version}-x86_64-linux" \
  --output "${download}"
printf '%s  %s\n' "${opam_sha256}" "${download}" | sha256sum --check --status
install -m 0755 "${download}" "${bin_dir}/opam"
rm -f -- "${download}"

export PATH="${bin_dir}:${PATH}"
export OPAMROOT="${opam_root}"
export OPAMSWITCH="${ocaml_version}"
if [[ ! -d "${opam_root}" ]]; then
  opam init --bare --no-setup --yes
fi
if opam switch list --short | grep -Fx -- "${ocaml_version}" > /dev/null; then
  opam switch set --yes "${ocaml_version}"
else
  opam switch create --yes "${ocaml_version}"
fi
[[ "$(opam var ocaml:version)" == "${ocaml_version}" ]]
{
  printf 'OPAMROOT=%s\n' "${opam_root}"
  printf 'OPAMSWITCH=%s\n' "${ocaml_version}"
} >> "${GITHUB_ENV}"
printf '%s\n' "${bin_dir}" >> "${GITHUB_PATH}"
