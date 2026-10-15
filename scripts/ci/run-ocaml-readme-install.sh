#!/usr/bin/env bash

set -euo pipefail

# Run the OCaml README's install commands verbatim, substituting only the
# deployment commit, against a fresh switch with opam's build sandbox enabled
# and an empty Cargo cache. Nothing is prefetched or set offline beforehand, so
# this fails if the documented recipe cannot build inside the sandbox (#1350).

readonly readme="ocaml/llm_temporal_worker/README.md"
readonly begin_marker="<!-- ci:readme-install:begin -->"
readonly end_marker="<!-- ci:readme-install:end -->"

fail() {
  printf '%s\n' "run-ocaml-readme-install: $*" >&2
  exit 1
}

: "${PACKAGE_REF:?PACKAGE_REF must identify the commit to install}"
[[ "${PACKAGE_REF}" =~ ^[0-9a-f]{40}$ ]] || fail "PACKAGE_REF must be a full commit hash"
[[ -f "${readme}" ]] || fail "${readme} is unavailable"

block="$(
  awk -v begin="${begin_marker}" -v end="${end_marker}" '
    $0 == begin { inside = 1; next }
    $0 == end { inside = 0; next }
    inside { print }
  ' "${readme}"
)"
[[ "$(printf '%s\n' "${block}" | head -n 1)" == '```sh' ]] ||
  fail "README install block must open with a sh fence directly after its begin marker"
[[ "$(printf '%s\n' "${block}" | tail -n 1)" == '```' ]] ||
  fail "README install block must close its fence directly before its end marker"
commands="$(printf '%s\n' "${block}" | sed '1d;$d')"
[[ "${commands}" == *'<commit>'* ]] || fail "README install block has no <commit> placeholder"
commands="${commands//<commit>/${PACKAGE_REF}}"

wrap="$(opam option --global wrap-build-commands)"
[[ "${wrap}" == *sandbox.sh* ]] || fail "opam build sandbox is not enabled: ${wrap}"

# Start from the user's environment, not CI's prefetch configuration.
unset CARGO_HOME CARGO_NET_OFFLINE
cargo_home="${XDG_CACHE_HOME:-${HOME}/.cache}/dune/cargo"
if [[ -d "${cargo_home}" ]] && [[ -n "$(ls -A -- "${cargo_home}")" ]]; then
  fail "${cargo_home} must be empty before the README install"
fi

script="$(mktemp)"
trap 'rm -f -- "${script}"' EXIT
printf '%s\n' "${commands}" > "${script}"
cat -- "${script}"
bash -euo pipefail "${script}"
