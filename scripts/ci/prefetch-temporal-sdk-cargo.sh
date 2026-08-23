#!/usr/bin/env bash

set -euo pipefail

readonly temporal_sdk_commit="8c8cf62b7f13bfa262b24df034ecfb899024b8a6"

fail() {
  printf '%s\n' "prefetch-temporal-sdk-cargo: $*" >&2
  exit 1
}

: "${OPAMROOT:?OPAMROOT must identify the isolated OPAM root}"
: "${OPAMSWITCH:?OPAMSWITCH must identify the selected OPAM switch}"
: "${XDG_CACHE_HOME:?XDG_CACHE_HOME must identify the isolated cache}"
: "${CARGO_HOME:?CARGO_HOME must identify the Dune-writable Cargo cache}"
: "${GITHUB_ENV:?GITHUB_ENV must be available in GitHub Actions}"

case "${CARGO_HOME}" in
  "${XDG_CACHE_HOME}"/dune/*) ;;
  *) fail "CARGO_HOME must be beneath the Dune writable cache mount" ;;
esac
[[ -d "${CARGO_HOME}" && -w "${CARGO_HOME}" ]] ||
  fail "CARGO_HOME must exist and be writable before dependency prefetch"

source_root="${OPAMROOT}/${OPAMSWITCH}/.opam-switch/sources/temporal-sdk"
cargo_manifest="${source_root}/rust/Cargo.toml"
cargo_lock="${source_root}/rust/Cargo.lock"

[[ -d "${source_root}/.git" ]] || fail "pinned Temporal SDK source is unavailable"
[[ -f "${cargo_manifest}" ]] || fail "pinned Temporal SDK Cargo.toml is unavailable"
[[ -f "${cargo_lock}" ]] || fail "pinned Temporal SDK Cargo.lock is unavailable"

actual_commit="$(git -C "${source_root}" rev-parse HEAD)" ||
  fail "unable to read pinned Temporal SDK revision"
[[ "${actual_commit}" == "${temporal_sdk_commit}" ]] ||
  fail "pinned Temporal SDK revision does not match the approved commit"

CARGO_NET_OFFLINE=false cargo fetch --locked --manifest-path "${cargo_manifest}"
printf '%s\n' 'CARGO_NET_OFFLINE=true' >> "${GITHUB_ENV}"
