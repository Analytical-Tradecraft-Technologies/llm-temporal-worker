#!/usr/bin/env bash

set -euo pipefail

fail() {
  echo "release image staging: $*" >&2
  exit 1
}

root="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"

: "${SOURCE_IMAGE_REFERENCE:?SOURCE_IMAGE_REFERENCE is required}"
: "${EXPECTED_IMAGE_DIGEST:?EXPECTED_IMAGE_DIGEST is required}"
: "${STAGED_OCI_LAYOUT:?STAGED_OCI_LAYOUT is required}"
: "${RUNNER_TEMP:?RUNNER_TEMP is required}"

command -v crane >/dev/null 2>&1 || fail "pinned crane is unavailable"
[[ "${EXPECTED_IMAGE_DIGEST}" =~ ^sha256:[0-9a-f]{64}$ ]] || fail "expected image digest is invalid"
[[ "${SOURCE_IMAGE_REFERENCE}" == *@"${EXPECTED_IMAGE_DIGEST}" ]] || fail "source reference is not bound to the expected digest"
[[ -d "${RUNNER_TEMP}" && ! -L "${RUNNER_TEMP}" ]] || fail "runner temporary directory must be a real directory"
runner_temp="$(CDPATH= cd -- "${RUNNER_TEMP}" && pwd -P)"
[[ "${STAGED_OCI_LAYOUT}" == "${runner_temp}/source.oci" ]] || fail "staged OCI layout must use the fixed runner-temporary path"
[[ ! -e "${STAGED_OCI_LAYOUT}" && ! -L "${STAGED_OCI_LAYOUT}" ]] || fail "staged OCI layout path must not already exist"

# With no platform selection, an OCI index and every child are materialized.
# No AWS identity exists while this untrusted-registry read occurs.
crane pull --format=oci "${SOURCE_IMAGE_REFERENCE}" "${STAGED_OCI_LAYOUT}"

[[ -d "${STAGED_OCI_LAYOUT}" && ! -L "${STAGED_OCI_LAYOUT}" ]] || fail "Crane did not create a real OCI layout directory"
[[ -f "${STAGED_OCI_LAYOUT}/oci-layout" && ! -L "${STAGED_OCI_LAYOUT}/oci-layout" ]] || fail "staged OCI layout marker is missing or indirect"
[[ -f "${STAGED_OCI_LAYOUT}/index.json" && ! -L "${STAGED_OCI_LAYOUT}/index.json" ]] || fail "staged OCI index is missing or indirect"
staged_digest="$(go -C "${root}/golang" run ./tools/releaseverify layout-digest -layout "${STAGED_OCI_LAYOUT}")"
[[ "${staged_digest}" == "${EXPECTED_IMAGE_DIGEST}" ]] || fail "staged OCI root digest does not match release evidence"
