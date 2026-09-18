#!/usr/bin/env bash

set -euo pipefail

fail() {
  echo "ECR release tag: $*" >&2
  exit 1
}

: "${PUBLISHED_IMAGE:?PUBLISHED_IMAGE is required}"
: "${EXPECTED_IMAGE_DIGEST:?EXPECTED_IMAGE_DIGEST is required}"
: "${RELEASE_REF:?RELEASE_REF is required}"
: "${ECR_REGISTRY:?ECR_REGISTRY is required}"
: "${ECR_REPOSITORY:?ECR_REPOSITORY is required}"
: "${GITHUB_OUTPUT:?GITHUB_OUTPUT is required}"

command -v crane >/dev/null 2>&1 || fail "pinned crane is unavailable"
[[ "${EXPECTED_IMAGE_DIGEST}" =~ ^sha256:[0-9a-f]{64}$ ]] || fail "expected image digest is invalid"
[[ "${RELEASE_REF}" =~ ^refs/tags/v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]] || fail "release reference is invalid"
[[ "${ECR_REPOSITORY}" == "llm-temporal-worker" ]] || fail "ECR repository is not the infrastructure-managed publication repository"
[[ "${PUBLISHED_IMAGE}" == "${ECR_REGISTRY}/${ECR_REPOSITORY}@${EXPECTED_IMAGE_DIGEST}" ]] || fail "published image is outside the exact ECR digest subject"

release_tag="${RELEASE_REF#refs/tags/}"
tagged_destination="${ECR_REGISTRY}/${ECR_REPOSITORY}:${release_tag}"
# This is deliberately the last mutating release step. ECR immutable-tag policy
# rejects a second publication or any attempt to move the release identity.
crane tag "${PUBLISHED_IMAGE}" "${release_tag}"
tagged_digest="$(crane digest "${tagged_destination}")"
[[ "${tagged_digest}" == "${EXPECTED_IMAGE_DIGEST}" ]] || fail "immutable release tag does not resolve to the signed digest"

printf 'published_tag=%s\n' "${tagged_destination}" >> "${GITHUB_OUTPUT}"
printf 'Published immutable release tag: `%s`\n' "${tagged_destination}" >> "${GITHUB_STEP_SUMMARY:-/dev/null}"
