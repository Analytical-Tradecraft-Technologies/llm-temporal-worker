#!/usr/bin/env bash

set -euo pipefail

fail() {
  echo "ECR publication: $*" >&2
  exit 1
}

root="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"

: "${STAGED_OCI_LAYOUT:?STAGED_OCI_LAYOUT is required}"
: "${EXPECTED_IMAGE_DIGEST:?EXPECTED_IMAGE_DIGEST is required}"
: "${AWS_ECR_PUBLISH_ROLE_ARN:?AWS_ECR_PUBLISH_ROLE_ARN is required}"
: "${AWS_ACCOUNT_ID:?AWS_ACCOUNT_ID is required}"
: "${AWS_REGION:?AWS_REGION is required}"
: "${ECR_REGISTRY:?ECR_REGISTRY is required}"
: "${ECR_REPOSITORY:?ECR_REPOSITORY is required}"
: "${GITHUB_OUTPUT:?GITHUB_OUTPUT is required}"
: "${RUNNER_TEMP:?RUNNER_TEMP is required}"

command -v crane >/dev/null 2>&1 || fail "pinned crane is unavailable"

[[ "${EXPECTED_IMAGE_DIGEST}" =~ ^sha256:[0-9a-f]{64}$ ]] || fail "expected image digest is invalid"
[[ -d "${RUNNER_TEMP}" && ! -L "${RUNNER_TEMP}" ]] || fail "runner temporary directory must be a real directory"
runner_temp="$(CDPATH= cd -- "${RUNNER_TEMP}" && pwd -P)"
[[ "${STAGED_OCI_LAYOUT}" == "${runner_temp}/source.oci" ]] || fail "staged OCI layout must use the fixed runner-temporary path"
[[ -d "${STAGED_OCI_LAYOUT}" && ! -L "${STAGED_OCI_LAYOUT}" ]] || fail "staged OCI layout must be a real directory"
[[ -f "${STAGED_OCI_LAYOUT}/oci-layout" && ! -L "${STAGED_OCI_LAYOUT}/oci-layout" ]] || fail "staged OCI layout marker is missing or indirect"
[[ -f "${STAGED_OCI_LAYOUT}/index.json" && ! -L "${STAGED_OCI_LAYOUT}/index.json" ]] || fail "staged OCI index is missing or indirect"
staged_digest="$(go -C "${root}/golang" run ./tools/releaseverify layout-digest -layout "${STAGED_OCI_LAYOUT}")"
[[ "${staged_digest}" == "${EXPECTED_IMAGE_DIGEST}" ]] || fail "staged OCI root digest does not match release evidence"

role_pattern='^arn:aws:iam::([0-9]{12}):role/[A-Za-z0-9+=,.@_/-]+$'
[[ "${AWS_ECR_PUBLISH_ROLE_ARN}" =~ ${role_pattern} ]] || fail "publisher role ARN is invalid"
role_account_id="${BASH_REMATCH[1]}"
[[ "${AWS_ACCOUNT_ID}" == "${role_account_id}" ]] || fail "OIDC session account does not match the configured publisher role"
[[ "${AWS_REGION}" =~ ^[a-z]{2}(-gov)?-[a-z]+-[0-9]+$ ]] || fail "AWS region is invalid"
[[ "${ECR_REPOSITORY}" == "llm-temporal-worker" ]] || fail "ECR repository is not the infrastructure-managed publication repository"
expected_registry="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"
[[ "${ECR_REGISTRY}" == "${expected_registry}" ]] || fail "authenticated ECR registry does not match account and region"

destination="${ECR_REGISTRY}/${ECR_REPOSITORY}@${EXPECTED_IMAGE_DIGEST}"

# A layout with one top descriptor auto-loads that exact image or index. Do not
# use Crane's index-wrapping option: it would create a different root digest.
pushed_reference="$(crane push "${STAGED_OCI_LAYOUT}" "${destination}")"
[[ "${pushed_reference}" == "${destination}" ]] || fail "Crane push output does not match the evidence-bound destination"
pushed_digest="${pushed_reference##*@}"
[[ "${pushed_digest}" == "${EXPECTED_IMAGE_DIGEST}" ]] || fail "pushed manifest digest does not match release evidence"

destination_digest="$(crane digest "${destination}")"
[[ "${destination_digest}" == "${EXPECTED_IMAGE_DIGEST}" ]] || fail "destination manifest digest does not match release evidence"
[[ "${destination_digest}" == "${staged_digest}" ]] || fail "staged and destination manifest digests differ"
[[ "${destination_digest}" == "${pushed_digest}" ]] || fail "pushed and resolved destination manifest digests differ"


printf 'published_image=%s\n' "${destination}" >> "${GITHUB_OUTPUT}"
printf 'Published immutable image digest: `%s`\n' "${destination}" >> "${GITHUB_STEP_SUMMARY:-/dev/null}"
