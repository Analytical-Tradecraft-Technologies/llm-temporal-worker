#!/usr/bin/env bash
set -euo pipefail

root="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"

fail() {
  echo "guarded release: $*" >&2
  exit 1
}

require_option_value() {
  local option="$1"
  local remaining="$2"
  if [[ "$remaining" -lt 2 ]]; then
    fail "missing value for $option"
  fi
}

validate_repository() {
  local repository="$1"
  local repository_pattern='^[a-z0-9]+([.-][a-z0-9]+)*(:[0-9]+)?/[a-z0-9]+([._-][a-z0-9]+)*(\/[a-z0-9]+([._-][a-z0-9]+)*)*$'
  local host="${repository%%/*}"

  if [[ ! "$repository" =~ $repository_pattern ]]; then
    fail "repository must be a lowercase fully qualified registry/repository path"
  fi
  if [[ "$host" != *.* && "$host" != *:* ]]; then
    fail "repository registry host must include a DNS suffix or an explicit port"
  fi
}

validate_request() {
  local release_ref=""
  local image_reference=""
  local evidence_run_id=""
  local trusted_repository=""
  local output=""

  while [[ "$#" -gt 0 ]]; do
    case "$1" in
      --release-ref)
        require_option_value "$1" "$#"
        release_ref="$2"
        shift 2
        ;;
      --image-reference)
        require_option_value "$1" "$#"
        image_reference="$2"
        shift 2
        ;;
      --evidence-run-id)
        require_option_value "$1" "$#"
        evidence_run_id="$2"
        shift 2
        ;;
      --trusted-repository)
        require_option_value "$1" "$#"
        trusted_repository="$2"
        shift 2
        ;;
      --output)
        require_option_value "$1" "$#"
        output="$2"
        shift 2
        ;;
      *)
        fail "unknown validate-request option $1"
        ;;
    esac
  done

  [[ -n "$release_ref" ]] || fail "release_ref is required"
  [[ -n "$image_reference" ]] || fail "image_reference is required"
  [[ -n "$evidence_run_id" ]] || fail "evidence_run_id is required"
  [[ -n "$trusted_repository" ]] || fail "RELEASE_PUBLICATION_IMAGE_REPOSITORY must be configured"
  [[ -n "$output" ]] || fail "output is required"

  if [[ ! "$evidence_run_id" =~ ^[1-9][0-9]{0,19}$ ]]; then
    fail "evidence_run_id must be a positive decimal GitHub workflow run ID"
  fi

  local release_tag_pattern='^refs/tags/v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$'
  if [[ ! "$release_ref" =~ $release_tag_pattern ]]; then
    fail "release_ref must be an exact refs/tags/vMAJOR.MINOR.PATCH reference"
  fi

  validate_repository "$trusted_repository"
  if [[ "$image_reference" != *@sha256:* ]]; then
    fail "image_reference must include an immutable sha256 digest"
  fi
  local image_repository="${image_reference%@sha256:*}"
  local image_digest="${image_reference##*@}"
  if [[ ! "$image_digest" =~ ^sha256:[a-f0-9]{64}$ ]]; then
    fail "image_reference must include a lowercase sha256 digest"
  fi
  validate_repository "$image_repository"
  if [[ "$image_repository" != "$trusted_repository" ]]; then
    fail "image_reference repository is not the externally configured trusted repository"
  fi

  if ! git show-ref --verify --quiet "$release_ref"; then
    fail "release tag does not exist in the protected checkout"
  fi
  local tag_commit
  if ! tag_commit="$(git rev-parse --verify "${release_ref}^{commit}")"; then
    fail "release tag cannot be resolved to a commit"
  fi
  if [[ ! "$tag_commit" =~ ^[0-9a-f]{40}$ ]]; then
    fail "release tag resolved to an invalid commit"
  fi
  if ! git show-ref --verify --quiet refs/remotes/origin/master; then
    fail "protected master reference is unavailable in the checkout"
  fi
  if ! git merge-base --is-ancestor "$tag_commit" refs/remotes/origin/master; then
    fail "release tag commit is not reachable from protected master"
  fi

  printf 'tag_commit=%s\n' "$tag_commit" >> "$output"
  printf 'image_digest=%s\n' "$image_digest" >> "$output"
  printf 'image_repository=%s\n' "$image_repository" >> "$output"
}

verify_public_run() {
  local repository=""
  local evidence_run_id=""
  local tag_commit=""

  while [[ "$#" -gt 0 ]]; do
    case "$1" in
      --repository)
        require_option_value "$1" "$#"
        repository="$2"
        shift 2
        ;;
      --evidence-run-id)
        require_option_value "$1" "$#"
        evidence_run_id="$2"
        shift 2
        ;;
      --tag-commit)
        require_option_value "$1" "$#"
        tag_commit="$2"
        shift 2
        ;;
      *)
        fail "unknown verify-public-run option $1"
        ;;
    esac
  done

  [[ -n "$repository" ]] || fail "repository is required"
  [[ -n "$evidence_run_id" ]] || fail "evidence_run_id is required"
  [[ -n "$tag_commit" ]] || fail "tag_commit is required"

  python3 "$root/scripts/release/verify-public-run.py" fetch-and-validate \
    --repository "$repository" \
    --run-id "$evidence_run_id" \
    --tag-commit "$tag_commit"
}

verify_evidence() {
  local evidence=""
  local tag_commit=""
  local image_reference=""

  while [[ "$#" -gt 0 ]]; do
    case "$1" in
      --evidence)
        require_option_value "$1" "$#"
        evidence="$2"
        shift 2
        ;;
      --tag-commit)
        require_option_value "$1" "$#"
        tag_commit="$2"
        shift 2
        ;;
      --image-reference)
        require_option_value "$1" "$#"
        image_reference="$2"
        shift 2
        ;;
      *)
        fail "unknown verify-evidence option $1"
        ;;
    esac
  done

  [[ -f "$evidence" ]] || fail "evidence file is missing"
  if [[ ! "$tag_commit" =~ ^[0-9a-f]{40}$ ]]; then
    fail "tag_commit must be a lowercase commit SHA"
  fi
  if [[ "$image_reference" != *@sha256:* ]]; then
    fail "image_reference must include an immutable sha256 digest"
  fi
  local image_digest="${image_reference##*@}"
  if [[ ! "$image_digest" =~ ^sha256:[a-f0-9]{64}$ ]]; then
    fail "image_reference must include a lowercase sha256 digest"
  fi

  python3 - "$evidence" "$tag_commit" "$image_digest" <<'PY'
import json
import sys

path, expected_revision, expected_digest = sys.argv[1:]
try:
    with open(path, encoding="utf-8") as evidence_file:
        evidence = json.load(evidence_file)
except (OSError, json.JSONDecodeError) as error:
    raise SystemExit(f"guarded release: cannot read evidence: {error}")

source = evidence.get("source")
image = evidence.get("image")
if not isinstance(source, dict) or source.get("revision") != expected_revision:
    raise SystemExit("guarded release: evidence revision does not match the release tag commit")
if not isinstance(image, dict) or image.get("digest") != expected_digest:
    raise SystemExit("guarded release: evidence image digest does not match the requested publication subject")
PY
}

case "${1:-}" in
  validate-request)
    shift
    validate_request "$@"
    ;;
  verify-evidence)
    shift
    verify_evidence "$@"
    ;;
  verify-public-run)
    shift
    verify_public_run "$@"
    ;;
  *)
    fail "usage: $0 {validate-request|verify-public-run|verify-evidence}"
    ;;
esac
