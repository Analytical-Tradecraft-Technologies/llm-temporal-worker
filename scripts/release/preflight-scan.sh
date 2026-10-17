#!/usr/bin/env bash
# Fresh vulnerability scans of the exact release subject for the guarded
# publication preflight (#1326). The retained master evidence bundle records
# scans from when the image was built; these checks re-evaluate the tag commit
# and the published digest against today's vulnerability data, so an older tag
# whose evidence is still retained cannot pass once its findings are known.
#
# Neither command executes code from the tag: govulncheck and the policy
# verifier are built from the trusted master module and only load the tag's
# packages, and Trivy pulls the image from the registry without running it.
# Both apply the same policy as the corresponding master gate and, like those
# gates, keep raw scanner output in private temporary files.
set -euo pipefail

root="$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
readonly platforms=(linux/amd64 linux/arm64)

fail() {
  echo "release preflight scan: $*" >&2
  exit 1
}

require_option_value() {
  local option="$1"
  local remaining="$2"
  if [[ "$remaining" -lt 2 ]]; then
    fail "missing value for $option"
  fi
}

# Runs govulncheck on a read-only worktree of the tag commit, using the Go
# toolchain the tag pins in golang/.go-version (the version master's container
# job built its image with), so reachable standard-library vulnerabilities in
# that release are found. The JSON findings are evaluated against the trusted
# master baseline (golang/tools/supplychainverify/baseline.json) with the same
# per-finding exception rules as the security-verify gate.
scan_tag_source() {
  local tag_commit=""
  local worktree=""

  while [[ "$#" -gt 0 ]]; do
    case "$1" in
      --tag-commit)
        require_option_value "$1" "$#"
        tag_commit="$2"
        shift 2
        ;;
      --worktree)
        require_option_value "$1" "$#"
        worktree="$2"
        shift 2
        ;;
      *)
        fail "unknown tag-source option $1"
        ;;
    esac
  done

  if [[ ! "$tag_commit" =~ ^[0-9a-f]{40}$ ]]; then
    fail "tag_commit must be a lowercase commit SHA"
  fi
  [[ -n "$worktree" ]] || fail "worktree is required"
  [[ ! -e "$worktree" && ! -L "$worktree" ]] || fail "worktree path must not already exist"

  work="$(mktemp -d)"
  tag_worktree=""
  # The worktree is made read-only below, so restore write access before
  # removing it. Only paths this command created are removed.
  trap 'if [[ -n "$tag_worktree" ]]; then chmod -R u+w -- "$tag_worktree" 2>/dev/null || true; git -C "$root" worktree remove --force "$tag_worktree" >/dev/null 2>&1 || true; fi; rm -rf -- "$work"' EXIT

  git -C "$root" worktree add --quiet --detach "$worktree" "$tag_commit"
  tag_worktree="$worktree"
  chmod -R a-w "$worktree"

  local go_version
  go_version="$(cat "$worktree/golang/.go-version")"
  if [[ ! "$go_version" =~ ^[1-9][0-9]*\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]; then
    fail "tag golang/.go-version is not an exact Go release"
  fi

  go -C "$root/golang" build -o "$work/govulncheck" golang.org/x/vuln/cmd/govulncheck
  go -C "$root/golang" build -o "$work/supplychainverify" ./tools/supplychainverify

  # CGO is disabled so package loading cannot invoke a C toolchain or
  # pkg-config, GOWORK=off ignores any workspace file in the tag, and
  # -mod=readonly keeps the tag's go.mod and go.sum authoritative. JSON output
  # reports module-, package-, and function-level findings, which the policy
  # verifier classifies exactly as the master gate does.
  local vulnerability_status=pass
  if ! (cd "$worktree/golang" && GOTOOLCHAIN="go$go_version" GOWORK=off CGO_ENABLED=0 GOFLAGS=-mod=readonly "$work/govulncheck" -format json ./...) >"$work/govulncheck.json" 2>"$work/govulncheck.stderr"; then
    vulnerability_status=fail
    echo "release preflight scan: govulncheck did not complete on the tag commit; raw output withheld" >&2
  fi
  if ! "$work/supplychainverify" -release-tag \
    -baseline "$root/golang/tools/supplychainverify/baseline.json" \
    -vulnerability-output "$work/govulncheck.json" \
    -vulnerability-status "$vulnerability_status" \
    -report "$work/release-tag-vulnerabilities.json"; then
    fail "tag commit has Go vulnerabilities outside the reviewed baseline exceptions, or the scan did not complete"
  fi
}

# Pulls both platform manifests of the requested image index from the registry
# and rescans them with the pinned Trivy, the checked-in configuration, and a
# freshly downloaded database, failing on fixable HIGH or CRITICAL findings
# exactly as master's container gate (scripts/ci/build-scanned-image.sh) does.
scan_image() {
  local image_reference=""

  while [[ "$#" -gt 0 ]]; do
    case "$1" in
      --image-reference)
        require_option_value "$1" "$#"
        image_reference="$2"
        shift 2
        ;;
      *)
        fail "unknown image option $1"
        ;;
    esac
  done

  if [[ ! "$image_reference" =~ ^[a-z0-9][a-z0-9.:/_-]*@sha256:[a-f0-9]{64}$ ]]; then
    fail "image_reference must be a registry/repository@sha256 digest reference"
  fi
  command -v trivy >/dev/null 2>&1 || fail "pinned Trivy is not installed"

  work="$(mktemp -d)"
  trap 'rm -rf -- "$work"' EXIT
  # An empty cache forces a current vulnerability database for this run.
  mkdir -- "$work/cache"
  local platform report
  for platform in "${platforms[@]}"; do
    report="$work/${platform//\//-}-scan.json"
    # The checked-in configuration supplies the master severity and
    # ignore-unfixed policy; --exit-code 1 matches the master container gate.
    if ! trivy image \
      --image-src remote \
      --platform "$platform" \
      --format json \
      --output "$report" \
      --config "$root/scripts/release/trivy.yaml" \
      --exit-code 1 \
      --cache-dir "$work/cache" \
      "$image_reference" >&2; then
      echo "release preflight scan: $platform image scan failed; sanitized finding summary follows" >&2
      python3 "$root/scripts/ci/summarize-image-scan.py" "$report" >&2 || true
      fail "$platform image has fixable HIGH or CRITICAL findings or could not be scanned"
    fi
  done
}

case "${1:-}" in
  tag-source)
    shift
    scan_tag_source "$@"
    ;;
  image)
    shift
    scan_image "$@"
    ;;
  *)
    fail "usage: $0 {tag-source|image}"
    ;;
esac
