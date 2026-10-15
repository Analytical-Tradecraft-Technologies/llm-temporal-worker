#!/usr/bin/env bash
# Fresh vulnerability scans of the exact release subject for the guarded
# publication preflight (#1326). The retained master evidence bundle records
# scans from when the image was built; these checks re-evaluate the tag commit
# and the published digest against today's vulnerability data, so an older tag
# whose evidence is still retained cannot pass once its findings are known.
#
# Neither command executes code from the tag: govulncheck is built from the
# trusted master module and only loads the tag's packages, and Trivy pulls the
# image from the registry without running it.
set -euo pipefail

root="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
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
# toolchain the tag pins in golang/.go-version (the version its image was built
# with), so reachable standard-library vulnerabilities in that release fail.
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
  [[ ! -e "$worktree" ]] || fail "worktree path must not already exist"

  git -C "$root" worktree add --quiet --detach "$worktree" "$tag_commit"
  chmod -R a-w "$worktree"

  local go_version
  go_version="$(cat "$worktree/golang/.go-version")"
  if [[ ! "$go_version" =~ ^[1-9][0-9]*\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]; then
    fail "tag golang/.go-version is not an exact Go release"
  fi

  tools="$(mktemp -d)"
  trap 'rm -rf -- "$tools"' EXIT
  go -C "$root/golang" build -o "$tools/govulncheck" golang.org/x/vuln/cmd/govulncheck

  # CGO is disabled so package loading cannot invoke a C toolchain or
  # pkg-config, and -mod=readonly keeps the tag's go.mod and go.sum
  # authoritative. govulncheck exits nonzero on any reachable vulnerability.
  if ! (cd "$worktree/golang" && GOTOOLCHAIN="go$go_version" CGO_ENABLED=0 GOFLAGS=-mod=readonly "$tools/govulncheck" ./...); then
    fail "govulncheck found reachable vulnerabilities in the tag commit or could not complete"
  fi
}

# Pulls both platform manifests of the requested image index from the registry
# and rescans them with the pinned Trivy and a freshly downloaded database,
# failing on fixable HIGH or CRITICAL findings.
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

  cache="$(mktemp -d)"
  trap 'rm -rf -- "$cache"' EXIT
  local platform
  for platform in "${platforms[@]}"; do
    # An empty cache forces a current vulnerability database. The checked-in
    # configuration supplies the master severity and ignore-unfixed policy;
    # only the report format and exit status differ for a direct gate.
    if ! trivy image \
      --image-src remote \
      --platform "$platform" \
      --config "$root/scripts/release/trivy.yaml" \
      --cache-dir "$cache" \
      --format table \
      --exit-code 1 \
      "$image_reference"; then
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
