#!/usr/bin/env bash

# Build the linux/amd64 and linux/arm64 images on the selected Buildx builder,
# scan each platform's exact OCI layout, and assemble the scanned layouts into
# one multi-platform OCI layout at OUTPUT_LAYOUT. Prints the image index
# digest. Never pushes: publishing (master only) copies OUTPUT_LAYOUT as is.
#
# Each platform is a separate single-platform build. Docker Build Cloud is a
# multi-node builder (one node per platform), and Buildx cannot export a
# multi-platform OCI archive from a multi-node builder (#971). A
# single-platform build runs on one node, so the OCI exporter works.
#
# Master runs this on Docker Build Cloud. The merge queue runs it on an
# uncredentialed local builder with the same one-node-per-platform shape
# (scripts/ci/setup-multinode-buildx.sh), so a builder or exporter
# incompatibility fails before merge.
set -euo pipefail

if [[ $# -ne 1 || -z "$1" ]]; then
  echo "usage: build-scanned-image.sh OUTPUT_LAYOUT" >&2
  exit 2
fi
output="$1"
: "${BUILDX_BUILDER:?BUILDX_BUILDER must name the Buildx builder}"
: "${RUNNER_TEMP:?RUNNER_TEMP must identify a runner-temporary directory}"
: "${TRIVY_CACHE_DIR:?TRIVY_CACHE_DIR must be set by scripts/ci/setup-trivy.sh}"
: "${IMAGE_VERSION:?IMAGE_VERSION is required}"
: "${IMAGE_REVISION:?IMAGE_REVISION is required}"
: "${IMAGE_BUILD_TIME:?IMAGE_BUILD_TIME is required}"
: "${IMAGE_SOURCE:?IMAGE_SOURCE is required}"
: "${IMAGE_GO_VERSION:?IMAGE_GO_VERSION is required}"
if [[ -e "$output" || -L "$output" ]]; then
  echo "image candidate layout already exists: $output" >&2
  exit 1
fi

repository_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
work="$(mktemp -d "${RUNNER_TEMP}/llmtw-image-candidate.XXXXXX")"
trap 'rm -rf -- "$work"' EXIT

(cd -- "$repository_root/golang" && go build -o "$work/ocimerge" ./tools/ocimerge)

architectures=(amd64 arm64)
for arch in "${architectures[@]}"; do
  docker buildx build \
    --builder "$BUILDX_BUILDER" \
    --file "$repository_root/golang/Dockerfile" \
    --platform "linux/$arch" \
    --pull --provenance=mode=max --sbom=true \
    --output "type=oci,oci-mediatypes=true,tar=true,dest=$work/$arch.tar" \
    --build-arg "VERSION=$IMAGE_VERSION" \
    --build-arg "REVISION=$IMAGE_REVISION" \
    --build-arg "BUILD_TIME=$IMAGE_BUILD_TIME" \
    --build-arg "SOURCE=$IMAGE_SOURCE" \
    --build-arg "GO_VERSION=$IMAGE_GO_VERSION" \
    "$repository_root/golang" >&2
  mkdir -- "$work/$arch.oci"
  tar -xf "$work/$arch.tar" -C "$work/$arch.oci"
  rm -f -- "$work/$arch.tar"
done

# Same scanner configuration as release evidence. --exit-code 1 makes any
# fixable HIGH or CRITICAL finding on either platform stop the job before
# anything is assembled or published.
for arch in "${architectures[@]}"; do
  config_digest="$("$work/ocimerge" config-digest -layout "$work/$arch.oci" -platform "linux/$arch")"
  if trivy image \
    --input "$work/$arch.oci" \
    --format json \
    --output "$work/$arch-scan.json" \
    --config "$repository_root/scripts/release/trivy.yaml" \
    --exit-code 1 \
    --cache-dir "$TRIVY_CACHE_DIR" >&2; then
    :
  else
    scan_status=$?
    echo "linux/$arch image scan failed; sanitized finding summary follows" >&2
    python3 "$repository_root/scripts/ci/summarize-image-scan.py" "$work/$arch-scan.json" >&2 || true
    exit "$scan_status"
  fi
  # Bind the scan to this platform's image: Trivy must report the image
  # config of the platform manifest that will be published.
  if ! jq -e --arg id "$config_digest" --arg arch "$arch" \
    '.Metadata.ImageID == $id and .Metadata.ImageConfig.architecture == $arch' \
    "$work/$arch-scan.json" >/dev/null; then
    echo "linux/$arch scan does not describe the built image config $config_digest" >&2
    exit 1
  fi
  echo "linux/$arch scanned: no fixable HIGH or CRITICAL findings" >&2
done

digest="$("$work/ocimerge" merge -output "$output" \
  -platform "linux/amd64=$work/amd64.oci" \
  -platform "linux/arm64=$work/arm64.oci")"
if [[ ! "$digest" =~ ^sha256:[0-9a-f]{64}$ ]]; then
  echo "image index assembly returned an invalid digest" >&2
  exit 1
fi
printf '%s\n' "$digest"
