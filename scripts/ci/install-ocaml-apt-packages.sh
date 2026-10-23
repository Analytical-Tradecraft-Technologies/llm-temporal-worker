#!/usr/bin/env bash

# Installs the system packages the OCaml jobs build with. The cargo package
# pulls about 71 MB of rustc/libstd-rust .debs from the runner's Ubuntu mirror,
# which has slowed far enough to hit the job timeout, so the workflow restores
# the downloaded archives from the Actions cache and apt only fetches packages
# that changed since. A stalled fetch times out and retries instead of hanging.

set -euo pipefail

: "${RUNNER_TEMP:?RUNNER_TEMP must identify a runner-temporary directory}"

# The workflow caches this path; keep it in sync with the cache step.
archives="${RUNNER_TEMP}/llmtw-apt-archives"
mkdir -p -- "${archives}/partial"

apt_options=(
  -o Acquire::Retries=5
  -o Acquire::http::Timeout=30
  -o Acquire::https::Timeout=30
  -o "Dir::Cache::Archives=${archives}/"
  -o APT::Keep-Downloaded-Packages=true
)

sudo apt-get "${apt_options[@]}" update
sudo apt-get "${apt_options[@]}" install --yes protobuf-compiler bubblewrap
sudo apt-get "${apt_options[@]}" install --yes cargo
# Drop archives the mirror no longer serves so restored caches do not grow.
sudo apt-get "${apt_options[@]}" autoclean
# apt writes the archives as root; hand them back so actions/cache can save them.
sudo chown -R -- "$(id -u):$(id -g)" "${archives}"
