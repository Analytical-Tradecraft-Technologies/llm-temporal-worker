#!/usr/bin/env bash

# OCI archives must be imported by the containerd image store. Run before any
# builders or services: switching stores hides existing containers and images.
set -euo pipefail

if [[ "${GITHUB_ACTIONS:-}" != true || "${RUNNER_ENVIRONMENT:-}" != github-hosted || "${RUNNER_OS:-}" != Linux ]]; then
  echo "containerd image-store setup requires a GitHub-hosted Linux runner" >&2
  exit 1
fi

has_containerd_store() {
  docker info --format '{{json .DriverStatus}}' | python3 -c '
import json, sys
sys.exit(0 if ["driver-type", "io.containerd.snapshotter.v1"] in (json.load(sys.stdin) or []) else 1)
'
}

if has_containerd_store; then
  exit 0
fi
if [[ -n "$(docker ps --all --quiet)" ]]; then
  echo "enable the containerd image store before creating any containers" >&2
  exit 1
fi

config="$(mktemp)"
trap 'rm -f -- "$config"' EXIT
sudo python3 - <<'PY' >"$config"
import json
from pathlib import Path

path = Path("/etc/docker/daemon.json")
config = json.loads(path.read_text()) if path.exists() else {}
config.setdefault("features", {})["containerd-snapshotter"] = True
print(json.dumps(config))
PY
sudo dockerd --validate --config-file "$config"
sudo install -m 0644 "$config" /etc/docker/daemon.json
sudo systemctl restart docker
if ! has_containerd_store; then
  echo "Docker did not enable the containerd image store" >&2
  exit 1
fi
