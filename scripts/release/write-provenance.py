#!/usr/bin/env python3
"""Write the closed SLSA v1 predicate used by the release attestation."""

from __future__ import annotations

import argparse
import json
import os
import re
from pathlib import Path
from urllib.parse import urlsplit

SHA = re.compile(r"^[0-9a-f]{40}$")
DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
VERSION = re.compile(r"^v(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)$")
CREATED = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:Z|[+-][0-9]{2}:[0-9]{2})$")


def reject(message: str) -> "NoReturn":
    raise SystemExit(f"release provenance: {message}")


def safe_source(value: str) -> str:
    parsed = urlsplit(value)
    if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
        reject("source must be a credential-free HTTPS repository URL")
    if parsed.port or parsed.query or parsed.fragment or parsed.path.rstrip("/") != parsed.path:
        reject("source must be a stable repository URL")
    return value


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--source", required=True)
    parser.add_argument("--created", required=True)
    parser.add_argument("--digest", required=True)
    parser.add_argument("--evidence-run-id", required=True)
    parser.add_argument("--workflow-run-id", required=True)
    args = parser.parse_args()

    if not VERSION.fullmatch(args.version):
        reject("version must be vMAJOR.MINOR.PATCH")
    if not SHA.fullmatch(args.revision):
        reject("revision must be a full Git commit")
    if not CREATED.fullmatch(args.created):
        reject("created must be an RFC 3339 timestamp")
    if not DIGEST.fullmatch(args.digest):
        reject("digest must be sha256")
    if not args.evidence_run_id.isdigit() or int(args.evidence_run_id) < 1:
        reject("evidence run ID must be positive")
    if not args.workflow_run_id.isdigit() or int(args.workflow_run_id) < 1:
        reject("workflow run ID must be positive")
    source = safe_source(args.source)

    output = Path(args.output)
    if output.exists() or output.is_symlink():
        reject("output must not already exist")
    output.parent.mkdir(parents=True, exist_ok=True)

    predicate = {
        "buildDefinition": {
            "buildType": "https://github.com/mfow/llm-temporal-worker/.github/workflows/master.yml@refs/heads/master",
            "externalParameters": {
                "version": args.version,
                "revision": args.revision,
                "source": source,
                "created": args.created,
                "platform": "linux/amd64",
                "dockerfile": "golang/Dockerfile",
                "buildImage": "docker.io/library/golang:1.26.5@sha256:2005724102f45917a63e9d092fc0e4ea56ea575048ce147caad5f5f61502c365",
                "runtimeImage": "gcr.io/distroless/static-debian12:nonroot@sha256:f5b485ea962d9bd1186b2f6b3a061191539b905b82ec395de78cbfae51f20e35",
            },
            "internalParameters": {
                "evidenceWorkflowRunId": int(args.evidence_run_id),
                "imageDigest": args.digest,
            },
            "resolvedDependencies": [
                {"uri": f"git+{source}.git", "digest": {"gitCommit": args.revision}}
            ],
        },
        "runDetails": {
            "builder": {
                "id": "https://github.com/mfow/llm-temporal-worker/.github/workflows/release.yml@refs/heads/master"
            },
            "metadata": {"invocationId": f"https://github.com/mfow/llm-temporal-worker/actions/runs/{args.workflow_run_id}"},
        },
    }
    temporary = output.with_name(f".{output.name}.{os.getpid()}.tmp")
    with temporary.open("x", encoding="utf-8", newline="\n") as handle:
        json.dump(predicate, handle, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.chmod(temporary, 0o600)
    os.replace(temporary, output)


if __name__ == "__main__":
    main()
