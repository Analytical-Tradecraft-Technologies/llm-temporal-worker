#!/usr/bin/env python3
"""Fail-closed verification for a public GitHub Actions evidence run."""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import HTTPRedirectHandler, Request, build_opener


MAX_METADATA_BYTES = 1024 * 1024
PUBLIC_GITHUB_API = "https://api.github.com"
REPOSITORY_PATTERN = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
RUN_ID_PATTERN = re.compile(r"^[1-9][0-9]{0,19}$")
COMMIT_PATTERN = re.compile(r"^[0-9a-f]{40}$")
# An old run's retained scans predate current vulnerability data, so the
# evidence run must have started recently (matching guard.sh verify-evidence).
MAX_RUN_AGE = timedelta(days=7)
MAX_CLOCK_SKEW = timedelta(minutes=5)


def fail(message: str) -> None:
    raise ValueError(f"guarded release: {message}")


class RejectRedirects(HTTPRedirectHandler):
    """Keep the metadata response at the fixed public GitHub API origin."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        fail("public GitHub Actions metadata request redirected")


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(add_help=False)
    commands = parser.add_subparsers(dest="command", required=True)

    for command in ("fetch-and-validate", "validate"):
        child = commands.add_parser(command, add_help=False)
        child.add_argument("--repository", required=True)
        child.add_argument("--run-id", required=True)
        child.add_argument("--tag-commit", required=True)
        if command == "validate":
            child.add_argument("--metadata", required=True)

    return parser.parse_args()


def validate_inputs(repository: str, run_id: str, tag_commit: str) -> tuple[str, str]:
    if not REPOSITORY_PATTERN.fullmatch(repository):
        fail("repository must be an owner/repository GitHub name")
    if not RUN_ID_PATTERN.fullmatch(run_id):
        fail("run_id must be a positive decimal GitHub workflow run ID")
    if not COMMIT_PATTERN.fullmatch(tag_commit):
        fail("tag_commit must be a lowercase Git commit SHA")
    return tuple(repository.split("/", 1))


def require_string(document: dict[str, Any], field: str) -> str:
    value = document.get(field)
    if not isinstance(value, str):
        fail(f"public workflow metadata field {field!r} is missing or not a string")
    return value


def validate_metadata(
    document: object,
    repository: str,
    run_id: str,
    tag_commit: str,
) -> None:
    if not isinstance(document, dict):
        fail("public workflow metadata must be a JSON object")

    if document.get("id") != int(run_id):
        fail("public workflow metadata run ID does not match the requested evidence run")
    if require_string(document, "event") != "push":
        fail("evidence run event must be push")
    if require_string(document, "head_branch") != "master":
        fail("evidence run branch must be master")
    if require_string(document, "status") != "completed":
        fail("evidence run must be completed")
    if require_string(document, "conclusion") != "success":
        fail("evidence run must conclude successfully")
    if require_string(document, "head_sha") != tag_commit:
        fail("evidence run revision does not match the release tag commit")
    if require_string(document, "path") != ".github/workflows/master.yml":
        fail("evidence run must use the protected master workflow")
    try:
        started = datetime.fromisoformat(require_string(document, "run_started_at").replace("Z", "+00:00"))
    except ValueError:
        fail("evidence run start time is not an ISO 8601 timestamp")
    if started.tzinfo is None:
        fail("evidence run start time has no time zone")
    age = datetime.now(timezone.utc) - started
    if age > MAX_RUN_AGE or age < -MAX_CLOCK_SKEW:
        fail("evidence run started more than 7 days ago or in the future")

    run_repository = document.get("repository")
    if not isinstance(run_repository, dict):
        fail("public workflow metadata repository is missing")
    if run_repository.get("full_name") != repository:
        fail("evidence run repository does not match the requested repository")


def read_fixture_metadata(path_text: str) -> object:
    path = Path(path_text)
    try:
        if path.is_symlink() or not path.is_file():
            fail("metadata fixture must be a regular file")
        data = path.read_bytes()
    except OSError as error:
        fail(f"cannot read metadata fixture: {error}")
    return decode_metadata(data)


def decode_metadata(data: bytes) -> object:
    if len(data) > MAX_METADATA_BYTES:
        fail("public workflow metadata exceeds the size limit")
    try:
        return json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        fail("public workflow metadata is not valid UTF-8 JSON")
    raise AssertionError("unreachable")


def fetch_public_metadata(owner: str, repository: str, run_id: str) -> object:
    url = (
        f"{PUBLIC_GITHUB_API}/repos/{quote(owner, safe='')}/"
        f"{quote(repository, safe='')}/actions/runs/{run_id}"
    )
    request = Request(
        url,
        headers={
            "Accept": "application/vnd.github+json",
            "User-Agent": "llm-temporal-worker-release-guard",
        },
        method="GET",
    )
    try:
        opener = build_opener(RejectRedirects())
        with opener.open(request, timeout=15) as response:
            if response.status != 200:
                fail(f"public GitHub Actions metadata request returned HTTP {response.status}")
            data = response.read(MAX_METADATA_BYTES + 1)
    except HTTPError as error:
        fail(f"public GitHub Actions metadata request returned HTTP {error.code}")
    except (TimeoutError, URLError, OSError):
        fail("public GitHub Actions metadata request failed")
    return decode_metadata(data)


def main() -> None:
    arguments = parse_arguments()
    owner, repository_name = validate_inputs(
        arguments.repository,
        arguments.run_id,
        arguments.tag_commit,
    )
    if arguments.command == "fetch-and-validate":
        metadata = fetch_public_metadata(owner, repository_name, arguments.run_id)
    else:
        metadata = read_fixture_metadata(arguments.metadata)
    validate_metadata(metadata, arguments.repository, arguments.run_id, arguments.tag_commit)


if __name__ == "__main__":
    try:
        main()
    except ValueError as error:
        print(error, file=sys.stderr)
        raise SystemExit(1) from error
