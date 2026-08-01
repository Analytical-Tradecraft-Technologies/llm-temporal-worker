#!/usr/bin/env python3
"""Record only allowlisted evidence from one protected live-provider probe."""

from __future__ import annotations

import argparse
import json
import os
import re
import stat
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SCHEMA_VERSION = 1
TENANT = "llmtw-live-contract"
CEILING_MICRO_USD = 25_000
MAX_INPUT_BYTES = 1_048_576
MAX_EVIDENCE_BYTES = 65_536
PROFILES = frozenset(
    {
        "openai-responses",
        "azure-responses",
        "openai-chat",
        "openrouter-chat",
        "exa-chat",
        "anthropic-direct",
        "anthropic-aws",
        "bedrock-anthropic",
        "bedrock-converse",
    }
)
ACTUAL_SERVICE_CLASS = "standard"
REPORTED_COST_METHODS = frozenset(
    {
        "provider_reported",
        "usage",
        "openrouter_reported",
        "exa_reported",
    }
)
IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
SOURCE_REVISION = re.compile(r"^[0-9a-f]{40}$")
TIMESTAMP = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
RESULT_LINE = re.compile(
    r"^\s+live_provider_test\.go:\d+: "
    r"profile=(?P<profile>[a-z0-9-]+) "
    r"tenant=(?P<tenant>[A-Za-z0-9-]+) "
    r"request_id=(?P<request_id>[^\s]+) "
    r"response_id=(?P<response_id>[^\s]+) "
    r"actual_service_class=(?P<actual_service_class>[a-z]+) "
    r"actual_spend_known=(?P<actual_spend_known>true|false) "
    r"actual_micro_usd=(?P<actual_micro_usd>[0-9]+) "
    r"cost_method=(?P<cost_method>[a-z_]+) "
    r"continuation_verified=(?P<continuation_verified>true|false)$"
)
SECRET_PREFIXES = (
    "sk-",
    "sk_",
    "eyj",
    "akia",
    "asia",
    "ghp_",
    "github_pat_",
    "xoxb-",
    "xoxp-",
    "aiza",
)


class ContractError(Exception):
    """A deliberately non-descriptive rejection for potentially sensitive input."""


def rejected() -> ContractError:
    return ContractError("live contract evidence rejected")


def regular_bytes(path: Path, maximum: int) -> bytes:
    try:
        details = path.lstat()
        if not stat.S_ISREG(details.st_mode) or details.st_size > maximum:
            raise rejected()
        return path.read_bytes()
    except (OSError, ValueError):
        raise rejected() from None


def safe_identifier(value: Any) -> str:
    if not isinstance(value, str) or not IDENTIFIER.fullmatch(value):
        raise rejected()
    if value.lower().startswith(SECRET_PREFIXES):
        raise rejected()
    return value


def exact_bool(value: Any) -> bool:
    if type(value) is not bool:
        raise rejected()
    return value


def exact_int(value: Any) -> int:
    if type(value) is not int:
        raise rejected()
    return value


def generated_at(value: Any) -> str:
    if not isinstance(value, str) or not TIMESTAMP.fullmatch(value):
        raise rejected()
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise rejected() from None
    if parsed.tzinfo is None:
        raise rejected()
    return value


def source_revision(value: Any) -> str:
    if not isinstance(value, str) or not SOURCE_REVISION.fullmatch(value):
        raise rejected()
    return value


def default_generated_at() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_raw_result(path: Path, selected_profile: str) -> dict[str, Any]:
    try:
        raw = regular_bytes(path, MAX_INPUT_BYTES).decode("utf-8")
    except UnicodeDecodeError:
        raise rejected() from None

    matches = [RESULT_LINE.fullmatch(line) for line in raw.splitlines()]
    matches = [match for match in matches if match is not None]
    if len(matches) != 1 or f"--- PASS: TestLiveProviderContracts/{selected_profile}" not in raw:
        raise rejected()
    result = matches[0].groupdict()
    if result["profile"] != selected_profile or result["tenant"] != TENANT:
        raise rejected()
    return result


def evidence_from_result(
    result: dict[str, Any], selected_profile: str, timestamp: str, revision: str
) -> dict[str, Any]:
    if selected_profile not in PROFILES:
        raise rejected()
    known = result["actual_spend_known"] == "true"
    continuation_verified = result["continuation_verified"] == "true"
    micro_usd = int(result["actual_micro_usd"])
    method = result["cost_method"]
    service_class = result["actual_service_class"]
    if service_class != ACTUAL_SERVICE_CLASS:
        raise rejected()
    if micro_usd > CEILING_MICRO_USD:
        raise rejected()
    if known:
        if method not in REPORTED_COST_METHODS:
            raise rejected()
    elif micro_usd != 0 or method != "not_reported":
        raise rejected()
    return validate_evidence(
        {
            "schema_version": SCHEMA_VERSION,
            "generated_at": timestamp,
            "source_revision": revision,
            "profile": selected_profile,
            "tenant": TENANT,
            "ceiling_micro_usd": CEILING_MICRO_USD,
            "actual_spend": {
                "known": known,
                "micro_usd": micro_usd,
                "method": method,
            },
            "request_id": result["request_id"],
            "response_id": result["response_id"],
            "actual_service_class": service_class,
            "continuation_verified": continuation_verified,
        }
    )


def validate_evidence(candidate: Any) -> dict[str, Any]:
    if not isinstance(candidate, dict):
        raise rejected()
    required = {
        "schema_version",
        "generated_at",
        "source_revision",
        "profile",
        "tenant",
        "ceiling_micro_usd",
        "actual_spend",
        "request_id",
        "response_id",
        "actual_service_class",
        "continuation_verified",
    }
    if set(candidate) != required:
        raise rejected()
    if exact_int(candidate["schema_version"]) != SCHEMA_VERSION:
        raise rejected()
    timestamp = generated_at(candidate["generated_at"])
    revision = source_revision(candidate["source_revision"])
    profile = candidate["profile"]
    if not isinstance(profile, str) or profile not in PROFILES:
        raise rejected()
    if candidate["tenant"] != TENANT or exact_int(candidate["ceiling_micro_usd"]) != CEILING_MICRO_USD:
        raise rejected()
    service_class = candidate["actual_service_class"]
    if service_class != ACTUAL_SERVICE_CLASS:
        raise rejected()
    spend = candidate["actual_spend"]
    if not isinstance(spend, dict) or set(spend) != {"known", "micro_usd", "method"}:
        raise rejected()
    known = exact_bool(spend["known"])
    micro_usd = exact_int(spend["micro_usd"])
    method = spend["method"]
    if micro_usd < 0 or micro_usd > CEILING_MICRO_USD or not isinstance(method, str):
        raise rejected()
    if known:
        if method not in REPORTED_COST_METHODS:
            raise rejected()
    elif micro_usd != 0 or method != "not_reported":
        raise rejected()
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": timestamp,
        "source_revision": revision,
        "profile": profile,
        "tenant": TENANT,
        "ceiling_micro_usd": CEILING_MICRO_USD,
        "actual_spend": {"known": known, "micro_usd": micro_usd, "method": method},
        "request_id": safe_identifier(candidate["request_id"]),
        "response_id": safe_identifier(candidate["response_id"]),
        "actual_service_class": service_class,
        "continuation_verified": exact_bool(candidate["continuation_verified"]),
    }


def redacted_log(evidence: dict[str, Any]) -> bytes:
    spend = evidence["actual_spend"]
    lines = [
        "live_provider_contract_evidence=v1",
        f"source_revision={evidence['source_revision']}",
        f"profile={evidence['profile']}",
        f"tenant={evidence['tenant']}",
        f"ceiling_micro_usd={evidence['ceiling_micro_usd']}",
        f"actual_spend_known={str(spend['known']).lower()}",
        f"actual_micro_usd={spend['micro_usd']}",
        f"cost_method={spend['method']}",
        f"actual_service_class={evidence['actual_service_class']}",
        f"continuation_verified={str(evidence['continuation_verified']).lower()}",
    ]
    return ("\n".join(lines) + "\n").encode("utf-8")


def write_atomic(path: Path, data: bytes) -> None:
    try:
        if not path.parent.is_dir():
            raise rejected()
        descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
        try:
            with os.fdopen(descriptor, "wb") as output:
                os.fchmod(output.fileno(), 0o600)
                output.write(data)
                output.flush()
                os.fsync(output.fileno())
            os.replace(temporary, path)
        except BaseException:
            try:
                os.unlink(temporary)
            except OSError:
                pass
            raise
    except (OSError, ValueError):
        raise rejected() from None


def command_record(arguments: argparse.Namespace) -> int:
    selected_profile = arguments.profile
    if selected_profile not in PROFILES or arguments.evidence == arguments.log:
        raise rejected()
    timestamp = arguments.generated_at or default_generated_at()
    revision = source_revision(arguments.source_revision)
    result = parse_raw_result(Path(arguments.input), selected_profile)
    evidence = evidence_from_result(result, selected_profile, generated_at(timestamp), revision)
    evidence_bytes = (json.dumps(evidence, sort_keys=True, indent=2) + "\n").encode("utf-8")
    write_atomic(Path(arguments.evidence), evidence_bytes)
    try:
        write_atomic(Path(arguments.log), redacted_log(evidence))
    except ContractError:
        try:
            Path(arguments.evidence).unlink(missing_ok=True)
        except OSError:
            pass
        raise
    print("live contract evidence recorded")
    return 0


def command_verify(arguments: argparse.Namespace) -> int:
    expected_revision = source_revision(arguments.source_revision)
    try:
        decoded = json.loads(regular_bytes(Path(arguments.evidence), MAX_EVIDENCE_BYTES))
    except (json.JSONDecodeError, UnicodeDecodeError):
        raise rejected() from None
    evidence = validate_evidence(decoded)
    if evidence["source_revision"] != expected_revision:
        raise rejected()
    if regular_bytes(Path(arguments.log), MAX_EVIDENCE_BYTES) != redacted_log(evidence):
        raise rejected()
    print("live contract evidence verified")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    record = commands.add_parser("record")
    record.add_argument("--profile", required=True)
    record.add_argument("--input", required=True)
    record.add_argument("--evidence", required=True)
    record.add_argument("--log", required=True)
    record.add_argument("--source-revision", required=True)
    record.add_argument("--generated-at")
    record.set_defaults(handler=command_record)
    verify = commands.add_parser("verify")
    verify.add_argument("--evidence", required=True)
    verify.add_argument("--log", required=True)
    verify.add_argument("--source-revision", required=True)
    verify.set_defaults(handler=command_verify)
    return parser


def main() -> int:
    try:
        arguments = build_parser().parse_args()
        return arguments.handler(arguments)
    except ContractError:
        print("live contract evidence rejected", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
