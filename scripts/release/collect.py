#!/usr/bin/env python3
"""Build compact, redacted release-evidence summaries from trusted CI inputs.

The command intentionally retains only allowlisted metadata and cryptographic
digests. Command and scanner output stay in a caller's temporary directory.
Service-log input is reduced to fixed, allowlisted event counts before it can
enter a release-evidence artifact.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile
from typing import Any
from urllib.parse import urlsplit


SHA256 = re.compile(r"^[a-f0-9]{64}$")
PROFILE = re.compile(r"^[a-z0-9][a-z0-9-]{0,127}$")
DATE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")
SAFE_SOURCE = re.compile(r"^[A-Za-z0-9._/-]{1,256}$")
SAFE_MODULE = re.compile(r"^[A-Za-z0-9._~+/-]{1,256}$")
SAFE_VERSION = re.compile(r"^v[A-Za-z0-9.+-]{1,127}$")
SAFE_LICENSE = re.compile(r"^[A-Za-z0-9.+-]{1,64}$")
SAFE_FINDING = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")
SAFE_DNS_LABEL = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
NUMERIC_DOTTED_HOST = re.compile(r"^[0-9]+(?:\.[0-9]+)+$")
SAFE_HTTPS_PATH = re.compile(r"^[A-Za-z0-9._~!$&'()*+,;=:@/-]*$")
GATE_KINDS = {"test_summary", "race_summary", "fuzz_summary"}
BENCHMARK_NAME = "BenchmarkGenerateMemoryAdmissionAndCompile"
MEMORY_TARGET_MS = 25
BENCHMARK_LINE = re.compile(
    rf"^{BENCHMARK_NAME}-[0-9]+\s+([0-9]+)\s+([0-9]+(?:\.[0-9]+)?)\s+ns/op\s+"
    r"([0-9]+(?:\.[0-9]+)?)\s+p99_ms/op(?:\s.*)?$"
)
LOG_SERVICES = {
    "redis_log": "redis",
    "temporal_log": "temporal",
    "compose_log": "compose",
}
REDIS_RUNTIME_EVENTS = {"redis_ready", "redis_initialized", "redis_loading"}
TEMPORAL_RUNTIME_EVENTS = {
    "temporal_started",
    "temporal_ready",
    "temporal_serving",
    "temporal_database_ready",
}
MAX_LOG_INPUT_BYTES = 8 * 1024 * 1024
MAX_LOG_LINES = 100000


def fail(message: str) -> None:
    raise ValueError(message)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        fail(f"cannot read valid JSON from {path}: {error}")


def require_string(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value:
        fail(f"{label} must be a non-empty string")
    return value


def require_list(value: Any, label: str) -> list[Any]:
    if not isinstance(value, list):
        fail(f"{label} must be an array")
    return value


def require_mapping(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        fail(f"{label} must be an object")
    return value


def is_safe_dns_hostname(hostname: str) -> bool:
    normalized = hostname.lower()
    if len(normalized) > 253 or NUMERIC_DOTTED_HOST.fullmatch(normalized):
        return False
    return all(SAFE_DNS_LABEL.fullmatch(label) for label in normalized.split("."))


def require_safe_https_url(value: str, label: str) -> str:
    """Accept only a stable, credential-free HTTPS provenance URL.

    Release evidence must not preserve credentials or mutable URL decorations.
    Keep this check deliberately independent of the redaction scanner: URI
    userinfo such as ``https://operator:opaque@example.invalid`` does not have
    a conventional secret field name and would otherwise be easy to miss.
    """

    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError:
        fail(f"{label} must be an absolute HTTPS URL with a DNS hostname and without a port, userinfo, backslash, query, or fragment")
    if (
        parsed.scheme != "https"
        or not parsed.netloc
        or parsed.hostname is None
        or not is_safe_dns_hostname(parsed.hostname)
        or parsed.netloc.lower() != parsed.hostname.lower()
        or port is not None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or "?" in value
        or "#" in value
        or "\\" in value
        or "%" in value
        or not SAFE_HTTPS_PATH.fullmatch(parsed.path)
    ):
        fail(f"{label} must be an absolute HTTPS URL with a DNS hostname and without a port, userinfo, backslash, query, or fragment")
    return value


def non_symlink_directory(path: Path) -> None:
    current = Path(path.anchor)
    for component in path.parts[1:]:
        current /= component
        try:
            metadata = current.lstat()
        except OSError as error:
            fail(f"cannot inspect output directory {path}: {error}")
        if current.is_symlink() or not current.is_dir():
            fail(f"output directory {path} must not traverse a symlink or file")
        if metadata.st_mode == 0:
            fail(f"output directory {path} is invalid")


def write_json(path: Path, value: dict[str, Any]) -> None:
    requested = path.absolute()
    requested_parent = requested.parent
    if not requested_parent.exists() or not requested_parent.is_dir():
        fail(f"output directory does not exist: {requested_parent}")
    if requested.is_symlink():
        fail(f"output path must not be a symlink: {requested}")
    # macOS commonly exposes /tmp through the system /private/tmp alias. Resolve
    # that parent before checking each physical path component, while still
    # rejecting a symlink at the requested final output path.
    parent = requested_parent.resolve(strict=True)
    destination = parent / requested.name
    non_symlink_directory(parent)
    if destination.is_symlink():
        fail(f"output path must not be a symlink: {destination}")
    encoded = (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
    descriptor, temporary = tempfile.mkstemp(prefix=f".{destination.name}.tmp-", dir=parent)
    temporary_path = Path(temporary)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, destination)
    finally:
        if temporary_path.exists():
            temporary_path.unlink()


def command_gate_summary(args: argparse.Namespace) -> None:
    if args.kind not in GATE_KINDS:
        fail(f"unsupported gate summary kind: {args.kind}")
    data = Path(args.input).read_bytes()
    write_json(
        Path(args.output),
        {
            "schema_version": 1,
            "kind": args.kind,
            "status": "pass",
            "output_sha256": sha256_bytes(data),
            "output_bytes": len(data),
            "redacted": True,
        },
    )


def metadata_values(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        match = re.match(r"^(profile|upstream_url|upstream_date):\s*(.*?)\s*$", line)
        if not match:
            continue
        value = match.group(2).strip().strip("\"'")
        values[match.group(1)] = value
    return values


def profile_manifest_sha256(directory: Path) -> str:
    digest = hashlib.sha256()
    files = sorted(directory.rglob("*"), key=lambda path: path.as_posix())
    for path in files:
        metadata = path.lstat()
        if path.is_symlink():
            fail(f"fixture profile contains a symlink: {path}")
        if not path.is_file():
            continue
        relative = path.relative_to(directory).as_posix().encode("utf-8")
        digest.update(relative)
        digest.update(b"\x00")
        digest.update(hashlib.sha256(path.read_bytes()).digest())
        digest.update(b"\x00")
        if metadata.st_size < 0:
            fail(f"fixture profile contains an invalid file: {path}")
    return digest.hexdigest()


def command_fixture_manifest(args: argparse.Namespace) -> None:
    root = Path(args.root).resolve()
    metadata_paths = sorted(root.glob("llm/provider/**/testdata/contracts/**/metadata.yaml"))
    fixtures: list[dict[str, str]] = []
    profiles: set[str] = set()
    for metadata_path in metadata_paths:
        if metadata_path.is_symlink():
            fail(f"fixture metadata must not be a symlink: {metadata_path}")
        values = metadata_values(metadata_path)
        profile = require_string(values.get("profile"), f"fixture profile in {metadata_path}")
        upstream_url = require_string(values.get("upstream_url"), f"fixture upstream URL in {metadata_path}")
        upstream_date = require_string(values.get("upstream_date"), f"fixture upstream date in {metadata_path}")
        if not PROFILE.fullmatch(profile) or profile in profiles:
            fail(f"fixture profile is invalid or duplicated: {profile}")
        require_safe_https_url(upstream_url, f"fixture upstream URL for {profile}")
        if not DATE.fullmatch(upstream_date):
            fail(f"fixture upstream date is invalid: {profile}")
        try:
            dt.date.fromisoformat(upstream_date)
        except ValueError:
            fail(f"fixture upstream date is invalid: {profile}")
        profiles.add(profile)
        fixtures.append(
            {
                "profile": profile,
                "upstream_url": upstream_url,
                "upstream_date": upstream_date,
                "manifest_sha256": profile_manifest_sha256(metadata_path.parent),
            }
        )
    if not fixtures:
        fail("no provider contract fixture metadata was found")
    write_json(
        Path(args.output),
        {
            "schema_version": 1,
            "kind": "fixture_manifest",
            "status": "pass",
            "version": 1,
            "fixtures": fixtures,
            "redacted": True,
        },
    )


def command_benchmark_summary(args: argparse.Namespace) -> None:
    """Retain only the bounded, in-memory benchmark measurement.

    The summary is deliberately a measurement artifact rather than an SLO
    assertion.  It proves that the release workflow ran the memory benchmark
    and preserves its p99 sample, while Redis and provider latency remain
    separate protected measurements.
    """

    if args.kind != "benchmark_summary":
        fail(f"unsupported benchmark summary kind: {args.kind}")
    data = Path(args.input).read_bytes()
    if not data or len(data) > MAX_LOG_INPUT_BYTES:
        fail("benchmark output has an invalid byte length")
    matches: list[tuple[int, float, float]] = []
    for raw_line in data.splitlines():
        try:
            line = raw_line.decode("ascii").strip()
        except UnicodeDecodeError:
            continue
        match = BENCHMARK_LINE.fullmatch(line)
        if match is None:
            continue
        samples = int(match.group(1))
        ns_per_op = float(match.group(2))
        p99_ms_per_op = float(match.group(3))
        if samples < 1 or ns_per_op < 0 or p99_ms_per_op < 0:
            fail("benchmark output contains an invalid non-negative measurement")
        matches.append((samples, ns_per_op, p99_ms_per_op))
    if len(matches) != 1:
        fail("benchmark output must contain exactly one memory benchmark measurement")
    samples, ns_per_op, p99_ms_per_op = matches[0]
    # The release job must fail closed when the offline memory proxy misses
    # its documented target.  This proves the memory half of the objective
    # without implying that the same-region Redis half has been measured.
    if p99_ms_per_op >= MEMORY_TARGET_MS:
        fail(f"memory benchmark p99 {p99_ms_per_op:g} ms/op is not below the {MEMORY_TARGET_MS} ms target")
    write_json(
        Path(args.output),
        {
            "schema_version": 1,
            "kind": "benchmark_summary",
            "status": "pass",
            "benchmark": BENCHMARK_NAME,
            "scope": "memory",
            "samples": samples,
            "ns_per_op": ns_per_op,
            "p99_ms_per_op": p99_ms_per_op,
            "target_ms": MEMORY_TARGET_MS,
            "target_status": "pass",
            "objective_status": "measurement_only",
            "output_sha256": sha256_bytes(data),
            "output_bytes": len(data),
            "redacted": True,
        },
    )


def command_service_summary(args: argparse.Namespace) -> None:
    if args.kind not in {"redis_summary", "temporal_summary"}:
        fail(f"unsupported service summary kind: {args.kind}")
    service = args.kind.removesuffix("_summary")
    if args.state != "running" or args.health != "healthy":
        fail(f"{service} service is not healthy")
    write_json(
        Path(args.output),
        {
            "schema_version": 1,
            "kind": args.kind,
            "status": "pass",
            "service": service,
            "state": "running",
            "health": "healthy",
            "redacted": True,
        },
    )


def command_compose_summary(args: argparse.Namespace) -> None:
    write_json(
        Path(args.output),
        {
            "schema_version": 1,
            "kind": "compose_summary",
            "status": "pass",
            "services": ["redis", "temporal"],
            "redacted": True,
        },
    )


def classify_log_event(kind: str, line: bytes) -> str:
    normalized = line.lower()
    if kind in {"redis_log", "compose_log"}:
        if b"ready to accept connections" in normalized:
            return "redis_ready"
        if b"server initialized" in normalized:
            return "redis_initialized"
        if b"loading" in normalized and (b"rdb" in normalized or b"aof" in normalized):
            return "redis_loading"
    if kind in {"temporal_log", "compose_log"}:
        if b"temporal" in normalized and (b"starting" in normalized or b"started" in normalized):
            return "temporal_started"
        if b"temporal" in normalized and b"ready" in normalized:
            return "temporal_ready"
        if b"temporal" in normalized and (b"listening" in normalized or b"serving" in normalized):
            return "temporal_serving"
        if (b"database" in normalized or b" db " in normalized) and (
            b"ready" in normalized or b"connected" in normalized or b"initialized" in normalized
        ):
            return "temporal_database_ready"
    return "redacted_line"


def command_redacted_log(args: argparse.Namespace) -> None:
    service = LOG_SERVICES.get(args.kind)
    if service is None:
        fail(f"unsupported redacted log kind: {args.kind}")
    data = Path(args.input).read_bytes()
    if not data or len(data) > MAX_LOG_INPUT_BYTES:
        fail("Compose log input has an invalid byte length")
    lines = data.splitlines()
    if not lines or len(lines) > MAX_LOG_LINES:
        fail("Compose log input has an invalid line count")
    event_counts: dict[str, int] = {}
    for line in lines:
        event = classify_log_event(args.kind, line)
        event_counts[event] = event_counts.get(event, 0) + 1
    saw_redis_boundary = any(event in REDIS_RUNTIME_EVENTS for event in event_counts)
    saw_temporal_boundary = any(event in TEMPORAL_RUNTIME_EVENTS for event in event_counts)
    if args.kind == "redis_log" and not saw_redis_boundary:
        fail("Redis Compose log input has no allowlisted runtime-boundary event")
    if args.kind == "temporal_log" and not saw_temporal_boundary:
        fail("Temporal Compose log input has no allowlisted runtime-boundary event")
    if args.kind == "compose_log" and (not saw_redis_boundary or not saw_temporal_boundary):
        fail("Compose log input must include allowlisted Redis and Temporal runtime-boundary events")
    write_json(
        Path(args.output),
        {
            "schema_version": 1,
            "kind": args.kind,
            "status": "pass",
            "service": service,
            "source": "docker_compose_logs",
            "redaction_policy": "allowlist-v1",
            "line_count": len(lines),
            "input_bytes": len(data),
            "event_counts": event_counts,
            "redacted": True,
        },
    )


def command_rendered_manifests(args: argparse.Namespace) -> None:
    manifests: list[dict[str, Any]] = []
    seen_sources: set[str] = set()
    for entry in args.entry:
        source, separator, file_name = entry.partition("=")
        if not separator or not SAFE_SOURCE.fullmatch(source) or source in seen_sources:
            fail(f"invalid rendered-manifest entry: {entry}")
        data = Path(file_name).read_bytes()
        objects = len(re.findall(rb"(?m)^kind:\s+[^\s#]+", data))
        if not data or objects == 0:
            fail(f"rendered manifest contains no Kubernetes objects: {source}")
        seen_sources.add(source)
        manifests.append(
            {
                "source": source,
                "sha256": sha256_bytes(data),
                "bytes": len(data),
                "objects": objects,
            }
        )
    if not manifests:
        fail("no rendered manifests were supplied")
    write_json(
        Path(args.output),
        {
            "schema_version": 1,
            "kind": "rendered_manifests",
            "status": "pass",
            "manifests": manifests,
            "redacted": True,
        },
    )


def command_dependency_license(args: argparse.Namespace) -> None:
    baseline_path = Path(args.baseline)
    baseline = require_mapping(read_json(baseline_path), "dependency baseline")
    direct_modules = require_list(baseline.get("direct_modules"), "dependency baseline direct_modules")
    normalized: list[dict[str, str]] = []
    for module in direct_modules:
        record = require_mapping(module, "dependency baseline module")
        path = require_string(record.get("path"), "dependency module path")
        version = require_string(record.get("version"), "dependency module version")
        license_name = require_string(record.get("license"), "dependency module license")
        source = require_string(record.get("source"), "dependency module source")
        if not SAFE_MODULE.fullmatch(path) or not SAFE_VERSION.fullmatch(version) or not SAFE_LICENSE.fullmatch(license_name):
            fail(f"dependency module has an invalid allowlisted value: {path}")
        require_safe_https_url(source, f"dependency module source for {path}")
        normalized.append({"path": path, "version": version, "license": license_name, "source": source})
    if not normalized:
        fail("dependency baseline has no direct modules")
    write_json(
        Path(args.output),
        {
            "schema_version": 1,
            "kind": "dependency_license",
            "status": "pass",
            "baseline_sha256": sha256_bytes(baseline_path.read_bytes()),
            "direct_modules": normalized,
            "redacted": True,
        },
    )


def safe_findings(value: Any, label: str) -> list[str]:
    values = require_list(value, label)
    result: list[str] = []
    for item in values:
        item = require_string(item, label)
        if not SAFE_FINDING.fullmatch(item):
            fail(f"{label} has an invalid identifier: {item}")
        result.append(item)
    return result


def command_vulnerability_results(args: argparse.Namespace) -> None:
    report = require_mapping(read_json(Path(args.input)), "security verification report")
    components = require_mapping(report.get("components"), "security verification components")
    expected_components = {"test": "pass", "source": "pass", "go_mod": "pass", "vulnerability": "pass"}
    if report.get("status") != "pass" or components != expected_components:
        fail("security verification report is not a complete pass")
    direct_module_count = report.get("direct_module_count")
    if not isinstance(direct_module_count, int) or isinstance(direct_module_count, bool) or direct_module_count < 1:
        fail("security verification direct module count is invalid")
    write_json(
        Path(args.output),
        {
            "schema_version": 1,
            "kind": "vulnerability_results",
            "status": "pass",
            "components": expected_components,
            "direct_module_count": direct_module_count,
            "findings": safe_findings(report.get("findings"), "security verification findings"),
            "approved_findings": safe_findings(report.get("approved_findings"), "security verification approved findings"),
            "redacted": True,
        },
    )


def immutable_subject(reference: str, digest: str) -> None:
    if not digest.startswith("sha256:") or not SHA256.fullmatch(digest.removeprefix("sha256:")):
        fail("image digest must be a sha256 digest")
    if reference != f"{reference.split('@', 1)[0]}@{digest}" or "@" not in reference or not reference.split("@", 1)[0]:
        fail("image reference must exactly match its immutable digest")


def command_annotate_sbom(args: argparse.Namespace) -> None:
    immutable_subject(args.reference, args.digest)
    document = require_mapping(read_json(Path(args.input)), "CycloneDX SBOM")
    if document.get("bomFormat") != "CycloneDX":
        fail("SBOM is not CycloneDX JSON")
    metadata = require_mapping(document.get("metadata"), "CycloneDX SBOM metadata")
    component = require_mapping(metadata.get("component"), "CycloneDX SBOM metadata component")
    if component.get("type") != "container":
        fail("SBOM does not describe a container")
    properties = component.get("properties", [])
    properties = require_list(properties, "CycloneDX SBOM component properties")
    expected = {
        "org.opencontainers.image.ref.name": args.reference,
        "org.opencontainers.image.manifest.digest": args.digest,
    }
    present: set[str] = set()
    normalized: list[Any] = []
    for item in properties:
        property_value = require_mapping(item, "CycloneDX SBOM component property")
        name = require_string(property_value.get("name"), "CycloneDX SBOM component property name")
        if name in expected:
            if name in present or property_value.get("value") != expected[name]:
                fail(f"SBOM has a stale or duplicate immutable image property: {name}")
            present.add(name)
        normalized.append(property_value)
    for name, value in expected.items():
        if name not in present:
            normalized.append({"name": name, "value": value})
    component["properties"] = normalized
    write_json(Path(args.output), document)


def command_annotate_scan(args: argparse.Namespace) -> None:
    immutable_subject(args.reference, args.digest)
    document = require_mapping(read_json(Path(args.input)), "Trivy image scan")
    if document.get("SchemaVersion") != 2:
        fail("image scan is not a Trivy schema version 2 report")
    if document.get("ArtifactType") != "container_image":
        fail("image scan is not a Trivy container-image report")
    artifact_name = require_string(document.get("ArtifactName"), "Trivy image scan artifact name")
    if artifact_name.replace("\\", "/").endswith("/") or artifact_name.replace("\\", "/").rsplit("/", 1)[-1] != "image.oci":
        fail("image scan was not generated from the temporary OCI directory")
    require_list(document.get("Results"), "Trivy image scan results")
    if "release_subject" in document:
        fail("image scan already has an immutable release subject")
    # Trivy reports the absolute runner-temporary input path. Retain only its
    # stable basename so the redacted evidence bundle cannot disclose runner
    # filesystem details while still proving the expected scan input.
    document["ArtifactName"] = "image.oci"
    document["release_subject"] = {"reference": args.reference, "digest": args.digest}
    write_json(Path(args.output), document)


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    commands = result.add_subparsers(dest="command", required=True)

    gate = commands.add_parser("gate-summary")
    gate.add_argument("--kind", required=True)
    gate.add_argument("--input", required=True)
    gate.add_argument("--output", required=True)
    gate.set_defaults(handler=command_gate_summary)

    fixture = commands.add_parser("fixture-manifest")
    fixture.add_argument("--root", required=True)
    fixture.add_argument("--output", required=True)
    fixture.set_defaults(handler=command_fixture_manifest)

    benchmark = commands.add_parser("benchmark-summary")
    benchmark.add_argument("--kind", required=True)
    benchmark.add_argument("--input", required=True)
    benchmark.add_argument("--output", required=True)
    benchmark.set_defaults(handler=command_benchmark_summary)

    service = commands.add_parser("service-summary")
    service.add_argument("--kind", required=True)
    service.add_argument("--state", required=True)
    service.add_argument("--health", required=True)
    service.add_argument("--output", required=True)
    service.set_defaults(handler=command_service_summary)

    compose = commands.add_parser("compose-summary")
    compose.add_argument("--output", required=True)
    compose.set_defaults(handler=command_compose_summary)

    log = commands.add_parser("redacted-log")
    log.add_argument("--kind", required=True)
    log.add_argument("--input", required=True)
    log.add_argument("--output", required=True)
    log.set_defaults(handler=command_redacted_log)

    rendered = commands.add_parser("rendered-manifests")
    rendered.add_argument("--entry", action="append", default=[])
    rendered.add_argument("--output", required=True)
    rendered.set_defaults(handler=command_rendered_manifests)

    dependencies = commands.add_parser("dependency-license")
    dependencies.add_argument("--baseline", required=True)
    dependencies.add_argument("--output", required=True)
    dependencies.set_defaults(handler=command_dependency_license)

    vulnerabilities = commands.add_parser("vulnerability-results")
    vulnerabilities.add_argument("--input", required=True)
    vulnerabilities.add_argument("--output", required=True)
    vulnerabilities.set_defaults(handler=command_vulnerability_results)

    sbom = commands.add_parser("annotate-sbom")
    sbom.add_argument("--input", required=True)
    sbom.add_argument("--output", required=True)
    sbom.add_argument("--reference", required=True)
    sbom.add_argument("--digest", required=True)
    sbom.set_defaults(handler=command_annotate_sbom)

    scan = commands.add_parser("annotate-scan")
    scan.add_argument("--input", required=True)
    scan.add_argument("--output", required=True)
    scan.add_argument("--reference", required=True)
    scan.add_argument("--digest", required=True)
    scan.set_defaults(handler=command_annotate_scan)

    return result


def main() -> None:
    arguments = parser().parse_args()
    try:
        arguments.handler(arguments)
    except (OSError, ValueError) as error:
        raise SystemExit(f"release evidence collection failed: {error}") from error


if __name__ == "__main__":
    main()
