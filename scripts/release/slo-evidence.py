#!/usr/bin/env python3
"""Record and verify redacted, immutable SLO measurement evidence."""

import argparse
import datetime as datetime_module
import hashlib
import json
import os
import re
import stat
import sys

MAX_BYTES = 64 * 1024
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
REVISION_RE = re.compile(r"^[0-9a-f]{40}$")
REGION_RE = re.compile(r"^[a-z0-9][a-z0-9-]{0,63}$")
TIMESTAMP_RE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$")
MAX_COUNT = 1_000_000_000
MAX_P99_MICROSECONDS = 3_600_000_000


class EvidenceError(Exception):
    pass


def reject():
    raise EvidenceError("SLO evidence rejected")


def read_regular_bytes(path):
    try:
        metadata = os.lstat(path)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > MAX_BYTES:
            reject()
        with open(path, "rb") as handle:
            data = handle.read(MAX_BYTES + 1)
        if len(data) > MAX_BYTES:
            reject()
        return data
    except (OSError, ValueError):
        reject()


def parse_json_bytes(data):
    try:
        # Python's default decoder silently keeps the last value for a
        # duplicate object key.  That is unsafe for release evidence: an
        # attacker could place a benign value first (for review tooling) and
        # rely on a later value being used by the recorder or verifier.  Keep
        # the input contract deterministic by rejecting duplicates at every
        # object depth.  Reject non-finite constants as well; they are outside
        # the JSON data model and can otherwise be accepted by json.loads.
        value = json.loads(
            data.decode("utf-8"),
            object_pairs_hook=reject_duplicate_object_keys,
            parse_constant=lambda _constant: reject(),
        )
    except (UnicodeDecodeError, json.JSONDecodeError):
        reject()
    if not isinstance(value, dict):
        reject()
    return value


def reject_duplicate_object_keys(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            reject()
        result[key] = value
    return result


def exact_object(value, keys):
    if not isinstance(value, dict) or set(value) != set(keys):
        reject()


def exact_int(value, minimum, maximum):
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum or value > maximum:
        reject()
    return value


def exact_string(value, pattern):
    if not isinstance(value, str) or not pattern.fullmatch(value):
        reject()
    return value


def timestamp(value):
    exact_string(value, TIMESTAMP_RE)
    try:
        return datetime_module.datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=datetime_module.timezone.utc)
    except ValueError:
        reject()


def measurement(value, redis):
    required = ("sample_count", "p99_microseconds", "redis") if redis else ("sample_count", "p99_microseconds")
    exact_object(value, required)
    result = {
        "sample_count": exact_int(value["sample_count"], 1, MAX_COUNT),
        "p99_microseconds": exact_int(value["p99_microseconds"], 0, MAX_P99_MICROSECONDS),
    }
    if redis:
        redis_value = value["redis"]
        exact_object(redis_value, ("major_version", "persistence", "function_digest"))
        if redis_value["persistence"] != "aof+rdb":
            reject()
        result["redis"] = {
            "major_version": exact_int(redis_value["major_version"], 7, 99),
            "persistence": "aof+rdb",
            "function_digest": exact_string(redis_value["function_digest"], SHA256_RE),
        }
    return result


def payload_from_candidate(candidate):
    exact_object(candidate, ("measured_at", "source_revision", "deployment_id_sha256", "region", "admission_compilation", "worker_error_rate"))
    measured_at = candidate["measured_at"]
    timestamp(measured_at)
    source_revision = exact_string(candidate["source_revision"], REVISION_RE)
    deployment_id = exact_string(candidate["deployment_id_sha256"], SHA256_RE)
    region = exact_string(candidate["region"], REGION_RE)
    admission = candidate["admission_compilation"]
    exact_object(admission, ("memory", "same_region_redis"))
    memory = measurement(admission["memory"], redis=False)
    same_region_redis = measurement(admission["same_region_redis"], redis=True)
    if memory["p99_microseconds"] >= 25_000 or same_region_redis["p99_microseconds"] >= 75_000:
        reject()
    error_rate = candidate["worker_error_rate"]
    exact_object(error_rate, ("window_started_at", "window_ended_at", "completed_attempts", "worker_failed_attempts"))
    window_started_at = error_rate["window_started_at"]
    window_ended_at = error_rate["window_ended_at"]
    if timestamp(window_ended_at) <= timestamp(window_started_at):
        reject()
    completed_attempts = exact_int(error_rate["completed_attempts"], 1, MAX_COUNT)
    worker_failed_attempts = exact_int(error_rate["worker_failed_attempts"], 0, MAX_COUNT)
    if worker_failed_attempts * 1_000 >= completed_attempts + worker_failed_attempts:
        reject()
    return {
        "schema_version": 1, "kind": "slo_measurement", "status": "pass", "measured_at": measured_at,
        "source_revision": source_revision, "deployment_id_sha256": deployment_id, "region": region,
        "admission_compilation": {"memory": memory, "same_region_redis": same_region_redis},
        "worker_error_rate": {"window_started_at": window_started_at, "window_ended_at": window_ended_at, "completed_attempts": completed_attempts, "worker_failed_attempts": worker_failed_attempts},
        "redacted": True,
    }


def canonical_json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii") + b"\n"


def content_digest(payload):
    return hashlib.sha256(canonical_json(payload)).hexdigest()


def record_from_payload(payload):
    record = dict(payload)
    record["content_sha256"] = content_digest(payload)
    return record


def payload_from_record(record):
    exact_object(record, ("schema_version", "kind", "status", "measured_at", "source_revision", "deployment_id_sha256", "region", "admission_compilation", "worker_error_rate", "redacted", "content_sha256"))
    if record["schema_version"] != 1 or record["kind"] != "slo_measurement" or record["status"] != "pass" or record["redacted"] is not True:
        reject()
    candidate = {key: record[key] for key in ("measured_at", "source_revision", "deployment_id_sha256", "region", "admission_compilation", "worker_error_rate")}
    payload = payload_from_candidate(candidate)
    if exact_string(record["content_sha256"], SHA256_RE) != content_digest(payload):
        reject()
    return payload


def write_new(path, data):
    parent = os.path.dirname(os.path.abspath(path))
    try:
        if not os.path.isdir(parent):
            reject()
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except (OSError, ValueError):
        reject()
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
    except OSError:
        try:
            os.unlink(path)
        except OSError:
            pass
        reject()


def command_record(arguments):
    record = record_from_payload(payload_from_candidate(parse_json_bytes(read_regular_bytes(arguments.input))))
    write_new(arguments.evidence, canonical_json(record))
    print("slo evidence recorded sha256=" + record["content_sha256"])


def command_verify(arguments):
    expected_revision = exact_string(arguments.source_revision, REVISION_RE)
    expected_digest = exact_string(arguments.content_sha256, SHA256_RE)
    raw = read_regular_bytes(arguments.evidence)
    record = parse_json_bytes(raw)
    payload = payload_from_record(record)
    if record["source_revision"] != expected_revision or record["content_sha256"] != expected_digest:
        reject()
    if raw != canonical_json(record_from_payload(payload)):
        reject()
    print("slo evidence verified")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    subcommands = parser.add_subparsers(dest="command", required=True)
    record = subcommands.add_parser("record")
    record.add_argument("--input", required=True)
    record.add_argument("--evidence", required=True)
    verify = subcommands.add_parser("verify")
    verify.add_argument("--evidence", required=True)
    verify.add_argument("--source-revision", required=True)
    verify.add_argument("--content-sha256", required=True)
    arguments = parser.parse_args()
    try:
        if arguments.command == "record":
            command_record(arguments)
        else:
            command_verify(arguments)
    except EvidenceError as error:
        print(str(error), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
