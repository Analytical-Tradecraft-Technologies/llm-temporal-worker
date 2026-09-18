#!/usr/bin/env python3
"""Derive fail-closed BTF25 engine and outer command deadlines from a frozen profile."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

QUESTION_COUNT = 25
BATCH_COUNT = 2
MAX_CONCURRENCY = 8
MAX_OVERALL_SECONDS = 24 * 60 * 60
OUTER_CONTAINER_RESERVE_SECONDS = 120
ENGINE_SEALING_RESERVE_SECONDS = 5 * 60
_DURATION = re.compile(r"(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?")


def duration_seconds(value: str) -> int:
    match = _DURATION.fullmatch(value)
    if match is None or not any(match.groups()):
        raise ValueError("BTF25_OVERALL_TIMEOUT must be a positive Go duration using ordered h/m/s units")
    hours, minutes, seconds = (int(part or 0) for part in match.groups())
    total = hours * 3600 + minutes * 60 + seconds
    if total < 1:
        raise ValueError("BTF25_OVERALL_TIMEOUT must be positive")
    return total


def positive_int(value: object, label: str) -> int:
    if type(value) is not int or value < 1:
        raise ValueError(f"{label} must be a positive integer")
    return value


def closed_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError(f"BTF25 profile repeats JSON field {key!r}")
        value[key] = item
    return value


def plan(profile_path: Path, concurrency: int, requested_overall: str | None, requested_command: int | None) -> dict[str, int | str]:
    if type(concurrency) is not int or concurrency < 1 or concurrency > MAX_CONCURRENCY:
        raise ValueError("BTF25_MAX_CONCURRENCY must be an integer in [1,8]")
    encoded = profile_path.read_bytes()
    if not encoded or len(encoded) > 16 << 20:
        raise ValueError("BTF25 profile is outside the size bound")
    profile = json.loads(encoded, object_pairs_hook=closed_object)
    if type(profile) is not dict or type(profile.get("run_profile")) is not dict:
        raise ValueError("BTF25 profile must contain a frozen run_profile")
    run_profile = profile["run_profile"]
    limits = run_profile.get("limits")
    submission = run_profile.get("submission_policy")
    if type(limits) is not dict or type(submission) is not dict:
        raise ValueError("BTF25 frozen run profile omits limits or submission policy")
    capacity = profile.get("resource_capacity")
    outer_limits = profile.get("limits")
    if type(capacity) is not dict or type(outer_limits) is not dict:
        raise ValueError("BTF25 profile omits signed resource capacity or outer limits")
    max_runtime = positive_int(profile.get("max_runtime_seconds"), "BTF25 profile max_runtime_seconds")
    frozen_runtime = positive_int(limits.get("max_runtime_seconds"), "BTF25 frozen max_runtime_seconds")
    deadline_reserve = positive_int(profile.get("deadline_reserve_seconds"), "BTF25 profile deadline_reserve_seconds")
    frozen_deadline_reserve = positive_int(submission.get("deadline_reserve_seconds"), "BTF25 frozen deadline_reserve_seconds")
    if max_runtime != frozen_runtime or deadline_reserve != frozen_deadline_reserve:
        raise ValueError("BTF25 profile runtime or deadline reserve differs from its frozen run_profile")
    ensemble_size = positive_int(
        outer_limits.get("ensemble_size"), "BTF25 ensemble_size"
    )
    forecast_capacity = positive_int(
        capacity.get("forecast_event_max_inflight"),
        "BTF25 forecast_event_max_inflight",
    )
    stage2_capacity = positive_int(
        capacity.get("python_stage2_max_inflight"),
        "BTF25 python_stage2_max_inflight",
    )
    effective_concurrency = min(
        concurrency,
        forecast_capacity,
        max(1, stage2_capacity // ensemble_size),
    )
    waves = (QUESTION_COUNT + effective_concurrency - 1) // effective_concurrency
    required_overall = BATCH_COUNT * waves * frozen_runtime + ENGINE_SEALING_RESERVE_SECONDS
    if required_overall > MAX_OVERALL_SECONDS:
        raise ValueError(
            f"BTF25 frozen timeout plan requires {required_overall}s, above the engine 24h cap; increase concurrency or use an explicit lower-runtime frozen profile"
        )
    overall = duration_seconds(requested_overall) if requested_overall else required_overall
    if overall < required_overall:
        raise ValueError(
            f"BTF25_OVERALL_TIMEOUT={overall}s cannot cover {BATCH_COUNT} batches x {waves} waves x {frozen_runtime}s plus {ENGINE_SEALING_RESERVE_SECONDS}s sealing reserve; require at least {required_overall}s"
        )
    if overall > MAX_OVERALL_SECONDS:
        raise ValueError("BTF25_OVERALL_TIMEOUT exceeds the engine 24h cap")
    required_command = overall + OUTER_CONTAINER_RESERVE_SECONDS
    command = requested_command if requested_command is not None else required_command
    if type(command) is not int or command < required_command:
        raise ValueError(
            f"BTF25_COMMAND_TIMEOUT_SECONDS={command!r} cannot cover the engine timeout plus {OUTER_CONTAINER_RESERVE_SECONDS}s bounded container reserve; require at least {required_command}s"
        )
    if command > MAX_OVERALL_SECONDS + OUTER_CONTAINER_RESERVE_SECONDS:
        raise ValueError("BTF25_COMMAND_TIMEOUT_SECONDS exceeds the bounded 24h engine plus container reserve")
    return {
        "requested_concurrency": concurrency,
        "effective_concurrency": effective_concurrency,
        "batch_count": BATCH_COUNT,
        "waves_per_batch": waves,
        "profile_max_runtime_seconds": frozen_runtime,
        "sealing_reserve_seconds": ENGINE_SEALING_RESERVE_SECONDS,
        "overall_timeout_seconds": overall,
        "overall_timeout": f"{overall}s",
        "command_timeout_seconds": command,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", type=Path, required=True)
    parser.add_argument("--max-concurrency", type=int, required=True)
    parser.add_argument("--overall-timeout")
    parser.add_argument("--command-timeout-seconds", type=int)
    args = parser.parse_args()
    print(json.dumps(plan(args.profile, args.max_concurrency, args.overall_timeout, args.command_timeout_seconds), sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()
