#!/usr/bin/env python3
"""Combine only a complete set of successful, redacted gate shard summaries.

Usage: merge-shards.py KIND PREFIX COUNT SOURCE DESTINATION

SOURCE must hold exactly PREFIX-0.json .. PREFIX-<COUNT-1>.json, each a passing
redacted KIND summary; any missing, extra or failing shard is refused and no
combined summary is written. Other files in SOURCE (such as per-target timing
records) are ignored.
"""
import hashlib
import json
from pathlib import Path
import re
import sys

KINDS = {"test_summary", "race_summary", "fuzz_summary"}

if len(sys.argv) != 6:
    raise SystemExit(__doc__.strip().splitlines()[2])
kind, prefix, count_text, source_text, destination_text = sys.argv[1:]
if kind not in KINDS:
    raise SystemExit(f"unsupported gate summary kind: {kind}")
if not re.fullmatch(r"[a-z]+", prefix):
    raise SystemExit(f"invalid shard prefix: {prefix}")
if not re.fullmatch(r"[1-9][0-9]*", count_text):
    raise SystemExit(f"invalid shard count: {count_text}")
count = int(count_text)
source, destination = Path(source_text), Path(destination_text)

pattern = re.compile(re.escape(prefix) + r"-([0-9]+)\.json")
present = sorted(
    int(match.group(1)) for path in source.iterdir() if (match := pattern.fullmatch(path.name))
)
if present != list(range(count)):
    raise SystemExit(f"want {prefix} shards 0..{count - 1}, found {present}")

data = bytearray()
for shard in range(count):
    path = source / f"{prefix}-{shard}.json"
    if path.is_symlink():
        raise SystemExit(f"{kind} shard {shard} is a symbolic link")
    raw = path.read_bytes()
    summary = json.loads(raw)
    if summary.get("kind") != kind or summary.get("status") != "pass" or summary.get("redacted") is not True:
        raise SystemExit(f"invalid {kind} shard {shard}")
    data.extend(raw)
destination.parent.mkdir(parents=True, exist_ok=True)
destination.write_text(json.dumps({"schema_version": 1, "kind": kind, "status": "pass", "output_sha256": hashlib.sha256(data).hexdigest(), "output_bytes": len(data), "redacted": True}) + "\n")
