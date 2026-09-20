#!/usr/bin/env python3
"""Combine only a complete set of successful, redacted fuzz shard summaries."""
import hashlib
import json
from pathlib import Path
import sys

source, destination = map(Path, sys.argv[1:])
data = bytearray()
for shard in range(3):
    raw = (source / f"fuzz-{shard}.json").read_bytes()
    summary = json.loads(raw)
    if summary.get("kind") != "fuzz_summary" or summary.get("status") != "pass" or summary.get("redacted") is not True:
        raise SystemExit(f"invalid fuzz shard {shard}")
    data.extend(raw)
destination.parent.mkdir(parents=True, exist_ok=True)
destination.write_text(json.dumps({"schema_version": 1, "kind": "fuzz_summary", "status": "pass", "output_sha256": hashlib.sha256(data).hexdigest(), "output_bytes": len(data), "redacted": True}) + "\n")
