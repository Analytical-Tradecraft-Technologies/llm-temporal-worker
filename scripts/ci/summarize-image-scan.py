#!/usr/bin/env python3
"""Print only allowlisted finding metadata; never print scan payloads or paths."""

import json
import re
import sys


def summarize(document):
    vulnerabilities = []
    for result in document.get("Results", []):
        for finding in result.get("Vulnerabilities") or []:
            identifier = finding.get("VulnerabilityID", "")
            severity = finding.get("Severity", "")
            vulnerabilities.append({
                "id": identifier if re.fullmatch(r"CVE-[0-9]{4}-[0-9]{4,}|GHSA-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}", identifier) else "unlisted",
                "severity": severity if severity in {"UNKNOWN", "LOW", "MEDIUM", "HIGH", "CRITICAL"} else "UNKNOWN",
                "has_fix": bool(finding.get("FixedVersion")),
            })
    return {"vulnerabilities": vulnerabilities}


if __name__ == "__main__":
    try:
        with open(sys.argv[1], encoding="utf-8") as report:
            print(json.dumps(summarize(json.load(report)), sort_keys=True))
    except (OSError, ValueError, TypeError, AttributeError, IndexError):
        print("Image scan summary unavailable", file=sys.stderr)
        sys.exit(1)
