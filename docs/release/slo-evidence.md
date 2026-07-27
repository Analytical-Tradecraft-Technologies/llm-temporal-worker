# Redacted SLO evidence

This contract records an operator-collected pass measurement for the two v1 objectives: admission-and-compilation p99 and the worker-caused error rate. It is deliberately separate from the release catalog: recording a measurement does not bind it to a release, authorize publication, or stand in for protected live-provider evidence.

The candidate input accepts only the typed measurement fields defined by [`slo-evidence-candidate.schema.json`](slo-evidence-candidate.schema.json). That candidate schema is deliberately distinct from the persisted [`slo-evidence.schema.json`](slo-evidence.schema.json), which adds the recorder-owned `schema_version`, `kind`, `status`, `redacted`, and `content_sha256` fields. The candidate never accepts provider credentials, prompts, model responses, endpoint addresses, IP addresses, or raw metric/benchmark output. The operator must collect and redact those sources before creating the candidate input.

Record a new, redacted pass measurement in a previously unused path:

```sh
python3 scripts/release/slo-evidence.py record \
  --input /secure/operator-supplied-redacted-candidate.json \
  --evidence /secure/release-evidence/slo-measurement.json
```

The command writes canonical JSON with mode `0600` using create-only semantics. It refuses to overwrite an existing path. Its printed SHA-256 is calculated over the canonical record excluding `content_sha256`; retain that digest when a future release candidate binds the measurement into immutable release evidence.

Verify a retained record before binding it:

```sh
python3 scripts/release/slo-evidence.py verify \
  --evidence /secure/release-evidence/slo-measurement.json \
  --source-revision <40-character-source-revision> \
  --content-sha256 <recorded-content-sha256>
```

The verifier checks the exact schema-shaped object, canonical serialization, content digest, expected source revision, and all pass invariants. It rejects tampering, non-regular files, oversized inputs, unknown fields, and unsafe value shapes.

The recorder and verifier also reject duplicate JSON object keys (including
nested measurement fields) and non-finite JSON constants. This prevents a
different parser from observing a different value than the release-evidence
tool before the content digest is bound.

The evidence has two admission-and-compilation samples: `memory` and `same_region_redis`. Their p99 values must be strictly below 25,000 and 75,000 microseconds respectively. The Redis sample records only its major version, persistence mode, and function digest; it intentionally omits its address and credentials.

The worker measurement records a closed UTC window plus completed and worker-failed attempt counts. It passes only when:

```text
worker_failed_attempts / (completed_attempts + worker_failed_attempts) < 0.001
```

This matches the worker-origin metric semantics in the operations guide. It is a retained measurement contract, not a claim that an SLO has been observed in any particular environment. The controlled Redis benchmark procedure remains in [the testing strategy](../testing/strategy.md).
