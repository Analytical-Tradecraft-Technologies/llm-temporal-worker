# Query audit logging

Completed `llm.query.v1` reads emit best-effort audit events through normal
worker logs. `control.QueryService.Audit` remains the observation hook for
future audit sinks, but neither audit encoding nor a sink error can turn a
successful query into a failure or a retry. Implementations must return promptly.
There is no durable delivery, replay, or exactly-once guarantee for log events.

`runtime.NewPersistedQueryServiceBuilder` requires authorization and cursor
keys, but no audit repository. It uses the optional supplied `Logger`, or a
logger configured from the snapshot's log format and level writing to stderr.
The low-level constructor also accepts an optional custom `Audit` callback;
when omitted it uses the same log behavior.

The `observability.Logger.QueryAudit` function emits an info-level
`control query completed` event with the query kind, API version, source,
request/response digests, timestamps, duration, and exact-or-unknown cost.
Tenant, project, and operation keys are hashed. Request/response JSON, prompts,
credentials, and raw provider payloads are never passed to the log handler.
Log levels apply normally, and an unavailable log destination does not fail
the query. Only validated successful responses reach the hook; these events
are not a complete security or access log.

This does not change the durable lifecycle or cost records for paid generation
and compaction attempts. Normal query reads no longer add entries to the SQL
query-execution ledger or its historical spend totals.

## Legacy SQL repository

`postgres.QueryExecutionRepository` remains available for direct callers until
the SQL persistence migration removes it. It is not bound by the runtime audit
builder. The following describes that legacy repository only.

Each row stores bounded, canonicalized request and response JSON, a SHA-256
digest over the canonical response bytes,
the closed query kind and source, exact-or-unknown cost metadata, and UTC
timestamps. Prompts, model output, credentials, provider bodies, and raw tool
payloads are rejected recursively. Lookup columns use keyed HMACs; request JSON may still contain scope and
operation values. These rows are not anonymous.

Rows are idempotent on `(scope_id, operation_key_hmac)`. Repeating an operation
with the same request fingerprint returns the persisted record. Reusing the
operation key with a different fingerprint returns
`ErrQueryExecutionConflict`, so a retry cannot silently overwrite audit data.

Cost metadata is explicit. Exact rows carry a validated `pricing.USD` amount
and one of `control_query_zero`, `provider_reported`, or `catalog_usage`;
`control_query_zero` can only be zero. Unknown rows carry no amount or method
and must provide a bounded lower-snake-case reason code. The repository applies
the configured retention interval when a caller omits the expiry timestamp.
PostgreSQL also enforces `retention_expires_at > completed_at`, so a direct
writer cannot create an already-expired audit row that bypasses the bounded
retention horizon.

`QueryExecutionRepository.RecordAudit` adapts the storage-neutral
`control.QueryService.Audit` callback to this ledger. It canonicalizes and
fingerprint-checks the request, converts exact USD text without floating-point
rounding, and delegates to `Record` for redaction, retention, and idempotency:

```go
queryService.Audit = repository.RecordAudit
```

`Record` also verifies that every request fingerprint matches the canonical
request JSON before it writes the row, so direct repository callers cannot
persist an audit identity that is detached from its request payload. On an
idempotent replay it performs the same binding against the persisted request
JSON and keyed fingerprint, so a direct database mutation cannot silently
change the audit payload returned by a retry.

The production factory still owns construction of the repository, query
handlers, and authorization policy; this adapter does not select provider
refreshes or implement query-specific read/index plans.

## Runtime composition

The reloadable runtime keeps the query service on the same immutable snapshot
as the engine. Missing read repositories remain unsupported capabilities;
authorization and cursor validation remain mandatory. If no query service is
composed, `llm.query.v1` still returns a configuration error rather than an empty
answer. Audit logging does not add a database readiness requirement.

Focused checks:

```sh
cd golang
go test ./control ./internal/runtime ./internal/observability
```
