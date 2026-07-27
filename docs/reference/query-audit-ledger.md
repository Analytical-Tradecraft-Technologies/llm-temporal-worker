# Query execution audit ledger

`postgres.QueryExecutionRepository` is the persistence boundary for the
`llm.query.v1` audit record. It is intentionally usable before the Activity is
composed: callers pass already validated, redacted control JSON and the
repository enforces the storage invariants.

Each row stores bounded, canonicalized request and response JSON, a SHA-256
digest over the canonical response bytes,
the closed query kind and source, exact-or-unknown cost metadata, and UTC
timestamps. Prompts, model output, credentials, provider bodies, and raw tool
payloads are rejected recursively. Scope and operation values are never stored
in the row; keyed HMACs bind the row to the configured namespace key.

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
as the engine. An `EngineFactory` that owns the durable query repositories may
implement the optional `runtime.QueryServiceFactory` seam and return a fresh
`activity.QueryService` from `BuildQueryService` for each snapshot. A custom
client set may instead expose the service through `runtime.QueryServiceSource`.
The Activity acquires a snapshot lease before dispatch, so reload closes the
old query repository only after in-flight query work releases its lease.

`runtime.NewPersistedQueryServiceBuilder` is the production composition seam:
it requires deployment-owned authorization and cursor key material and binds
`PostgresQueryRepositories.QueryAudit.RecordAudit` from the same immutable
client set as the read repositories. A snapshot without that audit capability
is rejected before worker polling.

If neither seam is supplied, the worker remains fail-closed: `llm.query.v1`
returns a configuration error and does not perform a provider or storage read.
This is deliberate until a deployment composes the PostgreSQL query handlers,
authorization policy, and `RecordAudit` callback together; wiring a query
service must never silently bypass the dedicated audit ledger.

Focused checks:

```sh
cd golang
go test ./storage/postgres
```

The optional PostgreSQL integration test runs when `LLMTW_POSTGRES_ADDR` points
at a disposable database and verifies HMAC-only storage, exact-cost precision,
and idempotent replay.
