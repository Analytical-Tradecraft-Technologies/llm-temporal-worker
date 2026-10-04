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

## Removed SQL repository

The SQL query-execution repository and its runtime factory wiring have been
removed. Audit logs are the current sink; no query-execution database or SQL
retention job is required. A future durable audit sink can implement the
existing best-effort hook without changing query success semantics.

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
