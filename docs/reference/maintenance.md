# Maintenance contract

Maintenance is a bounded, separately operated concern. It is not a Temporal
Activity and it must not run with the worker's `llmtw_runtime` role. Operators
run the maintenance adapter with `llmtw_maintenance`, which is granted the
destructive privileges needed for cleanup while runtime remains append-only
for checkpoints, provider state, and control history.

## One-shot retention run

`postgres.MaintenanceRepository.RunRetentionBatch` is the bounded one-shot
orchestration entry point for a maintenance-role adapter. The caller supplies
one explicit UTC `Now`, a positive `Limit` no greater than 10,000, every
resource cutoff, and the largest configured budget window. It runs the passes
in this order: cache, provider status, inventory, query executions, terminal
operations, budget buckets, then checkpoints. Each pass is represented in the
returned result with its resource name, bounded row counts, and error. The
runner stops at the first pass error or context cancellation; it never reports
later passes as successful. The repository's existing `MaintenanceObserver`
hooks emit the per-pass progress, failure, and latency metrics.

The worker CLI intentionally does not open a maintenance-role connection. The
repository ships the separately operated
[`llmtw-maintenance`](../../golang/cmd/llmtw-maintenance/main.go) binary for
this adapter. Build it with `go build ./cmd/llmtw-maintenance` from `golang/`.
It requires the dedicated `LLMTW_MAINTENANCE_POSTGRES_USERNAME` and
`LLMTW_MAINTENANCE_POSTGRES_PASSWORD` environment variables; worker runtime
credential variables are not used. The command reads namespace, TLS, and pool
settings from `--config`, never installs schema, performs one bounded pass,
emits the pass result as JSON, and exits non-zero on a failed pass.

For example, an operator-owned scheduled job can invoke:

```sh
LLMTW_MAINTENANCE_POSTGRES_USERNAME=llmtw_maintenance \
LLMTW_MAINTENANCE_POSTGRES_PASSWORD='(injected by the job secret store)' \
  ./llmtw-maintenance retention-once \
  --config /etc/llmtw/config.yaml \
  --now 2026-07-29T00:00:00Z \
  --limit 1000 \
  --cache-before 2026-07-01T00:00:00Z \
  --provider-status-before 2026-07-01T00:00:00Z \
  --inventory-before 2026-07-01T00:00:00Z \
  --query-executions-before 2026-07-01T00:00:00Z \
  --operations-before 2026-07-01T00:00:00Z \
  --budget-buckets-before 2026-07-01T00:00:00Z \
  --checkpoints-before 2026-07-01T00:00:00Z \
  --max-budget-window 30d
```

The schedule, secret injection, and role grants remain deployment-owned;
keeping them outside the worker prevents a runtime credential from acquiring
cleanup privileges.

Blob metadata eligibility is an explicit, separately bounded pass. Run it with
the same dedicated role when the object-store deleter is scheduled to consume
newly eligible rows:

```sh
LLMTW_MAINTENANCE_POSTGRES_USERNAME=llmtw_maintenance \
LLMTW_MAINTENANCE_POSTGRES_PASSWORD='(injected by the job secret store)' \
  ./llmtw-maintenance blob-gc-once \
  --config /etc/llmtw/config.yaml \
  --now 2026-07-29T00:00:00Z \
  --limit 1000
```

This command only marks expired, unreferenced blob metadata `eligible`; its
bounded SQL rechecks active operation, checkpoint, provider-state, cache-use,
and cache-fill references while each candidate is locked. It does not perform
an object-store delete. The deleter must claim eligible rows, delete the
external object after the SQL transaction commits, and finalize the row using
the fenced lifecycle described in [blob garbage collection](blob-garbage-collection.md).
Keeping eligibility explicit prevents a retention invocation from silently
claiming external deletion work or from treating a successful SQL mark as
proof that the object was physically removed.

To capture the current table-settings snapshot without running any cleanup,
use the same dedicated role with the read-only `inspect-settings` command:

```sh
LLMTW_MAINTENANCE_POSTGRES_USERNAME=llmtw_maintenance \
LLMTW_MAINTENANCE_POSTGRES_PASSWORD='(injected by the job secret store)' \
  ./llmtw-maintenance inspect-settings \
  --config /etc/llmtw/config.yaml > maintenance-settings.json
```

The JSON report is limited to the configured namespace and contains one entry
per expected maintenance relation. It includes only explicitly configured
reloptions and observed PostgreSQL tuple/timestamp statistics; omitted options
are not inferred defaults. The command is read-only and still requires the
dedicated maintenance credentials, never the worker's runtime credential
references. This snapshot is configuration evidence, not load or SLO evidence:
it does not establish p99 latency, error rate, or production suitability.

## Retention passes

`golang/maintenance` exposes a storage-neutral `RetentionPolicy` and
`RetentionStore`. Every pass has an explicit UTC `Now` and a batch `Limit`
(maximum 10,000). A cache row is eligible only when its `last_used_at` is
older than the cache cutoff and it is ready, inactive, has no retained
descendant, has no active fill, has no non-terminal operation recorded in
`response_cache_uses`, and has no retained blob reference owned by another row.
The candidate's own blob is released by its tombstone/delete lifecycle and is
not an external reference. Other resource kinds use their own expiry horizon.
The in-memory adapter rechecks these facts while holding its lock; PostgreSQL
adapters must repeat them in the locked SQL statement.

Cache rows are tombstoned rather than immediately deleted. This preserves the
dedupe boundary and lets the transaction enqueue an external blob deletion.
Rows that are active, recently used, referenced by a retained descendant, under
an active fill, or still referenced by another retained row are skipped. A
candidate-owned blob follows the candidate's tombstone/outbox lifecycle. A
batch never loads the active budget working set into a worker process.

The policy contract includes status, inventory, operation, budget, and
checkpoint horizons so adapters can add table-specific retention safely. It
also includes a query-execution horizon for the immutable control-query audit
ledger. The PostgreSQL adapter exposes bounded status-history,
inventory-snapshot, and query-execution cleanup in addition to the cache
tombstone path. Query executions have no durable children, so their expiry
index can be reclaimed directly without touching inference operations or
budget history. Status cleanup preserves every event referenced by
`provider_route_status.last_event_id`. Inventory
cleanup deletes child model rows in the same transaction and preserves the
latest snapshot for each configuration/provider/endpoint/account epoch, even when that
snapshot has expired. Both passes use `FOR UPDATE SKIP LOCKED` and bounded
expiry indexes (`provider_status_expiry_idx` and
`provider_inventory_expiry_idx`); query cleanup uses
`query_executions_retention_idx`; the inventory latest-row check uses the
account-epoch ordering rather than an unlocked pre-scan. The PostgreSQL
adapter also exposes `PruneExpiredOperations`, a deliberately conservative
orphan pass for terminal operations: eligible rows must have inline
request/result data, no attempt/audit, budget, cache, checkpoint, parent/child,
or blob references, and an exact settled cost. Unknown-cost and still-pending
cost states remain fenced until authoritative cost resolution is recorded. This
pass is bounded by
`operations_exact_terminal_expiry_idx` and repeats every reference predicate while
locking candidates with `FOR UPDATE SKIP LOCKED`. Full operation history and
journal/reservation retention remain disabled until their broader restrictive
foreign-key and audit/rebuild obligations can be handled in their own
transaction; the bounded empty-bucket fence is described below. Checkpoint retention is
available through `PruneExpiredCheckpoints`: it locks an
expired candidate batch, protects active origin operations, graph descendants,
compaction references, parent-operation references, and cache origins, then
deletes provider-state children and checkpoints in one transaction. Referenced
blobs are deliberately left for the independent blob-retention fence.

## Unknown-cost reconciliation queue

`postgres.UnknownCostRepository.List` is a read-only, scope-bound,
cursor-paginated queue of completed operations whose actual cost is still
unknown. It returns only an opaque operation UUID, completion time, and the
safe unknown-reason code; request/result data, provider identifiers, and
credentials are not exposed. The cursor is `(completed_at, operation_id)` in
descending order and is bounded to 10,000 rows, matching the scoped
`operations_unknown_cost_idx` contract.

This queue does not call a provider, Redis, or an external billing system. An
authorized billing adapter can pass validated exact USD events to
`postgres.BudgetJournalRepository.ResolveUnknownExact`. That bounded
transaction locks the terminal operation, appends every supplied
`resolve_unknown_exact` revision, updates the reservation and bucket
projections, and changes the operation from unknown to exact together. A
retry with the same event payload is idempotent; a changed amount, operation,
or event identity fails closed. The method does not reconcile Redis: callers
must do that only after commit through the fenced Redis-generation protocol,
and must retain the conservative bound if reconciliation fails. Provider
credentials and raw billing payloads are outside this contract. Until an
authorized adapter supplies evidence, unknown-cost rows remain conservatively
retained and must never be changed to zero.

The storage-neutral `durable.UnknownCostBoundary` is the handoff contract for
that caller. It validates one operation/generation/incarnation identity and a
set of exact `resolve_unknown_exact` events, commits them through the
PostgreSQL journal port, and only then invokes `BudgetMaterializer.Reconcile`.
PostgreSQL errors never call Redis. A Redis error returns the committed journal
receipt with `ErrReconcilePending`; retrying the same event set is safe because
the PostgreSQL transaction and Redis materialization are both idempotent. The
boundary does not claim a cross-store transaction and never lowers the
conservative Redis bound before reconciliation succeeds.

The PostgreSQL adapter also exposes `PruneExpiredBudgetBuckets`. This is a
narrow budget-retention fence, not full budget or operation retention: callers
must provide a cutoff no newer than the maximum configured window horizon, and
the pass deletes only zeroed buckets with no operation reservation row. Journal
events and every reservation row remain intact for audit and cold rebuild; the
active budget working set is never loaded into the worker.

## Outbox lifecycle

Cleanup publishes a safe event in the same PostgreSQL transaction that marks a
cache entry tombstoned. `Event` payloads are canonical JSON objects containing
identifiers or encrypted locators only; duplicate keys and payloads larger than
64 KiB are rejected. Prompt/response bytes and credentials are never accepted
by the contract. The PostgreSQL cache-retention path constructs its deletion
intent through the shared typed constructor, so SQL publication cannot drift
from this validator. The currently emitted `delete_blob` event has an exact typed
payload of `{"blob_id":"..."}` and that identifier must match the event's
`aggregate_id`; unknown fields (including prompt or credential fields) are
rejected before SQL. Other event kinds are rejected until their typed
constructors are added. `(event_kind, dedupe_key)` is idempotent, including retries
whose JSON object key order or whitespace differs. Aggregate type, aggregate
ID, and canonical payload must agree with the original event; a conflicting
dedupe key fails closed.

Workers claim at most the requested batch with `FOR UPDATE SKIP LOCKED` in a
short transaction. Pending/failed rows whose `available_at` has arrived and
processing rows whose lease expired are claimable. Every claim receives a new
opaque `lease_token`, and a live lease is not claimed twice. Completion and
retry must present that token while the lease is still live; a reclaimed row
therefore fences the old worker. The token is retained after the terminal
transition so a duplicate completion/failure request with the same token is an
idempotent success, while a different or expired token returns an ownership
error.

PostgreSQL evaluates lease expiry against the completion/retry timestamp
supplied by the maintenance caller, not an implicit database wall clock. This
keeps deterministic maintenance runs and the in-memory contract aligned: a
caller whose bounded clock has passed the lease is fenced even if the database
clock has not moved by the same amount.

External deletion happens after the transaction commits. A missing object is
success, so retries cannot turn an already-cleaned object into a permanent
failure. Other handler failures move the event to `failed` with a bounded
retry time. `Dispatcher.RunOnce` processes one batch and reports claimed,
completed, missing-object, and retried counts for metrics.

Explicit blob IDs supplied by an outbox worker are also bounded to the
maintenance batch maximum (10,000) and nil IDs are rejected before SQL is
issued. The result limit still bounds claims from that list; bounding the
input prevents a caller from constructing an arbitrarily large PostgreSQL
array.

The SQL adapter is deliberately separate from runtime repositories. It uses
the namespace renderer for every relation and rejects unbounded limits,
invalid leases, invalid identifiers, and malformed payloads before issuing
SQL. Claim and dispatch leases are additionally bounded to 24 hours
(`maintenance.MaxOutboxLease`); a lease is only a fencing interval, so longer
work should be split into a follow-up event rather than holding a row forever.

## Maintenance observability

`storage/postgres` accepts an optional `MaintenanceObserver` and the response
cache repository accepts an optional `CacheObserver`. A deployment can wire
these interfaces to the bounded Prometheus recorders in
`internal/observability`; leaving them nil is a no-op.
Retention and blob passes report eligible, tombstoned, deleted, skipped, and
failed counts, elapsed maintenance time, PostgreSQL pool state, and query
latency. After a pass, the adapter samples only its own namespace from
`pg_stat_user_tables` and reports approximate live/dead tuple counts using
logical resource labels; physical relation names and tenant identifiers never
become metric labels. Cache lookup/fill boundaries report hit, use, miss, and
fill outcomes, while provider-owned polls report started, completed, retry,
or failed outcomes. Cost status telemetry records exact versus unknown
without exporting the amount or unknown reason.

### Table-settings evidence

`MaintenanceRepository.InspectTableSettings` is a read-only diagnostic for
release and operations evidence. It reads `pg_class.reloptions` together with
`pg_stat_user_tables` for the maintenance tables in the configured namespace,
and returns logical resource labels, configured `fillfactor` and autovacuum
options (including explicit `autovacuum_enabled=false`) when they are
present, approximate live/dead tuple counts, and the last autovacuum/analyze
timestamps. Missing options remain unset:
the adapter never substitutes PostgreSQL defaults or a guessed production
threshold. The report also requires every expected maintenance relation; a
missing or unreadable relation, catalog, scan, or option-decoding failure
returns an error rather than producing a partial or inferred report.

This report demonstrates what is configured and what PostgreSQL currently
observes; it does not demonstrate the p99/error-rate objectives or prove that
the values are suitable for production load. Those values remain a separate,
load-tested release-evidence requirement. In particular, this method does not
mutate reloptions or install table-specific autovacuum settings.
