# Maintenance contract

Maintenance is a bounded, separately operated concern. It is not a Temporal
Activity and it must not run with the worker's `llmtw_runtime` role. Operators
run the maintenance adapter with `llmtw_maintenance`, which is granted the
destructive privileges needed for cleanup while runtime remains append-only
for checkpoints, provider state, and control history.

## Retention passes

`golang/maintenance` exposes a storage-neutral `RetentionPolicy` and
`RetentionStore`. Every pass has an explicit UTC `Now` and a batch `Limit`
(maximum 10,000). A cache row is eligible only when its `last_used_at` is
older than the cache cutoff and it is ready, inactive, has no retained
descendant, has no active fill, and has no non-terminal operation recorded in
`response_cache_uses`. Other resource kinds use their own expiry horizon. The
in-memory adapter rechecks these facts while holding its lock; PostgreSQL
adapters must repeat them in the locked SQL statement.

Cache rows are tombstoned rather than immediately deleted. This preserves the
dedupe boundary and lets the transaction enqueue an external blob deletion.
Rows that are active, recently used, referenced by a retained descendant, or
under an active fill are skipped. A batch never loads the active budget working
set into a worker process.

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
account-epoch ordering rather than an unlocked pre-scan. Operation and budget
rows remain disabled until their restrictive foreign keys and audit/rebuild
obligations can be handled in their own transaction. Checkpoint retention is
available through `PruneExpiredCheckpoints`: it locks an
expired candidate batch, protects active origin operations, graph descendants,
compaction references, parent-operation references, and cache origins, then
deletes provider-state children and checkpoints in one transaction. Referenced
blobs are deliberately left for the independent blob-retention fence.

## Outbox lifecycle

Cleanup publishes a safe event in the same PostgreSQL transaction that marks a
cache entry tombstoned. `Event` payloads are canonical JSON objects containing
identifiers or encrypted locators only; duplicate keys and payloads larger than
64 KiB are rejected. Prompt/response bytes and credentials are never accepted
by the contract. `(event_kind, dedupe_key)` is idempotent, including retries
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

External deletion happens after the transaction commits. A missing object is
success, so retries cannot turn an already-cleaned object into a permanent
failure. Other handler failures move the event to `failed` with a bounded
retry time. `Dispatcher.RunOnce` processes one batch and reports claimed,
completed, missing-object, and retried counts for metrics.

The SQL adapter is deliberately separate from runtime repositories. It uses
the namespace renderer for every relation and rejects unbounded limits,
invalid leases, invalid identifiers, and malformed payloads before issuing
SQL.

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
