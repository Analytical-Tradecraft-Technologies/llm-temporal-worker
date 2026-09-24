# Cloud request repository

`golang/storage/cloudstate` is the durable-request foundation for the migration
away from SQL. It uses the released `cloud-storage` v0.1.0 generic KV, blob, and
event sourcing modules. The provider factory currently supports AWS IAM,
DynamoDB, and S3. The repository itself uses only the generic store contracts.

This package is not yet wired into the worker CLI, activities, or workflows.
Existing request/checkpoint/cache/spend persistence and SQL dependencies remain
until their callers are moved. Budgets and provider status stay in Redis.
There is no SQL data import: this service has not been deployed.

## Opening existing stores

Decode this adapter configuration with `encoding/json` into `cloudstate.Config`:

```json
{
  "provider": {
    "type": "aws",
    "aws": {"region": "ap-southeast-2"},
    "key_value_stores": {"requests": "llm-requests"},
    "blob_stores": {"payloads": "llm-payloads"}
  },
  "request_table": "requests",
  "payload_store": "payloads",
  "namespace": "requests-v1"
}
```

Call `cloudstate.Open(ctx, config, secret)` with a stable, independently
provisioned 32-byte encryption secret from the service's secret resolver. The
secret is separate from JSON, IAM credentials, and Redis credentials. Retain it
with backups; losing or replacing it makes existing data unreadable. Rotation
and re-encryption are not implemented in this first adapter.

`Open` resolves application aliases and validates existing stores; it never
provisions infrastructure or lists account-wide resources. The AWS backend
requires a DynamoDB table with string `pk` and `sk` keys and an S3 bucket meeting
the [provider requirements](https://github.com/Analytical-Tradecraft-Technologies/cloud-storage/blob/master/golang/storage/providers/aws/README.md).
For other providers or tests, use `NewRepository` with already-open generic
store handles. Callers own the clients' lifecycle.

## Request state

Generate one `llmtw_req_<UUID>` with `NewRequestID`. Persist a `CreateRequest`
containing the authenticated tenant/project scope, kind (`generate` or
`compact`), nonnegative `RequestIndex` (normally zero), creation time, and a
JSON-object manifest. The manifest must contain the complete normalized request
and versioned configuration/policy references needed for recovery. Never mutate
these arguments when retrying an uncertain storage write.

`Create`, `Read`, and `TryUpdate` return a materialized `Record`, including its
revision and JSON-object `Progress`. The workflow owns the progress schema and
must include a schema version, selected route, provider job reference, budget
receipt, and any other context needed to continue. This storage package does
not interpret that schema or perform provider calls.

`Read(ctx, scope, id)` checks the authenticated scope before opening the payload;
a different scope gets the same not-found classification as a missing request.
`ReadForRecovery` and `ListPending` are privileged service-internal APIs. Never
expose them directly as public activities or treat an ID as authorization.

`TryUpdate` accepts the expected revision, a stable opaque update token, complete
replacement progress, status, and timestamp. Exactly one competing update can
win at a revision. Repeat the same arguments after an uncertain acknowledgement:
an identical committed update returns its original state even after later
updates have committed. A different update at that revision returns
`providercontracts.ErrConflict`. Completed and failed requests are immutable.
The repository does not expose event history through its public API.

Valid states are `pending`, `running`, `provider_pending`, `completed`, `failed`,
and `outcome_unknown`. An unknown provider outcome stays discoverable. A later
workflow may retry paid work from that state only after acquiring fresh budget;
the repository never refunds or reuses a lease. There is no cancellation state
or request-cancellation API. Storage calls still honor Go context cancellation;
cancelling an I/O wait does not undo a possibly committed mutation.

The keyed request fingerprint includes scope, kind, sample index, and canonical
manifest JSON. It excludes the independent request ID and creation time. JSON
numbers preserve exact integer precision, and duplicate object properties are
rejected. Changing the sample index changes the fingerprint. This is an identity
primitive, not a response cache: completion-age eligibility, successful-response
validation, compaction reuse, and cache lookups belong to the later cache layer.

## Persistence and recovery ordering

Creation writes, in this order:

1. A conditional recovery-index row at `<namespace>/pending/<shard>` with the
   internal request ID as its sort key, revision zero, and status `pending`.
2. An immutable encrypted blob containing the full request state.
3. An immutable event pointing to that blob in `<namespace>/request/<id>`.
4. A conditional advance of the recovery-index revision and status.

The shard is SHA-256 of the full prefixed ID modulo eight. Eight is a persisted
format constant. Event rows contain opaque payload references and keyed hashes
of the scope/request/update token; prompt text, provider job references, and
budget receipts live only inside encrypted payloads. AES-256-GCM authenticates
the blob key, and the keyed content digest also binds it to the request and
namespace. A payload reference is never published before the bytes exist.

Reads rebuild the authoritative pointer with the shared event sourcing library,
then authenticate/decrypt the payload. There is no local state required across
process restarts. Updates use the same blob/event/index sequence, and older
retries never move the index backward. If the event committed but updating the
index failed, the error matches `ErrIndexPending` as well as its underlying
cause. Retry the identical update to reconcile it; do not start another paid
attempt in response to a storage error alone.

`ListPending` reads one bounded page from one shard, without a table scan or
secondary index. Iterate all eight shards and follow every nonempty cursor,
including after an empty result page. Page size defaults to 100 and is capped at
1,000. An initializing row can exist before the first event: `ReadForRecovery`
must succeed before any work is attempted. A lagging index can still list a
completed request, so always read authoritative state before acting. Pagination
is not a snapshot. An external reconciler must repeat passes to discover new
requests. Automatic recovery and cleanup are future work.

## Limits and operational assumptions

- The full serialized state is capped at 8 MiB and each request at 10,000
  revisions. Payloads are complete state snapshots; event replay and historical
  retry lookup have linear read cost. This first adapter has no compaction or
  snapshot optimization for very long histories.
- Store handles must honor the shared library's conditional-write, immutable
  blob, and ordered partition-query contracts. The AWS adapter uses strong KV
  reads and disables SDK retries; the repository retries only definite index
  CAS conflicts, at most 16 times. Uncertain mutation outcomes are returned.
- Reserve the configured namespace for this repository. Do not expire, delete,
  or overwrite its index rows, immutable events, or referenced payloads with
  external TTL/lifecycle jobs. Removing an earlier event breaks replay and can
  invalidate the event library's concurrency guarantees.
- Terminal index rows are retained and filtered while listing. Abandoned
  initialization rows and blobs from failed competing updates may also remain.
  Retention and garbage collection require a separate coordinated design.
- Request IDs, status, revisions, timestamps, payload sizes, and access patterns
  are metadata, not concealed by payload encryption. Apply normal table/bucket
  access controls and provider encryption-at-rest settings as well.

## Verification boundary

The offline suite uses the released event sourcing implementation over shared,
linearizable KV/blob doubles. It exercises concurrent writers, restart reads,
lost acknowledgements at every creation write boundary, update retries, index
lag/repair, all eight discovery shards, scope isolation, corrupt data, encrypted
payload binding, bounded reads, and configured provider/store selection. Run
`go test -race ./storage/cloudstate` from `golang`.

These tests do not prove deployed IAM permissions, live DynamoDB/S3 behavior,
Temporal recovery, or a SQL-free running worker. Those gates belong to the
subsequent composition and deployment changes.
