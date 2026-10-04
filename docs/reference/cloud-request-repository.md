# Cloud request repository

`golang/storage/cloudstate` is the durable-request foundation for the migration
away from SQL. It uses the released `cloud-storage` v0.1.0 generic KV, blob, and
event sourcing modules. The provider factory currently supports AWS IAM,
DynamoDB, and S3. The repository itself uses only the generic store contracts.

The worker configuration and snapshot factory can now attach this repository
to an explicitly composed V1 activity runtime using `state.requests`. Generate
and Compact record their inputs before execution and save completed responses
before returning. The same configuration now supplies cloud checkpoint metadata,
blob writes/reads, continuation materialization, and response-cache persistence.
The bounded runtime builder composes execution phases; production CLI authorization
and spend aggregation remain separate integration steps. Budgets and provider
status stay in Redis.
There is no SQL data import: this service has not been deployed.

[Durable cloud budget plans](cloud-budget-plans.md) now save the initial selected
route and exact quote in encrypted request progress before Redis acceptance.
They retain immutable admission inputs for restart and uncertain-reply recovery;
they do not themselves authorize paid dispatch.

See [cloud response-cache persistence](cloud-response-cache.md) for the cache
identity, publication ordering and consuming-finalizer receipt contracts.

## Worker integration

Add this section under the existing `state` settings:

```yaml
state:
  kind: durable
  requests:
    provider:
      type: aws
      aws:
        region: ap-southeast-2
      key_value_stores:
        requests: llm-requests
      blob_stores:
        payloads: llm-payloads
    request_table: requests
    payload_store: payloads
    namespace: requests-v1
    secret:
      kind: env
      name: LLMTW_REQUEST_STORAGE_KEY
```

Durable mode requires `state.requests`; the strict loader rejects the removed
`state.postgres` section. No worker SQL credentials, pool, schema, maintenance
binary or driver dependencies remain. Readiness requires Redis, the result
blob store and cloud request storage. Missing capabilities or a failed cloud
open reject the snapshot and drain its clients. There is no SQL fallback or
SQL data migration.

Cloud composition uses `durable.StateIdentity.Cloud` instead of a fabricated
PostgreSQL namespace. Its comparable identity contains the provider type,
namespace, selected table/blob aliases, and a SHA-256 digest of the provider
configuration (including physical mappings, region and profile). Resolved
credentials are never part of that digest. The complete configuration digest
still binds the rest of the worker settings, including secret references.

Automatic preflight supplies the expected cloud identity before constructing
external clients. The complete runtime builder validates it again against the
snapshot, and composition reuse checks it before either phase gets its ports.
Missing or mismatched identities reject composition. This
validates the declared storage binding; deployment callbacks must still supply
ports backed by those stores. Both Generate and Compact share one validated
composition per snapshot. SQL identities and implementations have been removed.

Phase factories can now construct the parent-materialization callbacks with
`V1RuntimeCapabilities.NewCheckpointReplay(resolveScope, limits)`. The returned
`Generate` and `Compact` methods fit their respective `Replay` ports. The scope
resolver must authorize the caller and return the same opaque scope used for
checkpoint publication and handle signing; raw tenant/project names are never
used as guessed repository keys. Both callbacks use the captured snapshot's
handle materializer and enforce handle, tenant/project, and tool-frontier
bindings. Generate roots authorize the caller and return an empty base;
follow-ups and Compact load the parent without folding the current delta into it.

This helper performs only parent materialization. Completed-operation and
finalization-handoff replay remain in the outer cloud runtime. The bounded
cloud runtime composes pending-attempt recovery, route/cache/provider execution
and finalization; installing this helper alone does not authorize paid work.
Missing capabilities and invalid limits reject construction. Scope, handle,
transcript and storage failures stop before later phases, and raw resolver/SDK
errors are excluded from the serialized provider error.

The same phase factories can construct Redis admission callbacks with
`V1RuntimeCapabilities.NewBudgetAdmission(generatePlanner, compactPlanner)`.
`ReserveGenerate`/`ClaimGenerate` fit Generate's ports, and
`ReserveCompact`/`ClaimCompact` fit Compact's ports. The helper captures the
already validated composition's budget boundary, creates no clients, and holds
no invocation state. It does not use a separate leaser from another snapshot.
Both planners must quote the selected route and resolve budget windows from
their captured configuration. Planned operation/generation IDs must match the
route, and a reservation must include at least one window. Claiming validates
the reservation's identity/events without re-running the planner.

Reserve returns acquired or wait immediately. An uncertain acceptance can be
retried with the identical operation, windows, quote and `ExpiresAt`; changing
these inputs is not a safe acceptance retry. Redis fixes the start deadline at
first acceptance: at most 15 minutes, or an earlier planner expiry. Replaying
acceptance does not renew it. Claim must succeed immediately before provider
submission and can grant permission only once, across workers. A lost claim
reply, duplicate claim, invalid receipt, or cancellation after consumption
returns a non-retryable ambiguous result without dispatch permission. The
reservation remains charged for recovery; another paid attempt requires fresh
budget. Expired, unclaimed leases return a budget-wait error. Workflow timers
and recovery policy remain outside these callbacks. No provider cancellation
API is added. Planner/Redis error text is excluded from caller errors.

This adapter supplies admission and claiming only. The bounded runtime builder
composes route/pricing planners, provider execution and cache/compaction phases.
Production CLI authorization still requires explicit wiring. Budget settlement follows
durable result finalization, rather than refunding on a submission uncertainty.

For response-cache lookup, phase factories can construct
`V1RuntimeCapabilities.NewResponseCacheLookup(generatePlanner, compactPlanner)`
and install its `Generate` and `Compact` methods as the `CacheLookup` ports.
The typed planners authorize the scope/route, compute the fingerprint, and
persist a stable fill lease whose attempt matches the Redis budget generation.
The helper captures the validated snapshot's response/fill repositories and
clock, binds the request's freshness/sample policy, and skips planners and stores
when caching is omitted. Its decisions retain the existing ownership gate:
hits bypass budgets, wait/recovery stop before routing, and an owned miss must
start its fill before claiming Redis budget. Lookup/acquisition failures stop
execution without being treated as misses or exposing SDK error text. See
[cloud cache execution](cloud-cache-execution.md) for planning, finalization,
recovery, and remaining production composition requirements.

Without `state.requests`, durable configuration fails validation. Spend summary
remains unsupported unless the deployment supplies a cloud aggregation reader
implementing `control.SpendSummaryReader`. Query audits use structured logs.
The bounded runtime builder is available; the CLI still requires explicit
production authorization composition before it can poll for paid work.

`secret` references standard base64 encoding of an independent, stable 32-byte
key. File references are also accepted; workload tokens are not suitable for
durable encryption. Resolved bytes never enter the configuration digest or
effective configuration output. AWS uses its IAM/default credential chain;
optional `aws.profile` and `aws.temp_directory` are supported. Inline AWS keys
and unknown configuration fields are rejected.

The factory opens existing aliased resources once per configuration snapshot,
exposes the repository through `V1RuntimeCapabilities.Requests`, and wraps the
configured V1 runtime. A failed open rejects the snapshot and drains its existing
clients. Readiness revalidates the named table/bucket and requires both a bounded
table query and a blob read (a missing probe object is normal, a missing bucket
is not). These checks establish read access, not write access;
they do not provision or modify resources. Durable mode rejects a missing
`state.requests` section.

This setting does **not** supply missing Generate/Compact phase factories or
start a production worker on its own. An explicit durable V1 runtime is still
required, even in development when request recording is enabled. The factory
supplies the complete cloud checkpoint bundle before invoking the V1 builder,
using the snapshot's continuation keyring. No worker PostgreSQL configuration
or backend remains.

The operation binding includes tenant, project, activity kind, and
`operation_key`. Its internal ID is a prefixed UUIDv8 derived using a separate
HMAC domain. Reusing the same operation key with a different input is a conflict;
changing the key creates a separate operation. For both Generate and Compact,
`cache.variant` supplies the independent sample index (default zero).
The current request/response wire format and activity names remain unchanged.

The first write is the eight-shard discovery row. Concurrent initializers reuse
its timestamp, including after a crash before the first event. The encrypted
manifest contains the full submitted V1 input. A `running` record is an
observation, **not a dispatch lease**: the inner durable runtime must still
enforce operation idempotency and Redis's single-use claims. Retrying an
unfinished operation invokes that same inner operation; this wrapper never
submits to a provider or acquires/refunds budget itself.

Only an inner success, after its checkpoint finalization and Redis settlement,
is persisted as a completed response. A completed operation replays that exact
response without invoking the inner runtime. This includes incomplete model
responses as operation results; it does not admit them to a cross-request
success cache. Failed inner calls remain discoverable for recovery and preserve
the original error/retry policy. This layer does not infer a provider outcome
from an error, mark paid work refundable, or implement new paid retries.

If result publication loses its acknowledgement, retrying repairs the discovery
index or replays the inner runtime's already finalized result. A returned response
is saved with the configured, bounded `server.finalization_timeout`, even if its
caller context has just ended. Different terminal responses cannot overwrite
one another. Query calls pass through unchanged.

The bounded cloud runtime now composes cross-operation cache reuse, persisted
route/provider/budget progress, independent paid attempts, and finalization.
Worker startup registers the public generation and compaction workflows and
their internal execution and budget workflows; see
[activity runtime](activity-runtime.md). Production authorization, deployment
verification, and background cleanup/recovery orchestration remain separate
work. No cancellation API is exposed.

## Checkpoint persistence

`repository.Checkpoints()` implements `state.CheckpointStore`: the existing
repository and blob-reader interfaces plus an immutable blob writer. The worker
exposes these through `V1RuntimeCapabilities.Checkpoints`, including `BlobWriter`
and an opaque-handle materializer. A custom cloud factory must provide
`CloudCheckpointSource`; missing stores or handle verification reject the
snapshot; there is no fallback checkpoint backend.

A finalizer encodes delta, response, settings and optional snapshot blobs with
`state.CheckpointBlobCodec`, writes them using `BlobWriter.Write`, and places the
returned references in a `state.DurableCheckpoint`. It stages that checkpoint
with `BeginCheckpoint` / `PutCheckpoint` and publishes with `Commit` (or uses
`state.WithCheckpointUnitOfWork`). One unit accepts one distinct checkpoint;
there is no portable multi-checkpoint transaction. Staging copies caller-owned
data and performs no storage writes. Rollback discards local staged data; it
cannot undo a commit with an unknown outcome.

All blobs are encrypted and immutable. References authenticate the opaque scope,
media type, digest and byte length before opening an object, and reads verify
the encrypted content and its digest. Publishing checks parent depth, scoped
lineage references and every referenced blob. The complete checkpoint metadata,
including provider-state references and cache affinities, is also encrypted.
Operation/cache origin IDs remain finalizer-supplied provenance: the finalizer
must bind them to an authorized operation/cache result. This adapter does not
authorize a paid request.

Publication uses only generic conditional `Create` operations:

1. Persist encrypted checkpoint metadata after validating its existing blobs.
2. Reserve the checkpoint ID, then its public-handle HMAC, with immutable rows.
3. Create the operation row, which makes exactly one checkpoint visible for
   that scope and origin operation.

Reads require a matching ID reservation and committed operation row. A losing
or interrupted reservation stays unreadable. Concurrent identical writes are
idempotent; different contents, reused IDs/handles or competing checkpoints for
one operation conflict. After a lost acknowledgement, retry the identical
checkpoint, including its IDs and timestamps, in a new unit. Completed retries
perform no writes. Blob/row failure after any intermediate step can leave orphan
objects or reservations; automatic cleanup is deferred, and these names must
not be repurposed. There is no automatic TTL or deletion.

Checkpoint keys use separate HMAC domains under `<namespace>/checkpoint/` in
the same configured table; payloads share the configured bucket. They do not
enter the eight pending-request shards. Scope and checkpoint IDs are not
plaintext table keys. `Get` retains immutable history; the materializer enforces
expiry and graph limits when deciding whether a continuation is usable.

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
payload binding, bounded reads, and configured provider/store selection. The
checkpoint suite additionally covers all publication failure boundaries,
competing IDs/handles/operations, authenticated blob references, continuation
replay after restart, staging/rollback, and expiry. Run
`go test -race ./storage/cloudstate` from `golang`.

These tests do not prove deployed IAM permissions, live DynamoDB/S3 behavior,
Temporal recovery, or a SQL-free running worker. Those gates belong to the
subsequent composition and deployment changes.

## Workflow integration gates

The concurrency gate starts 100 independent public workflow executions for one
operation across two workers and verifies one provider submission and identical
completed responses. It then checks 100 independent operations consuming that
cached response, each with its own operation ID and checkpoint. Concurrent
completion replays the saved winner; poll/settlement conflicts retry observation.
Callers observing submission in progress recheck within five seconds without
shortening the 15-minute deadline for uncertain-submission recovery.

From `golang/`, run `make cloud-workflow-integration`. Docker Compose starts
isolated, digest-pinned Temporal and Redis services on random loopback ports,
then removes that test project's containers and volumes. PostgreSQL in this
harness belongs to the Temporal server; the worker has no SQL backend. The gate
runs on pull requests, merge-queue builds and master.

The test uses real Temporal workflows, activities and Redis budget accounting,
with the production cloud repository and encryption over in-memory generic KV
and blob stores. It covers synchronous and polling generation, restart between
submission and completion, exact operation replay, cache reuse, independent
samples, compaction, denied scope access, exhausted budget followed by timer
resumption, and terminal pending-index cleanup. The LLM adapter is deterministic
and never contacts a paid provider. These results are not AWS integration or
production authorization evidence.

Recovery cases also cover an unused authorization expiring on Redis's clock,
lost responses from both synchronous and create/poll providers, and loss of a
Redis settlement acknowledgement. Aged request records are created through the
normal runtime clock seam; the production 15-minute deadlines and Redis clock
are unchanged. Fresh workers and Temporal callers must use a new paid attempt
after an uncertain submission, retain the original claim and pending index,
and replay settlement without another provider call or refund. Tests inspect
both the lease records and the aggregate Redis budget totals.

`make cloud-workflow-ocaml-integration` builds the nested OCaml package against
its pinned Temporal SDK and uses the same isolated services. One native client
process starts generation twice with identical request IDs; another resumes the
saved execution after the Go worker restarts, checks repeated result observation,
and runs compaction. An OCaml parent on a separate task queue also invokes both
public child-workflow helpers. The test verifies actual child queue routing,
`Abandon` parent-close policies, response identity/sample/lineage, provider call
counts and settled Redis leases. The OCaml CI job runs this gate on PRs,
merge-queue builds and master; ordinary Go tests do not require an OCaml compiler.

`make cloud-workflow-aws-integration` runs the polling lifecycle against existing
disposable DynamoDB and S3 resources. It requires explicit operator configuration:

- `LLMTW_CLOUD_TEST_AWS=1` enables AWS writes.
- `LLMTW_CLOUD_TEST_CONFIG` contains the JSON adapter configuration shown above,
  with disposable table and bucket aliases. The test replaces `namespace` with a
  fresh UUID-based namespace and prints it for inspection.
- `LLMTW_TEMPORAL_ADDRESS` and `LLMTW_REDIS_ADDR` address an existing test Temporal
  namespace (`default`) and Redis. `LLMTW_REDIS_USERNAME`,
  `LLMTW_REDIS_PASSWORD` and `LLMTW_REDIS_KEY_PREFIX` configure Redis access.
- Ordinary AWS IAM credentials resolve through the AWS SDK. Redis must already
  contain the matching admission Functions library; this target does not load or
  replace shared server code and refuses CI execution.

The AWS gate retains synthetic encrypted table/blob data for inspection; the
operator owns deletion of the disposable resources. Redis cleanup is limited to
that run's random hash tag. Test encryption keys are fixed synthetic fixtures,
not deployment secrets. Neither gate creates cloud resources or calls an LLM.

When a public operation completes, its active attempt is removed from pending
recovery before the root publishes its terminal response. A provider attempt
must have a successful, settled result; a cache-hit attempt must have no provider
execution. Completion preserves the attempt's encrypted execution details and
repairs uncertain event/index writes on replay. Older outcome-unknown paid
attempts remain pending independently, even if a later cache hit or paid attempt
completes the public operation.
