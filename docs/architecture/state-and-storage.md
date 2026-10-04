# State and Storage

Cloud key-value/blob storage and Redis are the worker's durable backends. The
initial cloud implementation uses DynamoDB and S3 with IAM authentication.
Temporal's own persistence is operated independently of the worker.

## Storage responsibilities

| State | Storage | Purpose |
| --- | --- | --- |
| Requests, attempts and pending index | Cloud KV | Durable identity, recovery and bounded pending discovery |
| Checkpoints and cache successes | Cloud KV and encrypted blobs | Context, output and completion-based cache freshness |
| Budget reservations, claims and settlement | Redis | Atomic admission across matching windows and idempotent accounting |
| Provider status, inventory and throttles | Redis | Shared operational state and bounded queries |
| Budget policies | Immutable worker settings | JSON/YAML policy configuration |
| Audit events | Structured logs | Best-effort operational audit |

See [cloud request storage](../reference/cloud-request-repository.md),
[response caching](../reference/cloud-response-cache.md),
[Redis budgets](../reference/redis-budget-leases.md) and
[provider state](../reference/provider-control.md) for the concrete contracts.

## Durable requests and immutable artifacts

The pending index is written before the corresponding request progress. It uses
bounded partition selection so recovery can discover outstanding requests
without depending on a surviving Temporal workflow. Conditional updates fence
competing writers and stale attempts. Provider identifiers and recovery context
are stored internally rather than returned as caller continuation handles.

Checkpoints and response artifacts use immutable encrypted blobs with metadata
in the KV store. Finalization publishes references only after the required
content exists. Interrupted publication resumes from durable progress; cloud
publication and Redis settlement are separate idempotent phases, not one atomic
transaction. Failed or incomplete responses do not replace eligible successes.

The S3 implementation uses create-if-absent writes. If content already exists,
`HeadObject` must prove its length, media type and digest metadata match before
the write is treated as idempotent. Reads verify the reference's media type,
digest and length. An unverified or replaced object fails closed.

Cache identity includes the semantic request and request index. The default
index is zero; changing it asks for an independent sample. Eligible cache age is
measured from successful completion. Compaction can reuse an artifact matching
its source and policy independently of the final generation's freshness bound.

## Redis budget authority

`durable.BudgetLeaser` exposes `Accept`, `Claim` and `Reconcile`. One atomic Redis
mutation reserves every matching window. The single-use claim must occur within
15 minutes. Unused authorizations can expire; claimed or uncertain paid work
cannot be refunded merely because that start deadline passes. A replacement
paid attempt obtains another reservation while the original uncertainty remains
accounted for.

Production requires a permanent cloud initialization receipt and matching Redis
authority marker. Startup and readiness verify them but never initialize or
repair them. The receipt contains identity metadata, not balances or a rebuild
journal. Missing markers fail closed; partial dataset loss or restoration of an
older accounting snapshot still requires separate recovery validation. See the
[recovery boundary](../runbooks/redis-budget-generation-recovery.md).

The versioned Redis Function or explicitly preloaded Lua compatibility script
owns mutations. The runtime verifies its identity and does not install or replace
shared code. Monetary accounting uses conservative integer nano-USD; exact USD
values remain in the domain and durable request facts. Throttles are separate
operational limits and do not supply financial accounting facts.

## Redis key layout and coordination

`state.redis.key_prefix` and `state.redis.admission_hash_tag` select the Redis
namespace. Keys touched by an atomic mutation share a Cluster slot. Changing the
namespace selects different accounting state and requires an explicit migration;
it is not a way to reset a budget safely. Redis key identity also depends on its
configured key secret, so credential/key rotation must preserve that identity.

When coordination events are enabled, budget mutations append Stream hints in
the same atomic invocation. Every reader needs its own cursor to see every
event; a shared consumer group would distribute events among readers instead.
The runtime publishes events but does not automatically start a background
tailer. Events are hints, not an accounting ledger or an admission authority.

## Retention and recovery

Cloud records retain recovery context and successful artifacts according to
explicit metadata. Automatic production blob deletion and record retention are
not implemented; see [maintenance](../reference/maintenance.md). Never remove an
artifact while a retained checkpoint, result, cache entry or pending request
still needs it.

Redis budget operation records and settlement deduplication tombstones currently
have no TTL. Claimed and ambiguous reservations remain until settlement. Account
for their growth and the untrimmed Stream when sizing Redis. The 15-minute claim
deadline is not a data-retention policy.

## Memory and legacy adapters

Memory mode and the older Redis admission/continuation adapters support direct
engine tests and development fixtures. Their `Begin`, `MarkDispatching`,
`Continue`, `Complete` and `Fail` protocol is not the cloud workflow persistence
path. In-memory state disappears on restart and cannot establish multi-replica
or recovery guarantees. The production cloud runtime does not silently fall back
to these adapters when a dependency is missing.

## Dependency boundary

The worker has no SQL backend, schema installer, database credentials or SQL
migration. Architecture tests inspect every Go package and its tests, all source
imports regardless of build tags, and module requirements/replacements. The
only permitted transitive SQL-named package is Go's `database/sql/driver`, used
by UUID serialization interfaces; it does not supply a database connection or
registered database driver. Worker source cannot import it directly.
