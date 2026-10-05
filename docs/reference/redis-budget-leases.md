# Redis budget leases

Redis is the authority for the durable v1 budget boundary. The worker no longer
uses a SQL budget journal. Operations, checkpoints, results, and cache records
use the configured cloud storage provider; none can rebuild lost Redis budget
authority automatically.

## Initialization and readiness

Before the first production worker starts, deployment automation must provision
the pinned admission Function or Lua script and then run
[`budget-initialize --apply`](cli.md#budget-initialize). Keep workers stopped
during this step, including older binaries that do not enforce the marker.
The command checks Redis persistence, `noeviction`, time, and code identity,
then conditionally creates a small receipt through the generic cloud KV store.
It scans the unused Redis namespace outside Lua and writes the ready marker
and first `initialize` Stream event atomically. A final cloud conditional update
marks the receipt ready. The full Redis readiness checks must pass before the
command reports `ready`.

The cloud receipt uses partition `<state.requests.namespace>/budget-initialization`
and sort key SHA-256 of the literal Redis namespace. It contains only a schema,
namespace, key-secret fingerprint, UUID epoch, creation time, and ready flag.
It has no TTL and records no balances, limits, operations, or journal events;
budget accounting remains entirely in Redis. Limits and the payload encryption
key are excluded from its identity so changing them cannot reset accounting.
The Redis marker is `<prefix>:{<admission_hash_tag>}:budget:authority`, a string
without expiry. A changed Redis key secret fails the identity check and needs
a separately designed migration. The default CLI derives that key secret from
the Redis password, so password rotation also requires preserving accounting
identity through an explicit migration.

Production startup reads the ready receipt once for its snapshot. Readiness
then checks the matching persistent **budget authority marker** using read-only
Redis calls. Every reserve, claim, and settlement also checks that marker inside
its atomic Redis invocation, before any balance, expiry, or event changes.
Missing, preparing, expiring, wrongly typed, or mismatched markers fail closed,
including when coordination events are disabled. Startup and readiness never
initialize or repair state. Non-production fixtures and callers constructing a
materializer directly must explicitly opt into this guard with a ready receipt.

Only the process that receives an acknowledged conditional receipt creation
may send the first Redis marker creation, once, without transport retries.
Other invocations can resume using an existing preparing/ready marker but can
never recreate an absent marker. This prevents a delayed initializer from
treating a lost dataset as a fresh installation. A lost create acknowledgement,
or a crash before that one Redis write, therefore requires investigation rather
than automatic retry from empty state. A lost final Redis/cloud response is
safe to retry while the matching marker survives. Initialization refuses an
already occupied namespace, never deletes keys, and never resets spending.

The initializer needs the existing cloud table's conditional create/read/update
permissions and Redis `SCAN`, `TYPE`, `GET`, `PTTL`, `SET`, and `XADD`, plus the
normal readiness and Function/Lua execution permissions. Scans use bounded pages
and the command deadline; Redis Cluster scans all primaries. No table, bucket,
Redis code, or SQL schema is created by this command.

This receipt detects marker loss; it is not a recovery journal or proof that
every accounting key survived. Restoring an older Redis snapshot, or losing
individual budget keys while preserving the marker, requires separate recovery
validation. Full recovery after data loss remains tracked in
[#856](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/856).
Never delete receipts or manually reset markers to reopen spending.

## Acquisition and paid work

`durable.BudgetLeaser` exposes `Accept`, `Claim`, and `Reconcile`. Both Generate
and Compact require a successful claim before provider dispatch.

1. `Accept` atomically checks all matching budget windows and reserves the
   conservative request bound. Insufficient capacity returns a wait result with
   a retry hint, including when the request exceeds the current limit. It does
   not store a permanent denial. The budget workflow waits with a Temporal
   timer and tries again; this boundary does not sleep while waiting for budget.
2. `Claim` consumes the authorization once, immediately before submission. Redis
   time limits the start deadline to 15 minutes from acquisition. An explicit
   shorter deadline is supported. Replaying acquisition never renews it.
3. Unclaimed reservations expire and are refunded by bounded cleanup during
   subsequent admission. A claimed reservation no longer has that expiry:
   elapsed time must not refund work that might already have been paid for.
4. `Reconcile` replaces the reservation with actual cost, or retains the full
   bound for an ambiguous outcome. It validates the whole supplied batch before
   applying any settlement. Confirmed cost counts through the budget window
   and its final bucket; the start deadline is unrelated to cost retention.

Amounts use exact decimal USD at the Go boundary and conservative integer
nano-USD in Redis. Charges round up and limits round down. All authorization
and settlement mutations run atomically in the versioned Redis Function (or
explicitly provisioned Lua compatibility script).

## Retries and recovery

The operation ID identifies one budget authorization for one possible paid
attempt, not a response-cache key. Identical acquisition retries return the
original reservation. A changed payload under an existing operation ID is an
idempotency conflict.

A claim is deliberately single-use. If Redis accepted a claim but its response
was lost, repeating it returns `ErrAlreadyClaimed`; it cannot authorize another
HTTP submission. If a provider submission might have succeeded, a new paid
retry needs a new operation ID and another reservation. The original bound
stays accounted until its outcome is resolved.

Settlement event IDs deduplicate retries, including a lost Redis response.
Changing an already recorded event is rejected. An unused authorization can be
released; a claimed authorization cannot use that release path. A later exact
cost correction has its own event ID and revision.

Operation records and deduplication tombstones currently have no TTL. Claimed
and ambiguous reservations also remain until settlement. Automatic cleanup of
these records is a future task: Redis memory capacity must include their growth.
This prevents old activity retries from reacquiring or refunding a paid attempt.
There is no SQL journal from which to rebuild lost budget state. Redis
persistence and HA protect this authority; a dataset loss needs an explicit
recovery policy rather than treating missing reservations as unused budget.

## Configuration and worker coordination

Budget policies come from worker settings, using `budgets_json` or the existing
`budgets` YAML field. See [configuration](configuration.md#pricing-and-budget-matching).
Changing limits under stable policy/window identities preserves existing usage.
The runtime uses stable Redis budget identities across configuration reloads.
Changing the Redis namespace or policy/window identities is not a limit update:
it selects different accounting keys. A window identity is
`<policy id>/<window id>`, where the window id is the configured `id` or, when
omitted, `<duration>-<bucket>`; it does not depend on the window's position in
the list, so editing other windows does not move a window's accounting. A
reload that keeps an identity but changes its duration or bucket is rejected,
because the stored bucket layout would no longer match; a geometry change
takes a new identity and starts from zero. See
[budget window identity](configuration.md#budget-window-identity), including
how to keep accounting recorded under the former positional identities.

Workers using the same namespace see the same atomic budget state. When
`state.redis.coordination_stream_enabled` is true, the runtime publishes budget
events to `<prefix>:{<admission_hash_tag>}:budget:events` in the same Redis
Function or Lua invocation as the accounting change. The Stream and accounting
keys share a Redis Cluster slot. Provision the updated admission library before
starting workers; its digest is pinned in the example and deployment settings.
Stream publication requires Redis 7 or later and permission for `XADD` and
`XINFO STREAM`, in addition to the existing accounting commands. A wrong key
type, denied `XADD`, or exhausted Stream ID is rejected before accounting writes.

Each affected budget member produces a `reserve`, `claim`, `reconcile`, or
`release` hint. Idempotent acquisition and settlement replays do not republish;
claim replays remain rejected. `denial` records each unsuccessful capacity check,
so a later workflow retry may produce another denial. `expire` is emitted when
bounded cleanup actually removes an unused reservation or aged settled cost,
which happens during a later mutation rather than at the expiry instant.

Events carry opaque member and operation digests, a generation, revision,
timestamp, and non-negative nano-USD delta magnitude. Claims have zero delta;
reconciliation deltas are absolute changes, so these hints cannot reconstruct an
accounting ledger. Expiry has revision zero and no operation digest because the
expiry index identifies a reservation fingerprint rather than an operation key.
No raw policy, window, or operation identifiers are published.

Every reader uses its own cursor through `BudgetEventPort`, allowing each worker
to see the same events. A shared consumer group would distribute events instead
and is unsuitable here. Publication does not start a background tailer in the
runtime; wiring adoption and wake-ups remains separate work. The Stream never
authorizes provider dispatch. No automatic trimming is enabled, so retention
must account for Stream growth until cursor-aware maintenance is implemented.

See [durable runtime composition](durable-v1-runtime.md) for workflow wiring.

## Verification

The reference-model and boundary tests cover ordering and reject dispatch
without a valid claim. Real Redis integration tests exercise both Functions
and Lua, with the race detector enabled locally, covering:

- concurrent acquisition, single-use claims, and duplicate settlement;
- lost acquisition, claim, and settlement replies after the actual mutation;
- server-enforced deadlines, unused expiry, retained paid work, and cost expiry;
- atomic multi-window failures and malformed Redis keys;
- configuration reloads, wait/retry behavior, and ambiguous paid retries;
- AOF restart recovery of claimed work, settled cost, and deduplication records.
- atomic event publication, independent readers, and duplicate suppression after
  lost replies, including wrong-type, ACL-denied, and exhausted-ID Streams.
- concurrent initialization, interrupted writes, immutable cloud receipts,
  startup rejection, read-only readiness, and atomic refusal of spending when
  the authority marker is missing, expiring, preparing, or mismatched.

Run `make verify`, `make redis-integration`, and, for race-enabled integration,
`GOFLAGS=-race make redis-integration` from `golang/`.
