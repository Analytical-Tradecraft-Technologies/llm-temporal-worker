# Redis budget leases

Redis is the authority for the durable v1 budget boundary. The worker no longer
uses a SQL budget journal. Operations, checkpoints, results, and cache records
use the configured cloud storage provider; none can rebuild lost Redis budget
authority automatically.

## Acquisition and paid work

`durable.BudgetLeaser` exposes `Accept`, `Claim`, and `Reconcile`. Both Generate
and Compact require a successful claim before provider dispatch.

1. `Accept` atomically checks all matching budget windows and reserves the
   conservative request bound. Insufficient capacity returns a wait result with
   a retry hint, including when the request exceeds the current limit. It does
   not store a permanent denial. The future workflow can wait with a Temporal
   timer and try again; this boundary does not sleep while waiting for budget.
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
it selects different accounting keys. Window geometry changes need a separate
migration policy.

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

Run `make verify`, `make redis-integration`, and, for race-enabled integration,
`GOFLAGS=-race make redis-integration` from `golang/`.
