# Redis budget leases

Redis is the authority for the durable v1 budget boundary. The SQL budget
journal writer and its exact-cost correction writer have been removed.
PostgreSQL operation, checkpoint, result, query, and legacy schema code remain
for the next persistence migration; this change does not remove all SQL
requirements from the worker.

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

Workers using the same namespace see the same atomic budget state. The existing
Redis Stream publisher and tailer contracts are not wired to these mutations or
started automatically by the runtime. There is currently no automatic broadcast
of every budget change. A readiness Stream setting does not enable publishing.

The deployment still needs complete Generate and Compact phase factories. See
[durable runtime composition](durable-v1-runtime.md). This change supplies the
Redis budget capability and removes the SQL journal phase; it does not create
the planned budget-waiting or LLM orchestration workflows.

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

Run `make verify`, `make redis-integration`, and, for race-enabled integration,
`GOFLAGS=-race make redis-integration` from `golang/`.
