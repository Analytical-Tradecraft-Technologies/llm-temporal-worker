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
The command checks the configured Redis persistence/eviction policies, time, and code identity,
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

This receipt detects marker loss; it does not by itself prove that every
accounting key survived. See
[recovery after Redis data loss](#recovery-after-redis-data-loss) for how a lost
marker, a failover, or a restored older snapshot is reconciled. Never delete
receipts or manually reset markers to reopen spending.

## Recovery after Redis data loss

The cloud request records are the durable source for recovery. Each paid
attempt saves its immutable budget plan (the reservation request) before Redis
acceptance, its accepted reservation before any claim, and its exact settlement
event batch before Redis settlement. Every claim that could dispatch paid work
is therefore preceded by a durable execution record, and every settlement
Redis applied is preceded by a durable copy of the same events.

`budget-initialize --apply`, which the deployment runs before each worker
starts, recovers when the cloud receipt is ready and the Redis marker is not:

1. A Function call sets a persistent `recovering:<marker>` value. Reserve,
   claim and settlement still require `ready:<marker>`, so normal budget
   operations fail closed against the partially restored state. Readiness
   reports the namespace as unavailable.
2. One process holds a disposable lease key
   (`<prefix>:{<tag>}:budget:recovery-lease`, PX expiry, renewed per page).
   Others wait for it; an expired lease is taken over.
3. The holder pages through all eight request discovery shards at once,
   including finished requests whose settled spend can still count. Each
   page's records are read concurrently and its operations merged
   concurrently; merges are independent and idempotent, so order does not
   matter. A finished request
   last updated before the recovery horizon (the longest configured window
   plus its bucket, start lease and clock skew, and at least 31 days) is
   skipped without reading its payload.
4. For each paid execution, one atomic Function call merges the durable state
   into Redis: the operation record with its original request fingerprint and
   reservation event IDs, the claim, the settled amounts, revisions and
   settlement event fingerprints, and the matching bucket aggregate and expiry
   members. A surviving record at the same or later progress is left
   unchanged; a record behind the durable state is advanced and only the
   difference is applied to the aggregate. A record that disagrees with its
   durable identity stops recovery with a `conflict` reason.
5. A surviving operation record does not prove its bucket hash or expiry
   member survived. Each merge re-adds a missing live expiry member and records
   the reservation's live aggregate in a floor ledger
   (`budget:recovery-floor`, one-day expiry). After the last shard, every
   bucket field below the sum of its recorded live charges is raised to that
   sum. Floors only add spend: charges Redis holds without a durable record
   are kept.
6. A per-shard cursor (`budget:recovery-progress`, one-day expiry) is stored
   after each page so an interrupted run resumes. A shard's cursor advances
   only after every operation on its page merged. A process that obtains the
   lease with less than 30 seconds left exits and leaves the work to the next
   start rather than time out mid-page. Merges are idempotent, so losing the
   cursor, the floor ledger or the lease only repeats work.
7. Only after every shard is merged and every floor applied does one Function
   call publish an `initialize` hint and set `ready:<marker>`. The cursor and
   floor ledger are deleted.

Recovery is conservative where the durable record cannot prove less spend:

- An attempt without a durable claim is restored as claimed unless its record
  proves the claim never applied (a failed, not-dispatched attempt). An attempt
  still in `claiming` therefore keeps its full reservation and settles as an
  unknown outcome after its recovery window, rather than risk a lost charge.
- Unclaimed reservations are restored with a start deadline no earlier than
  the one Redis set.
- Spend window expiries come from Redis acceptance time. New acceptances save
  that expiry with the reservation result. Results saved by earlier versions
  lack it, so recovery uses the execution start plus five minutes of clock
  skew as the latest possible acceptance; spend may count slightly longer,
  never shorter.
- Spend whose retention already ended is not re-added, matching what Redis
  would have expired.

Acceptances that Redis granted but no execution record saved (a crash between
acceptance and the record) are not restored. None of them could have been
claimed, so no paid work used them; a retry of the same operation reserves
again.

`--recover` fences and merges even while the marker survives. Use it after a
failover that may have dropped acknowledged writes, or after restoring an
older snapshot. Without it, a surviving marker hides that loss: recovery cannot
detect missing keys behind a ready marker.

### Recovery boundary for existing deployments

- Recovery needs the admission Function from this release. Provision it before
  running `budget-initialize`; an older library rejects the recovery actions.
- Durable records exist only for the durable v1 cloud budget path. Legacy
  admission keys and unpriced or free attempts hold no recoverable budget.
- The horizon follows the configured windows. If a window longer than 31 days
  was removed before the loss, finished requests older than the horizon are
  not restored into it; keep that identity removed until its own duration has
  elapsed.
- Coordination streams, budget generation manifests and status caches are not
  rebuilt; they are disposable hints and regenerate under normal operation.
- Exact-cost corrections applied directly through the unknown-cost boundary
  have no durable record and are not recovered. Production settles through the
  provider execution record only.

## Acquisition and paid work

`durable.BudgetLeaser` exposes `Accept`, `Claim`, and `Reconcile`. Both Generate
and Compact require a successful claim before provider dispatch.

1. `Accept` atomically checks all matching budget windows and reserves the
   conservative request bound. Insufficient capacity returns a wait result with
   a retry hint, including when the request exceeds the current limit. It does
   not store a permanent denial. The budget workflow waits with a Temporal
   timer and tries again; this boundary does not sleep while waiting for budget.
   A quote whose lease has already expired by Redis `TIME` is refused as a
   lease expiry (`durable.ErrLeaseExpired`), exactly as in the reference model,
   so the attempt is renewed rather than retried in place.
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
   The window is measured from Redis `TIME` at acceptance, not from the
   persisted quote: a quote that waited for capacity keeps its quote-time
   bucket identity, but Redis raises the cost's window expiry to the end of the
   acceptance bucket plus the window duration, so waited spend cannot leave the
   window early.

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
and ambiguous reservations also remain until settlement. A claimed provider
attempt whose outcome stays unknown after its recovery window (the 15-minute
start lease) is settled with `finalize_unknown` events at its full reservation
by the next poll or budget acquisition that observes it, so the conservative
charge counts through each budget window like confirmed cost and then ages out
instead of being held forever. A later exact cost is a `resolve_unknown_exact`
correction, never a second settlement. Automatic cleanup of
these records is a future task: Redis memory capacity must include their growth.
This prevents old activity retries from reacquiring or refunding a paid attempt.
There is no SQL journal; lost Redis budget state is rebuilt from the durable
cloud execution records, never by treating missing reservations as unused
budget. See [recovery after Redis data loss](#recovery-after-redis-data-loss).

## Configuration and worker coordination

Budget policies come from worker settings, using `budgets_json` or the existing
`budgets` YAML field. See [configuration](configuration.md#pricing-and-budget-matching).
Changing limits under stable policy/window identities preserves existing usage.
### Clock agreement

Workers renew a quote's lease from their own clock, one minute
(`UnusedAttemptRenewMargin`) before it expires, while Redis judges every lease
against its own `TIME`. Keep each worker's clock within well under a minute of
the Redis primary's, for example with NTP on both. A worker behind Redis by
more than that margin has its reservations refused as lease expiries for the
last part of every lease, and at a skew of 15 minutes or more (the start
deadline) every reservation it makes expires on arrival. Readiness does not
measure this skew, so monitor host clock synchronization alongside Redis.

The runtime uses stable Redis budget identities across configuration reloads.
Changing the Redis namespace or policy/window identities is not a limit update:
it selects different accounting keys. A window identity is
`<policy id>/<window id>`, where the window id is the configured `id` or, when
omitted, `<duration>-<bucket>`; it does not depend on the window's position in
the list, so editing other windows does not move a window's accounting. A
reload that uses an identity the running worker has already accounted with
another duration or bucket is rejected, even if the identity was removed in
between, because the stored bucket layout would no longer match; a geometry change
takes a new identity and starts from zero. See
[budget window identity](configuration.md#budget-window-identity), including
how to keep accounting recorded under the former positional identities.

Workers using the same namespace see the same atomic budget state. When
`state.redis.coordination_stream_enabled` is true, the runtime publishes budget
events to `<prefix>:{<admission_hash_tag>}:budget:events` in the same Redis
Function or Lua invocation as the accounting change. The Stream and accounting
keys share a Redis Cluster slot. On Redis Cluster, readiness and
`budget-initialize` check persistence and eviction on that slot's primary and
its replicas, and the admission code on every primary. Provision the release's admission library
on every primary before starting its workers; its digest-derived name and digest are pinned in
the example and deployment settings, and it is loaded beside earlier releases'
libraries (see [Readiness and Redis budget policy](configuration.md#readiness-and-redis-budget-policy)).
Stream publication requires Redis 7 or later and permission for `XADD`, `EXPIRE`, and
`XINFO STREAM`, in addition to the existing accounting commands. A wrong key
type, denied `XADD` or `EXPIRE`, or exhausted Stream ID is rejected before accounting writes.

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
authorizes provider dispatch. Every publication approximately caps history at 10,000 events and refreshes a
24-hour idle TTL. Redis can evict the Stream under `volatile-lru`; balances,
operation tombstones and authority markers keep their non-expiring protection.
A retained-prefix gap triggers an authoritative reload. An absent Stream uses
`0-0` as its pre-reload checkpoint so events created during reload remain
replayable. Hints never authorize spending, even after a retention gap.

The worker lease store atomically publishes a session's liveness and persistent
roster. First registration succeeds only while its lease is still absent;
registration of an existing session and renewal compare the exact lease read
before writing. An intervening registration or renewal returns an ownership
conflict, and removal of an observed lease returns a missing-lease error. These
failures leave the winning lease and roster unchanged, so a stale reconnect
cannot move its Stream cursor backwards. A fresh registration retry preserves
the current cursor and original roster registration time. This store contract
does not itself enable production tailer wiring or Stream retention.

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
- AOF restart and abrupt `SIGKILL` recovery of the production initialization
  marker, claimed work, settled cost, deduplication records and coordination
  events; ready receipt adoption and paid replays do not duplicate events. Both
  Function and Lua adapters reject further reserve/claim/settlement if the
  recovered marker is subsequently lost. Lua script-cache loss stays unavailable
  until the pinned script is explicitly provisioned; no automatic `EVAL`
  fallback is permitted.
- atomic event publication, independent readers, and duplicate suppression after
  lost replies, including wrong-type, ACL-denied, and exhausted-ID Streams.
- concurrent initialization, interrupted writes, immutable cloud receipts,
  startup rejection, read-only readiness, and atomic refusal of spending when
  the authority marker is missing, expiring, preparing, or mismatched.

Run `make verify`, `make redis-integration`, and, for race-enabled integration,
`GOFLAGS=-race make redis-integration` from `golang/`. The persistence gate
uses an isolated digest-pinned daemon with `appendfsync always`; it proves
local acknowledged-write recovery, not deployed HA, a different fsync policy,
or detection of partial key loss or rollback of an intact marker and dataset.
