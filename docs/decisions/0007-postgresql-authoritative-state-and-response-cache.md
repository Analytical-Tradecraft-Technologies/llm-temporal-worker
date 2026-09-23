# ADR 0007: PostgreSQL Durable State and Exact-Response Cache

> **Budget design superseded:** Redis now owns budget leases and settlement;
> the SQL budget journal writer has been removed. The SQL budget rebuild and
> automatic Stream broadcast described below are not the current runtime. See
> [Redis budget leases](../reference/redis-budget-leases.md) for the implemented contract.

- Status: Accepted design; implementation pending
- Date: 2026-07-18
- Complements: ADR 0004; Redis remains the low-latency throttle and admission accelerator

## Context

Forkable checkpoints, optional exact-response caching, resumable provider job IDs,
queryable provider status, model inventories, budget/spend reporting, and
precise per-operation cost need transactional relationships and indexed
queries. Those durable relationships belong in one PostgreSQL transaction.
Redis still serves a different, latency-sensitive purpose: distributed
request/token/concurrency throttles, cross-worker coordination, and a complete
active-window monetary budget materialization. The design must not pretend that
Redis and PostgreSQL can atomically commit one shared fact.

This repository currently uses Redis for the worker's v1 operation,
continuation, result, and budget state. The PostgreSQL service in local Compose
belongs to Temporal and is not a worker application database.

## Decision

Introduce a worker-owned PostgreSQL physical namespace. Its database, schema,
and table prefix are independently configurable. A separate database/schema is
recommended, while a shared server or database is supported when the resulting
relations and grants cannot overlap Temporal-owned objects. PostgreSQL is the
system of record for durable operations and budget journal, checkpoints,
response cache, provider status, inventory, and exact accounting. Redis remains
a required production optimization: it is the provably-current materialization
of the active budget horizon, the cross-replica decision serializer, and the
request/token/concurrency throttle and coordination layer. Its independently
configurable key prefix applies to every worker-owned Redis key. Calling Redis
the “authority” is avoided because it obscures the record/materialization split.

Normal budget admission performs no PostgreSQL budget reads. One Redis Function
validates the namespace generation and complete active-window coverage, checks
every matching window, and atomically creates an idempotent reservation. The
worker then appends/inserts the corresponding durable PostgreSQL operation and
budget-journal facts before provider dispatch. A PostgreSQL write failure means
no dispatch and causes a best-effort Redis release; an unconfirmed release
temporarily over-throttles until TTL. Completion durably updates PostgreSQL
before idempotently reconciling Redis. No provider call is allowed unless both
the Redis reservation and PostgreSQL journal write succeeded.

Every Redis mutation also appends to one namespace-scoped Redis Stream in the
same Function. Workers may use that key as the monotonic budget change feed and
generation-switch wake-up optimization. A
generation manifest, complete-coverage sentinels, per-window bucket sentinels,
and stream high-water mark make missing or partially lost data detectable.
Missing state, failed persistence health, or an unexpected generation fails
admission closed. Stream-tail hints can reject early but never authorize;
disabling or losing the feed only increases Redis calls. Normal admission,
worker joins/restarts, Stream-gap recovery, and budget status use Redis without
PostgreSQL budget reads. An intact generation is adopted rather than rebuilt.
The exhaustive adoption proof, the only two exceptional read conditions,
session/incarnation rules, fenced rebuild, same-incarnation outage posture, and
workload envelope are normative only in the
[control-plane design](../architecture/postgresql-state-cache-and-control-plane.md#the-only-postgresql-budget-read-conditions).

PostgreSQL remains exact **NUMERIC(38,18)**. Redis materializes conservative
integer nano-USD: charges round up, limits round down, and finalize/release uses
the identity-keyed stored integer. This removes hand-written arbitrary-precision
Lua while ensuring Redis can over-throttle but never under-throttle. Public
contracts still expose exact decimal USD, not the internal unit.

Store USD amounts as **NUMERIC(38,18)**. Do not use PostgreSQL floating-point
types or Go floating-point values for money. The initial public contract names
USD properties directly and has no currency discriminator. No FX adapter,
snapshot schema, or refresh job is built while every supported provider is
priced in USD. If a concrete non-USD catalog appears, a separate ADR must keep
conversion inside Go and persist/report only normalized USD. This replaces
integer micro-USD and supports 18 fractional digits plus
20 whole-dollar digits, including sub-micro-dollar prices, $1, $10, and large
aggregate budgets.

After the core checkpoint and compaction phases, implement an optional
exact-response cache keyed by a versioned semantic fingerprint. Its supported
caller is repeated staging workflow verification and incident reproduction;
ordinary unit tests continue to use record/replay fixtures.
A request opts in by supplying a maximum acceptable cache age; omission means
neither read nor populate the cache. The signed 32-bit **variant** defaults to
zero. A positive variant is valid only when the fully materialized temperature
is explicitly greater than zero. Variant is a cache discriminator and is never
sent to a provider as a random seed.

Cache freshness is based on the entry's completion time. Garbage-collection
eligibility is based on **last_used_at**, so an old but recently used entry is
retained. **use_count** is a signed 32-bit counter, initialized to one for the
origin operation, incremented once for each distinct logical operation served,
and saturated at the maximum value.

Catalog prices and completed-operation **actual_cost_usd NUMERIC(38,18)** are
nullable when the real amount is not known. NULL means unknown; it never means
zero or “not applicable.” A closed status and safe reason preserve that fact.
Exact costs require a method. Estimates, reservations, and conservative budget
charges stay separate. Cache hits and free control-plane queries record exact
zero.

Lookup uses a tenant-scoped HMAC-SHA-256 semantic fingerprint. Operations retain
a content-free canonical request manifest as JSONB plus an envelope-encrypted
inline payload or blob reference; cache entries retain a canonical materialized
manifest JSONB plus digest. JSONB is for audit and verification, has no broad
index, is not the lookup path, and never contains raw prompt/tool/output text.

The initial cache is route-isolated because providers do not publish enough
artifact, quantization, template, or hidden-transform evidence to certify
cross-provider identity honestly. A concrete verified pair may add sharing only
through a superseding ADR, schema change, negative tests, and new cache epoch.

## Consequences

- One PostgreSQL transaction can coordinate idempotency, cache use, the durable
  budget-journal event, cost finalization, and child-checkpoint publication.
  The preceding Redis reservation remains a separate atomic operation governed
  by the conservative cross-store protocol.
- Redis retains the fast atomic budget path without becoming the durable audit
  or rebuild ledger. A PostgreSQL journal write remains mandatory before any
  paid dispatch.
- PostgreSQL constraints and explicit indexes become part of the application
  contract and require production capacity planning.
- The pre-release switch is deliberately clean because there is no released
  data. It includes no copying, backfill, dual-read/write, or legacy namespace
  fallback. A future post-release move requires a separate migration design.
- Exact decimal PostgreSQL storage preserves sub-micro-dollar accounting and
  deterministic aggregation. Conservative nano-USD Redis admission trades less
  than one nano-dollar of headroom per positive event for simple audited integer
  arithmetic.
- Nullable prices preserve honest uncertainty, but spend summaries must report
  known totals and unknown counts instead of one falsely complete total.
- PostgreSQL writes and Redis are both on the provider-call critical path for
  new paid dispatches, but PostgreSQL budget reads are not. Either dependency
  being unavailable fails closed. Replays of an already completed PostgreSQL
  operation do not consume a new Redis reservation.

## Rejected alternatives

- PostgreSQL-only admission with `SELECT ... FOR UPDATE` would be adequate at
  the stated steady-state scale and would remove the materialization/rebuild
  protocol. It is rejected deliberately because Redis coordination, atomic
  multi-window throttles, low-latency batch-drain decisions, and shared
  invalidation are desired product optimizations. PostgreSQL remains the system
  of record so this choice can be revisited from measured latency/complexity
  data without changing accounting semantics.
- Keeping exact monetary budgets only in Redis without a durable journal makes
  Redis loss unrecoverable. The accepted design writes a PostgreSQL journal
  before dispatch and detects/fences Redis loss, while preserving Redis as the
  steady-state active-window decision/materialization path. Redis persistence
  and backups improve availability but do not replace that record.
- Resolving any configured worker relation to a Temporal-owned relation couples
  application state to Temporal upgrades and support boundaries.
- Float/double money loses decimal precision and makes budget boundary behavior
  platform-dependent.
- A cache key based on raw request JSON makes semantically identical requests
  miss and risks including caller-only metadata.
- Updating freshness on every read makes stale entries appear newly produced;
  freshness and retention therefore use separate timestamps.
