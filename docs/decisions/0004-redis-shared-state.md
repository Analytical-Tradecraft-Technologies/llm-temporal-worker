# ADR 0004: Redis Shared State

- Status: Accepted
- Date: 2026-07-13

> This remains part of staged Phase B. ADR 0007 narrows Redis to the complete
> active budget/throttle working set and adds PostgreSQL durable state/journaling;
> it does not retire Redis.

## Context

Horizontal workers must share overlapping-window budget reservations and
operational throttles. Admission must check and update all matching windows
atomically. Durable operation deduplication and continuation records live in
PostgreSQL under ADR 0007; the live admission access pattern remains small
key-value state with time-indexed buckets.

## Decision

Use Redis as the production atomic budget/throttle materialization and
coordination optimization. Keep an explicitly non-durable **memory** mode for
unit/conformance tests and single-process local development; configuration
validation rejects it in production or multi-replica mode. Implement admission as a
versioned Redis Function (Lua script fallback), use Redis server time,
conservative checked nano-USD integers derived from exact PostgreSQL decimals,
and one
configured cluster hash tag for all budget keys. The active generation includes
complete bucket coverage, reservation indexes, a manifest, worker leases, and
one broadcast Redis Stream for optional cross-worker invalidation and wake-up.
The Stream is an optimization; only the atomic Function authorizes. Every worker-owned key also
uses one validated configurable prefix, defaulting to **llmtw** and overridable
by **LLMTW_REDIS_KEY_PREFIX** at process start.

PostgreSQL is the system of record and stores the durable operation/budget
journal before provider dispatch,
but normal budget decisions and status queries read Redis only. PostgreSQL
budget-read/adoption/rebuild rules are specified only in the
[control-plane design](../architecture/postgresql-state-cache-and-control-plane.md#the-only-postgresql-budget-read-conditions).

The embedded admission Function is also a defensive boundary: every action
checks its exact argument arity, key count, key/value byte bounds, reservation
count, and Redis-safe integer range before reading or mutating shared state.
Operation and attempt fields are length-bounded as well. This keeps malformed
direct `FCALL`/preloaded-Lua invocations fail-closed instead of allowing a
missing key or oversized value to become a Redis runtime error after a partial
mutation. The supported envelope is at most 253 reservation windows per
operation (the remaining Redis key slots carry the operation indexes) and
256-byte route/endpoint/provider/model fields.

Production fails closed on Redis errors. Require TLS/auth, `noeviction`, explicit
AOF durability choice plus RDB, persistence monitoring, backups, and restore
tests.

## Consequences

- Admission is atomic and low latency across replicas.
- Memory mode and the Redis Function share one atomic-budget state-transition
  conformance suite; durable/restart behavior is intentionally absent in memory
  mode and production authorization always uses Redis.
- The single budget hash slot is intentional under the normative
  [workload envelope](../architecture/postgresql-state-cache-and-control-plane.md#the-only-postgresql-budget-read-conditions).
- `appendfsync everysec` has an acknowledged crash-loss window; stronger
  durability costs latency.
- Durable blobs still require a separate object-store port at larger sizes.

## Rejected alternatives

- Per-process counters allow aggregate overspend.
- Eventual counter reconciliation does not enforce a hard budget.
- Multiple Redis Cluster slots cannot provide the required v1 atomic Function.
- Using worker clocks creates cross-replica window disagreement.
