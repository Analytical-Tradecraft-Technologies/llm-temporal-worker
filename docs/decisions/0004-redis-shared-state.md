# ADR 0004: Redis shared state

- Status: Accepted; storage ownership refined by ADR 0007
- Date: 2026-07-13

## Decision

Horizontal workers share monetary reservations, claims, settlement, operational
throttles and provider observations in Redis. Every paid decision checks all
matching budget windows in one atomic Function or explicitly provisioned Lua
script. Redis server time governs authorization expiry. Limits and policy come
from the worker configuration; clients cannot grant themselves budget.

Budget transitions publish coordination events in the same atomic operation.
Events may wake consumers or invalidate local hints; only the authoritative
Redis mutation can authorize work. Runtime stream consumption is a separate
feature from publication.

Every worker key uses a validated configurable prefix. Keys participating in
one atomic budget mutation share a configured Redis Cluster hash tag. The
single-slot throughput limit is an intentional v1 constraint.

Production requires authentication, persistence, no eviction and fail-closed
readiness. A permanent cloud initialization receipt binds the Redis authority
marker to its original namespace and epoch. A missing marker cannot be treated
as a new empty budget. Neither cloud request records nor the stream are a
replacement accounting ledger.

Memory mode remains limited to tests and single-process development. Cloud
stores hold durable requests, checkpoints and results as described in
[ADR 0007](0007-cloud-storage-and-redis.md).

## Consequences

- Concurrent workers cannot each spend against independent local counters.
- Unused reservations expire; claimed paid work remains accounted until settlement.
- Persistence and HA are necessary; recovery after data loss requires explicit
  accounting reconciliation rather than reinitialization.
- A slower consumer or a stream gap cannot grant extra spending capacity.

See [Redis budget leases](../reference/redis-budget-leases.md) for the current
implementation and [recovery](../runbooks/redis-budget-generation-recovery.md)
for its limits.
