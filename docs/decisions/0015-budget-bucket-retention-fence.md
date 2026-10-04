# ADR 0015: Budget retention requires accounting evidence

- Status: Superseded implementation; conservative retention principle retained
- Original date: 2026-07-26

The former bucket-pruning adapter and its maintenance command have been removed.
They are not supported operations on the current Redis accounting store.

Redis owns monetary reservations and settlement. Unclaimed authorizations can
expire after their start deadline, but claimed or unresolved paid work cannot
be refunded by a retention sweep. Operation identities and settlement event
tombstones must continue to prevent old retries from granting or refunding
budget again. Automatic cleanup of these records remains deferred.

See [Redis budget leases](../reference/redis-budget-leases.md) and
[maintenance](../reference/maintenance.md). Future retention must preserve these
invariants and account for every supported retry horizon before deleting state.
