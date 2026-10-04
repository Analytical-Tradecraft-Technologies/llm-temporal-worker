# PostgreSQL response-cache repository

> Historical reference: the worker SQL implementation and its maintenance CLI
> have been removed. These SQL procedures are not supported. Use
> [cloud request storage](cloud-request-repository.md) for current persistence.

`postgres.ResponseCacheRepository` is the first bounded implementation slice
of the exact-response cache described by [ADR 0007](../decisions/0007-postgresql-authoritative-state-and-response-cache.md)
and the [checkpoint/cache design](../architecture/conversation-checkpoints-and-compaction.md).
It is deliberately separate from operation replay: one cache entry can serve
many distinct operation IDs, while each logical consumer is recorded once in
`response_cache_uses`.

## Indexed identity and eligibility

Callers supply a `CacheKey` containing the scope, semantic-fingerprint
version/HMAC, route-identity HMAC, and non-negative signed 32-bit variant. The
route identity is required on both fill and lookup, so a ready entry cannot be
reused across provider/endpoint/model lowering identities. The caller must
explicitly provide a positive `MaxAge`; lookup compares `completed_at` with one
PostgreSQL clock observation. `last_used_at` never makes stale output fresh.

An opted-in lookup authenticates the envelope before recording a hit. A retry
with the same operation ID does not increment `use_count`; a different
operation increments it with signed 32-bit saturation. Reusing one operation
ID against a different cache entry is rejected rather than silently changing
the operation's cache provenance. Publication records the origin operation in
the same uses table, so the origin is included in the durable use count and
cannot later consume a different cache entry. A miss returns without a
PostgreSQL budget read or provider dispatch decision.

## Fill leases and bounded responses

`BeginFill` acquires an expiring row-level lease keyed by the same route
identity as the entry. An active lease is reported as busy only for that route,
while a failed or expired lease may be taken over. A completed fill is reused
only while its entry remains ready; tombstoned entries can acquire a fresh
route-isolated lease. `Publish` inserts the ready entry and marks the fill
completed in one durable transaction. The response
template is authenticated envelope ciphertext and is capped by
`DefaultResponseCacheMaxInlineBytes` (256 KiB by default). Responses above the
cap require the future blob-backed publication path; this implementation does
not silently put unbounded model output in PostgreSQL TOAST. `FailFill` leaves a
failed marker so a later owner can safely retry.

The repository does not materialize checkpoints, route affinity, or provider
requests. The origin operation and checkpoint must already exist and are bound
by foreign keys. Cache hits still need the surrounding Generate/Compact code to
create a distinct cache-replay operation and child checkpoint, as required by
the architecture; this slice only provides the durable cache boundary.

Integration tests are opt-in through `LLMTW_POSTGRES_ADDR`, using the same
clean PostgreSQL 17 schema gate as the operation repository.

The concurrent-fill integration test repeats two simultaneous lease attempts
to cover the unique-conflict reread path. One caller must acquire the lease and
the other must receive `busy`; a database `no rows` error is never a valid
concurrency outcome.
