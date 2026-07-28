# PostgreSQL repository foundation

The PostgreSQL repository slices are implemented in
[`golang/storage/postgres`](../../golang/storage/postgres). They cover the
connection, namespace, scope, encrypted-locator, exact USD codec, one-shot
operation/attempt/result boundaries, and the landed Phase B write-only budget
journal foundation. Checkpoints remain a separate delivery slice; response
cache publication is described in [the cache reference](postgresql-response-cache.md).

The Phase B budget boundary is deliberately split across two packages:

- `postgres.BudgetJournal` appends validated reservation and completion events
  and updates the durable bucket/reservation projections in one transaction.
  It has no active-budget read method, so normal admission cannot turn
  PostgreSQL into a budget-state fallback. Idempotent retries return the
  existing journal identity only when the complete event identity and payload
  match; unknown costs retain SQL `NULL` with an explicit reason.
- `redis.BudgetGenerationPort` and `redis.BudgetEventPort` define active
  generation adoption/publication and broadcast Stream coordination. Their
  memory implementations provide deterministic offline contract coverage; a
  Stream event is only a coordination hint and never authorizes work.

These are ports and durable write primitives, not the production composition.
Task 19 still has to sequence Redis acceptance, the PostgreSQL journal write,
and provider dispatch in the runtime, and must implement the guarded
dependency/readiness, generation recovery, and cross-store crash-boundary
proofs before this composition can be enabled for paid production work.
When a `durable.Composition` is assembled, validation is fail-closed: missing
capabilities and interfaces containing typed-nil pointers are rejected before
any Redis or PostgreSQL side effect. This guard does not itself enable the
runtime composition; the sequencing and recovery work above remains required.

## Connection and transaction boundary

`postgres.BuildPoolConfig` validates one immutable `Namespace`, configures a
bounded `pgxpool`, and supports multiple hosts on one port. When TLS is
enabled, a configured CA and server name are required and certificate
verification remains enabled. New connections set UTC, statement/lock/idle
transaction timeouts, and `synchronous_commit=on`. `postgres.Health` is a
read-only readiness check: it verifies `current_database()` and the UTC
session; it never creates schema objects.

Use `postgres.WithTransaction` for durable writes. The helper owns begin,
commit, and rollback, sets UTC and synchronous commit locally, and passes a
`pgx.Tx` to the callback. Blob/provider I/O must happen outside that callback;
the transaction helper does not expose a pool through the transaction port.

## Scope and encrypted locators

`ScopeRepository` derives tenant and project HMAC-SHA-256 lookup values from a
versioned keyring and upserts only those digests. Raw tenant/project values are
never SQL parameters or persisted columns. `BlobRepository` stores bounded
object metadata and an authenticated envelope-encrypted locator. The content
digest and byte length describe the object bytes, not the locator string. The
AEAD context binds scope, stable blob identity, payload kind, and content
digest, so a copied or tampered ciphertext fails authentication even when two
operations reuse one content-addressed row. Key rotation retains old read
keys while new values use the active key.

## Exact USD values

`EncodeUSD` and `DecodeUSD` use the existing exact `pricing.USD` type and bind
canonical decimal text to PostgreSQL `NUMERIC(38,18)`. Exponents, signs,
rounding, floating-point values, and overflow are rejected. Nullable helpers
preserve unknown cost as SQL `NULL`; unknown is never coerced to numeric zero.

All identifiers are generated through the validated namespace and all values
are positional SQL parameters. This package does not use `search_path`, broad
JSONB lookup, raw provider IDs, or streaming APIs.

The first response-cache slice is implemented by
[`ResponseCacheRepository`](postgresql-response-cache.md). It provides
route-isolated, age-bounded inline cache lookup/use accounting plus durable fill
leases and publication; checkpoint materialization and blob-backed cache
publication remain separate delivery slices.
