# Implementer report: PostgreSQL exact-USD catalog snapshots

## Scope

This change implements the bounded durable boundary from pricing-plan Task 16:
`storage/postgres.PricingCatalogRepository` publishes and loads validated USD
catalog snapshots using the existing `price_catalogs` and `price_entries`
schema. It does not change the schema, runtime admission, Redis functions, or
the in-memory catalog loader.

## Requirements covered

- Revalidates the compiled catalog digest before publication.
- Requires source and compiled SHA-256 digests.
- Writes the catalog header and all entries in one synchronous transaction.
- Preserves exact USD values through `NUMERIC(38,18)` text binding.
- Stores omitted components as NULL with explicit partial/unknown status.
- Makes identical version/digest replay idempotent and rejects conflicts.
- Retires the prior active snapshot atomically and loads the effective snapshot.
- Serializes all catalog publications within a worker namespace with a
  transaction-scoped advisory lock, including first publication of a new
  version where no row exists to lock yet.
- Canonicalizes decimal formatting before persistence because PostgreSQL
  `NUMERIC` does not preserve source lexical formatting, then verifies the
  canonical digest on load.
- Canonicalizes entry interval timestamps to UTC before compiling the
  persisted projection, rejects unsupported persisted price statuses, and
  verifies every entry's source digest against the catalog source digest on
  load.

## Validation

- `go test ./storage/postgres`
- `go test -race ./storage/postgres`
- `go test ./...`
- `make docs-verify`
- `make fmt-check`
- `make schema-verify`
- `make postgres-integration` (live pinned PostgreSQL service)

All passed in this worktree, including the live future-dated replacement
assertion (the predecessor remains active until the replacement takes effect)
and tampered entry source-digest rejection. The integration suite also holds
one publisher after it acquires the namespace lock and proves a concurrent
publisher cannot enter the publication transaction until the predecessor
commits; the effective timeline contains exactly one active snapshot afterward.

## Self-review and concerns

The existing schema intentionally omits source-document prose provenance and
does not have a separate entry-version column. The repository therefore
rejects entries carrying either unrepresentable digest input rather than
silently changing the compiled digest. No schema expansion was made. A future
schema revision can add those projections if operators need them materialized.
