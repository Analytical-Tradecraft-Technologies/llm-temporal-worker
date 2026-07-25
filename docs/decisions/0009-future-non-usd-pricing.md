# ADR 0009: Future Non-USD Pricing Normalization

- Status: Deferred design requirement
- Date: 2026-07-25
- Complements: ADR 0007; the v1 pricing contract is USD-only

## Context

The v1 worker accepts only exact decimal USD catalog values. Provider price
sources that are denominated in another currency are rejected at the catalog
boundary. Introducing a foreign-currency amount or a caller-supplied exchange
rate would make reservations, accounting, cache identity, and release audits
ambiguous.

## Decision

The first concrete non-USD provider must be enabled by a superseding ADR before
its catalog can be configured. That ADR must define all of the following:

1. A worker-owned, authenticated rate source and its refresh/failure policy.
2. The exact decimal conversion algorithm, precision, rounding direction, and
   effective-time semantics used for admission and settlement.
3. Maximum rate staleness and the fail-closed behavior when a rate is missing,
   expired, or unavailable.
4. The catalog, operation, query, and audit provenance fields needed to retain
   the source currency, rate identity, and converted USD result without
   allowing a caller to override conversion.
5. A migration and replay strategy for operations admitted under each rate
   snapshot, including cache and idempotency behavior.

Until that ADR is accepted and implemented, all public and persisted monetary
values remain exact decimal USD, and non-USD provider entries remain invalid.

## Consequences

This keeps v1 accounting and budget admission single-currency and avoids an
incomplete FX subsystem. A future provider cannot be enabled by adding a
`currency` field or a rate to existing configuration; it requires the
worker-owned, auditable conversion boundary described above.
