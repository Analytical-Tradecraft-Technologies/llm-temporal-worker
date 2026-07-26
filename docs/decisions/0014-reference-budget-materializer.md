# ADR 0014: Reference budget materializer

- Status: Accepted test/reference implementation only
- Date: 2026-07-26
- Complements: ADR 0004, ADR 0007, and ADR 0010

## Context

The durable budget port needs a storage-neutral executable model so Redis
implementations can be checked against the same exact-money and journal
transition invariants. A test model must also make retry behavior explicit:
multi-window admission is atomic, operation and event identities are
idempotent, and a generation or incarnation mismatch fails closed.

## Decision

`durable.ReferenceBudgetMaterializer` implements `BudgetMaterializer` as an
isolated in-memory model. It uses exact `pricing.USD` values, validates and
canonicalizes reservations, performs all capacity checks before mutation,
tracks reserved and accounted amounts per window/bucket, and removes both
amounts when an entry expires. Replays with an identical operation or event
are successful no-ops; a changed payload with the same identity is a typed
conflict. Completion events are matched by both `WindowID` and `BucketStart`,
so one operation may safely contain multiple buckets for one window.

This is deliberately a conformance/reference implementation. It is not a
production Redis adapter, is not selected by the runtime factory, and is not a
fallback when Redis is unavailable. Production activation still requires the
Redis implementation and protected integration evidence described by the v1
release requirements.

## Evidence

- `golang/storage/durable/reference_materializer.go` contains the model and
  its strict identity, idempotency, accounting, and expiry rules.
- `golang/storage/durable/reference_materializer_test.go` covers atomic
  denial, operation replay/conflict, bucket-specific reconciliation,
  completion replay/conflict, and expiry of reserved plus accounted values.
