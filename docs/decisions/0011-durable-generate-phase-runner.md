# ADR 0011: Durable Generate phase runner

- Status: Accepted implementation slice
- Date: 2026-07-25
- Complements: ADR 0010, ADR 0006, and ADR 0007

## Context

ADR 0010 deliberately leaves production V1 composition fail-closed until the
durable state and provider lifecycle are wired. The implementation needs a
small, reviewable seam that makes the required ordering explicit without
pretending that the legacy `llm.Engine` implements checkpoint, cache, Compact,
or Query semantics.

## Decision

`storage/durable.GenerateV1` is the storage-neutral Generate orchestration
seam. A snapshot-owned composition supplies typed ports for this order:

```text
replay/materialize
  -> route-isolated cache lookup
  -> compaction decision
  -> route selection
  -> Redis reservation
  -> PostgreSQL journal
  -> one-shot provider dispatch
  -> PostgreSQL finalization
  -> Redis reconciliation
```

The runner validates operation, generation, reservation, journal, and bounded
response identities at every phase boundary. It fails closed before provider
dispatch when any pre-dispatch phase fails. A cache hit finalizes without
dispatching inference. A reconciliation failure is returned as
`ErrReconcilePending` after finalization so Temporal can retry reconciliation
without silently rerunning provider work. Ports must be idempotent across
Activity retries and must not log or serialize raw prompt/provider state.

This slice intentionally does not construct clients or claim that V1 is
production-complete. Concrete Redis/PostgreSQL/provider ports, snapshot
factory wiring, the distinct Compact runner, and the query-only control-plane
service remain required before `UnconfiguredV1Runtime` can be replaced.

## Evidence

- `golang/storage/durable/v1_runner.go` defines the typed phase ports and
  ordering.
- `golang/storage/durable/v1_runner_test.go` covers normal ordering, cache-hit
  short-circuiting, pre-dispatch fail-closed behavior, identity mismatches, and
  retryable reconciliation failure.
- `make -C golang test` passes with the runner included.
