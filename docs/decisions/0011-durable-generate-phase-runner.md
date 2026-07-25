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
  -> compaction decision and (when required) Compact child/rematerialization
  -> route selection
  -> Redis reservation
  -> PostgreSQL journal
  -> one-shot provider dispatch
  -> PostgreSQL finalization
  -> Redis reconciliation
```

The runner validates operation, generation, reservation, journal, and bounded
response identities at every phase boundary. A replayed completed operation
returns its durable response before cache lookup or admission. It fails closed
before provider dispatch when any pre-dispatch phase fails. A required
compaction invokes the distinct Compact child and replaces the replay with its
newly materialized state before routing. A cache hit finalizes without
dispatching inference and must publish a distinct `cache_replay` child with
`cache.disposition=hit` and exact zero cost; the runner rejects an origin
operation/checkpoint or an unmarked finalization. Reservation denial is a
typed, retry-after budget error. A reconciliation failure is returned as a typed retryable
`ErrReconcilePending` condition after finalization so Temporal can retry
reconciliation without silently rerunning provider work. Replay therefore
returns a bounded `GenerateReconciliation` handoff when PostgreSQL has already
committed the response but Redis completion is still pending; the runner
validates its operation, generation, reservation, and response identities,
reconciles it, and only then returns the committed response. Ports must be
idempotent across Activity retries and must not log or serialize raw
prompt/provider state.

This slice intentionally does not construct clients or claim that V1 is
production-complete. Concrete Redis/PostgreSQL/provider ports, snapshot
factory wiring, and the query-only control-plane service remain required
before `UnconfiguredV1Runtime` can be replaced. The distinct Compact
orchestration seam is recorded in [ADR 0013](0013-durable-compact-phase-runner.md);
its concrete storage/provider composition and protected integration evidence
remain pending.

## Evidence

- `golang/storage/durable/v1_runner.go` defines the typed phase ports and
  ordering.
- `golang/storage/durable/v1_runner_test.go` covers normal ordering, cache-hit
  short-circuiting and replay-child invariants, pre-dispatch fail-closed
  behavior, identity mismatches, and retryable reconciliation failure, including
  replay of a finalized response with pending Redis reconciliation without a
  second provider dispatch.
- `make -C golang test` passes with the runner included.
