# ADR 0013: Durable Compact phase runner

- Status: Accepted implementation slice
- Date: 2026-07-25
- Complements: ADR 0010, ADR 0011, and the forkable conversation plan Task 11

## Context

The v1 Activity boundary has a distinct `llm.compact.v1` record. Compact is a
lossy context-reduction child, not a normal Generate answer: its checkpoint is
of kind `compaction`, its parent remains reusable, application tools and
structured output are isolated inside the provider adapter, and an exact-cache
hit creates a new child rather than returning the origin checkpoint.

ADR 0011 added the Generate phase seam but deliberately left Compact as a
follow-on. Reusing that runner would make it too easy to accidentally return a
Generate response, permit a non-zero Compact cache variant, or skip the cache
child/finalization boundary.

## Decision

`storage/durable.CompactV1` is a separate storage-neutral orchestration seam.
Its snapshot-bound ports run this order:

```text
replay/materialize
  -> route-isolated Compact cache lookup (variant zero)
  -> route selection
  -> Redis reservation
  -> PostgreSQL journal
  -> one-shot summarizer dispatch
  -> PostgreSQL checkpoint/cost finalization
  -> Redis reconciliation
```

The runner validates the Compact request and parent checkpoint identity,
short-circuits a completed operation before cache/admission, and fails closed
before provider dispatch when a pre-dispatch phase fails. A cache hit must be
finalized as a fresh compaction child with `cache.disposition=hit`, worker-cache
provenance, and exact zero cost. A normal finalization must use the reserved
operation identity and retain the request's parent handle. Reservation denial
and post-finalization reconciliation failure preserve typed retry semantics. A
materialized replay must match the request's opaque parent handle and tenant /
project scope. When finalization has already committed but Redis reconciliation
is pending, replay carries the finalized identities and retries reconciliation
without dispatching the summarizer again.

The runner checks the Activity context after each read/decision/routing port
returns and before the first paid admission, and again before reconciliation
after finalization. This closes the race where a storage call finishes
concurrently with Activity cancellation without adding a new exit after an
accepted Redis reservation or known summarizer result. Post-admission
cancellation cleanup remains the existing port/recovery contract; this slice
does not claim compensation. The context error is returned directly where it
is safe to stop so Temporal preserves its cancellation/deadline semantics; this
does not roll back state already committed by a completed port.

The runner does not construct Redis/PostgreSQL clients, select a provider, or
claim that the production factory is wired. Provider adapters remain
responsible for generic/native compaction policy, plain-text/tool isolation,
usage decoding, and checkpoint publication. Those concrete ports and protected
integration evidence remain required before `UnconfiguredV1Runtime` can be
replaced.

## Evidence

- `golang/storage/durable/compact_runner.go` defines the distinct Compact
  ports, ordering, cache-child invariants, and bounded typed errors.
- `golang/storage/durable/compact_runner_test.go` covers normal ordering,
  completed replay, cache-hit child finalization, pre-dispatch fail-closed
  behavior, reservation/reconciliation retry mapping, pending reconciliation
  recovery, materialized scope validation, identity/variant rejection, and
  cancellation at the pre-dispatch and post-finalization boundaries.
- The existing `llm.CompactRequestV1`/`llm.CompactResponseV1` wire contracts
  and `compaction` package remain the authority for policy and provider-input
  isolation.
