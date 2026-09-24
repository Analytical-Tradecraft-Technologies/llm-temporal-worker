# ADR 0008: Resumable Provider Operations and Typed Queries

- Status: Accepted design; adapter contract and durable Activity integration
  implemented for adapters that opt into the resumable port
- Date: 2026-07-18

## Context

Some provider APIs accept work and return an identifier that can be polled.
Treating an Activity retry as a new submission can create duplicate billable
work. The worker also needs a cheap control-plane interface for provider
availability, model inventory, billing/credit incidents, budgets, and spend.
Those queries have different result shapes.

## Decision

Extend the durable operation state machine with **provider_pending**. Persist a
deterministic provider idempotency key before submission and the returned
provider operation ID before the first poll. A restarted Activity loads the
operation and polls the recorded ID; it never submits again.

Where a crash can occur after provider acceptance but before the ID is stored,
an adapter may recover only through a documented provider idempotency or
lookup-by-key contract. Without that contract, the outcome is **ambiguous** and
automatic resubmission is prohibited. The design promises durable at-most-once
automatic submission, not external exactly-once behavior.

Add one versioned **llm.query.v1** Temporal Activity. Its request and response
are closed, tagged unions. Initial query kinds are:

- provider status;
- provider model inventory;
- credit/billing status;
- budget status; and
- spend summary.

Each response repeats the request tag and contains the result shape associated
with that tag. Query results are bounded and pageable. They normally read
persisted worker state; an explicit freshness policy may invoke a supported
provider management API but never an inference API.

Current budget status is the deliberate exception: it reads the verified Redis
budget generation only and returns its generation, manifest digest, and Stream
high-water mark. It never falls back to PostgreSQL. Spend summary reads
completed PostgreSQL operation/cost rows, not budget journal/working-set rows.
Each Query carries an operation key and a distinct query-execution ID.
Completed reads emit best-effort metadata to normal logs, including
exact-or-unknown cost; local stored-state reads report exact zero. They do not
require durable query-audit storage. The legacy `query_executions` repository
remains for direct callers until SQL removal; its existing rows may still be
included in historical spend, but normal query logging does not add rows.

The OCaml package exposes exact wire variants at its protocol layer and a GADT
at its ergonomic layer, associating each request constructor with its result
type. A mismatched response tag is a decoding error, not an open JSON value.

## Consequences

- Activity retry after worker loss can resume provider polling without a second
  submission.
- The unavoidable acceptance/persistence gap is visible and conservative.
- One Activity name avoids an unbounded set of tiny Activities while closed
  tags retain type safety.
- Cheap stored-state queries avoid an unnecessary blob put and provider-operation
  row while still meeting durable cost-audit and replay requirements.
- Provider health, inventory, credit state, budgets, and spend must be shared,
  scoped state rather than per-process observations. Redis owns live budget
  status; PostgreSQL owns the durable journal and historical operation cost.
- Adding a query kind requires coordinated Go, JSON fixture, OCaml codec, GADT,
  authorization, and compatibility work.

## Implementation boundary

The Go provider package now exposes an optional `ResumableAdapter` contract
with typed pending, completed, failed, and not-found outcomes, plus a separate
`IdempotencyRecovery` extension for providers that document key lookup. The engine's
bounded polling helper accepts only a previously persisted provider operation
ID, honors provider delay guidance subject to a worker cap, and reports retryable
poll limits without submitting again. It emits heartbeat progress containing
only phase/count fields; provider IDs remain durable state.

The shared provider contract-test package also validates recorded submit/poll
sequences without making network calls. It proves the common invariants (a
pending submit has one provider operation identity, polls do not change it, and
terminal outcomes stop the sequence) while leaving provider-specific status,
delay, and idempotency-lookup semantics to the adapter's documented fixtures.

The production Activity path connects the optional contract to durable
operation repositories. A `Submit` result in `provider_pending` is persisted
before polling, and a retry loads the encrypted identifier and calls `Poll`
only. Poll limits and cancellation leave the durable operation pending for the
next Activity attempt. Providers that do not implement the optional port remain
one-shot adapters. When `Submit` reports an accepted or ambiguous failure, an
adapter implementing `IdempotencyRecovery` receives exactly one lookup by the
deterministic operation key. A recovered pending identifier is validated,
persisted, and polled; recovered completed or failed outcomes follow the normal
ledger finalization path. A not-found, invalid, failed lookup, or adapter
without this extension remains ambiguous; automatic resubmission is never
inferred.

Every completed resumable result is also bound to the compiled call's semantic
operation key before polling or finalization. A provider response for a
different operation is rejected as an invalid provider response; the engine
never relies on finalization to overwrite that identity after the fact.

## Rejected alternatives

- Storing a poll ID only in a Temporal heartbeat is insufficient because
  heartbeats are not the authoritative operation ledger and can be lost around
  process failure.
- Resubmitting whenever a poll ID is absent can duplicate charges.
- Returning every query as untyped JSON pushes protocol mismatches into
  workflow application code.
- Separate Activity names for every filter/result permutation create a larger
  compatibility surface without improving result typing.
