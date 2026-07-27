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
response identities at every phase boundary. Every newly finalized checkpoint
is also bound to its effective replay branch: a root response omits its parent,
a non-compacted response names the request parent, and a response produced
after automatic compaction names the newly materialized compaction child.
Cache finalization occurs before compaction and therefore remains bound to the
request parent. Completed and pending-reconciliation replays return the
already-authoritative committed relationship rather than incorrectly
re-deriving it from the original request. These checks prevent a finalization
or cache port from publishing a valid response on another branch without
rejecting a correct post-compaction retry. A replayed completed operation
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
prompt/provider state. The runner checks the Activity context after each
read/decision/routing callback and before the first paid admission, and again
before reconciliation after finalization. A port may finish concurrently with
Activity cancellation, so an entry-only check could otherwise continue into
another pre-dispatch side effect or return a cache/finalization result as a
success. Automatic Compact is a committed child boundary: cancellation after
that child returns stops before the parent Generate admission so a retry can
reuse the child. Once Redis accepts a reservation or a provider dispatch
returns, this slice deliberately leaves the existing journal/finalization
behavior unchanged; it does not claim compensation or cancellation-safe
cleanup for those committed boundaries. The guard returns the context error
directly where it is safe to stop, preserving Temporal cancellation/deadline
handling without making already-committed state disappear.

Before cache lookup or routing, the runner also validates the materialized
transcript's tool frontier and applies the bounded `GenerateRequestV1.append`
delta to that frontier in memory. An unmatched tool result, duplicate tool
call, or new turn inserted before outstanding tool results therefore fails
closed before admission or provider dispatch. The same frontier check guards
Compact replay. This validation does not copy ancestor state into the
Temporal payload and does not replace the durable materializer's graph/blob
integrity checks. The replay state is also bound to the request scope at this
boundary: a root request must not carry a materialized handle or scope, a
child replay must name the exact requested parent handle, and every
materialized handle must carry the request's tenant and project. Automatic
Compact may replace the parent with its newly materialized child, but it may
not change that tenant/project scope. These checks fail before cache lookup,
routing, Redis reservation, PostgreSQL journaling, or provider dispatch.

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
  behavior, operation and effective checkpoint-parent identity mismatches, and
  retryable reconciliation failure. The tests cover post-compaction parent
  binding, replay of a committed post-compaction response, and replay of a
  finalized response with pending Redis reconciliation without a second
  provider dispatch. Cancellation tests cover the pre-dispatch phase
  boundaries, prove cancellation after Compact stops before parent admission,
  and cover the post-finalization reconciliation handoff. The suite also
  proves that a tool-result delta resolves the replay frontier and that an
  unmatched result is rejected before cache or admission. Replay binding tests
  cover root-state rejection, wrong-parent rejection, cross-tenant/project
  rejection, and valid scope-preserving post-compaction replacement.
- `make -C golang test` passes with the runner included.
