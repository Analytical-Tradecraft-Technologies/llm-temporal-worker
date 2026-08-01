# Redis/PostgreSQL budget boundary

`storage/durable.BudgetBoundary` is the first storage-neutral composition seam
for the durable budget path. It binds one immutable `StateIdentity` to a Redis
`BudgetMaterializer` and a PostgreSQL `Journal`.

`storage/durable.CompositionBuilder` provides the snapshot-owned assembly seam
around that boundary. A deployment supplies the complete operation,
continuation, result, journal, and materializer ports in one
`CompositionPorts` value; `Build` validates them and returns a `Composition`
whose `BudgetBoundary()` and `NewLifecycle()` are tied to the same identity.
The builder has no client-construction or provider-dispatch side effects, so
an incomplete reload fails before the worker can poll.

## Ordering

For a new operation the caller must advance the lifecycle through
`operation_replay`, then call `Reserve`:

1. Redis evaluates the request and returns an operation- and generation-bound
   result.
2. Every accepted reservation event is appended to PostgreSQL, one event at a
   time, in the returned order.
3. Only after all appends succeed may the caller dispatch a provider request.

After the provider has returned, the caller advances the lifecycle through
`dispatched` and calls `Finalize`:

1. Every completion event is appended to PostgreSQL.
2. The same events are sent to Redis for reconciliation.
3. PostgreSQL finalization is authoritative when Redis reconciliation fails;
   the failure is returned as `ErrReconcilePending` for retry.

The boundary validates operation, generation, incarnation, event identities,
and snapshot identity before allowing a side effect. Typed-nil Redis or
PostgreSQL ports are rejected during validation so a reload cannot silently
construct a partial snapshot.

The ports intentionally do not expose mutable identity state. The caller must
construct both adapters from the same immutable `StateIdentity` and replace
the complete boundary on reload; mixing a Redis adapter or journal from another
snapshot is outside this contract and must be rejected by the runtime factory.
The runtime capability bundle's optional `DurableCompositionFactory` invokes
the builder-owned value and validates it before exposing it to an Activity.

## Failure semantics

There is no cross-store transaction. If a reservation append fails after Redis
accepts, the boundary returns `ErrJournalPending` and marks the result as not
dispatch-ready. It makes a best-effort Redis reconciliation using deterministic
zero-cost `JournalRelease` events for every accepted reservation event. Those
cleanup events are returned as bounded retry metadata when reconciliation
fails. The current `Journal` port does not append those cleanup events to
PostgreSQL; recovery must account for that append-only gap before dispatching
the operation. There is no dispatch until the reservation journal is complete,
or cleanup has been confirmed and a new reservation has been admitted. A
cleanup attempt aborts the current lifecycle; recovery must reconcile any
partial PostgreSQL journal before starting a fresh operation lifecycle.

The lifecycle is retry-aware: a reservation failure marks the lifecycle
aborted, while a reconciliation retry may resume from `postgres_finalized`
without appending completion events a second time.

Completion append failures also return `ErrJournalPending` and do not call
Redis reconciliation. A reconciliation failure occurs only after PostgreSQL
completion has been recorded and is retryable as `ErrReconcilePending`. A
completion digest is bound to the lifecycle, so a retry cannot reconcile a
different completion batch after PostgreSQL has committed the original.

## Scope

This slice is intentionally separate from the legacy engine and runtime
factory. It does not yet wire provider dispatch, operation/checkpoint/cache
stores, Compact, Query, reload orchestration, or the complete V1 runtime
composition. The builder and factory capability are validation seams only;
they are not evidence that a paid production deployment, protected recovery
run, or complete V1 composition is enabled. Those pieces must consume this
boundary only after their own snapshot and recovery contracts are defined.
