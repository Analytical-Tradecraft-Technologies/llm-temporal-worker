# System Overview

## Architectural style

The system is a hexagonal Go application with a reusable inference core. The
Temporal Activity, CLI process, provider SDKs, Redis, and observability exporters
are adapters around domain packages. The domain has no dependency on Temporal,
provider SDK structs, Redis, Kubernetes, or environment variables.

The central metaphor is a compiler:

```text
semantic request
  -> normalization
  -> capability resolution
  -> route and price resolution
  -> budget admission
  -> provider lowering
  -> official SDK dispatch
  -> provider response lifting
  -> normalized result and continuation
```

## Component relationships

```mermaid
flowchart LR
    W["Caller Temporal Workflow"] -->|"llm.generate.v1"| A["Activity adapter"]
    G["Go library caller"] --> E["Inference engine"]
    A --> E
    E --> N["Normalizer and validator"]
    N --> C["Capability compiler"]
    C --> R["Router"]
    R --> P["Price resolver"]
    P --> B["Redis exact budget admission"]
    B --> J["PostgreSQL operation and budget journal"]
    J --> L["Provider adapter"]
    L --> S["Official provider SDK"]
    S --> X["External LLM endpoint"]
    X --> L
    L --> E
    E --> T["Checkpoint and cache store"]
    B --> D[("Redis budget generation and Stream")]
    J --> Q[("Worker PostgreSQL namespace")]
    T --> Q
    E --> O["Logs, metrics, traces"]
```

## Request lifecycle

1. **Decode and normalize.** The boundary validates `api_version`, rejects
   unknown enum values, turns an omitted service class into `standard`, and
   canonicalizes IDs and JSON Schema.
2. **Load continuation.** When a handle is present, the engine loads the
   immutable state record, verifies tenant ownership and expiry, and merges the
   new input items after the stored transcript.
3. **Compile candidates.** The capability compiler intersects request features,
   logical model routes, endpoint/model capability declarations, requested
   service class, portability mode, and explicit class fallbacks. It produces
   ordered candidates and diagnostics without contacting a provider.
4. **Resolve price.** Each candidate is tied to one immutable price-catalog
   version when a current quote exists. A candidate without one is excluded
   when it matches a budget, and otherwise is eligible only under the explicit
   unpriced policy with `cost_status=unknown`.
5. **Estimate and admit.** The estimator computes the maximum single-attempt
   bound across the authorized plan. After PostgreSQL operation replay checks,
   one Redis Function atomically reserves the exact amount against every
   matching active window and publishes the budget Stream event. The worker
   then appends the idempotent PostgreSQL budget journal/projection writes; it
   does not read PostgreSQL budget state. Dispatch is forbidden until that
   durable write commits.
6. **Lower.** The selected adapter converts semantic items to official SDK
   parameter types. Provider extensions are applied only after namespaced
   allow-list validation.
7. **Mark dispatch.** Immediately before the SDK transport can write bytes, the
   ledger moves from `reserved` to `dispatching`. Transport instrumentation
   records whether failure occurred before or after a possible write.
8. **Invoke once.** SDK retries are disabled. The Activity context controls the
   deadline and cancellation. Long calls emit small heartbeat details.
9. **Lift and reconcile.** The adapter converts a one-shot response back to
   semantic types, captures request IDs and actual service tier, and normalizes
   usage. Pricing uses provider-reported cost when authoritative, otherwise the
   pinned exact catalog. If neither establishes the real charge, actual cost is
   NULL with an explicit unknown reason; reservations/bounds remain separate.
   If a definitely charged failure permits safe fallback, PostgreSQL durably
   records the attempt/journal transition before Redis atomically reconciles
   the active windows and reserves the remaining plan before another dispatch.
10. **Commit result.** The engine atomically records the completed result,
    child checkpoint, exact-or-unknown cost, and budget journal/projection in
    PostgreSQL, then idempotently reconciles the Redis reservation. A repeated
    operation key returns the recorded result without another budget charge.

If a dispatch outcome is ambiguous, step 10 records `ambiguous` and keeps the
full reservation. It returns a non-retryable error that contains a safe
reconciliation reference, not prompt content.

## Core ports

The implementation must preserve these domain-facing interfaces. Method names
may gain context-specific options, but responsibilities must not collapse into a
single provider-aware service.

```go
type Engine interface {
	Generate(context.Context, llm.Request) (llm.Response, error)
}

type Adapter interface {
	Compile(context.Context, provider.CompileInput) (provider.Call, error)
	Invoke(context.Context, provider.Call, provider.Observer) (provider.Result, error)
}

type Router interface {
	Plan(context.Context, routing.Input) (routing.Plan, error)
}

type PriceResolver interface {
	Resolve(context.Context, pricing.Query) (pricing.Quote, error)
}

type OperationStore interface {
	BeginOrReplay(context.Context, operation.BeginRequest) (operation.BeginResult, error)
	MarkDispatching(context.Context, operation.DispatchRequest) error
	Complete(context.Context, operation.CompleteRequest) error
}

type BudgetStore interface {
	Acquire(context.Context, budget.AcquireRequest) (budget.AcquireResult, error)
	Reconcile(context.Context, budget.ReconcileRequest) error
}

type BudgetJournal interface {
	AppendReservation(context.Context, budget.ReservationEvent) error
	AppendCompletion(context.Context, budget.CompletionEvent) error
}

type CheckpointStore interface {
	Get(context.Context, state.Handle) (state.Checkpoint, error)
	PutChild(context.Context, state.PutChildRequest) (state.Handle, error)
}
```

`Engine.Generate` is the only supported v1 inference entry point. The Temporal
Activity invokes it once for each Activity execution and returns the final
normalized response. Residual streaming types or decoders are unsupported in
v1 and must not be wired into the Temporal runtime.

`BudgetStore` is implemented by the atomic Redis Function and is the only live
active-window read path. `BudgetJournal` performs PostgreSQL writes before
dispatch but normal service never calls its read methods; the exceptional
cold-bootstrap reader is a separate recovery interface available only after
the fleet/Redis-loss fence. Pricing, policy matching, estimation, and window
semantics remain reusable packages outside the stores.

The first Phase B foundation slice keeps that split explicit in code. The
`budget` package owns validated `ReservationEvent` and `CompletionEvent`
records, `storage/postgres.BudgetJournal` exposes write-only append methods,
and `storage/redis.BudgetGenerationPort`/`BudgetEventPort` expose generation
adoption and broadcast Stream boundaries. These ports do not compose the
runtime yet: Redis acceptance, the PostgreSQL append, and provider dispatch
remain a deliberately sequenced Task 19 change. The Redis event port is
coordination-only; every authorization still goes through the atomic Redis
Function.

The bounded v1 composition exposes `runtime.NewDurableV1RuntimeBuilder` for
the complete one-shot Generate and Compact boundary. It accepts a complete
snapshot-scoped capability bundle and deployment-owned phase factories,
validates both resulting durable port sets, and attaches them to the
snapshot. This does not supply the Task 19 Redis/PostgreSQL/provider
implementations: those bindings remain deployment owned, and a missing or
incomplete binding still fails closed. Query remains separate so
authorization and cursor/audit policy cannot be inferred from inference
composition. The phase-only Generate and Compact builders remain available
for contract tests and staged assembly, but are not production-ready runtime
values.

## Configuration snapshots

Configuration is parsed, defaulted, validated, and compiled into one immutable
snapshot. An Activity captures the current snapshot at start and uses it for its
entire execution. A reload builds a new snapshot off to the side and swaps one
atomic pointer only after all endpoint references, route cycles, capability
claims, prices, budgets, and secret references validate.

The result records the non-secret snapshot digest, route ID, capability version,
and price-catalog version. A continuation retains the snapshot facts needed to
interpret prior provider state but may use a newer route snapshot only when
portability checks permit it.

## Failure domains

| Domain | Required behavior |
| --- | --- |
| Invalid caller input | Fail before state or provider access; non-retryable |
| Unsupported semantic feature | Fail before admission in strict mode; diagnostic drop only in best-effort mode |
| Budget denial | Fail before dispatch; retryable only when `retry_after` fits the Activity schedule |
| Redis unavailable or unexplained same-incarnation incomplete | Fail closed for new budgeted work; do not read PostgreSQL. Only a proven full-fleet cold start or verified new Redis incarnation without persisted state may run the fenced rebuild |
| Worker PostgreSQL unavailable | Fail closed before dispatch because operation and budget journal cannot be made durable |
| Provider definite transient failure | Release/refund as documented; try next candidate only within route bounds |
| Provider ambiguous failure | Keep reservation, record ambiguity, stop automatic retries |
| Worker termination | Temporal retries; ledger returns a completed result or ambiguity instead of blindly repeating |
| Configuration reload failure | Keep serving the last valid snapshot and return the reload error; readiness remains based on the active snapshot and dependency probes |

## Horizontal scaling

With memory state, one worker process is an isolated development environment and
must not advertise distributed durability. In production any replica can use
the shared Redis budget generation/Stream and worker PostgreSQL namespace.
Checkpoints/replay come from PostgreSQL; atomic active-window decisions come
from Redis. A joining worker uses Redis only for budget state. Provider SDK
clients and compiled configuration are process-local and immutable; no sticky
routing to a pod is required.
