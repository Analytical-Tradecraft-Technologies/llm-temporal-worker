# Cloud budget planning

The [cloud admission integration](cloud-budget-admission.md) connects these plans
to original request recovery, durable plan persistence and Redis Reserve/Claim.

`internal/runtime.V1RuntimeCapabilities.NewBudgetPlanning` supplies shared Generate
and Compact selection and quoting for cloud phase factories. It uses prepared
semantic input and the same captured snapshot as provider compilation. Public
activity contracts remain v1.

```go
planning, err := capabilities.NewBudgetPlanning(ctx)
// Check each error before continuing.
planned, err := planning.Generate(ctx, prepared, BudgetAttempt{
    OperationID: attemptOperationID,
    GenerationID: budgetGenerationID,
    QuotedAt: persistedQuoteTime,
    ExpiresAt: persistedAdmissionExpiry,
})
// Compact uses planning.Compact(ctx, preparedCompact, attempt).
```

Candidates are compiled in the configured routing/class order. A candidate must
also satisfy required policy matching and have usable pricing before selection
succeeds. Missing/inactive prices, unknown required price components and unsafe
Redis reservation amounts allow the next authorized candidate. Invalid resolver
responses, price version mismatches, tokenizer errors and ambiguous compilation
stop planning. Exhausting candidates returns `no_route`; no budget balance was
consulted or changed. Insufficient available budget remains Redis's immediate
acquired/wait decision and does not authorize a route change here.

Policy matching uses tenant, project, actor, environment, the application's
logical model and the actually attempted service class/endpoint. Pricing uses
the resolved provider, family, endpoint, region, model and provider tier at the
fixed quote time. The returned entry must match all those dimensions, its active
interval and the route's configured price version. An omitted entry version
uses the catalog version. `planned.Route.PriceVersion` contains the actual quote
version, including when the configured route had none.

The estimator receives the detached provider request: resolved model, attempted
class, no fallback classes, and the same digest used by the compiler. It reuses
the configured token estimator, output/reasoning bounds and safety ratio. Each
matched window receives the same exact USD quote, its exact configured limit,
the bucket at `QuotedAt`, and the window's durations. Legacy window limits are
explicitly converted to USD. Redis materializes charges upwards and limits
downwards to nanoUSD; unsafe amounts and limits cannot cross this boundary.
Compatibility microUSD fields remain alongside the authoritative exact values.
Duplicate policies/window IDs, unbounded matchers, invalid windows and limits
smaller than Redis can represent fail before planning.

Routing, policy slices and the built-in price resolver's catalog are captured
together. Source mutation or a later price reload cannot change that capture.
Custom injected resolvers, planners, registries and tokenizers must be
snapshot-owned and must not perform provider I/O. The estimator's safety ratio
is copied. Production capabilities carry the configured output bound, safety
ratio and maximum buckets per window. A previously captured `ProviderPlanning`
can also construct a budget planner with `NewBudgetPlanning(estimator, maxBuckets)`.
Each result owns its reservation vector and unknown-price markers.

`BudgetAttempt` supplies stable operation/generation IDs, quote time and expiry.
It performs no wall-clock reads. Composition must persist the selected route,
quote, estimate and complete reservation before any uncertain Redis mutation.
Reuse those stored facts after an uncertain acceptance; do not replan, move the
bucket, refresh expiry or select newly reloaded prices. The Redis leaser sets
the separate 15-minute deadline to start paid work when it accepts admission.
A consumed claim remains charged. This planner neither renews nor releases it.

[Durable cloud budget plans](cloud-budget-plans.md) provide the typed encrypted
save/load boundary for these initial planning facts. Phase factories must load
the saved plan before replanning and save a new proposal before Redis acceptance.

`RequiresReservation` is false for a known-free quote or an unmatched request
when matching is optional. `Quote == nil` distinguishes an unpriced, unmatched
request explicitly allowed by `require_price_when_budgeted`; it is not a free
price. A matched request can never use that exception. Never pass an empty
reservation vector to Redis or treat it as a claim. The current durable runners
require a positive reservation; concrete factories must support an explicit
authorized path for free/unbudgeted work before enabling those configurations.

Use the selected provider/cache identity from this combined planner when
building cache fingerprints and fill leases. Choosing a different route after
acquiring a fill would break its identity fence. Cache lookup may short-circuit
before any actual admission. The planning helper does not acquire fills or
automatically invoke plan persistence, install phase factories, submit providers
or poll. Production wiring, paid-attempt recovery and remaining SQL removal are
subsequent work.

Offline tests exercise both activities, exact sub-micro prices, all matched
windows, alias/class resolution, fallback, snapshot mutation/reload, invalid
quotes, cancellation, concurrent use, and uncertain admission followed by one
claim through the reference Redis accounting contract. They are not live AWS,
Redis, provider or Temporal verification.
