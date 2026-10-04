# ADR 0008: Resumable Provider Operations and Typed Queries

- Status: Accepted; bounded cloud execution and typed query contracts implemented
- Date: 2026-07-18

## Resumable operations

Provider adapters may implement `ResumableAdapter` with pending, completed,
failed and not-found outcomes. Adapters without that capability remain
synchronous. Provider identifiers and recovery context belong in durable
storage, not exclusively in Temporal heartbeats or caller-supplied references.

Generate and Compact submit or recover the current attempt. A pending result
returns control to the workflow. Each `llm.poll.v1` execution retrieves provider
status once; the workflow supplies the timer before another poll. Completion
publishes durable output and idempotently settles accounting.

Activity replay must not silently buy another submission. When acceptance may
have succeeded but its response was lost, the runtime records `outcome_unknown`
and retains the original charge and pending record. The internal workflow can
retry, but explicit acquisition creates a separate attempt with fresh budget.
This accepts the possibility of paying twice; it does not claim exactly-once
provider execution. An adapter's documented idempotency/recovery support can
resolve uncertainty where available, but it must not be invented for a provider.

Every result is bound to the expected request/operation identity. An unrelated
provider response is rejected before it can become a completed worker result.
There is no service cancellation API; workflow cancellation does not refund or
abandon paid work.

## Typed queries

`llm.query.v1` uses closed tagged request/response unions for provider status,
model inventory, credit status, budget status and spend summary. Each response
must match the request's tag. Reads are bounded and pageable. Explicit provider
refresh policy may invoke supported management APIs, never inference APIs.

Query composition is independently authorized. Redis supplies operational
provider state and the budget authority; cloud records preserve request and
cost facts. A budget-status or spend aggregation reader must be explicitly
provided when that query is enabled. Missing capabilities return typed
unsupported/unavailable errors rather than fabricated data or storage fallbacks.

Completed query reads emit best-effort structured audit logs. Local reads report
exact zero cost; paid management APIs retain exact-or-unknown cost semantics.
There is no query-execution database. The OCaml protocol layer represents exact
wire variants; its ergonomic GADT associates each request with its result type.
A mismatched response tag is a decode error.

## Evidence and consequences

Workflow tests cover cache/budget waits, polling, new-budget retries and terminal
identity checks. Provider contract fixtures verify submit/poll identity and
terminal outcomes without contacting a provider. Live provider and cloud
recovery qualification remain separate evidence gates.

See [workflow behavior](../reference/internal-workflows.md),
[provider execution](../reference/cloud-provider-execution.md) and
[persisted queries](../reference/persisted-query-service.md) for the implemented
boundaries. Adding a query kind requires coordinated Go, fixtures, OCaml codec,
authorization and compatibility changes.
