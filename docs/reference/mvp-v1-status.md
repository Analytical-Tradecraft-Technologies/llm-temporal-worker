# MVP v1 implementation boundary

This page describes checked-in worker behavior. It is not deployment, live
provider or cloud restore evidence. The current code and tests take precedence
over historical implementation plans.

## Temporal entry points

The worker registers four workflows and eight activities on its configured task
queue. Public callers use `llm.generate.workflow.v1` or
`llm.compact.workflow.v1`. Generation plans compaction first and calls the
compaction workflow as a child when necessary. Both use `llm.request.execute.v1`
for cached results, durable progress, provider execution and completion, and
`llm.budget.wait.v1` for acquisition with Temporal timers.

| Activity | Responsibility |
| --- | --- |
| `llm.generate.plan.v1` | Inspect authorized context and decide whether compaction is needed |
| `llm.request.prepare.v1` | Prepare or reload the durable request and eligible cached work |
| `llm.budget.acquire.v1` | Try acquisition once or recover existing work |
| `llm.generate.v1` | Claim authorization and submit/resume generation |
| `llm.compact.v1` | Submit/resume compaction, or complete a no-work compaction |
| `llm.poll.v1` | Retrieve provider status once using the internal request reference |
| `llm.complete.v1` | Publish durable output/checkpoint/cache and settle the reservation |
| `llm.query.v1` | Execute an explicitly configured typed control-plane query |

Synchronous providers hold the submission activity through the HTTP response.
Resumable providers return pending and are polled by later activities. Provider
identifiers and budget receipts stay inside durable runtime storage. Unknown
paid outcomes acquire a new reservation before another submission; the original
attempt remains accounted and pending. `limits.route_attempts` bounds provider
executions per request (default six), including unknown outcomes. Unused quote
renewals do not consume attempts. At exhaustion the root returns a terminal
`provider_error`; unknown children remain accounted and discoverable for recovery.
Retries prefer eligible candidates with fewer prior attempts, preserving route
priority when counts tie. No service cancellation API is exposed.
See [workflow behavior](internal-workflows.md) and [activity contracts](activity-runtime.md).

## Storage and callers

Cloud KV/blob interfaces hold requests, attempts, pending discovery, encrypted
checkpoints and cache artifacts. The initial provider uses DynamoDB and S3.
Redis owns budgets, throttles, provider status and inventory. Budget policy comes
from worker settings. The cloud execution path records provider availability
into Redis but not yet credit/billing evidence (see
[provider control](provider-control.md)).
Content-free access audit events are not yet emitted for Generate/Compact
access or scope denials. Query authorization decisions (allowed and denied),
completed queries and configuration reloads do reach structured logs. Temporal's own storage
is independently operated. See [storage responsibilities](../architecture/state-and-storage.md).

The production CLI requires explicit trusted-Temporal authorization policy and
valid cloud/Redis configuration. The typed OCaml client supports starting or
calling the public workflows and decoding their final responses. Tests cover
workflow composition, retries, cache and compaction with deterministic adapters;
local service gates distinguish real Temporal/Redis from in-memory cloud stores.

Configuration reloads do not prevent finalization of a saved terminal provider
result. An attempt that has already reached its provider is also polled or
recovered under a different configuration digest when its route and its
endpoint's own configuration are unchanged; it is never submitted again. If
either changed, an incompatible worker returns a retryable state-unavailable
error until compatible settings return. Work that still needs planning, budget
admission or a new attempt (including after a retryable provider failure) has
nothing unfinished paid. A worker whose configuration started serving after
such a request was prepared returns the same retryable wait for its first 15
minutes, which covers a rolling deployment, and then fails fast with a
non-retryable `configuration` error; a worker on an older configuration always
waits, since a newer one may be rolling out. Submit a failed request again under
a new operation key; it is never re-planned under the new configuration. The
saved request is not changed, so restoring its settings still resumes it, and
only a compatible worker applies the request's attempt limit.

## Separate or deferred capabilities

- The production CLI composes authenticated control queries. Provider status,
  model inventory and credit status read Redis provider state. Budget status
  reads the active Redis budget generation once the `llmtw_budget_status_v3`
  Function is loaded and a generation is published; the worker publishes
  neither. See [persisted queries](persisted-query-service.md). Spend summary
  (which needs a durable cloud spend reader), provider management refresh, and
  Temporal-level query evidence remain tracked in
  [#817](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/817).
  Missing capabilities return typed unsupported errors; no fabricated results
  substitute for a reader.
- Complete Redis data-loss reconciliation is [#856](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/856).
  The initialization receipt detects a missing authority marker, not arbitrary
  partial loss or restoration of an older accounting snapshot.
- Automatic retention/blob deletion and unknown-cost reconciliation are deferred
  in [#818](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/818)
  and [#819](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/819).
- Live AWS/provider execution, backup/restore qualification and deployment remain
  separate evidence gates. Passing offline tests does not complete those gates.

The overall tracker is [#812](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/812).
