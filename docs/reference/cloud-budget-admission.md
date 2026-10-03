# Cloud budget admission

`V1RuntimeCapabilities.NewCloudBudgetAdmission` connects the stored request and
budget plan to the snapshot-owned Redis budget boundary. The caller must already
authorize the request's scope and materialize its checkpoint. Generate and
Compact share the same sequence:

1. `PrepareGenerate` or `PrepareCompact` loads the original request manifest and
   prepares its effective input. A caller cannot supply a replacement operation
   key or prompt alongside a saved plan.
2. Load the existing budget plan. Only `ErrBudgetPlanMissing` permits selecting
   and quoting a new plan. Save it before any Redis mutation, then read the
   durable winner back. A competing plan wins through the repository's CAS.
3. Reconstruct the saved provider selection with the original request key and
   configuration identity. Changed routing configuration rejects recovery;
   current prices and a replacement `BudgetAttempt` do not alter an existing
   quote, operation ID, bucket or expiry.
4. After cache lookup/fill ownership, `Reserve` verifies the saved plan again and
   returns one acquired/wait decision. Retry after an uncertain Redis reply uses
   that same reservation. Waiting belongs to a workflow timer.
5. Immediately before submission, `Claim` rechecks durable eligibility and
   consumes Redis's single-use start permission. A duplicate, expired or
   uncertain claim cannot authorize submission.

`CloudBudgetCall` is invocation-local. Its private bindings retain the request
scope, ID and saved plan. `Plan()` returns detached reservation vectors and
`Provider()` supplies the reconstructed SDK call; neither is a dispatch grant.
An activity restart calls preparation again instead of serializing this value.

Request reads, plan saves and discovery-index repair failures stop before Redis.
Provider-pending, outcome-unknown, terminal and finalizing requests cannot resume
initial admission. A paid retry after an unknown outcome requires separate
attempt orchestration and fresh budget. Known-free/unbudgeted requests still
need an explicit execution path and are not accepted as positive budget plans.

The helper performs no submission, polling, settlement, resource provisioning or
Temporal registration. The complete runtime composes those operations separately.
Tests cover both request kinds, restart after lost Redis/save replies, competing
plans, one dispatch claim across concurrent callers, waiting, claim expiry,
storage failures, scope/configuration binding and state changes between phases.
