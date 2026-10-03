# Durable cloud budget plans

`cloudstate.Repository.SaveBudgetPlan` and `LoadBudgetPlan` persist the initial
paid attempt's planning facts before Redis acceptance. They use the configured
request table's event stream and encrypted payload store. The existing pending
discovery row already exists before any plan write. No SQL storage is involved.

The plan contains the resolved route/cache identity, requested and attempted
service classes, provider tier, compiled request digest, configuration and
compiler/capability versions, exact price quote, token/cost estimate, every
matched window reservation, quote time and reservation expiry. A typed
projection excludes adapter clients, SDK request objects and provider operation
keys. Prices and costs keep their exact decimal representation; token counts
remain integers, including values larger than JavaScript's exact number range.

Phase factories use `V1RuntimeCapabilities.NewCloudBudgetPlans()` to bind to the
same repository as operation discovery. `PlannedBudgetCall.BudgetPlan(kind)`
provides the durable projection; the helper's `Save` validates the current
configuration identity before writing it. Both Generate and Compact use this
same storage contract. Known-free and unbudgeted paths are deliberately outside
this initial paid-plan contract.

Composition must follow this order, checking every result:

1. Begin the cloud operation, binding the caller input and authenticated scope.
2. Load its saved budget plan before planning again. Only
   `cloudstate.ErrBudgetPlanMissing` permits initial planning. A missing request
   or referenced blob is a storage failure.
3. On an initial miss, compile/select/quote and save the complete plan. A
   competing plan conflicts. Load the committed winner rather than dispatching
   from an uncommitted proposal.
4. Pass the saved `Route` and identical `Reservation` to Redis admission. An
   uncertain acceptance retries that reservation without moving its bucket,
   updating prices, replacing operation/generation IDs or extending expiry.
5. Obtain a fresh single-use Redis claim immediately before paid submission.

Each running request accepts one immutable initial plan. Identical saves do not
append more events or rewrite blobs. Event/index acknowledgement loss is
recoverable across repository instances; reads repair a lagging recovery index
before returning usable admission facts. Storage errors are sanitized at the
runtime helper boundary. Unrelated progress fields survive plan publication.
Private route, pricing and budget details remain encrypted, just like request
inputs and finalization handoffs.

Loading uses the saved identity and price interval at its original quote time,
even after today's configuration or prices change. Recovery must reconstruct
compatible compilation from that identity or stop; this helper does not permit
substituting a current route or SDK request. Admission expiry is not refreshed
on replay; Redis decides whether an unused lease is still eligible.

Provider-pending, outcome-unknown, terminal and finalizing requests cannot resume
initial admission through these functions. Outcome-unknown retry orchestration
must record a separate paid attempt, obtain fresh budget, and retain the old
consumed charge. This PR does not add that state machine, provider submission,
polling, production phase factories or workflow orchestration. Saving or loading
a plan never grants dispatch permission or settles/refunds budget.

Offline tests cover both activities, restarts, racing incompatible proposals,
exact decimals and large integers, malformed/changed bindings, scope isolation,
encrypted storage, finalization fences, blob/event/index uncertainty and index
repair. Runtime tests combine serialized plan recovery after a settings reload
with an uncertain admission and a single successful claim using the reference
Redis accounting contract. They do not establish live cloud/provider behavior.
