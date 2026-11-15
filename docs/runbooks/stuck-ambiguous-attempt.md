# Stuck ambiguous attempt

This is the interim procedure for provider attempts whose outcome is unknown.
It covers what the worker already does on its own, how to tell a normal wait
from a stuck attempt, the tools that exist today, and what is not yet
possible. The long-term design is
[authorized unknown-cost reconciliation](../architecture/unknown-cost-reconciliation.md)
([#819](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/819)),
none of which is implemented yet. Start from
[on-call escalation and severity](on-call.md) for the rules that apply to
every incident.

## How an attempt becomes ambiguous

A provider attempt moves to the `outcome_unknown` stage when its submission may
have reached the provider and the worker can neither poll it nor recover it by
idempotency key, for example when the connection dropped after the request was
written. The worker then:

1. Keeps the attempt, its pending index row and its claimed reservation. The
   attempt is never submitted again.
2. Waits until 15 minutes after the attempt started (`durable.BudgetStartLease`).
3. On the request's next acquisition step, charges the old attempt at its full
   reservation (a `finalize_unknown` settlement with reason
   `provider_cost_unknown`) and starts a new, separately budgeted attempt. A
   request that has used all its attempts fails instead.

A request that pauses for about 15 minutes after an ambiguous attempt is
therefore working as designed. The charge is conservative and stays: unknown
is never treated as zero.

## Ambiguous dispatch above zero

- **Signal:** `llmtw_ambiguous_total{endpoint}` increases, together with
  `llmtw_operation_state_total{state="ambiguous"}` and a
  `provider attempt failed` warning with `outcome=outcome_unknown` and the
  attempt's `route_id`, `endpoint`, `model`, `error_code`, `phase`,
  `dispatch` and, when known, the upstream HTTP `status` or a `cause` such as
  `deadline_exceeded`.
- **Readiness reason:** none. Readiness stays `200`.
- **Default severity:** SEV3 for an isolated attempt; SEV2 when one endpoint
  produces a sustained rate (the provider or the network path to it is
  failing mid-request); SEV1 when attempts stay unsettled because budget
  authority is unavailable.
- **Safe action:**
  1. Group the `provider attempt failed` warnings by `endpoint` and `cause`. A
     burst on one endpoint usually means provider timeouts or a broken egress
     path; follow [provider failures](provider-and-routing.md#provider-failures).
  2. Confirm the attempts settle. Within roughly 15 minutes plus one
     acquisition retry, each request either starts a new attempt or fails
     with its attempt limit. In Temporal, the request workflow
     `llm.request.execute.v1` shows the `outcome_unknown` state followed by a
     budget child workflow.
  3. If settlement does not happen, use the next section to find out why.
- **Forbidden:** resubmitting the request outside the worker, deleting the
  attempt or its pending row, editing the Redis reservation, or lowering the
  charge. Each of these can buy the same work twice or hide a charge.

## Finding a stuck attempt

An attempt is stuck when nothing will run its next step. Check, in order:

1. **Is the owning workflow still running?** In Temporal, look for the
   request's `llm.request.execute.v1` workflow and its budget child. If an
   administrator terminated it, or it timed out, nothing will settle the
   attempt: its pending row and its claimed reservation remain until a
   recovery tool runs them. A tool that lists and recovers such attempts is
   tracked in
   [#1544](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/1544)
   and is not in this release. Until it lands, record the workflow and run
   IDs, leave the records in place, and escalate to the service owner. Do not
   start a second workflow with a new operation key for the same work.
2. **Is settlement failing?** The settling step returns `state_unavailable`
   (Temporal type `llm_provider_transient`, counted in
   `llmtw_activity_failure_total{origin="worker"}`) while Redis cannot accept
   the settlement. Fix Redis first: see
   [Redis readiness failures](state-stores.md#redis-readiness-failures) and,
   if the authority marker is missing or fenced,
   [Redis budget authority and recovery](redis-budget-generation-recovery.md).
   Settlement retries on its own once Redis recovers.
3. **Is the request a historical compacted Generate that fails as corrupt?**
   Operations prepared before the original public binding was recorded fail
   closed. If the request's history is retained, run
   [`generation-recover`](../reference/cli.md#generation-recover) as a dry run
   with the original workflow ID and run ID, then repeat with `--apply` when it
   reports `recovery_required`:

   ```sh
   llm-temporal-worker generation-recover --config /etc/llmtw/config.yaml \
     --workflow-id ORIGINAL_WORKFLOW_ID --run-id ORIGINAL_RUN_ID
   ```

   It restores only the planning binding. It never acquires budget, submits
   provider work or settles an attempt. See
   [durable preparation and recovery](../reference/cloud-request-preparation.md#durable-preparation-and-recovery).

## What the status reader can and cannot show

There is no supported way today to read one reservation's state from Redis.
The `budget_status` control query returns `unsupported_capability` in the
production CLI, and the only built-in Redis budget status reader reads
generation-scoped `budget:v2:` window hashes that nothing in the worker writes,
so it does not reflect admission spend
([#1500](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/1500),
[#817](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/817)).
Do not seed `budget:v2:` hashes to make it answer: the figures would not match
the authoritative ledger. See
[production CLI composition](../reference/persisted-query-service.md#production-cli-composition).

Use these instead:

- Temporal history of the request workflow and its Activity results, which
  show each execution state (`outcome_unknown`, `budget_wait`, `acquired`,
  `failed`).
- The `provider attempt failed` warning for the attempt's route, endpoint and
  cause.
- `llmtw_cost_status_total{status="unknown"}` and
  `llmtw_budget_admission_total`, which count settlements and admissions in
  aggregate but never per request.

## What is not yet possible

The [#819 phased plan](../architecture/unknown-cost-reconciliation.md#phased-implementation-plan)
has not started:

- Phase 1 (an open index of unknown-cost attempts and a read-only
  `unknown-cost-reconcile` report) does not exist, so there is no list of
  conservatively charged attempts.
- Phases 2 and 3 (durable resolution receipts, operator attestation and
  provider evidence adapters) do not exist, so a conservative charge cannot be
  lowered or raised to the exact cost.
- Phase 4 (scheduled passes) does not exist.

Until then, every unknown cost stays charged at its full reservation. If a
provider later confirms the exact cost of an attempt, record that evidence in
the incident, outside the worker, and keep the charge.

## Remaining implementation

- Listing and recovering pending attempts of terminated workflows:
  [#1544](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/1544).
- Exact-cost reconciliation:
  [#819](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/819).
- A budget status reader over the durable ledger:
  [#817](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/817).
