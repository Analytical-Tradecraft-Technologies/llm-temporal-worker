# Terminated workflows and pending-request recovery

A request workflow is the only thing that advances its durable request. If an
administrator terminates it, or it times out, after its attempt reached a
provider, the request stays in the pending-request index with a charged Redis
reservation that nothing observes. There is no cancellation API and children
use the abandon parent-close policy, so caller cancellation never causes
this; termination and timeouts do.

This runbook finds those requests and finishes their accounting with
[`pending-list`](../reference/cli.md#pending-list) and
[`pending-recover`](../reference/cli.md#pending-recover). Both run with the
worker's configuration and cloud storage access, as a one-off job or from an
operator shell. Neither needs the workers stopped.

## What recovery may do

Recovery runs only the steps a request workflow would run next, through the
same runtime code:

- poll a provider job once, or try documented idempotency recovery once;
- after the 15-minute recovery window, record an unresolved submission as
  `outcome_unknown` and charge it at its full reservation (`finalize_unknown`);
- apply a settlement already saved in the record;
- close a request whose attempt failed permanently;
- publish a settled success, so a caller retrying the same operation key gets
  that result without another provider call.

Recovery never acquires budget, submits a provider request, starts a
replacement attempt, refunds or lowers a charge, or deletes a record. An
unknown cost stays charged in full: lowering it to the exact cost needs
authenticated, attempt-specific evidence, which is
[authorized unknown-cost reconciliation](../architecture/unknown-cost-reconciliation.md)
(#819), not this tool. Index rows, including terminal ones, are never deleted
or rewritten outside those steps, because
[budget recovery after Redis data loss](../reference/redis-budget-leases.md#recovery-after-redis-data-loss)
reads every row to rebuild accounting.

The steps are idempotent Activity steps that live workers run concurrently.
Recovering a request whose workflow is in fact still running is therefore
safe; at worst the workflow observes the result recovery recorded.

## Procedure

1. List the pending requests. The default `--stale-after` is 24 hours; a
   shorter threshold finds a just-terminated workflow sooner.

   ```sh
   llm-temporal-worker pending-list --config /etc/llmtw/config.yaml --stale-after 1h > pending.jsonl
   ```

   Each line describes one request; the last line is a summary. Lines with
   `"orphan":true` are requests no workflow is expected to advance:
   superseded attempts (`"owner":"none"`) and requests not updated within the
   threshold (`"stale":true`). Lines with `"action"` other than `none` can be
   recovered now; `waiting` lines show when in `not_before`.

2. Confirm the workflows are gone. Records do not name their workflow, so
   staleness is only a hint. Check Temporal for terminated or timed-out
   request workflows in the same window, for example:

   ```sh
   temporal workflow list --namespace NAMESPACE \
     --query 'WorkflowType="llm.request.execute.v1" AND (ExecutionStatus="Terminated" OR ExecutionStatus="TimedOut")'
   ```

   A request that is merely waiting for budget or a slow provider can look
   stale too; recovering it is still safe.

3. Dry-run the recovery. Without `--apply` it only reads and prints the action
   each request would take:

   ```sh
   llm-temporal-worker pending-recover --config /etc/llmtw/config.yaml --request-id llmtw_req_…
   llm-temporal-worker pending-recover --config /etc/llmtw/config.yaml --stale --stale-after 1h
   ```

4. Apply it. `--apply` composes the worker's runtime, so it needs the same
   Redis, budget initialization and provider configuration as a worker:

   ```sh
   llm-temporal-worker pending-recover --config /etc/llmtw/config.yaml --stale --stale-after 1h --apply --timeout 10m
   ```

   Each line reports the request after recovery with `applied` and the number
   of `steps` run. A request still `waiting` (for example a provider job whose
   next poll is not due, or a submission inside its 15-minute window) is left
   alone; rerun after its `not_before`. Rerun after a timeout too: completed
   steps are not repeated.

5. List again. What remains should be only dispositions with action `none`
   (see below), each with a reason to stay.

## Dispositions

| Disposition | Action | Meaning and next step |
| --- | --- | --- |
| `initializing` | `none` | `Create` never published the request. Nothing exists or was paid; the row is retained. |
| `unprepared` | `none` | Created but never prepared. Nothing was paid. A caller retrying the operation key resumes it. |
| `terminal` | `none` | The index lags a completed or failed record. Nothing to do. |
| `unpaid` | `none` | No provider execution started. Nothing is charged; a caller retrying the operation key resumes it. |
| `waiting` | `none` | Recovery is not due until `not_before`. Rerun then. |
| `poll_due` | `poll` | A provider job can be polled. |
| `recovery_due` | `poll` or `settle` | A possibly dispatched attempt is past its recovery window: recovery tries idempotency recovery, then charges it in full. |
| `settlement_due` | `poll` or `settle` | A saved settlement has not reached Redis. |
| `closure_due` | `poll` | A settled permanent failure has not closed its request. |
| `completion_due` | `complete` | A settled success has not been published. |
| `retry_required` | `none` | A retryable failure needs a new paid attempt, which only a caller's retry may start. Nothing is held. |
| `settled_unknown` | `none` | Charged at its full reservation. The charge ages out of each budget window. Only #819 may correct it. |
| `unknown_unpaid` | `none` | Unresolved, but it reserved no budget, so nothing is held. |
| `superseded` | `none` | An attempt its root replaced or outlived, or a root from before independent attempts, with nothing recovery may do. |
| `unreadable` | `none` | The record could not be read; see `error` below. The command exits `1`. |

Roles: `root` is the public operation's request; `attempt` is one paid
attempt, with its `root_id`. `poll` and `complete` run on the root, so
recovering an active attempt recovers its root. `settle` runs on a superseded
attempt (or a root from before independent attempts) itself, and only finishes
its accounting.

## Errors

Every line's `error` is a stable code, never a message or payload:

- `corrupt`: the record or its payload failed validation. Preserve it and
  investigate; never delete it to clear the report.
- `unavailable`, `state_unavailable`, `timeout`: storage, Redis or a provider
  was unreachable. Rerun.
- `permission_denied` and other provider codes: the step itself failed, for
  example because the request's tenant and project are no longer in the
  `authorization` allowlist. Restore the policy (or leave the request) and
  rerun.
- `conflict`: a worker advanced the request concurrently. Rerun.

`pending-recover --apply` fails before touching any request when budget
initialization is missing or the runtime cannot be composed; fix that as for a
worker that cannot start.

## What stays

Rows are never removed: settled unknown attempts remain discoverable until
#819 can resolve them, and terminal rows remain for budget recovery. Their
retention is the
[cloud retention and deletion](../architecture/cloud-retention-and-deletion.md)
design (#818).
