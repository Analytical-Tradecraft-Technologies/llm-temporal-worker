# Internal execution workflows

`llm.request.execute.v1` accepts exactly one Generate or Compact v1 request. It
calls the bounded prepare activity, then follows the returned state:

| State | Next action |
| --- | --- |
| `completed` | Return the typed response, including application tool calls. |
| `budget_required`, `outcome_unknown` | Start the budget child workflow. |
| `budget_wait`, `cache_wait` | Wait with a Temporal timer, then start the budget child. |
| `acquired` | Submit/resume through `llm.generate.v1` or `llm.compact.v1`. |
| `pending` | Wait with a timer, then call `llm.poll.v1` once. |
| `provider_completed` | Call `llm.complete.v1` to publish the checkpoint and result. |
| `failed` | Return a stable failure, or acquire a new attempt if marked retryable. |

`llm.budget.wait.v1` repeatedly calls `llm.budget.acquire.v1` once and waits with a
Temporal timer when capacity or a cache fill is unavailable. It returns as soon
as acquisition succeeds or saved work supplies a different result. It never
submits a model request. Redis enforces the 15-minute start deadline; if the
workflow resumes after that deadline, the runtime renews the unused attempt.

Activity retries replay the same durable step. An uncertain paid attempt follows
the explicit acquisition path, which retains the old charge and pending record
and obtains a new reservation before another submission. Provider identifiers
and budget receipts remain inside the activity runtime.

Both workflows check response kind and internal request identity. Final responses
must also match the public operation key. Each run continues as new after 128
state transitions, or when Temporal recommends it, with no unfinished child or
activity. The new request run prepares the original request again and reloads its
saved cloud progress. Budget continuation carries only the scoped reference.

There is no cancellation API. Work runs in a disconnected workflow context and
children use the abandon parent-close policy so cancellation of a caller does
not cancel paid work. An administrator can still terminate executions; the
pending-request index remains the recovery source in that case. Automated orphan
cleanup is a separate feature.

Activities have a five-minute start-to-close limit, 30-second heartbeat timeout,
and a 24-hour schedule-to-close retry horizon with exponential backoff capped at
one minute. These are failure/recovery limits, not provider or budget wait loops.
The provider timeout must leave time inside that activity window for accounting.

The workflows are available through `workflows.RegisterInternal`; this PR does
not yet register them in production. Public generation/compaction workflows,
production cloud runtime composition, and real dependency E2E verification follow.
The tests use Temporal's workflow test environment and virtual timers.
