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

`workflows.Register` registers these internal workflows alongside the public
workflows in the worker. The normal durable CLI supplies the cloud execution
runtime with explicit trusted-Temporal authorization. Tests use Temporal's
workflow test environment and virtual timers; local service gates provide
separate real Temporal/Redis evidence.

## Public workflows and compaction planning

`llm.generate.workflow.v1` accepts a Generate v1 request and returns a Generate v1
response. It calls `llm.generate.plan.v1` first. When required, it runs
`llm.compact.workflow.v1` as a child, uses the returned checkpoint as the Generate
parent, and delegates generation to `llm.request.execute.v1`. Append, settings
patch, operation key, and final-answer cache freshness are preserved. A failed
compaction stops generation. Application tool calls are returned unchanged.

`llm.compact.workflow.v1` also accepts standalone Compact v1 requests. It delegates
to the same internal request workflow and therefore shares cache, budget,
provider polling, and completion behavior. Automatic compaction always enables
its content/policy cache with no age limit; final-answer freshness does not expire
an otherwise compatible summary. A deterministic child operation key derived
from the original request prevents replay from creating independent summaries.

The planning activity authorizes before materializing a parent. Roots and parents
with no safe prefix skip compaction. Otherwise it evaluates inherited policy
token/byte thresholds and the selected route's context-byte limit against the
projected Generate input. Token counting uses the admission estimator's exact
provider tokenizer when configured, or its UTF-8 byte estimate otherwise. This
fallback is an estimate, not a guarantee that every provider context window fits.
The parent's compaction policy governs this summary; a new Generate settings
patch takes effect after compaction and governs subsequent turns.

Planning performs no writes, reservations, claims, or provider requests. It
returns only a boolean; materialized history stays out of its activity result.
The public and internal workflow implementations are registered through
`workflows.Register`. The activity registry registers eight v1 activities,
including planning. See the [implementation boundary](mvp-v1-status.md) for the
complete list and remaining optional capabilities.

### Failed attempts and retry timing

Known provider failures retain a bounded retry classification and earliest retry
instant in durable state. Their Redis settlement and cache-fill release finish
before the failed child leaves pending discovery. A retryable failure keeps the
public request running; explicit budget acquisition waits for the provider delay,
then creates a distinct child and reservation. Retrying Prepare, Submit or Poll
never creates that replacement. Unknown paid work retains its separate pending
record and original claim, as described above.

Permanent provider failures and incomplete compaction results also close the
public request. So does Generate output that cannot extend the request
transcript: two tool calls sharing an ID in one response, a tool result without
a matching call, or other content after tool results have started. The provider
attempt stays succeeded and settled at its actual cost, its cache fill is
released without an entry, and the request fails with `incomplete_response` in
the same step that saved the response.

The saved, sanitized failure replays after restart without
loading an expired parent or contacting a provider. Authorization still runs
before replay. Terminal writes repair their pending indexes after uncertain
acknowledgements, and a stale child cannot close a newer active attempt.
