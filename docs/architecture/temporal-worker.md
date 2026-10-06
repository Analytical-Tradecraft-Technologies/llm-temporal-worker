# Temporal Worker

The worker registers four workflows and eight activities on its configured task
queue. Public callers normally start `llm.generate.workflow.v1` or
`llm.compact.workflow.v1` and receive a final typed response. Application tool
execution and agent loops remain the caller's responsibility.

## Workflow and activity boundary

| Workflow | Role |
| --- | --- |
| `llm.generate.workflow.v1` | Plan compaction, run its child when needed, then generate |
| `llm.compact.workflow.v1` | Standalone or child compaction through shared execution |
| `llm.request.execute.v1` | Internal cache, budget, submit, poll and completion orchestration |
| `llm.budget.wait.v1` | Internal acquisition loop with Temporal timers |

| Activity | Role |
| --- | --- |
| `llm.generate.plan.v1` | Authorize and inspect inherited context for compaction |
| `llm.request.prepare.v1` | Prepare or recover a durable request and eligible cached result |
| `llm.budget.acquire.v1` | Attempt budget acquisition once |
| `llm.generate.v1` | Claim and submit/resume a generation attempt |
| `llm.compact.v1` | Claim and submit/resume compaction, or finish a no-work compaction |
| `llm.poll.v1` | Retrieve status once for the stored provider work |
| `llm.complete.v1` | Publish result/checkpoint/cache and finish accounting |
| `llm.query.v1` | Independently authorized typed control-plane read |

Generate and Compact activities return execution state; they do not necessarily
return a finished model response. Synchronous provider HTTP requests remain in
the submission activity, while resumable providers return pending. Workflow
timers handle provider polling and budget waits. See
[workflow behavior](../reference/internal-workflows.md) and the
[activity boundary](../reference/activity-runtime.md).

The normal durable CLI constructs `NewCloudV1RuntimeBuilder` with explicit
trusted-Temporal scope authorization. It binds cloud, Redis, signing and provider
capabilities to one immutable snapshot. Each activity acquires that snapshot for
its whole step. Reload cannot drain clients still used by an in-flight step.
Missing or inconsistent capabilities fail closed before polling.

Query retains a separate `QueryService` seam and its own authorization/cursor
policy. Registration alone does not enable every optional query reader. The
legacy `V1Runtime` one-shot helpers and reusable engine remain for explicit
embeddings; production generation and compaction use `ExecutionRuntime` and
`GenerationPlanningRuntime` together. See
[ADR 0010](../decisions/0010-durable-v1-runtime-composition.md).

## Payload contract

Public workflow inputs are `llm.GenerateRequestV1` and `llm.CompactRequestV1`;
outputs are their corresponding typed response records. The workflows decode
their input themselves, so an invalid or oversize request fails as a
non-retryable `llm_invalid_argument` that does not echo caller values. Internal activities
exchange bounded execution-state records and scoped internal request references.
Provider identifiers and budget receipts remain in durable runtime storage.
`llm.query.v1` uses its separate tagged request/response union.

Checkpoint-aware calls send an opaque parent handle and the new delta/settings
patch. Materialization loads ancestors inside the worker rather than copying
lineage into workflow payloads. Inputs and outputs still contain caller content,
so history confidentiality requires an independently configured Temporal Payload
Codec; a Data Converter alone is not encryption. Errors and heartbeats carry
bounded, redacted details rather than provider payloads or secrets.

## Required caller options

The library exports validated helpers for callers to build Activity options:

```go
type ActivityPolicy struct {
	StartToClose        time.Duration
	ScheduleToClose     time.Duration
	HeartbeatTimeout    time.Duration
	HeartbeatKeepaliveInterval time.Duration
	InitialRetry        time.Duration
	BackoffCoefficient float64
	MaximumRetry        time.Duration
	MaximumAttempts     int32
}
```

Defaults are documented examples, not universal provider timeouts. Validation
requires:

- schedule-to-close greater than start-to-close;
- heartbeat timeout shorter than start-to-close for long calls;
- a provider-wait keepalive interval that matches
  `temporal.worker.heartbeat_keepalive_interval` and is no more than one third
  of `HeartbeatTimeout` (the default cadence is `1s`);
- operation-record retention covering the emitted schedule-to-close deadline
  (including queue delays and retries), and any longer declared retry horizon;
- maximum attempts bounded;
- no Temporal retry for application errors marked non-retryable;
- the request's provider deadline shorter than Activity start-to-close, leaving
  time to finalize ledger state.

Temporal task-queue priority, when configured by a deployment, is an
orchestration concern. It is never derived from the request's
`economy | standard | priority` provider processing class.

## Retry ownership

Provider SDK retries are disabled. Activity retries recover the same durable
attempt and never silently authorize another paid submission. Completed results
replay, pending provider identifiers resume polling, and unknown paid outcomes
remain accounted for. A retryable failure or unknown submission returns control
to the request workflow, which obtains a new budget reservation for a distinct
attempt before resubmitting. See [Redis budget leases](../reference/redis-budget-leases.md).

There is no service cancellation API. Disconnected workflow contexts and child
abandon policies protect already-paid work from caller cancellation. An
administrator can terminate a workflow, so pending discovery remains independent
of Temporal history; automatic orphan cleanup is deferred.

## Heartbeats

> **Legacy engine.** The keepalive, phase and ledger behaviour in this section
> and in [Cancellation](#cancellation) describe the legacy engine Activity
> (`llm.generate.legacy.v1`), which only memory mode registers. On the durable
> v1 path each internal Activity step (prepare, acquire, generate, poll,
> complete) is short and bounded. It persists its progress in the cloud request
> record rather than in heartbeat details, and recovery is driven by the
> internal workflows; see [internal workflows](../reference/internal-workflows.md)
> and [cloud provider execution](../reference/cloud-provider-execution.md).

The one-shot v1 Activities invoke their runtime exactly once and return a final
normalized or control-plane response. No streaming or token-event API is
supported in v1, including for reusable library callers. Text/JSON deltas,
tool arguments, and opaque provider-state events never enter Temporal history.
Residual decoder code is not a Temporal dispatch path.

Heartbeats contain small, redacted progress only:

```go
type HeartbeatDetails struct {
	OperationID  string
	Phase        string
	RouteIndex   int
	ClassIndex   int
	StartedAt    time.Time
	LastEventAt  time.Time
	OutputItems  int
}
```

Phases are `planning`, `admission`, `pre_write`, `provider_wait`,
`response_received`, `lift`, `finalization`, and, when applicable,
`continuation_write`. Unexpected adapter progress is reduced to the fixed
`other` phase; its source text is never copied into Temporal history. Operation
identifiers containing control characters or exceeding 128 bytes are omitted,
and route/class/output counts are clamped to bounded limits. No text, tool
arguments/results, provider state, secret, raw error, or SDK object is allowed.

The Activity heartbeats:

- throughout the bounded one-shot engine lifecycle, using only redacted phase,
  route, class, and output-count facts;
- at `temporal.worker.heartbeat_keepalive_interval` (default `1s`) while the
  one-shot `Engine.Generate` call is blocked, with a fixed `provider_wait`
  phase and no operation ID, route, class, or output facts;
- before returning a finalized semantic response;

The keepalive is independent of provider SDK timeout settings. A heartbeat
transport failure cancels the child engine context, joins the keepalive before
the Activity returns, and reports a non-retryable ambiguous outcome because
the provider result can no longer be safely proven. A `context.Canceled` result
caused by normal keepalive shutdown after `Generate` returns is ignored; an
external Activity cancellation remains cancellation. The implementation watches
`ctx.Done()` through all provider, blob, and storage calls.

## Cancellation

Cancellation before a possible write releases the reservation and records
`canceled`. Cancellation after possible write is ambiguous unless a provider
response or status lookup proves rejection/acceptance. The Activity attempts a
bounded, shielded final ledger write during shutdown, then returns the
classified error.

Adapters must use the Activity context in official SDK calls. A detached
background context is allowed only for the short, bounded ambiguity/finalization
record and must retain tracing identifiers without prompt data.

## Resumable provider operations

> **Legacy engine.** This section describes the legacy engine's ledger. On the
> durable v1 path a resumable job's provider ID and poll schedule are saved in
> the cloud provider execution record. Polling and `RecoverByIdempotencyKey`
> recovery run from the poll and acquire steps, as described in
> [cloud provider execution](../reference/cloud-provider-execution.md) and
> [cloud provider recovery](../reference/cloud-provider-recovery.md); the
> no-resubmission guarantee is the same.

Adapters that implement the optional `ResumableAdapter` port submit once and
return a provider-owned operation identifier. The worker envelope-encrypts that
identifier and the provider's next-poll guidance in the durable operation
ledger before its first poll. It waits for that initial guidance before
polling, and restores the persisted schedule after a retry. If the
Activity is retried while the operation is `provider_pending`, the worker loads
the encrypted identifier and calls `Poll` on the pinned endpoint; it never
calls `Submit` or `Invoke` again. Polls honor provider delay guidance subject
to a bounded worker limit. A limit, cancellation, or transient poll failure
leaves the operation pending for the next Activity attempt. A provider
not-found or other terminal poll outcome is classified through the existing
ambiguous/definite-failure ledger transitions. Adapters without this optional
port retain the existing one-shot `Invoke` path.

If a worker stops after the durable `dispatching` transition but before it can
store a provider operation ID, a retry pins the recorded endpoint and invokes
only the adapter's documented `RecoverByIdempotencyKey` lookup. A recovered
pending ID enters the same encrypted `provider_pending` path before polling; a
completed result is finalized directly. If the adapter has no lookup contract,
or the lookup fails or returns not-found, the operation remains fail-closed and
is recorded as ambiguous. The retry path never falls back to `Submit` or
`Invoke`, so a missing poll ID cannot cause a duplicate provider submission.

## Error mapping

The Activity converts common errors to Temporal Application Errors:

| Common class | Temporal type | Retry |
| --- | --- | --- |
| invalid request/config/capability | `llm_invalid_argument` | non-retryable |
| auth/permission | `llm_authentication` | non-retryable until configuration changes |
| budget denial within retry horizon | `llm_budget_wait` with safe details | retryable with calculated delay |
| definite transient provider failure | `llm_provider_transient` | retryable |
| ambiguous provider outcome | `llm_ambiguous_dispatch` | non-retryable |
| operation digest conflict | `llm_operation_conflict` | non-retryable |
| canceled | Temporal cancellation | controlled by caller |
| corrupt durable state | `llm_state_corrupt` | non-retryable and alert |

Error details include operation ID, safe code, retry-after, and request ID only.
The two correlation IDs are copied only when they are at most 128 bytes and
contain no control characters; oversized or malformed values are omitted.
Code, phase, and dispatch are closed allow-listed enums, with unknown values
collapsed to the stable internal/finalize/not-dispatched facts. Retry hints are
encoded as a non-negative millisecond integer (including a one-millisecond
minimum for a positive sub-millisecond hint). No provider message, response
body, arbitrary detail map, prompt, credential, or SDK object crosses this
boundary.

## Worker lifecycle

Startup order:

1. parse and validate configuration;
2. resolve secret references;
3. create provider/Redis/blob/telemetry clients and compile the immutable
   snapshot, including bounded checks of every required Redis and bucket
   dependency before the snapshot is published;
4. construct the Temporal client and register Activities on the configured
   task queue;
5. bind the health and metrics listeners;
6. recheck required dependencies and start the Temporal worker only when all
   checks pass;
7. mark readiness true and keep periodically checking dependencies, pausing
   and resuming polling as their combined state changes.

Redis readiness verifies connectivity, the configured persistence and eviction
policy, and the configured preloaded Function or Lua digest without loading or
replacing server-side code. Durable production mode also reads the configured
active budget-generation pointer and canonical manifest, failing closed when
the worker namespace is missing or incomplete; this check never publishes or
rebuilds state. Blob readiness performs a bucket-only check without reading a
tenant object. An initial failed check rejects the unpublished snapshot; a
reload failure leaves the old snapshot in place.

On `SIGTERM`/`SIGINT`, readiness turns false first. The process stops polling,
allows the Temporal worker's configured graceful stop timeout, flushes telemetry
within a bound, and exits nonzero when shutdown integrity fails. Kubernetes
termination grace must exceed worker graceful stop plus telemetry flush.
`SIGHUP` is distinct from termination: it requests a validated configuration
reload and keeps the current snapshot serving if replacement validation or
dependency verification fails.

`cmd/llm-temporal-worker` installs a signal-aware context and delegates the
worker command to `internal/runtime`. Runtime shutdown closes probe listeners,
drains the captured snapshot clients, and closes the Temporal SDK client after
polling has stopped. Runtime errors are bounded, actionable messages and never
include resolved secret bytes or provider payloads.

Liveness proves only that the process event loop is responsive. Readiness
requires a valid snapshot, a polling Temporal worker, and healthy required
Redis and blob dependencies. A later dependency failure keeps liveness
responsive, makes readiness false, and stops polling; the bounded monitor
resumes polling only after every required check recovers. Provider availability
is a route-health concern and is evaluated by request planning rather than by
this probe. The monitor checks cancellation before and after every bounded
dependency probe, so a late success from a client that did not observe
cancellation cannot be treated as a healthy dependency result. The existing
worker drain/stop sequencing still owns the final poller transition.

The monitor keeps running while a paused Temporal poller drains. A pause lets
in-flight Activities finish within the Activity start-to-close timeout instead
of cancelling them after the graceful stop timeout, which only termination
applies. The monitor does not start a replacement poller until that drain
completes, so a transient dependency recovery cannot create overlapping
pollers.

## Local Compose recovery proof

`LLMTW_COMPOSE_LIVE=1 make compose-live-integration` is the opt-in local
operational proof. It starts a uniquely named Compose project with pinned
Postgres-backed Temporal and Redis services, the development-only file blob
store, and the same worker health and metrics endpoints used by Kubernetes. It
checks liveness and readiness through the worker binary, scrapes the
`llmtw_worker_polling` gauge, stops Redis while the worker runs, and requires
readiness to become unavailable, liveness to remain available, and polling to
transition from `1` to `0` before polling returns to `1` after Redis returns.
This Compose lifecycle evidence is Redis-only; it does not prove cloud
backup or restoration of the production accounting authority.

The same gate runs a real Temporal SDK Activity with two worker replicas and a
shared Redis admission store. Its adapter is content-free and in-process: it
records a possible provider write, the first worker stops, and the replacement
must receive either a completed durable replay or the conservative ambiguity
terminal result. The fixture asserts one dispatch and one bounded shared-budget
reservation, never makes a provider-network request, and does not relax the
Docker-private provider-egress denial.

It also holds a successful, content-free one-shot provider call for three
seconds, longer than that Activity's two-second Temporal heartbeat timeout.
The Activity-owned 500 ms `provider_wait` keepalive must preserve the single
completed attempt: the gate requires one provider call and one durable result,
rather than a heartbeat-timeout retry.

## Temporal tests

The Temporal Go SDK Activity test environment covers:

- registration under all three exact versioned names;
- payload round trips and oversized BlobRef behavior;
- heartbeat detail schema and cancellation delivery;
- completed-operation replay after simulated worker loss;
- definite pre-write retry and ambiguous post-write non-retry;
- budget retry-after details;
- non-retryable type names and safe details;
- graceful worker shutdown with an in-flight Activity.

The opt-in Compose gate complements those deterministic tests with a real
Temporal service, shared Redis admission ledger, worker-stop recovery, and
the live readiness and `llmtw_worker_polling` transitions. It also proves a
periodic `provider_wait` keepalive across a long successful one-shot provider
call. It is intentionally excluded from normal offline tests and pull-request
CI.

Workflow-level tests live in an example package and demonstrate caller-owned
tool execution, but the worker does not ship a general agent Workflow.
