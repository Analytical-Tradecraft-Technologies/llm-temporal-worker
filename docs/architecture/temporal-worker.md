# Temporal Worker

> This chapter describes current worker behavior. Target phase status and
> authority are centralized in [scope](../scope.md#staged-delivery-and-document-authority).
> The current v1 composition registers **llm.generate.v1**,
> **llm.compact.v1**, and **llm.query.v1** as one-shot Activities. The staged
> design continues that boundary while adding the durable implementations
> behind it; caller Workflows still execute tools and own agent loops. See
> [conversation checkpoints and compaction](conversation-checkpoints-and-compaction.md)
> and [the typed OCaml client](ocaml-conversation-and-query-client.md).

## Activity boundary

The production v1 registration installs three exact versioned Activities on the
configured task queue:

```text
llm.generate.v1
llm.compact.v1
llm.query.v1
```

`llm.generate.v1` performs exactly one inference turn and may return model
output or tool calls. `llm.compact.v1` returns one final compacted checkpoint
response. `llm.query.v1` returns one final control-plane response. None of these
Activities executes tools, owns an agent loop, or exposes token events; agent
orchestration belongs in caller Workflows so every tool result and decision is
durable and visible. The closed request/response records and registration
behavior are specified in the [v1 Activity runtime boundary](../reference/activity-runtime.md).

The reusable engine remains usable without Temporal. The v1 Activity adapter
receives its durable implementation through a one-shot runtime seam:

```go
type V1Runtime interface {
	GenerateV1(context.Context, llm.GenerateRequestV1) (llm.GenerateResponseV1, error)
	CompactV1(context.Context, llm.CompactRequestV1) (llm.CompactResponseV1, error)
	QueryV1(context.Context, llm.QueryRequestV1) (llm.QueryResponseV1, error)
}

type QueryService interface {
	Execute(context.Context, llm.QueryRequestV1) (llm.QueryResponseV1, error)
}
```

`QueryService` is an independent query-only seam. When it is supplied, it is
used for `llm.query.v1`; it cannot dispatch Generate or Compact work. The
Activity layer validates the closed records, applies payload limits, converts
errors to bounded Temporal details, and never constructs clients, reads
environment variables, or mutates global state. `Activities.Register` uses this
three-name v1 registration whenever a v1 runtime or query service is present.
Only an Activity assembled without either seam uses the legacy direct
`llm.generate.v1` engine helper, which is retained for pre-release callers and
tests and does not register Compact or Query.

The process-level composition lives in `internal/runtime`. It validates and
publishes one non-secret configuration snapshot, creates the Temporal client
with TLS roots loaded from bounded regular files, starts separate health and
metrics listeners, and injects the Activity's engine through a snapshot lease.
Provider/state construction is an explicit `EngineFactory` seam. The CLI uses
`ProductionEngineFactory` to compose verified catalogs, provider adapters,
Redis state, and blob-backed results; tests and custom deployments can inject a
different factory, and unsupported configured dependencies still fail closed
before a worker starts. The durable implementation for the `V1Runtime` seam is
an additional required composition input for every environment except the
checked-in `development` fixture: until it is supplied, `Runtime.Start`
returns `ErrV1RuntimeUnavailable` before opening listeners or allowing
Temporal polling. This keeps production readiness truthful while the v1
Activity names remain registered for contract inspection and fail-closed
behavior. The local Compose worker mounts `environment: development`
specifically as a parser/configuration/readiness fixture. When that
development composition has no durable v1 implementation, it omits the v1
Activity registrations entirely; it does not dispatch inference or advertise
a partially configured v1 worker. Any other environment value, including
`production`, remains fail-closed.

The explicit composition guard is recorded in
[ADR 0010](../decisions/0010-durable-v1-runtime-composition.md). In
particular, the production factory's reusable `llm.Engine` is not a v1
runtime: it cannot be losslessly adapted to Compact or Query, nor can it
perform checkpoint materialization and the required Redis/PostgreSQL phase
ordering. A durable v1 implementation must therefore be supplied separately
or through an adapter that owns the complete phase order.

## Payload contract

Each v1 Activity has a closed request and response record in the `llm` package:
`llm.GenerateRequestV1`/`llm.GenerateResponseV1`,
`llm.CompactRequestV1`/`llm.CompactResponseV1`, and
`llm.QueryRequestV1`/`llm.QueryResponseV1`. The legacy `GenerateRequest` and
`GenerateResponse` envelope remains only for the direct engine helper.

Provider SDK types, clients, secrets, raw prompts in errors, and unbounded binary
content never enter Temporal payloads. Inline payload limits default well below
Temporal's service limits. Larger inputs and completed responses use immutable
`BlobRef` values containing store, locator, digest, byte length, media type, and
expiry. The worker verifies the digest after reading.

Checkpoint-aware Generate and Compact requests carry only an opaque parent
handle. The Activity adapter materializes ancestor deltas inside the worker,
then passes the replayed state to the runtime; ancestor items are never copied
into the Temporal request or response. This keeps Activity I/O bounded and
independent of whether a lineage contains one turn or thousands. Cancellation
and deadline errors from state loading retain their Temporal cancellation or
retry semantics instead of being reported as an unrelated generic state
failure.

Installations that require history confidentiality configure a Temporal Payload
Codec outside the worker. The worker's contract remains codec-agnostic and does
not claim that a Data Converter alone encrypts data.

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
- retry horizon no longer than operation-record retention;
- maximum attempts bounded;
- no Temporal retry for application errors marked non-retryable;
- the request's provider deadline shorter than Activity start-to-close, leaving
  time to finalize ledger state.

Temporal task-queue priority, when configured by a deployment, is an
orchestration concern. It is never derived from the request's
`economy | standard | priority` provider processing class.

## Retry ownership

Provider SDK retries are set to zero. One Activity attempt may traverse safe
route candidates under the engine's bounded plan, but it never resubmits after
an ambiguous write. Temporal retries an Activity only after the common error
classifier marks the outcome safe.

On Activity retry:

1. the same `operation_key` and normalized request digest reach `Begin`;
2. a completed operation returns its stored result;
3. a reserved operation with a proven expired pre-dispatch lease can resume;
4. a dispatching/ambiguous operation returns a non-retryable ambiguity error;
5. a conflicting digest returns a non-retryable conflict.

This makes worker crashes recoverable without assuming an external provider has
exactly-once semantics.

## Heartbeats

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

The monitor keeps running while a paused Temporal poller completes its graceful
drain. It does not start a replacement poller until that drain completes, so a
transient dependency recovery cannot create overlapping pollers.

## Local Compose recovery proof

`LLMTW_COMPOSE_LIVE=1 make compose-live-integration` is the opt-in local
operational proof. It starts a uniquely named Compose project with pinned
Postgres-backed Temporal and Redis services, the development-only file blob
store, and the same worker health and metrics endpoints used by Kubernetes. It
checks liveness and readiness through the worker binary, scrapes the
`llmtw_worker_polling` gauge, stops Redis while the worker runs, and requires
readiness to become unavailable, liveness to remain available, and polling to
transition from `1` to `0` before polling returns to `1` after Redis returns.
This current Compose lifecycle evidence is Redis-only; it does not stop and
restore worker PostgreSQL.

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
