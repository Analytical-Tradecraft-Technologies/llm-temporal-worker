# Shared semantic request preparation

`internal/runtime.PrepareGenerateInput` and `PrepareCompactInput` turn an already
authorized checkpoint replay into detached provider-neutral input. They perform
no network or storage work and grant no budget or dispatch permission. The
snapshot-bound cache lookup helper calls them before enabled cache planning;
route, estimate, compile and checkpoint planners can use the same helpers.
Omitting the cache policy still skips its planner and repositories.

The [shared provider planner](cloud-provider-planning.md) uses these prepared
inputs to resolve a route and compile its provider model and attempted class
before admission.

```go
generate, err := runtime.PrepareGenerateInput(ctx, request, replay)
// Handle err before routing, estimating, or compiling.
semantic := generate.Request
exactSettings := generate.Settings
sampleIndex := generate.SampleIndex // Include in the semantic cache identity.
```

A root uses deterministic standard-service/strict-portability defaults and must
set a model. A child inherits its authenticated parent's settings, applies each
sparse `set`/`clear` leaf, and appends only the current request's delta to the
materialized transcript. `state.ApplySettingsPatchV1` performs that conversion
without encoding and decoding the full state. Collection replacement and
clearing keep their existing checkpoint semantics.

The effective `llm.Request` supplies the current operation key and actor, routing
controls, instructions, tools, output, sampling, reasoning and extensions. A
checkpoint handle never becomes a provider-hosted continuation ID. Exact
temperature decimals remain in `Settings.TemperatureDecimal` for checkpoint
publication and identity construction; `Request.Sampling` contains the float64
projection required by the existing provider adapters. Do not reconstruct exact
settings from that projection. Generate's sample index remains separate from
the provider request and must be included in its cache identity.

Replay must match the specified parent, tenant and project and have a valid
stored tool frontier. Root replay must be the empty state. A generation delta
must resolve every pending tool call before another provider call is prepared;
orphan results, reused call IDs and incomplete frontiers fail before cache
planning. Successful model responses containing application tool calls remain
valid; their results must be supplied by the application on a later request.

Compaction uses the existing versioned generic policy defaults, inherits a
validated checkpoint `CompactionPolicy`, and applies the compact envelope's
`target_tokens`/`summary_style` overrides. The effective target must remain below
the effective trigger threshold, as required by `compaction.Policy`. Unknown or
invalid stored policy fields fail as corrupt state, and incompatible caller
overrides fail as invalid arguments. Policy and prompt versions must accompany
source-content identity in the compaction cache key.

`PreparedCompactInput.Selection` separates the summarizable prefix from the
recent verbatim suffix through `compaction.SelectRequestPrefix`: the suffix is
the policy's recent window, shortened oldest turn first when the request that
remains with it would exceed the effective `target_tokens`, so an explicit
`target_tokens` override changes the boundary and the summarized prefix.
Whole tool exchanges stay together and an unresolved
final exchange remains in the suffix. A nil `Request` means there is no safe
prefix: the surrounding runtime must handle that case without reserving budget
or submitting a summarizer. An enabled cache lookup stops before its planner in
that case. Otherwise the request carries the prefix as one delimited human
message of quoted transcript text and uses the repository-owned prompt, plain-text
output and explicit output-token bound; it strips application tools, tool policy,
structured output, reasoning and provider continuation through
`compaction.PrepareRequest`. Application checkpoint settings are retained
separately for publication. Both operation kinds preserve the requested cache sample index (default zero).

The returned settings, request, and compaction selection are detached from the
caller and checkpoint data. There is no shared invocation state, so concurrent
workers and activity retries can prepare independently. Errors expose only
stable typed codes, with no prompt, response, raw policy or underlying error.
Canceled contexts are propagated; no public cancellation operation is added.

Preparation expects the currently authenticated parent view. After an automatic
compaction child has been validated, phase composition must explicitly prepare
an execution view whose parent is that child, retaining the original durable
operation identity for replay. It must re-evaluate any cache plan whose source
or route changed. Preparation does not authorize a caller-selected substitute
checkpoint, settle paid attempts, or implement that compaction workflow.

## Durable preparation and recovery

`V1RuntimeCapabilities.NewCloudRequestPreparation` creates the cloud preparation
boundary with an explicit authorization callback. `Prepare` authorizes first.
For an operation key with no record yet, it then materializes and validates the
parent and input without writing anything, so rejected input (an invalid,
expired or foreign parent, invalid settings, an oversize parent) never becomes a
`running` record that `ListPending` would report forever. Only then does it begin
the discoverable operation and save the versioned parent snapshot before budget
planning or provider effects. If a concurrent worker created the record first
and has not saved its preparation, the validated snapshot is still used, with its
preparation time advanced to the record's creation time, rather than reopening a
parent that may have expired. An operation that already exists skips this
pre-check and replays its saved result or preparation without reopening the
parent. The original typed request remains in the immutable request manifest.
Root generation saves no parent snapshot.

The encrypted preparation has one immutable CAS winner. A retry after a lost
write acknowledgement reads that winner and repairs its discovery entry before
returning; storage errors never become permission to materialize again. Exact
decimal settings and JSON integers retain their original precision. The parent
snapshot is bounded at 4 MiB to leave room for later execution progress.

By default the parent snapshot is stored inline in the request record
(`parent_snapshot`). With `state.requests.parent_snapshot_storage: blob`, the
repository stores it once, as its own immutable encrypted blob in a per-scope
stream (`request-parent/<scope tag>`). The preparation in the request record
then holds only `parent_snapshot_ref`: the snapshot's SHA-256, its length and
the blob's content-addressed key. Later progress writes (budget plan, attempt,
reservation, execution stages, finalization) rewrite a small record instead of
the transcript (#1112). The blob is written and read back before the record
that references it. Every initializer of one preparation derives the same
reference, and a save accepts an identical winner stored in either form, so
concurrent initializers still agree on one immutable value whatever their
setting. Loading verifies the blob against the reference and fails as corrupt,
never as a missing preparation, if the blob is gone or does not match. Attempt
children copy the root's stored form.

Every build that has this setting reads both forms, whatever it is set to, so
the setting only affects new writes and may change on reload. Roll it out in
two steps:

1. Deploy a build that reads references, leaving the setting at its default
   `inline`. Wait until no worker runs an older build.
2. Set `parent_snapshot_storage: blob`.

A later release may make `blob` the default. Rolling back to a build that
cannot read references is unsafe once any worker has written one: that build
fails those in-flight requests as corrupt. Setting the option back to `inline`
stops new references but leaves existing ones in place, so wait for every
request prepared with `blob` to finish before such a rollback.

A Generate may extend a parent only while the parent plus its appended input,
measured as an encoded snapshot, stays within 3 MiB. Beyond that, preparation
refuses the turn with a non-retryable `invalid_argument` before any budget or
provider work, and `PlanGenerationV1` reports `compact_before_generate` whatever
the compaction policy says. The remaining 1 MiB is headroom for the turn's
output, so a published child can still be prepared as a parent and compacted. A
single turn whose output exceeds that headroom can still yield a child too
large to prepare.

`Load` authorizes the current caller before accessing storage and restores the
saved input using its internal request identifier. The identifier is a locator,
not authorization. Recovery does not reopen an expired parent. Completed
operation replay needs neither the preparation nor the parent. A different
configuration digest does not prevent restoring saved input or completing a
saved terminal provider result. An attempt that may already have reached its
provider is polled or recovered, never resubmitted, when
[recovery](cloud-provider-recovery.md) finds its route and endpoint
configuration unchanged. Work that still needs provider routing, new budget
admission or a replacement attempt returns a retryable state-unavailable error
until a worker with the original compatible configuration handles it. This
prevents both silent rerouting and permanent workflow failure during a rollout
or rollback. Publication's parent metadata
must remain available until outstanding requests have finished.

The bounded cloud runtime composes this boundary, and worker startup registers
the public and internal workflows. SQL packages have been removed. Production
CLI authorization and deployment verification remain integration gates; see
[cloud workflow composition](../decisions/0010-durable-v1-runtime-composition.md#cloud-workflow-composition).

## Independent paid attempts

`cloudstate.Repository.BeginRequestAttempt` allocates a separate, discoverable
child request for each paid attempt. The root keeps the caller's immutable
manifest, materialized preparation, and active-child reference. The child copies
that input and owns its own budget plan, provider execution and settlement.
Allocation does not reserve budget or authorize a provider call.

Pass an empty previous ID for initial allocation. Pass the current child ID to
replace an expired, unused quote or an unknown paid outcome after its recovery
interval. Compare-and-swap retirement fences a concurrent start; known pending
or successful work cannot be replaced. Retrying a lost acknowledgement returns
the same child and repairs discovery before returning. Child discovery is
written before the root pointer or any admission effects.

A replacement uses its own request ID as its budget operation ID and derives a
separate provider idempotency key. The original public operation key and sample
index stay unchanged, so a transport retry is not a different requested sample.
Redis's materialization generation remains separate from this attempt identity.
The admission boundary validates the child identity before persisting a new
plan, and provider reconstruction validates the saved binding again.

Retiring an unknown attempt does not release or reuse its charged reservation.
The previous child remains independently discoverable even after the root
returns a later success. Eventual recovery can still record its actual result
and settlement without replacing the active child. A background recovery or
cleanup service is deferred. These primitives do not themselves run retries or
activate the production workflow.
