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
recent verbatim suffix. Whole tool exchanges stay together and an unresolved
final exchange remains in the suffix. A nil `Request` means there is no safe
prefix: the surrounding runtime must handle that case without reserving budget
or submitting a summarizer. An enabled cache lookup stops before its planner in
that case. Otherwise the request uses the repository-owned prompt, plain-text
output and explicit output-token bound; it strips application tools, tool policy,
structured output, reasoning and provider continuation through
`compaction.PrepareRequest`. Application checkpoint settings are retained
separately for publication. Compaction sample index is always zero.

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
boundary with an explicit authorization callback. `Prepare` authorizes first,
begins the discoverable operation, materializes and validates its parent, and
saves the versioned parent snapshot before budget planning or provider effects.
The original typed request remains in the immutable request manifest. Root
generation saves no parent snapshot.

The encrypted preparation has one immutable CAS winner. A retry after a lost
write acknowledgement reads that winner and repairs its discovery entry before
returning; storage errors never become permission to materialize again. Exact
decimal settings and JSON integers retain their original precision. The parent
snapshot is bounded at 4 MiB to leave room for later execution progress.

`Load` authorizes the current caller before accessing storage and restores the
saved input using its internal request identifier. The identifier is a locator,
not authorization. Recovery does not reopen an expired parent. Completed
operation replay needs neither the preparation nor the parent. A different
configuration digest fails with a configuration error rather than silently
rerouting an existing paid request; routing across configuration changes still
requires the original compatible configuration. Publication's parent metadata
must remain available until outstanding requests have finished.

This boundary is not yet registered as the production execution runtime. The
remaining phase composition, workflow registration, and removal of legacy SQL
packages remain migration work.
