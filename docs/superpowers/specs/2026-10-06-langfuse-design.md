# Langfuse operation tracing

Date: 2026-10-06
Status: Implemented in PR #1232; live Langfuse validation remains a deployment check.
Repository baseline: master 630f7b085fceff38317276b6d511772cdd8225ee.

## Intent and agreed decisions

Capture LLM requests and responses in Langfuse without changing model execution,
accounting, or the result returned to callers. Use one trace per logical operation.
Compaction is its own operation and trace, including automatic compaction.
Export known usage, actual costs, unit prices and pricing provenance. Identify
the provider endpoint separately from the model vendor: OpenAI direct and an
OpenAI model through OpenRouter must be distinguishable.

Langfuse is globally enabled only with project credentials. Each provider endpoint
can disable capture; endpoints default to enabled except OpenRouter. Export is a
separate optional final Temporal activity with bounded retries. Export failure
does not turn a successful model query into an error or mask an existing error.

Scope is llm-temporal-worker. No infrastructure changes, external project creation,
new database, caller-owned agent loop, or streaming support are included.

## Existing integration boundaries

- `golang/workflows/public.go`: Generate plans compaction, optionally runs Compact
  as a child, then runs the internal request workflow. Compact uses that same
  internal workflow.
- `golang/workflows/internal.go`: ExecuteRequest orchestrates prepare, budget,
  submit, poll and complete. Continue-as-new reloads durable progress.
- Checkpoints form an immutable parent graph with caller deltas and model output.
  Tools execute in callers, and results arrive in subsequent Generate deltas.
- Cloud provider execution records and budget plans carry attempt identity,
  timing, response, route, captured price quote and accounting information.
- Existing operational tracing deliberately excludes content. Langfuse uses a
  dedicated content-bearing sink and does not relax that tracer's allowlist.

## Configuration and enablement

Add an optional top-level `langfuse` configuration with base URL, project public
key and secret key references using the existing secret-resolution conventions.
No configured keys means disabled. Both keys are required when configured;
partial or malformed credential configuration is a configuration error. Do not
make Langfuse network availability a startup or readiness requirement.

Add optional `langfuse.enabled` to each endpoint configuration. Preserve its
three states: omitted selects the provider-family default, false disables, true
enables. OpenRouter detection uses the endpoint's semantic provider identity,
not the model vendor or a substring in the model name. Each attempt is eligible
only if global credentials are present and its actual endpoint permits export.
Route fallbacks apply their respective gates independently.

Capture eligibility is fixed with the attempt's configuration snapshot; an export
also honors a currently disabled destination or endpoint as a kill switch. Enabling
later does not backfill previously disabled calls. Secrets never enter workflow
payloads, checkpoint content, trace metadata or logs.

## Trace identity, sessions and branching

One logical operation has one trace with an operation root span and child
generation observations for distinct provider attempts. Internal Temporal activity
retries, polling and continue-as-new do not create new generations. Use scoped,
stable opaque IDs derived from tenant/project and durable operation/attempt
identities; avoid exporting signed checkpoint handles as identifiers.

Group descendant operations into a session derived from the original conversation
root and tenant/project scope. Resolve ancestry through the worker's checkpoint
repositories; do not depend on Temporal run IDs. Retain the stable lineage/session
anchor through checkpoint snapshots and compaction. Carry bounded parent-operation,
parent-trace and source/result checkpoint identifiers in metadata.

Fork children have distinct operation traces and a shared parent reference. Do
not copy ancestor observations into either branch. Session grouping does not
promise a native graph view in Langfuse; explicit metadata retains graph edges.

Automatic compaction has a distinct trace in the same session. Record the
automatic-compaction flag and source/result checkpoint identities.
The subsequent Generate references the resulting compacted checkpoint and its
compaction trace through the parent edge. The current public contract has no
separate triggering-operation field. Export
the summarizer input and summary, policy version, usage and costs. Prior
observations retain the original content and are never rewritten by compaction.

## Captured request, response and provider identity

Export caller attribution from the existing Temporal request context as filterable
`tenant`, `project`, and `actor` metadata. Map `actor` to Langfuse's user ID.
There is no dedicated client-ID field in the current contract; callers can
identify their application through actor and project. The generic semantic
context supports tags, which the exporter can preserve, but the v1 Temporal
contract currently rejects nonempty tags. These are caller-supplied
labels, not authenticated Temporal transport identities. Apply the same mapping
to Generate, Compact and each eligible provider generation.

Capture the effective semantic input and settings after parent materialization,
compaction and route-specific preparation, plus normalized response items. This
includes instructions, tool definitions, tool arguments, tool-result content,
output settings and exposed reasoning where available. Preserve only the bounded
context actually used by that attempt, not an unlimited ancestral transcript.
Do not claim opaque continuation state is readable reasoning. Exclude credentials,
authorization headers and opaque provider state. Do not fetch referenced media to
embed it in tracing; references remain references.

Capture original attempt start/end timestamps. Export duration is not model
latency. Preserve failed attempts with their known input, safe failure category
and unknown-outcome status; absent responses remain absent.

Each generation has filterable provider, endpoint ID, API family, route ID,
requested logical model and resolved provider model metadata, plus known region
and requested/attempted/actual service classes. The provider is the service used
for the request, such as OpenRouter, not the organization named in its model ID.
Record an underlying serving provider only when the response supplies that fact;
do not infer it from model names.

Tool calls and results remain structured generation input/output in this version.
Matching uses call IDs within checkpoint lineage. The worker does not fabricate
tool execution spans, durations or completion at the time a call is emitted.
Caller tracing integration is a later extension.

## Pricing, usage and cache semantics

Use the attempt's captured pricing/accounting data, not the current catalog at
export time. Emit input/output/cache-read/cache-write/reasoning usage with their
existing semantics. Do not sum overlapping reasoning and output counters.

When available, emit actual total USD cost through Langfuse cost details; component
costs are included only when independently known. Preserve exact decimal amounts
as metadata alongside numeric display values, cost method, catalog version,
pricing provenance and applicable service class. Include known unit rates for
input/output/cache/reasoning per million tokens and per-request charges, with
their units and unknown-component markers. Reservations and estimates are metadata,
never actual cost. Unknown actual cost is omitted from actual-cost fields and
explicitly marked unknown; zero is emitted only when known free. Langfuse-inferred
prices are not authoritative worker accounting.

A worker response-cache hit creates an operation root recording its request,
returned response, cache provenance and exact zero incremental provider cost.
It creates no new paid generation and does not charge or repeat origin usage.
Apply provider eligibility using cached-origin endpoint facts before exporting
cached content. A no-work compaction likewise creates no generation.

Completed-operation replay reuses trace identity and must not routinely re-export
an acknowledged batch. Origin cost/usage can be referenced as metadata without
adding it to current-operation metrics.

## Capture and delivery activity

Separate capture from transport. Persist the bounded eligible observation facts
with existing encrypted request/attempt artifact storage, using versioned records
and existing retention. Full content never becomes an extra Temporal payload.
Telemetry capture failures are reported safely and do not buy another model call
or alter model/accounting success. Missing capture is an export failure, not a
fabricated generation.

Add `llm.ExportLangfuse.v1`, accepting a scoped internal execution reference.
Authorize before loading artifacts. It loads finalized capture, applies enablement,
assembles completed spans and sends OTLP/HTTP to Langfuse. Use the supported
`/api/public/otel/v1/traces` endpoint, project-key Basic authentication and
`x-langfuse-ingestion-version: 4`. Explicitly populate session and trace attributes
on every observation and bypass ordinary tracing sampling for eligible calls.

Call the activity at the shared request workflow's terminal boundary, after result
publication/accounting. Cover completed/cache results and terminal failures with
known request identity. Errors before durable request identity do not require
export. All paths preserve the original result or error after export handling.
Add replay-safe workflow versioning for the new activity command so existing
histories do not become nondeterministic. Workflow decisions do not read live
configuration or credentials; the activity returns skipped when disabled.

Use dedicated export activity options: 10-second start-to-close, 30-second
schedule-to-close, maximum 3 attempts, 1-second initial backoff, coefficient 2.
Bound each transport call within its attempt deadline. Credential rejection and
invalid payload are nonretryable; transient transport/429/5xx may retry. Do not
inherit the model workflow's 24-hour unbounded-attempt policy. Avoid stacked
exporter retries that exceed these limits. Await actual exporter acceptance or
failure, rather than returning after merely queueing spans in process memory.

After exhaustion, log only safe delivery identifiers/status and increment bounded
export-failure metrics. Return the original workflow outcome. This design adds at
most the export schedule-to-close budget to normal completion. It is not durable
background delivery and does not recover traces from administratively terminated
workflows automatically.

Use existing durable artifact facilities for scoped export acknowledgment and
concurrent export ownership. Retries after a known acknowledgment skip delivery.
An acceptance followed by lost acknowledgment is still ambiguous: Langfuse v4
does not guarantee ID-based deduplication. Delivery is best effort with bounded
retries and possible duplicates, not exactly once. Stable IDs aid correlation;
they are not a deduplication guarantee.

## Validation

Meaningful tests must cover:

- Credential gating, endpoint override tri-state and OpenRouter default-off,
  including two endpoints serving the same vendor model through different providers.
- Actual endpoint gates during fallback and cache replay; disabled capture is
  not leaked through enclosing spans or ordinary exporters.
- Fork traces have one session and correct shared parent; compaction retains
  ancestry and records its own input/output/cost without duplicate generations.
- Exact prices, known free versus unknown, captured catalog versus later reload,
  overlapping usage, and response-cache hits with zero incremental spend.
- Resumable polling and activity retries retain original timestamps and attempt
  identities; final failure exports do not mask the original error.
- Export success, disabled skip, nonretryable rejection, transient retry,
  deadline exhaustion, persisted acknowledgment replay and ambiguous acceptance.
- Workflow result preservation on export failure and old-history replay compatibility.
- Content and keys do not enter extra activity arguments, logs, operational traces
  or errors; referenced media is not fetched and payload limits remain enforced.

Use an HTTP test receiver and Temporal workflow tests for normal verification.
Do not send real prompts to an external Langfuse project as part of offline tests.

## External protocol references

- https://langfuse.com/integrations/native/opentelemetry
- https://langfuse.com/docs/observability/data-model
- https://langfuse.com/docs/observability/features/sessions
- https://langfuse.com/faq/all/tracing-data-updates
- https://langfuse.com/integrations/gateways/openrouter

These references were checked during the 2026-10-06 design investigation. Confirm
the OTLP attribute mapping and ingestion behavior when implementing the exporter.
