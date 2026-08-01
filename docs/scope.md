# Product Scope and Success Criteria

## Problem

Callers that use Temporal should not need provider-specific request structs,
retry behavior, pricing arithmetic, or continuation state. They need a stable
Activity contract that can target multiple LLM APIs without pretending those
APIs are identical.

The worker solves that problem by acting as a small compiler and admission
controller:

1. validate a versioned semantic request;
2. resolve capabilities, service class, route, and price snapshot;
3. reserve the maximum plausible cost across every applicable budget;
4. lower the semantic request to one provider wire format;
5. perform one externally observable dispatch attempt;
6. normalize output, usage, actual service class, cost, and diagnostics;
7. persist an immutable continuation and completed operation result;
8. finalize or conservatively retain the budget reservation.

## Intended users

- Temporal workflow authors who want one inference Activity contract.
- Go services that want the same router, adapters, pricing, and budget layers
  without running a Temporal worker.
- Platform teams that need centralized endpoint credentials and cost policy.
- Test authors who need deterministic conversion fixtures without live model
  calls.

## In scope for v1

### API generations

- OpenAI Responses.
- OpenAI-compatible Chat Completions.
- Anthropic Messages.

### Endpoint profiles

- OpenAI direct.
- Azure OpenAI for Responses and Chat Completions where the deployment supports
  the selected operation.
- OpenRouter Chat Completions.
- Exa OpenAI-compatible Chat Completions.
- A configurable OpenAI-compatible endpoint profile with capabilities declared
  by configuration rather than guessed.
- Anthropic direct.
- Claude Platform on AWS through a closed `anthropic_aws_messages` endpoint
  family using the Anthropic AWS gateway client and the AWS default credential
  chain; it is distinct from Bedrock.
- Amazon Bedrock Anthropic through the current Messages-compatible Mantle path,
  with the legacy Bedrock runtime path isolated behind a separate endpoint
  profile when required by a region or model.
- Amazon Bedrock Runtime Converse through a dedicated `bedrock_converse`
  endpoint family using the AWS default credential chain and one-shot Temporal
  Activity semantics; live token streaming is out of scope.

### Semantic features

- Developer/system instructions and ordered human/model messages.
- Text and image input by URL, inline bytes, or external blob reference.
- First-class tool definitions, tool calls, and tool results.
- JSON Schema Draft 2020-12 structured output.
- Sampling, stop sequences, output limits, and reasoning intent where supported.
- Provider-state parts that remain opaque and byte-for-byte stable.
- Strict and best-effort portability with machine-readable diagnostics.
- One-shot `Generate` and a final normalized response only. No live streaming
  or token-event API is supported in v1.
- Exactly three request service classes: `economy`, `standard`, and `priority`.
- Explicit ordered service-class fallback, disabled by default.
- Durable continuation and endpoint pinning.
- Configurable deterministic routing, bounded failover, and circuit breaking.
- Versioned price catalogs and provider-reported cost reconciliation.
- Multiple overlapping, conservatively enforced sliding-window budgets.
- In-memory and Redis state implementations.

### Runtime and delivery

- A named Temporal Activity, `llm.generate.v1`.
- Docker and Kubernetes deployment artifacts.
- Structured logging, Prometheus metrics, and OpenTelemetry tracing.
- Separate pull-request and master GitHub Actions workflows.
- A master build scheduled daily at 05:00 `Australia/Sydney` as well as on push.

## Explicitly out of scope for v1

- Owning a Temporal Workflow definition or an agent loop.
- Executing tool calls returned by a model.
- Provider-hosted web search, file search, computer use, code interpreter, MCP,
  or other hosted tools.
- Embeddings, reranking, moderation, fine-tuning, batch APIs, file stores,
  image generation, speech, video, and realtime sessions.
- A public HTTP inference gateway.
- Automatic model substitution based on model-name similarity.
- Automatic service-class escalation or downgrade.
- Cross-region active-active Redis budget accounting.
- Exactly-once claims for external LLM APIs that do not expose a supported
  idempotency contract.
- Persisting secrets, raw credentials, or bearer tokens in Temporal payloads.
- Live streaming, token-event delivery, and interactive response transports.

## Behavioral invariants

### No hidden semantic loss

Every field is either represented natively, deliberately emulated, rejected,
or dropped with a diagnostic in best-effort mode. An adapter may not silently
flatten tool calls, discard opaque reasoning state, or ignore an unsupported
request parameter.

### No hidden cost escalation

An omitted `service_class` becomes `standard`. A requested class is attempted
exactly. A different class may be attempted only when it appears in the
request's ordered `service_class_fallbacks`. The result reports requested,
attempted, and provider-observed classes separately.

### Admission before dispatch

When any budget applies, the worker must have a price and a bounded maximum
output. It reserves a conservative upper bound before a provider request can
leave the process. Missing or stale price data fails closed.

### One retry authority

Provider SDK automatic retries are set to zero. The operation ledger and
Temporal retry policy decide whether another dispatch is safe. A transport
failure before bytes are written can be retried; a timeout after dispatch is
ambiguous and is not retried automatically.

### Continuation integrity

Continuation records are immutable. Provider-native state is retained without
interpretation and is reused only by the adapter and endpoint family that
created it. Switching routes requires a portable canonical transcript; strict
mode rejects a switch that would lose required provider state.

### Bounded history

Activity inputs, outputs, heartbeat details, and errors stay well below
Temporal payload limits. Large binary parts and oversized normalized histories
use an external `BlobRef`; secrets never use a blob reference passed through
workflow history.

## Quality targets

These are initial service objectives for the worker itself, excluding provider
latency:

| Measure | Target |
| --- | --- |
| Admission and compilation p99 | Under 25 ms with memory state; under 75 ms with same-region Redis |
| Worker-caused successful-call error rate | Below 0.1% |
| Budget overspend from known usage | Zero under the documented clock and durability assumptions |
| Configuration reload | Atomic snapshot swap; no partially applied configuration |
| Graceful shutdown | No new Activities accepted; in-flight work given its configured stop timeout |
| Adapter conversion coverage | Every capability cell has a positive or negative fixture |

The implementation must benchmark these targets rather than treating them as
guaranteed properties of the design.

## Staged delivery and document authority

The current v1 in-scope/out-of-scope lists describe shipped behavior. The new
design is delivered in independently releasable phases; it is not one enlarged
“initial release” gate:

Accordingly, the current v1 readiness gate covers the shared Redis backend and
the configured blob store used for oversized payloads and results. Worker-owned
PostgreSQL readiness, the durable operation/budget journal, and PostgreSQL
rebuild/restore proof belong to the staged Phase A/B targets below; they are not
current v1 release prerequisites. The traceability catalog must not record those
target requirements as current v1 evidence until their implementation and
protected verification runs exist.

1. **Phase A — durable conversation core:** worker-owned PostgreSQL namespace,
   encrypted inline/blob payloads, operation/attempt ledger, immutable
   checkpoints and forks, exact USD accounting, restart-safe provider polling,
   and the natural two-layer OCaml Generate client. Existing Redis throttling
   remains operational while these foundations land.
2. **Phase B — compaction and budget materialization:** explicit/automatic
   compaction, provider prompt-cache affinity, PostgreSQL budget journal, and
   the self-validating Redis materialization/coordination optimization using
   conservative nano-USD admission. This phase includes the loss/rebuild
   runbook and restore proof.
3. **Phase C — opt-in exact-response cache:** route-isolated cache identity,
   variants, retention/usage accounting, and concurrent fill collapse for a
   named staging workflow or incident-reproduction caller. It is not a
   prerequisite for checkpoints or compaction.
4. **Phase D — typed control queries:** the five typed query families, bounded
   dedicated query audit rows, provider status/inventory projections, and the
   OCaml GADT. Queries do not share the paid LLM operation/blob state machine.
5. **Future ADRs only:** cross-provider cache equivalence and FX. Neither has
   schema, runtime tasks, or a release gate until a concrete verifiable provider
   pair or non-USD price sheet exists.

Each phase runs its own focused acceptance gates and can ship before the next.
The [implementation plan](superpowers/plans/2026-07-18-forkable-conversation-state.md)
owns phase ordering and file lists. The
[PostgreSQL/control-plane design](architecture/postgresql-state-cache-and-control-plane.md)
is the single normative home for storage ownership, budget-read conditions,
workload envelope, and DDL. The
[conversation design](architecture/conversation-checkpoints-and-compaction.md)
owns Generate/Compact/cache semantics, and the
[OCaml design](architecture/ocaml-conversation-and-query-client.md) owns the
client API. Other chapters summarize current behavior or link to these owners;
they must not restate competing normative rules.

These design documents constrain implementation but are revisable. A latent
specification defect or measured implementation evidence may be addressed by a
short superseding ADR amendment that states the changed invariant, alternatives,
schema/API impact, test updates, and why existing documents are being amended.
The implementer must not silently diverge, but also must not knowingly implement
a defect merely because the original prose said “do not redesign.”
