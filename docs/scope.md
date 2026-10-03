# Product Scope and Success Criteria

## Problem

Callers that use Temporal should not need provider-specific request structs,
retry behavior, pricing arithmetic, or continuation state. They need a stable
workflow contract that can target multiple LLM APIs without pretending those
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

- Temporal workflow authors who want typed generation and compaction calls.
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
- Generation and compaction workflows that return final normalized responses.
  No live streaming or token-event API is supported in v1.
- Exactly three request service classes: `economy`, `standard`, and `priority`.
- Explicit ordered service-class fallback, disabled by default.
- Durable continuation and endpoint pinning.
- Configurable deterministic routing, bounded failover, and circuit breaking.
- Versioned price catalogs and provider-reported cost reconciliation.
- Multiple overlapping, conservatively enforced sliding-window budgets.
- Generic cloud KV/blob persistence and shared Redis budgeting/provider state;
  in-memory implementations for development and tests.

### Runtime and delivery

- Public `llm.generate.workflow.v1` and `llm.compact.workflow.v1` workflows,
  internal execution/budget workflows, and bounded v1 activities. See the
  [runtime registration](reference/activity-runtime.md#worker-registration).
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
Temporal workflows decide whether another dispatch is safe. A transport
failure before bytes are written can be retried. After a possibly accepted
submission loses its response, the original attempt remains outcome-unknown
and charged; recovery may retry only with a new paid reservation.

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

The implementation and release evidence are separate. Core cloud workflows,
Redis budgeting, compaction, cache and typed OCaml clients are implemented.
Production CLI caller authorization, deployed IAM/configuration and real
DynamoDB/S3 backup/restore evidence remain outstanding. The
[migration tracker](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/812)
and [release gate](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/821)
record those phase exits; a merged PR or local test is not deployment evidence.

1. **Durable conversation core:** generic cloud key-value/blob storage (AWS
   DynamoDB/S3 initially) for requests, attempts, encrypted payloads and immutable
   checkpoints. No worker SQL database, data import, journal or SQL fallback.
2. **Compaction and budgets:** a separate compaction workflow shares the internal
   execution and budget workflows. Redis owns reservations, claims and settlement;
   JSON worker settings define the budget limits. Unused start authorization
   expires after 15 minutes. Claimed uncertain work remains accounted for.
3. **Exact-response cache:** successful completion determines freshness. Different
   sample indexes separate cache identities; newer failures do not invalidate an
   older eligible success. Compaction reuse binds source content and policy versions.
4. **Control queries:** provider state and inventory live in Redis. Query reads
   require independent authorization and cursor keys; audit hooks emit normal
   structured logs. Missing budget/spend readers remain unsupported. Control
   queries are a separate phase, not a prerequisite for paid workflow composition.
5. **Deferred work:** cloud cleanup, unknown-cost reconciliation and managed secret
   delivery have separate tickets. Retain referenced data until safe cleanup is
   implemented. Cross-provider cache equivalence and FX remain future designs.

[State and storage](architecture/state-and-storage.md), the
[cloud request reference](reference/cloud-request-repository.md), and
[Redis budget leases](reference/redis-budget-leases.md) describe current storage
ownership and recovery behavior. The previous PostgreSQL ADR, physical schema,
SQL runbooks and SQL portions of older plans are historical and superseded.
[Conversation design](architecture/conversation-checkpoints-and-compaction.md)
and [OCaml client documentation](../ocaml/llm_temporal_worker/README.md) describe
semantic/API contracts; current implementations and focused tests establish what
is available. Keep the v1 names and do not expose a cancellation API.
