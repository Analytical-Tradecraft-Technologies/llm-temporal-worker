# Verified Upstream Contracts

This design was checked against primary upstream documentation and source on
2026-07-14. Implementation phases must re-check these contracts before pinning
dependencies because SDKs, model capabilities, pricing, and service tiers
change independently.

## OpenAI

- [Responses API](https://platform.openai.com/docs/api-reference/responses)
  provides typed input/output, tools, streaming, usage, and response state.
- [Conversation state](https://platform.openai.com/docs/guides/conversation-state)
  documents response/conversation continuation and storage behavior.
- [Priority processing](https://platform.openai.com/docs/guides/priority-processing)
  uses a priority service tier and reports the actual tier, which can differ.
- [Flex processing](https://platform.openai.com/docs/guides/flex-processing)
  is lower-cost/slower and can return resource-unavailable failures.
- [OpenAI Go SDK](https://github.com/openai/openai-go) supports Responses and
  configurable retry count; this design sets retries to zero.

Design consequence: economy/standard/priority are semantic values mapped per
capability profile to flex/default/priority; actual response tier is retained.

### Responses streaming implementation boundary

As of 2026-07-14, `llm/provider/openairesponses` has an SSE decoder only.
The exact Task 6 blocker is: no `StreamingAdapter`/typed stream port and no
official OpenAI SDK stream dispatch exists in
`llm/provider/openairesponses`. The direct and Azure Responses fixture
profiles therefore record `streaming: unsupported`; their decoder fixtures
prove transport-fragment reconstruction only, not an end-to-end client stream.
Task 6 must add the typed stream contract, direct and Azure SDK dispatch, and
deterministic transport coverage before either profile can claim enforced
streaming coverage.

### Current v1 scope (superseding the Task 6 record)

The 2026-07-14 Task 6 record above is retained as historical upstream research.
V1 now supports only one-shot `Generate` and a completed normalized response;
it has no supported streaming or token-event API. Retained decoders are legacy
parser-regression coverage, not a v1 client dispatch path, and must not be
wired into the Temporal runtime.

Before lowering a request, an adapter must reject every required feature whose
capability state is `unsupported` or `unknown`, regardless of whether the
caller selected strict or best-effort mode. Only `native` and `emulated`
capabilities may reach SDK parameter lowering; rejected requests remain in the
compile phase and are never dispatched. This guard is part of the provider
contract and does not add a streaming or token-event dispatch path.

### Responses fixture boundary

The direct `openai-responses` and `azure-responses` profiles each have an
enforced offline fixture matrix selected by their declared capability facts.
Both exercise Responses lowering/lifting, opaque response state,
continuation-compatibility, usage, service classes, classified errors,
strict-loss handling, and redaction in their own fixture roots. Their
full and fragmented decoder fixture corpora prove SSE reconstruction only, not
an end-to-end client dispatch API. Azure continuation evidence validates the
adapter's endpoint-pinned conversion path offline; it is not evidence of Azure
deployment capability.

## Azure OpenAI

- [Azure priority processing](https://learn.microsoft.com/en-us/azure/ai-foundry/openai/how-to/priority-processing)
  documents deployment/model availability, request tier, and returned tier.
- [Azure OpenAI REST reference](https://learn.microsoft.com/en-us/azure/ai-services/openai/reference)
  is authoritative for deployment API versions and supported request fields.

Design consequence: each Azure deployment has its own capability profile.
OpenAI compatibility is not evidence that every OpenAI field/tier works.
The independently owned Azure Responses fixtures validate the configured path
and auth construction offline, but they do not establish deployment-specific
model, economy-tier, continuation, or token-event availability.

## Anthropic and AWS

- [Anthropic service tiers](https://docs.anthropic.com/en/api/service-tiers)
  distinguish standard-only and automatic priority capacity and report usage
  tier.
- [Anthropic Messages](https://docs.anthropic.com/en/api/messages) defines
  system content, message/tool blocks, thinking blocks, streaming, and usage.
- [Anthropic Go SDK](https://github.com/anthropics/anthropic-sdk-go) contains
  direct Messages support, configurable retry count, and AWS/Bedrock client
  support; retries are disabled here.
- [Amazon Bedrock Anthropic Claude Messages API](https://docs.aws.amazon.com/bedrock/latest/userguide/model-parameters-anthropic-claude-messages.html)
  defines the native Messages request/response shape and the base invocation
  and response-stream operations.
- [Anthropic Go SDK AWS gateway source](https://github.com/anthropics/anthropic-sdk-go/blob/v1.57.0/aws/aws.go)
  documents that API-key inputs and `ANTHROPIC_AWS_API_KEY` take precedence
  over SigV4, while unset credentials use the default AWS credential chain.
  It also gives configured values precedence over environment defaults for the
  region, workspace ID, and base URL.
- [Amazon Bedrock service tiers](https://docs.aws.amazon.com/bedrock/latest/userguide/service-tiers-inference.html)
  define flex, default, priority, and reserved behavior and availability.
- [Amazon Bedrock Converse API](https://docs.aws.amazon.com/bedrock/latest/APIReference/API_runtime_Converse.html)
  defines the one-shot model invocation request/response union used by the
  `bedrock_converse` adapter, including tool-use blocks, usage, stop reasons,
  and service-tier reporting.

Design consequence: direct synchronous Anthropic has no assumed economy mapping.
`anthropic_aws_messages` supplies the AWS gateway base URL, region, and
workspace ID explicitly and permits only `aws_default_chain`; its constructor
rejects API-key/static modes and `ANTHROPIC_AWS_API_KEY` so the SDK cannot
silently choose a different auth mode. AWS gateway and Bedrock have separate
endpoint families, catalog state, and pinned continuations. AWS profiles
declare exact offering/model mappings; reserved capacity is not a fourth public
request class.

### Bedrock Anthropic fixture boundary

As of 2026-07-14, the `bedrock-anthropic` profile has an enforced offline
fixture matrix for native Messages lowering/lifting, service-tier facts,
opaque thinking state, tool argument fragments, usage with unreported cost,
classified errors, and source-date/redaction checks. Opaque state remains
endpoint-pinned: direct Anthropic and AWS-gateway continuation state cannot be
replayed through the Bedrock adapter without an explicit portable transcript.
The captured SSE fixtures prove decoder and assembler semantics under
fragmentation; they are not evidence of an end-to-end client dispatch
implementation. Runtime route compilation also rejects a Bedrock Messages
route when its service classes are absent from the selected model capability
profile's explicit `service_classes` claims.

### Bedrock Converse fixture boundary

The `bedrock-converse` profile has an enforced offline fixture matrix for the
official AWS SDK Converse request/response shape, usage accounting, service-tier
mapping, classified errors, and redaction markers. Converse is deliberately a
one-shot Temporal activity boundary: these fixtures do not claim credentialed
AWS execution or live token streaming. Protected live-provider evidence remains
separate and requires the release-run authorization described in the runbook.
The matrix uses `amazon.nova-pro-v1:0`, whose AWS model card documents Standard,
Priority, and Flex support. It deliberately does not use
`amazon.nova-lite-v1:0`: that model card currently documents Standard only, so
its model ID would make the priority fixture an invalid provider claim.

## OpenAI-compatible endpoints

- [OpenAI Chat Completions](https://platform.openai.com/docs/api-reference/chat)
  documents service-tier request values and returns the tier actually used in
  the response.
- [OpenRouter provider routing](https://openrouter.ai/docs/guides/routing/provider-selection)
  documents provider ordering, fallback controls, and required-parameter
  routing.
- [OpenRouter models API](https://openrouter.ai/docs/api/reference/list-available-models)
  exposes model pricing metadata.
- [Exa API reference](https://docs.exa.ai/reference/exa-answer)
  documents answer responses including provider-reported dollar cost.

Design consequence: direct Chat, OpenRouter, and Exa retain separately
enforced fixtures for lowering, response lifting, service class, error,
strict-loss, redaction, and decoder behavior despite using an
OpenAI-compatible SDK transport. Decoder fixtures do not advertise a v1
dispatch API. OpenRouter hidden provider fallback is disabled by
default, and Exa's authoritative cost is preferred when available.

## Temporal

- [Go Activities](https://docs.temporal.io/develop/go/core-application#develop-activity)
  support struct methods and dependency injection.
- [Activity heartbeats](https://docs.temporal.io/develop/go/failure-detection#activity-heartbeats)
  deliver cancellation and progress details for long-running work.
- [Go SDK test suite](https://pkg.go.dev/go.temporal.io/sdk/testsuite) provides
  Activity/Workflow test environments.
- [Temporal payload limits](https://docs.temporal.io/cloud/limits#payload-size-limits)
  motivate application-level inline limits and BlobRefs.

Design consequence: LLM I/O occurs in an Activity, heartbeats remain small and
redacted, and large content stays outside Workflow history.

## Redis

- [Redis Functions](https://redis.io/docs/latest/develop/programmability/functions-intro/)
  execute server-side atomic logic.
- [Redis TIME](https://redis.io/docs/latest/commands/time/) supplies shared
  server time.
- [Cluster hash tags](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/#hash-tags)
  colocate Function keys in one slot.
- [Redis persistence](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/)
  explains RDB/AOF tradeoffs, including the every-second durability window.
- [Eviction policies](https://redis.io/docs/latest/develop/reference/eviction/)
  document `noeviction` behavior.

Design consequence: one atomic admission hash slot, integer arithmetic,
server-time windows, fail-closed errors, and an explicit persistence profile.

## GitHub Actions and Go

- [Workflow schedule syntax](https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax#onschedule)
  supports a schedule timezone, used for 05:00 `Australia/Sydney`.
- [checkout](https://github.com/actions/checkout) and
  [setup-go](https://github.com/actions/setup-go) are the official actions used
  in CI.
- [Go releases](https://go.dev/doc/devel/release) identify the current stable
  release; CI and the container build use the reviewed Go patch from
  `golang/.go-version` while the module records Go 1.26 language compatibility.

## Upgrade rule

An SDK or API upgrade PR must update:

1. dependency/source date and pinned version;
2. affected capability and price catalog provenance;
3. redacted golden wire fixtures;
4. service-tier, retry, error, decoder, usage, and continuation assertions;
5. this source-contract record when behavior changed.
