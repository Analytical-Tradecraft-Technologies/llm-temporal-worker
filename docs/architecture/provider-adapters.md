# Provider Adapters

## Adapter boundary

An adapter is a bidirectional compiler around one official provider SDK and one
endpoint family. It does not select routes, read secrets, enforce budgets, or
retry. Its responsibilities are:

1. declare capabilities for an endpoint/model/service-class tuple;
2. lower one normalized semantic request to SDK parameter types;
3. invoke the SDK exactly once with the supplied context and dispatch observer;
4. lift a one-shot response into semantic output and typed usage;
5. classify the provider result without deciding whether another route runs.

An SDK response that is nil without an accompanying error is treated as an
accepted malformed response and becomes a safe provider-invalid-response
error. Adapters must never dereference a provider response before this guard;
an Activity panic would bypass the operation ledger's terminal classification.

Every SDK client is constructed with automatic retries disabled. This prevents
an SDK retry from escaping the operation ledger, spending twice, or using a
different provider tier without a second admission decision.

## Compile and invoke types

```go
type Adapter interface {
	Name() string
	Capabilities(context.Context, CapabilityQuery) (CapabilitySet, error)
	Compile(context.Context, CompileInput) (Call, error)
	Invoke(context.Context, Call, Observer) (Result, error)
}

type Call struct {
	EndpointID   string
	Family       Family
	Model        string
	ServiceClass llm.ServiceClass
	SDKParams    any
	Metadata     CallMetadata
}

type Observer interface {
	BeforePossibleWrite(context.Context) error
	AfterResponseHeaders(context.Context, ResponseMetadata) error
	OnProgress(context.Context, Progress)
}
```

`SDKParams` may contain only the parameter type for the adapter's official SDK.
It never crosses the adapter package boundary or enters Temporal history.
`CallMetadata` contains the redacted facts needed to validate the compiled call,
including schema digests, estimated bytes, capability version, provider tier
value, and whether opaque state is required in the response.

## Capability model

Capabilities are data, not hard-coded assumptions. A compiled endpoint profile
declares each feature as:

- `native`: provider preserves the semantic contract;
- `emulated`: a named, reviewed transform preserves the promised result;
- `unsupported`: the endpoint cannot satisfy the request;
- `unknown`: no verified claim exists, which behaves as unsupported in strict
  mode.

The lookup key includes endpoint family, endpoint deployment, resolved model,
service class, and capability-catalog version. Relevant capabilities include:

- text, image, and document input forms and size limits;
- tool calling, named/required tools, parallel calls, and strict tool schemas;
- JSON Schema output and the accepted schema subset;
- reasoning controls, summaries, and opaque thinking state;
- continuation identifiers and storage behavior;
- maximum context and output tokens;
- service-class request and response fields;
- request IDs, idempotency headers, and provider-reported cost.

A model-name prefix is never sufficient evidence for a capability. Unknown model
revisions remain ineligible until configuration or live discovery supplies a
verified profile.

## Initial endpoint profiles

| Profile | Official client | Initial API family | Service-class lowering | Important policy |
| --- | --- | --- | --- | --- |
| OpenAI | `openai-go` | Responses | economy -> `flex`, standard -> `default`, priority -> `priority` when the model supports them | Capture response `service_tier`; a downgrade is observable |
| Azure OpenAI | `openai-go` with Azure base URL/auth options | Responses or Chat, declared per deployment | deployment capability maps supported public classes to Azure tier values | Never infer capability from the base URL alone |
| OpenRouter | `openai-go` with compatible base URL | Chat Completions | configured only when a verified provider/model path offers the requested behavior | Disable hidden provider fallback and require declared parameters |
| Exa | `openai-go` with compatible base URL | Chat Completions | profile-declared; normally standard until another tier is verified | Preserve Exa request ID and authoritative `costDollars` when present |
| Anthropic | `anthropic-sdk-go` | Messages | standard -> `standard_only`; priority -> `auto` only for accounts/models where priority capacity is explicitly enabled; economy unsupported synchronously | Lift the actual service tier from usage |
| Claude Platform on AWS | Anthropic SDK AWS gateway support | Messages | capability-declared from the selected AWS offering | AWS auth belongs to client construction, not semantic input |
| Amazon Bedrock | Anthropic SDK Bedrock/Mantle client | Messages | economy -> `flex`, standard -> `default`, priority -> `priority` where the model supports them | `reserved` is deployment capacity, never a public service class |

This table is a starting profile, not a promise that every model supports every
cell. Configuration compilation intersects it with exact model/deployment
claims. Direct Anthropic synchronous economy is ineligible; the router may use a
different explicitly configured endpoint for economy, but may not rewrite it to
standard.

OpenRouter's own provider routing is pinned by default with an explicit provider
order, `allow_fallbacks=false`, and `require_parameters=true`. This project owns
fallback observability and budget admission; hidden upstream failover would
break both.

## Lowering rules

### OpenAI Responses

- Instructions lower to the supported top-level instruction/developer form.
- Semantic messages, tool calls, and tool results become separate typed input
  items; they are never concatenated.
- A continuation may use a stored response/conversation identifier only when it
  is pinned to the same endpoint, account, family, and compatible model.
- Strict structured output uses the provider's JSON Schema form after local
  subset validation.
- Reasoning encrypted content is retained as provider-state only when requested
  by capability/configuration.
- `service_tier` is set from the resolved public class and the response tier is
  lifted independently.

### OpenAI-compatible Chat Completions

- Instructions lower to the declared system/developer role supported by the
  endpoint.
- Tool calls remain assistant tool-call objects and tool results remain tool
  messages with their call IDs.
- Multimodal parts use only the endpoint's declared compatible wire forms.
- Structured output chooses native response format or a strict tool emulation
  only when the capability profile declares semantic equivalence.
- Provider-specific routing bodies are typed namespaced extensions; they cannot
  be injected as arbitrary JSON.

Compatible does not mean identical. Azure, OpenRouter, Exa, and optional
endpoints each receive their own profile, fixtures, limits, error decoder, and
usage/cost lifter.

### Anthropic Messages

- Instructions lower to top-level system blocks in order.
- Human/model messages lower to user/assistant messages; tool use and tool
  result blocks retain their IDs.
- Consecutive-role merging is permitted only as an explicit, proven transform
  and is recorded as a diagnostic.
- Thinking, redacted-thinking, and signatures are opaque provider-state. They
  round-trip byte-for-byte and stay pinned to the compatible Anthropic route.
- JSON Schema constraints are lowered through native output/tool facilities
  only when the exact endpoint profile supports the required strictness.

## Response lifting

The lifter produces a common ordered output sequence and never discards unknown
provider blocks silently. In strict mode, an unknown block causes a conversion
error. In best-effort mode, it becomes opaque provider state when safe; otherwise
the response is marked incomplete with a diagnostic.

Usage distinguishes:

- input and output tokens;
- cache-read and cache-write tokens when reported;
- reasoning/thinking tokens when reported separately;
- provider-reported cost and currency;
- provider tier value and mapped actual public class;
- request, response, and generation identifiers.

An unrecognized actual provider tier maps to no public class and returns a
diagnostic. It must not be mislabeled as `standard`.

## Model inventory pagination

Model management APIs are optional adapter capabilities. A profile that does
not implement `ModelLister` is explicitly unsupported for refresh; configured
routes remain authoritative. Profiles that do implement it should be consumed
through the shared `provider.CollectModelInventory` helper. The helper validates
every bounded page, follows only the returned opaque cursor, rejects repeated
cursors and model-ID regressions across pages, caps a refresh at 10,000 models,
and checks cancellation before each provider request. The resulting normalized
models can then be persisted as one immutable inventory snapshot with explicit
current, stale, or unsupported provenance. No inference request is used as a
model-list substitute.

## Legacy decoder regression coverage

Some provider packages retain decoders for fragmented provider event payloads.
They are parser-regression coverage only: v1 has no supported streaming adapter
or token-event API, and this code must not be wired into Temporal dispatch.
The supported adapter boundary lifts a one-shot provider response into the
final normalized response.

## Error decoding

Adapters translate provider errors into the common error model with:

- safe code and message;
- provider HTTP/gRPC status and request ID;
- definite pre-dispatch, definite rejected, definite accepted, or ambiguous
  dispatch observation;
- retry hint and provider retry-after;
- whether any usage/cost was reported;
- redacted raw metadata allowed by configuration.

The adapter supplies facts. The engine combines those facts with ledger state,
route policy, and caller deadline to choose the next action.

## Adapter acceptance gate

An adapter is not registered until it has:

- golden lower/lift fixtures for every supported semantic part;
- request and response tier fixtures for every public service-class mapping;
- errors before write, after possible write, rate limit, auth, invalid input,
  timeout, cancellation, and malformed response;
- retained decoder regression fixtures for representative fragmented provider
  payloads, where those fixtures exist;
- strict rejection fixtures for every unsupported/lossy feature;
- provider-state byte-preservation and continuation-pinning tests;
- a redacted fixture provenance record tied to an upstream API/SDK version.
