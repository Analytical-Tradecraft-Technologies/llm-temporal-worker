# Adapter Fixture Matrix

## Profiles

The initial fixture suite defines one exact test profile for each path:

| ID | Family | Transport/client | Purpose |
| --- | --- | --- | --- |
| `openai-responses` | OpenAI Responses | official OpenAI Go SDK | native typed Responses contract |
| `openai-chat` | OpenAI-compatible Chat | official OpenAI Go SDK | common Chat fixture contract |
| `azure-responses` | Azure OpenAI Responses | official OpenAI Go SDK with Azure options | deployment-specific compatibility/tier |
| `openrouter-chat` | OpenAI-compatible Chat | official OpenAI Go SDK with pinned OpenRouter endpoint | provider routing controls and compatible differences |
| `exa-chat` | OpenAI-compatible Chat | official OpenAI Go SDK with Exa endpoint | answer/search response and reported cost |
| `anthropic-direct` | Anthropic Messages | official Anthropic Go SDK | system/tool/thinking/tier behavior |
| `anthropic-aws` | Claude Platform on AWS Messages | official Anthropic Go SDK AWS support | AWS auth/gateway differences |
| `bedrock-anthropic` | Amazon Bedrock Messages | official Anthropic Bedrock/Mantle support | Bedrock model/tier/error behavior |
| `bedrock-converse` | Amazon Bedrock Converse | official AWS Bedrock Runtime Go SDK | one-shot Converse lowering/lifting, usage, and service-tier behavior |

An optional compatible endpoint copies `openrouter-chat`'s common suite but must
add its own profile directory and difference fixtures before registration.

Azure OpenAI Chat composition is intentionally verified separately in
`llm/provider/openaichat/azure_test.go` and
`internal/runtime/factory_test.go`: those tests assert the required deployment
path, declared API version, `Api-Key` header, and configuration model pin. It
is not registered as a generic compatible or decoder-fixture profile.

## Manifest governance and staged coverage

Every profile directory owns a strict `manifest.yaml` and `metadata.yaml`.
The manifest has a stable profile ID, provider/family, coverage state, service
class facts, and only code-owned case IDs. Each listed case explicitly names
its local semantic, wire, or event artifact; a missing or traversal path fails
the offline adapter-contract gate. `metadata.yaml` records the profile ID,
HTTPS source URL, ISO-8601 source review date, SDK version, provenance,
redactions, capability facts, and the narrow list of generated fields that may
be ignored in equivalence checks.

Every `service_classes` mapping contains non-empty `economy`, `standard`, and
`priority` facts; `provider_default` is not a public class. Profiles may retain
supplemental documented scenarios such as `priority_downgrade`, `unknown_tier`,
`reported_cost`, and provider-routing facts alongside those three public
classes.

The shared validator reports the two coverage states separately:

| State | Gate behavior | Meaning |
| --- | --- | --- |
| `bootstrap` | validates schema, declared artifacts, source metadata, and raw fixture-byte redaction | a structurally governed profile; it does not claim full-matrix coverage |
| `enforced` | also requires every code-owned case applicable to its capability facts | a profile whose dedicated coverage task has supplied its complete fixture matrix |

Every checked-in production profile is currently `enforced` for its declared
capability facts. `bootstrap` remains available only while a new profile's
dedicated coverage task is incomplete; repository validation rejects it from
the checked-in release matrix until it supplies the required semantic request,
captured wire request, response, usage/cost, error, loss/diagnostic,
service-class, continuation, and security fixtures.

The runtime-facing `llm/provider.Registry` is a separate, explicit profile
catalog. Each registration is keyed by `(family, profile_id)` and validates a
concrete adapter, every governed capability fact, all three public service
classes, a private adapter-owned SDK-parameter check, a side-effect-free
compile probe, and one successful configured client construction. It does not
infer a profile from a hostname, expose SDK request types, retain a client
constructed for validation, or allow a provider-default service class. The
checked-in `llm/provider/testdata/contracts/required-cases.yaml` inventory is
compared with the code-owned case registry by `TestFixtureManifestComplete`;
the same test rejects any bootstrap fixture profile. This fixture gate is
deliberately separate from runtime route composition: startup code must bind
each configured `Registration` to the corresponding enforced fixture profile
when it assembles the deployment registry. Adapter SDK types remain inside
their owning packages.

The Bedrock Messages suite proves exact opaque-state replay, service-tier
lowering/lifting, classified-error redaction, and captured SSE decoding and
assembly across deterministic fragment boundaries. The Bedrock Converse suite
proves deterministic one-shot request/response lowering and lifting, usage,
classified-error metadata, and service-tier mapping without enabling live token
streaming. Its three-class matrix is pinned to Amazon Nova Pro
(`amazon.nova-pro-v1:0`), which AWS documents as supporting Standard, Priority,
and Flex; Amazon Nova Lite (`amazon.nova-lite-v1:0`) is Standard-only and is
therefore not used for those tier fixtures. Anthropic Direct declares
its native usage, service-class, strict-loss, and continuation paths; its
checked-in decoder fixture proves parser behavior only. In both cases, captured
SSE coverage is protocol evidence only; it does not establish a v1 client
dispatch path. The registry is intentionally code-owned so
a future semantic field or capability cannot silently escape an enforced
profile's fixture matrix; runtime registration and fixture profile binding
remain an explicit startup composition responsibility.

Profiles using the shared `stream.decoder.events` corpus also record a
`stream.decoder.semantic.json` artifact. The shared contract test assembles the
captured events and compares that normalized response with the artifact,
including deterministic fragmentation. This is a parser/assembly regression
check only; it does not enable live token streaming or establish a Temporal
streaming dispatch API.

### Responses profile boundary

The direct `openai-responses` and `azure-responses` profiles have separate
fixture roots, provenance/redaction records, deterministic conversion tests,
and enforced continuation-compatibility fixtures. Their stream artifacts
exercise the decoder with full input, every split point, an empty read,
one-byte fragments, and deterministic random fragments. Those artifacts are
deliberately decoder-only and do not establish any supported v1 token-event
dispatch. The Azure fixture also tests its own `/openai/v1/responses` path and
authentication shape; it is not evidence that a direct OpenAI model/tier is
available from an Azure deployment.

Bootstrap metadata can use `pending` while a capability is still being
researched. An enforced profile must instead declare every registry capability
as `native`, `emulated`, `supported`, `unsupported`, or `not_applicable`; this
prevents an omitted or undecided fact from silently shrinking its required
matrix.

Run the offline gate with:

```sh
make adapter-contracts
```

It rejects any checked-in `bootstrap` profile, never calls a provider, and
never rewrites a fixture. The raw-byte scan covers every file under each
`testdata/contracts` root, including shared event files. Failure output
contains only a clean repository-relative fixture path, never its bytes.

## Common semantic cases

The following is the required case inventory for an `enforced` profile. A
`bootstrap` profile may declare only a subset while its dedicated coverage work
is in progress, but it cannot be part of the checked-in release matrix. An
enforced profile has either a success fixture or an explicit compile-rejection
fixture for each applicable row:

| Area | Cases |
| --- | --- |
| Instructions | empty, one text block, ordered multiple blocks, unsupported non-text |
| Messages | one human, alternating human/model, empty retained content, Unicode, refusal |
| Text | ASCII, multi-byte UTF-8, newline, maximum accepted, over limit |
| Image | URL, bytes, BlobRef, media type, detail, unsupported source, over limit |
| Document | URL/bytes/BlobRef, title, media type, unsupported |
| JSON part | scalar, array, object, duplicate input key rejection, depth limit |
| Tools | none, auto, required, named, parallel true/false, multiple calls, tool error result |
| Tool schema | primitives, nested object/array, enum, refs if allowed, unsupported keyword |
| Structured output | text, JSON object, strict Draft 2020-12 subset, invalid model JSON |
| Sampling | absent, temperature, top-p, stop, conflicting/unsupported control |
| Output limit | absent/default normalization, exact max, endpoint max exceeded |
| Reasoning | absent, effort, summary, opaque/encrypted state, unsupported |
| Continuation | none, compatible ID, canonical replay, expired, wrong endpoint/model |
| Extensions | absent, allowed typed extension, unknown namespace, forbidden override |
| Portability | strict native, strict rejection, best-effort transform diagnostic |
| Usage | input/output, cache read/write, reasoning, absent optional, malformed/overflow |
| Completion | complete, length, tool calls, refusal, content filter, provider error |
| Identity | request/response/generation IDs and missing optional ID |

## Service-class matrix

For an enforced profile, each applicable cell has request-lowering,
response-lifting, and unsupported fixtures. Bootstrap manifests record only the
service-class facts already verified for that profile:

| Profile | economy | standard | priority |
| --- | --- | --- | --- |
| OpenAI Responses | `flex` | `default` | `priority` |
| Azure Responses | only when deployment profile declares it | `default` | `priority` when declared |
| OpenRouter Chat | only when pinned provider profile declares it | profile value | only when profile declares it |
| Exa Chat | unsupported (enforced) | profile value | unsupported (enforced) |
| Anthropic direct | unsupported synchronously | `standard_only` | `auto` only with priority-capacity claim |
| Anthropic AWS | exact offering profile | exact offering profile | exact offering profile |
| Bedrock Anthropic | `flex` when supported | `default` | `priority` when supported |

For every supported priority path, include requested priority/actual standard
downgrade and requested priority/actual priority. For every unknown provider
tier, assert `actual` is unset with a diagnostic, never guessed as standard.
For every unsupported economy path, assert compile rejection occurs before
admission/dispatch unless another configured route supports economy.

## Error and transport matrix

Every profile covers:

| Stage | Cases | Required dispatch fact |
| --- | --- | --- |
| Client compile | invalid model/control/schema/extension | not dispatched |
| DNS/connect/TLS before write | refusal, cert, client-local deadline | certified not dispatched; provider-unavailable/next-route |
| Write | zero bytes, partial/unknown bytes | not dispatched or ambiguous from observer evidence |
| HTTP/provider rejection | auth, permission, invalid, rate, capacity | profile-specific rejected/cost fact |
| Headers then disconnect | with/without request ID | accepted or ambiguous |
| Body | malformed JSON, unknown block, usage overflow | accepted; cost reconciled conservatively |
| Decoder input | malformed event, truncated terminal, error after delta | parser result as profile specifies |
| Cancellation | caller cancel/deadline before writable connection; during body; after terminal before finalize | not dispatched/never; ambiguous, or accepted |

Assertions include common code, phase, retry disposition, reservation treatment,
request ID, safe Temporal details, and no raw body leakage.

## Legacy decoder fixture matrix

Retained parser-regression fixtures may cover provider event payloads in one
compound input. They are not a supported v1 streaming profile:

- response/message start;
- content/output item start and finish;
- text delta including split UTF-8;
- tool call start, argument fragments, and completed canonical JSON;
- reasoning/thinking, signature, redacted, and encrypted opaque blocks;
- refusal/status delta;
- usage update and cache/reasoning details;
- provider tier and response metadata;
- terminal success, terminal provider error, and truncated stream.

The checked-in provider tests execute representative fixtures as complete
inputs, single-byte chunks, and deterministic seeded random chunks. This
coverage remains limited to parser behavior and is not a v1 API claim.

## Reverse-conversion assertions

For semantics supported natively:

```text
semantic -> provider parameters -> captured wire
captured provider response -> semantic
semantic -> provider -> semantic equivalence
```

Equivalence ignores only fields explicitly marked generated (provider IDs and
timestamps). Opaque provider state compares exact bytes and order. Best-effort
transforms compare the documented transformed form plus diagnostics.

## Fixture paths

Fixtures are owned by the adapter package that knows how to interpret them:

```text
llm/provider/openaichat/testdata/contracts/common/chat/
llm/provider/openaichat/testdata/contracts/openrouter-chat/
llm/provider/openaichat/testdata/contracts/exa-chat/
llm/provider/openairesponses/testdata/contracts/openai-responses/
llm/provider/openairesponses/testdata/contracts/azure-responses/
llm/provider/anthropicmessages/testdata/contracts/anthropic-direct/
llm/provider/anthropicmessages/testdata/contracts/anthropic-aws/
llm/provider/bedrockmessages/testdata/contracts/bedrock-anthropic/
```

Each listed profile has a `manifest.yaml` listing its declared matrix cases.
Error fixtures live beside the provider profile that emits them, and security
cases are package tests rather than a root-level fixture directory. Bootstrap
profiles are structurally checked during isolated profile development, while
repository validation requires every checked-in profile to be enforced.
Enforced profiles are compared against the complete code-owned required-case
list, so adding a new semantic field or enum fails every enforced adapter until
support/rejection fixtures are added.
