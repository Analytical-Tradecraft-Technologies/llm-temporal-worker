# Unified API

## Contract goals

The public contract represents meaning, not one provider's JSON. It preserves
ordered, typed items so an adapter can make an explicit capability decision for
each feature. The canonical JSON form is versioned and is also the Temporal
Activity payload form.

The first API version is `llm.temporal/v1`. Unknown major versions fail before
state lookup. Additive fields may be introduced within v1 only when old readers
ignore them safely; enum additions require a new version because generated Go
clients must not silently accept unknown behavior.

Every public JSON record is parsed with a recursive duplicate-key check before
semantic decoding. This includes nested objects retained as extension,
provider-state, tool-argument, or schema JSON; a repeated key is never resolved
by taking the last value. Canonical request hashing applies the same rule.

> This chapter describes the current pre-release v1 shape. The staged target,
> unimplemented delta, cache, exact USD, Compact, and Query contracts replace
> it in place before the first release and are defined in
> [conversation checkpoints and compaction](conversation-checkpoints-and-compaction.md)
> and [PostgreSQL state and control plane](postgresql-state-cache-and-control-plane.md).
> No compatibility-only v2 is introduced. The coordinated pre-release change
> removes the generic downstream currency field.

## Generate request

```json
{
  "api_version": "llm.temporal/v1",
  "operation_key": "invoice-4831-summary-attempt-1",
  "context": {
    "tenant": "acme",
    "project": "accounts-payable",
    "actor": "workflow:invoice-4831",
    "tags": {"environment": "production"}
  },
  "model": "invoice-summarizer",
  "service_class": "standard",
  "service_class_fallbacks": [],
  "portability": "strict",
  "instructions": [
    {"kind": "text", "text": "Return a concise accounting summary."}
  ],
  "input": [
    {
      "kind": "message",
      "actor": "human",
      "content": [
        {"kind": "text", "text": "Summarize invoice 4831."}
      ]
    }
  ],
  "tools": [
    {
      "name": "lookup_invoice",
      "description": "Fetch an invoice by numeric identifier.",
      "input_schema": {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {"invoice_id": {"type": "integer"}},
        "required": ["invoice_id"],
        "additionalProperties": false
      }
    }
  ],
  "tool_policy": {"mode": "auto", "parallel": false},
  "output": {
    "max_tokens": 800,
    "format": {
      "kind": "json_schema",
      "name": "invoice_summary",
      "strict": true,
      "schema": {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {
          "summary": {"type": "string"},
          "risk": {"enum": ["low", "medium", "high"]}
        },
        "required": ["summary", "risk"],
        "additionalProperties": false
      }
    }
  },
  "sampling": {"temperature": 0.2},
  "reasoning": {"effort": "medium", "summary": "auto"},
  "continuation": null,
  "extensions": {}
}
```

`operation_key` is caller-chosen and must be stable across Temporal Activity
retries of the same logical inference. It is scoped by tenant and Activity
execution identity; reusing it with a different normalized request digest is a
non-retryable conflict.

## Service class

The request enum is exactly:

```go
type ServiceClass string

const (
	ServiceClassEconomy  ServiceClass = "economy"
	ServiceClassStandard ServiceClass = "standard"
	ServiceClassPriority ServiceClass = "priority"
)
```

There is no `provider_default`, `auto`, `flex`, `batch`, `scale`, or `reserved`
public value.

- Missing JSON input normalizes to `standard` before request hashing.
- Marshaled normalized requests always include the field.
- `service_class_fallbacks` defaults to an empty list.
- Fallback entries use the same three-value enum, cannot repeat the requested
  class, cannot contain duplicates, and are attempted in order.
- A fallback is an authorization to try another cost/latency class, not a
  command to do so. Route and capability checks still apply.
- Budget reservation covers the most expensive eligible class in the explicit
  list before any attempt begins.

Provider mappings are endpoint capabilities, not public defaults. For example,
an OpenAI profile can map economy/standard/priority to
`flex`/`default`/`priority`; a direct Anthropic profile supports standard and a
conditional priority request but not synchronous economy; a Bedrock profile can
map the same three values to `flex`/`default`/`priority`. Unsupported mappings
are rejected or routed elsewhere before dispatch.

## Semantic items

Input is an ordered list of tagged unions. A v1 implementation supports:

| Item kind | Required fields | Semantics |
| --- | --- | --- |
| `message` | `actor`, `content` | A human or model turn; never overloaded as a tool result |
| `tool_call` | `id`, `name`, `arguments` | Model request to invoke a caller-owned tool |
| `tool_result` | `call_id`, `content`, `is_error` | Caller-provided result paired to one tool call |
| `provider_state` | `provider`, `endpoint_family`, `media_type`, `opaque` | Uninterpreted continuation data retained byte-for-byte |
| `reference` | `uri`, optional metadata | External content reference accepted only by declared endpoint capability |

Instructions are a separate ordered part list because OpenAI Responses,
Chat-style developer/system messages, and Anthropic's top-level system content
have different lowering rules.

### Actors

The semantic actor enum is `human` or `model`. Provider labels such as `user`,
`assistant`, `developer`, and `system` belong in adapters. Tool calls and tool
results are separate item kinds, so they cannot be confused with actors.

### Content parts

| Part kind | Fields | Notes |
| --- | --- | --- |
| `text` | `text` | UTF-8; empty text is retained when provider semantics distinguish it |
| `image` | exactly one of `url`, `bytes`, `blob`; `media_type`, optional `detail` | Size and scheme validated before admission |
| `document` | exactly one source plus `media_type`, optional title | Compiled only for endpoint/model combinations that support it |
| `json` | `value` | Canonical JSON value, not a JSON-encoded string |
| `refusal` | `text`, optional provider code | Model refusal remains typed |
| `provider_state` | same provenance fields as the item | Used inside provider-defined content sequences such as thinking blocks |

Binary bytes use standard base64 in JSON. The canonical request digest is over
decoded bytes and normalized JSON, not the caller's original base64 spelling.
Payloads over configured inline limits must use `blob` with a digest, byte
length, media type, and opaque store locator.

## Tools

Tool names must match `^[A-Za-z0-9_-]{1,64}$` in the semantic contract. Each
tool has a description and a canonical Draft 2020-12 input schema. Tool policy
is one of:

- `none`
- `auto`
- `required`
- `named`, with exactly one tool name

`parallel` is a separate requested capability. An adapter must reject
unsupported required tool behavior in strict mode rather than changing it to
`auto`.

Tool execution is outside this project. A response containing `tool_call`
items is terminal for one Activity invocation. The caller executes tools and
starts another invocation with the returned continuation handle and matching
`tool_result` items.

## Structured output

The canonical schema dialect is JSON Schema Draft 2020-12. Processing is:

1. parse and reject duplicate object keys;
2. normalize the schema and compute a digest;
3. locally validate that the schema uses the v1 supported subset;
4. lower to provider-specific strict structured output or tool schema;
5. locally validate the final model JSON against the canonical schema.

Provider restrictions are expressed as diagnostics. For example, a provider
may require every property to be required and `additionalProperties: false`.
The compiler may produce an equivalent provider schema only when it can prove
round-trip equivalence. Otherwise strict mode fails before dispatch.

## Portability

`portability` is either `strict` or `best_effort`.

- `strict` rejects every unsupported, unknown, or lossy conversion before
  budget admission.
- `best_effort` may apply a documented transform and emits a diagnostic with
  field path, capability, action, and semantic consequence.

Portability never authorizes a different model, endpoint, or service class.
Those decisions belong to routing and explicit fallbacks.

## Extensions

Extensions are namespaced by adapter profile:

```json
{
  "extensions": {
    "openrouter": {"provider_order": ["Lambda", "Together"]},
    "openai_responses": {"include": ["reasoning.encrypted_content"]}
  }
}
```

Each endpoint profile has an allow-list and schema. An extension for a different
profile is an error in strict mode and a diagnostic drop in best-effort mode.
Extensions are included in request hashing and price/capability resolution.
They may not override model, service class, maximum output, routing, auth, base
URL, timeout, retry count, or budget fields.

## Generate response

```json
{
  "api_version": "llm.temporal/v1",
  "operation_key": "invoice-4831-summary-attempt-1",
  "operation_id": "op_01J...",
  "status": "tool_calls",
  "output": [
    {
      "kind": "tool_call",
      "id": "call_01J...",
      "name": "lookup_invoice",
      "arguments": {"invoice_id": 4831}
    }
  ],
  "route": {
    "route_id": "openai-primary",
    "endpoint_id": "openai-prod",
    "api_family": "openai_responses",
    "requested_model": "invoice-summarizer",
    "resolved_model": "gpt-5.6-2026-06-01"
  },
  "service": {
    "requested": "priority",
    "attempted": "priority",
    "actual": "standard",
    "provider_value": "default",
    "fallback_index": 0
  },
  "usage": {
    "input_tokens": 612,
    "output_tokens": 33,
    "reasoning_tokens": 0,
    "cache_read_tokens": 0,
    "provider_raw": {}
  },
  "cost": {
    "reserved_cost_usd": "0.041300000000000000",
    "actual_cost_usd": "0.007920000000000000",
    "method": "catalog_usage",
    "catalog_version": "prices-2026-07-13"
  },
  "provider": {
    "response_id": "resp_...",
    "request_id": "req_...",
    "finish_reason": "tool_calls"
  },
  "continuation": {
    "handle": "ctn_v1.key-2026-07.random.mac",
    "expires_at": "2026-08-12T00:00:00Z",
    "pinned": true
  },
  "diagnostics": [
    {
      "code": "service_class_provider_downgrade",
      "severity": "warning",
      "path": "/service_class",
      "message": "Provider served standard after a priority request."
    }
  ]
}
```

`service.actual` is omitted when the provider does not report enough evidence;
it is never guessed from a request field. `provider_value` retains the safe raw
tier label for audit. A provider-initiated downgrade is distinct from a router
fallback: `fallback_index` remains zero but the diagnostic is required.

Status is one of `completed`, `tool_calls`, `refused`, `length`, or
`content_filtered`. Provider-native finish reasons remain in provider metadata.

## One-shot Generate boundary

`llm.Engine.Generate` is the v1 public inference boundary. It accepts one
normalized request and returns one final normalized response after admission,
dispatch certainty, continuation handling, and ledger finalization.

No streaming or token-event API is supported in v1. The Temporal Activity
depends only on `llm.Engine`, invokes `Generate` once, and returns the final
normalized response. Raw deltas, tool arguments, and opaque provider state
never enter workflow history as heartbeat details.

Some packages retain decoder code for fragmented provider payloads. That code
is legacy parser-regression coverage, not a supported v1 dispatch path; it must
not be wired into the engine or Temporal runtime. This boundary does not claim
that residual public types have been physically deleted.
