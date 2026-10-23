# OpenAI Responses API listener

The worker can serve a subset of the
[OpenAI Responses API](https://platform.openai.com/docs/api-reference/responses)
over plain HTTP. It is a thin wrapper over the public Generate workflow
(`llm.generate.workflow.v1`): it adds no routing, budgeting, caching or
provider logic of its own. Standard OpenAI SDKs work against it by pointing
their base URL at the listener.

Enable it with `server.responses_api_listen_address` (see
[configuration](configuration.md#optional-openai-responses-api)). Every worker
that sets the address serves the API; workflows run on the worker's own
`temporal.task_queue`, so any worker on that queue may execute them.

## Identity

The API key is the caller identity, not a secret:

```text
Authorization: Bearer <tenant>/<project>/<actor>
```

The key is split on its first two `/` characters, so the actor may contain
`/`. Each part must be 1 to 256 characters without control characters. A
missing or malformed key returns `401 invalid_api_key`. The identity becomes
the Generate request context, so the worker's
[caller authorization](configuration.md#caller-authorization) still applies.

Responses are scoped to the tenant and project: a key for another project
cannot read or resume them, and gets `404`. Any actor in the same project can.

```python
from openai import OpenAI

client = OpenAI(base_url="http://llm-worker.internal:8000/v1", api_key="acme/search/batch-job")
response = client.responses.create(model="fast", input="Summarise this ticket.")
```

## Endpoints

| Endpoint | Behaviour |
|---|---|
| `POST /v1/responses` | Starts the Generate workflow and waits for its result. Returns the completed `response` object. |
| `POST /v1/responses` with `"background": true` | Starts the workflow and returns at once with `status: "queued"`. |
| `GET /v1/responses/{id}` | Returns the response's current state: `queued`, `in_progress`, `completed`, `incomplete`, `failed` or `cancelled`. |

Every create response carries the response ID in the `X-Response-Id` header,
including a `503` from a start that Temporal may have accepted before the call
failed. Poll that ID before retrying such a request without an
`Idempotency-Key`, or the retry may pay for the same work twice.

### Response IDs and idempotency

A response ID is also the Generate operation key, and names a Temporal
workflow derived from the caller's tenant, project and that ID. Without an
`Idempotency-Key` header the ID is random (`resp_` and 32 hex digits).

With `Idempotency-Key: <key>` the ID is `resp_<key>`. The key is 1 to 200
letters, digits, `-`, `_`, `.` or `:`. A repeated request with the same key
attaches to the existing workflow, whether it is running or finished, rather
than starting new paid work, so a client can safely retry a dropped
connection. The repeat's body is not compared with the original's.

### Waiting and disconnects

A waiting create request ends without affecting the workflow if the caller
disconnects; the result can then be polled with the ID (use an
`Idempotency-Key` so the ID is known in advance). When the worker shuts down,
it ends every wait with `503`, a `Retry-After` header and the response ID,
before it stops polling Temporal. Those workflows keep running on other
workers.

### Polling

`GET /v1/responses/{id}` first reads a Redis copy of terminal results. When a
Generate operation completes or fails permanently, the worker that ran it
copies the result to Redis after the durable record commits. A poll for a
recently finished request is then answered from Redis alone, without durable
storage or Temporal. The copy expires after an hour, and results larger than
1 MiB are not copied. Values are AES-GCM encrypted under a key derived from
the Redis key secret and bound to their scope-keyed name, so Redis holds no
readable output. Only workers with `server.responses_api_listen_address` set
write the copy. It is never authoritative: if it is missing, expired or
unreadable, the poll falls through to the durable record. A terminal durable
answer is copied back so later polls skip durable storage. Non-terminal states
are never copied.

On a Redis miss the poll reads the durable operation record (the cloud
request repository, `state.requests`). A completed or failed record answers
the poll without a Temporal call. Temporal's `DescribeWorkflowExecution` is
consulted only when the record cannot answer:

- No record yet: the workflow has not reached its first Activity, which
  creates the record. A running workflow reports `queued`.
- A non-terminal record: a running workflow reports `in_progress`. A workflow
  that ended without writing a result (terminated, timed out) reports
  `failed`.
- No cloud request repository is configured, or it cannot be read: Temporal
  answers alone.

An ID with neither a record nor a workflow returns `404`. Once Temporal's
namespace retention removes a workflow, only its durable record remains.

## Request mapping

Settings map to the Generate request's settings patch. On a continuation an
omitted `model`, `tools`, `temperature`, `top_p`, `reasoning` or
`service_tier` inherits the previous response's value. The rest follow OpenAI,
which applies them per request rather than carrying them across
`previous_response_id`:

- An omitted `instructions` clears them.
- `tool_choice` with `parallel_tool_calls`, and `max_output_tokens` with
  `text.format`, are each one setting in the worker. On a continuation both
  are always set in full, with OpenAI's defaults (`auto`, parallel calls, text,
  no token limit) for anything omitted, so sending one half never keeps a
  stale inherited other half.

| OpenAI field | Generate request |
|---|---|
| `model` | `settings_patch.model` (required unless `previous_response_id` is set) |
| `input` (string) | One human message |
| `input` messages, `user` / `assistant` | Human / model messages |
| `input` messages, `system` / `developer` | Instructions, after `instructions`; only before other input items |
| `function_call`, `function_call_output` | Tool call and tool result items |
| `input_text`, `output_text`, `refusal` | Text and refusal parts |
| `input_image` | Image part from an `https` URL with an image extension, or a base64 data URL |
| `input_file` | Document part from `file_url` or a base64 `file_data` URL |
| `instructions` | `settings_patch.instructions` |
| `previous_response_id` | `parent`: the checkpoint the previous completed response published |
| `tools` (function only) | `settings_patch.tools` |
| `tool_choice`, `parallel_tool_calls` | `settings_patch.tool_policy` |
| `temperature`, `top_p` | Exact decimal settings |
| `max_output_tokens`, `text.format` | `settings_patch.output` |
| `reasoning.effort` | `reasoning_effort`; `none` sets `reasoning_mode: disabled` |
| `reasoning.summary` | `reasoning_summary` |
| `service_tier` | `settings_patch.service_class`, as below |

`service_tier` maps to the closed [service class](configuration.md#service-class-rules)
enum. `auto` sets nothing, which inherits the parent's class or defaults to
standard. The response's `service_tier` reports the class that served the
request, mapped back the same way.

| `service_tier` | Service class |
|---|---|
| `flex` | `economy` |
| `default` | `standard` |
| `priority` | `priority` |
| `auto` | unset |

The API rejects with `400` what it cannot honour, rather than ignoring it:
`stream: true`, hosted tools, `reasoning` and `item_reference` input items
(continue with `previous_response_id` instead), `file_id`, non-empty
`metadata`, `include`, `truncation` other than `disabled`, `text.verbosity`,
the `scale` tier, and any unknown field. `user`, `safety_identifier`,
`prompt_cache_key` and `store` are accepted and ignored: the actor comes from
the API key, and every result is stored.

The mapped request must pass the v1 request validation and fit
`server.inline_payload_bytes`; a larger request returns `413`.

## Response mapping

| Generate result | OpenAI response |
|---|---|
| Model message text and JSON parts | `message` item with `output_text` parts |
| Refusal parts | `refusal` parts |
| Tool calls | `function_call` items |
| Status `completed`, `tool_calls`, `refused` | `status: "completed"` |
| Status `length` | `incomplete`, reason `max_output_tokens` |
| Status `content_filtered` | `incomplete`, reason `content_filter` |
| Status `paused` | `incomplete`, reason `paused` |
| Route resolved model | `model` |
| Usage | `usage`; `input_tokens` includes cache reads and writes |

Provider state and reference items stay in the checkpoint that
`previous_response_id` resumes from. The request-echo fields (`tools`,
`tool_choice`, `instructions`, `metadata`) carry OpenAI defaults, because a
poll is answered from the durable result, which does not keep the request
settings.

## Errors

Errors use the OpenAI shape, `{"error": {"type", "code", "message", "param"}}`.
Messages never contain provider messages or storage errors.

| Worker error code | HTTP status | OpenAI code |
|---|---|---|
| `invalid_argument`, `unsupported_capability`, `no_route` | 400 | the worker code |
| `permission_denied` | 403 | `permission_denied` |
| `operation_conflict` | 409 | `idempotency_conflict` |
| `budget_denied` | 429 | `insufficient_quota` |
| `provider_rate_limited` | 429 | `rate_limit_exceeded` |
| `provider_unavailable`, `provider_invalid_response`, `ambiguous_dispatch`, `authentication` | 502 | the worker code |
| `deadline_exceeded` | 504 | `deadline_exceeded` |

A failed background response is returned by `GET` with `status: "failed"`
and the same `error` object.
