# `llm-temporal-ocaml`

Typed OCaml bindings for the Go worker's public generation, compaction, and control-query
workflows, with exact request and response codecs and an immutable conversation
facade. `Client` starts remote workflows from application processes and resumes
waiting after a process restart. Provider polling, budget waiting, and paid
retries are owned by the Go workflows.

Inside deterministic OCaml workflows, `Generate.invoke`, `Conversation.respond`,
and their asynchronous equivalents start the public Go child workflows. Supply
`~task_queue` for the Go worker and `~id` for a deterministic child workflow ID
unique within the Temporal namespace. Use a durable caller/run identity plus a
per-call suffix; do not use a process clock or random generator. Reusing an ID
for another active child start is a Temporal conflict, not a cache lookup.
The request's operation key and sample index retain their separate meanings.

These helpers use parent-close policy `Abandon`, so already-started paid work
continues if its parent closes. They expose no cancellation handle and install
no parent-side retry policy: the Go workflow owns budget waits, paid retries,
and polling. Await the returned future when the parent needs the response.

The public v1 API uses exact request and response records: service classes are
exactly `Economy | Standard | Priority`; request controls include portability,
instructions, items, tools, output, temperature, top-p, stop sequences, seed,
reasoning mode/token budget/effort/summary, and extensions; responses carry the v1 checkpoint, route, usage, settled cost, and
diagnostics. Only deliberately open contract leaves (schemas, tool arguments,
extension/provider metadata) use `Yojson.Safe.t`.

## Install

Pin the nested package at the deployment commit:

```sh
opam pin add --yes --kind=git llm-temporal-ocaml \
  'git+https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker.git#<commit>' \
  --subpath=ocaml/llm_temporal_worker
opam install --yes llm-temporal-ocaml
```

Its metadata pins `temporal-sdk` to the validated child-workflow-routing SDK commit
`0d3f618590ed09501b004d5052b7bd88b81e2723`. Commit an application lock file
after `opam lock .`, then deploy with `opam install . --locked`.

Add `(libraries llm-temporal-ocaml)` to your Dune stanza.

The repository also contains a separate [downstream Dune consumer smoke
project](../consumer_smoke). CI installs this package from the Git subpath,
builds that project against the installed public library, and runs its
deterministic smoke test. This catches packaging, public-name, and basic
runtime-dispatch regressions that a build from the source directory cannot
detect; it never contacts Temporal or an LLM provider.

## Remote workflow client

Use `Llm_temporal.Client` outside deterministic workflow code:

```ocaml
let ( let* ) = Result.bind

let generate client request =
  let* handle = Client.start_generate client
      ~task_queue:(Temporal_task_queue.of_string "llm-worker")
      ~id:"invoice-42-workflow" ~request_id:"invoice-42-start" request in
  let execution = Client.execution handle in
  (* Persist [execution] together with [request] to resume after a restart. *)
  let* response = Client.await handle in
  Ok (execution, response)
```

Create the connection with `Client.create ~target_url ~namespace ()` and release
it with `Client.shutdown`. Persist the workflow ID, start request ID, and request
before starting. If the start response is lost, retry with the same values.
`request_id` deduplicates Temporal start requests; the request's `operation_key`
is worker idempotency, and `cache.variant` selects an independent cache sample.
These identifiers serve different purposes.

`Client.start_query` and `resume_query` support the control-query workflow.
`Client.start_compact` returns a typed compaction handle. `resume_generate` and
`resume_compact` attach to a saved execution without starting a workflow. They
require the original request so completed results can be checked against its
operation key, sample index, and checkpoint lineage.

`Client.wait` preserves the exact run's typed terminal result. `Client.await`
follows continue-as-new runs of the same workflow and returns its final decoded
response or a `Temporal.Error.t`; it never restarts a failed workflow. The client
does not expose cancellation. Neither method executes application tool calls
returned in a model response.

## Use

```ocaml
open Llm_temporal

let request =
  Generate.make
    ~operation_key:(Operation_key.of_string "invoice-42")
    ~context
    ~model:(Model_selector.of_string "gpt-5")
    ~settings:(Generate.Settings.make
                 ~service_class:Priority
                 ~instructions:[ Text_instruction { level = Application; text = "Return JSON." } ]
                 ~service_class_fallbacks:[ Standard ]
                 ~reasoning_effort:High
                 ~reasoning_summary:Concise
                 ~output:{ max_tokens = Some 200; format = Json_format }
                 ())
    ~input:[ Message { actor = Human; content = [ Text "Summarise this invoice." ] } ]
    ()

let result = Generate.invoke
  ~task_queue:(Temporal_task_queue.of_string "llm-worker")
  ~id:"invoice-42-child" request
(* Handle [result] in the application Workflow. *)
```

## Immutable conversations

For a multi-turn workflow, `Llm_temporal.Conversation` keeps the v1 checkpoint
branch head as an immutable value. `fork` is a cheap persistent branch
operation: it does not schedule a workflow or mutate the parent. A successful
`respond` returns the v1 provider response together with a child conversation
carrying the returned checkpoint. Callers therefore choose explicitly which
child to retain.

```ocaml
let settings =
  Llm_temporal.Conversation.Settings.make
    ~service_class:Llm_temporal.Priority ()
in
let root =
  Llm_temporal.Conversation.root ~context ~model ~settings ()
in
let branch = Llm_temporal.Conversation.fork root in
match Llm_temporal.Conversation.respond
        ~task_queue:(Temporal_task_queue.of_string "llm-worker") ~id:"conversation-42-turn-1"
        ~operation_key:(Llm_temporal.Operation_key.of_string "turn-1")
        ~append:[ Message { actor = Human; content = [ Text question ] } ]
        branch with
| Ok { response; conversation } ->
    (* [conversation] is the next immutable branch head. *)
    ignore (response, conversation)
| Error error -> handle_temporal_error error
```

When the facade is opened, the ergonomic builders are also available as
`Settings`, `Cache_policy`, `Decimal`, and `Compaction_policy`.  These are
aliases over the same types used by `Conversation`; they do not introduce a
second protocol or package.  `tool` and `output_config` are source-level
aliases for the v1 function-tool and output records, so the following is
valid in an external Dune package that depends only on
`llm-temporal-ocaml`:

```ocaml
let temperature =
  match Decimal.of_string "0.5" with
  | Ok value -> value
  | Error message -> invalid_arg message

let settings = Settings.make ~temperature ()
let root = Conversation.root ~context ~model ~settings ()
```

`Conversation.Settings.make` also accepts the typed v1
`~reasoning_effort` and `~reasoning_summary` controls.  Omitting either leaves
the corresponding sparse patch as `Keep`, so a checkpoint's inherited worker
setting is preserved; supplying one emits an exact closed v1 enum rather than
an untyped string.  Per-turn overrides remain available through
`Settings.Patch.set_reasoning_effort` and
`Settings.Patch.set_reasoning_summary`.

`~top_p`, `~stop_sequences`, `~seed`, `~reasoning_mode` and
`~reasoning_token_budget` follow the same rules, with matching `set_*` and
`clear_*` patch helpers.  The codec enforces the worker's bounds before
dispatch: `top_p` in (0, 1], one to 16 distinct non-empty stop sequences of at
most 256 characters, a seed between 0 and 2^53 - 1, and a token budget between
1 and 2147483647.  A route whose API cannot honour a control (for example a
seed on Anthropic Messages) rejects the request rather than dropping it. The
exception is Bedrock Converse with `Best_effort` portability, which silently
drops `reasoning_mode` and `reasoning_token_budget`; under `Strict` it
rejects them.

The repository's `ocaml/consumer_smoke` project is that downstream-package
check.  It executes deterministic injected dispatchers for one root Generate,
three immutable sibling forks, Compact, the post-compaction Generate, and all
five typed Query constructors.  It never starts a Temporal server or contacts
a provider; CI first installs the package through its Git subpath and then
builds and runs the smoke executable against the installed public interface.

`Conversation.to_request` is available when a workflow needs to inspect or
inject the exact low-level v1 request. `respond_with` accepts an injectable
typed dispatcher for deterministic tests; production code normally uses
`respond` or `start_respond`. The asynchronous helper returns a future whose
successful value is a `(turn, Temporal.Error.t) result`; protocol validation,
including the response operation-key binding, stays in that value channel so
workflow callbacks never raise for a mismatched response. Settings changes are explicit persistent
builders, for example:

```ocaml
let patch =
  Llm_temporal.Conversation.Settings.Patch.set_service_class
    Llm_temporal.Economy
    Llm_temporal.Conversation.Settings.Patch.keep
in
let cache =
  Llm_temporal.Conversation.Cache_policy.accept_up_to
    ~max_age_seconds:60L ~variant:1l ()
in
let branch =
  Llm_temporal.Conversation.respond ~task_queue ~id:"conversation-42-turn-2"
    ~settings_patch:patch ?cache
    ~operation_key:(Llm_temporal.Operation_key.of_string "turn-2")
    ~append:[ Message { actor = Human; content = [ Text "Continue." ] } ] branch
in
```

`Cache_policy.variant` is a nonnegative 32-bit sample index. Zero is the
default; a different index selects a different cache identity without changing
the provider's temperature. Generation and compaction both support independent
samples, including requests with zero or inherited temperature.

The synchronous `respond_with`, `compact_with`, and `Query.execute_with`
helpers also bind an injected response to the request's `operation_key` before
exposing it to the caller. A dispatcher that returns a response for another
operation is rejected as a typed codec error; this keeps deterministic test
dispatchers subject to the same idempotency boundary as the remote workflow.

For a single generation request, `Llm_temporal.Generate` provides
the same v1 request shape without requiring a synthetic conversation branch:

```ocaml
let request =
  Llm_temporal.Generate.make
    ~operation_key:(Llm_temporal.Operation_key.of_string "invoice-42")
    ~context
    ~model:(Llm_temporal.Model_selector.of_string "gpt-5")
    ~settings:(Llm_temporal.Generate.Settings.make
                 ~service_class:Llm_temporal.Priority ())
    ~input:[ Message { actor = Human; content = [ Text prompt ] } ]
    ()
in
match Llm_temporal.Generate.invoke ~task_queue ~id:"invoice-42-child" request with
| Ok response -> handle_response response
| Error error -> handle_temporal_error error
```

`Generate.make` returns the exact request record for `llm.generate.workflow.v1`.
`Generate.invoke_with` accepts a typed workflow dispatcher for deterministic
tests. `Generate.start` returns a future whose successful value is a
`(generate_response, Temporal.Error.t) result`: the outer error represents
Temporal execution or decoding failure; the inner error represents a response
that fails request binding. The Conversation futures use the same convention.
Generation and compaction responses are checked for the operation key, sample
index, and checkpoint lineage before callers accept them.

The package-level `execute` invokes the public child workflow. `workflow ()`
returns its remote descriptor; it does not define a second OCaml implementation
under the Go workflow name. The deprecated `Request` and `invoke_once` shim
converts representable pre-checkpoint requests before invoking an injected
workflow dispatcher. Generation/compaction Activity descriptors with the old
final-response shape have been removed: those activities now return internal
execution results and are orchestrated by the Go workflows.

`Conversation.compact` creates an explicit compaction child from a checkpoint;
the following Generate restores the branch's application tools and output
configuration. The wrapper returns model tool calls to the application and
does not execute them.

Advanced workflow code can use `generate_v1_workflow` and `compact_v1_workflow`,
or `start_generate`/`start_compact_v1` and their `invoke_*` equivalents with
`~task_queue` and `~id`. These low-level helpers return exact wire responses;
use `Generate` and `Conversation` for request-bound response validation.
`query_v1_workflow`, `start_query_v1`, and `invoke_query_v1` call the public
query workflow. The query facade validates query result tags and full payloads.

`Conversation.of_checkpoint` intentionally treats the checkpoint's effective
settings as unknown: a handle does not materialize worker state in Workflow
code. If such an imported conversation is compacted, the wrapper leaves the
post-compaction settings patch as `Keep` rather than turning its local defaults
into an implicit `Set []`/`Clear` patch. Callers that know the desired tools or
output configuration should provide them explicitly on the next Generate;
those explicitly supplied fields are then restored after a later compaction,
while still-unknown fields remain inherited by the worker.

## Typed query facade

`Query.execute`, `Query.start`, `invoke_query_v1` and `start_query_v1` require
`~task_queue` and `~id`: the `llm.query.workflow.v1` child workflow is
registered on the Go worker's task queue, so the calling workflow's own queue
cannot serve it. Use a deterministic child ID unique within the namespace. The
production worker answers `provider_status`, `model_inventory` and
`credit_status` from its Redis provider state for the tenant/project pairs in
its `authorization.allowed_scopes`. It answers `budget_status` from the
active Redis budget generation, but only when Redis uses Function admission
mode, the `llmtw_budget_status_v3` library is loaded and a budget generation
is published. Otherwise `budget_status` returns an unsupported-query error. A
`budget_status` result is one complete snapshot with no cursor, and
`include_windows = false` returns an empty window list.
`spend_summary` always returns an unsupported-query error there until a cloud
spend reader exists (tracked in
[#817](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/817)).
A positive `refresh_if_older_than_seconds` refreshes `model_inventory` only
for endpoints with a provider model-list fetcher (currently direct OpenAI).
Other endpoints, and every `provider_status` and `credit_status` refresh,
return an unsupported-query error. A failed or timed-out refresh returns the
last persisted listing marked `stale`. See
[provider management refresh](../../docs/reference/persisted-query-service.md#provider-management-refresh).

`Llm_temporal.Query` adds a closed GADT over the five query kinds. Each
constructor carries its filter and fixes the result type, so pagination and
result handling remain associated at the call site:

```ocaml
let query =
  Llm_temporal.Query.Budget_status {
    policy_key = None; active_at = None; include_windows = true;
  }

match Llm_temporal.Query.execute
        ~task_queue:(Temporal_task_queue.of_string "llm-worker") ~id:"budget-query"
        ~operation_key:(Llm_temporal.Operation_key.of_string "budget-check")
        ~context query with
| Ok { value = budget; cost; _ } -> inspect_budget budget cost
| Error error -> handle_temporal_error error
```

For application code, `Query.Filter` provides validated builders instead of
requiring callers to assemble page records by hand.  The builders apply the
wire bounds before the GADT is constructed: page sizes are 1--1000, refresh
ages are 1--86400 seconds, tagged cursors must belong to the corresponding
query, and spend intervals must be strictly increasing with unique grouping
dimensions.  The raw filter records remain available for protocol fixtures.

```ocaml
let* provider_filter =
  Llm_temporal.Query.Filter.provider_status
    ~include_healthy:false ~page_size:100 ()
in
Llm_temporal.Query.execute ~task_queue ~id ~operation_key ~context
  (Llm_temporal.Query.Provider_status provider_filter)
```

Model-inventory prefixes use the nominal `Model_prefix.t` wrapper, keeping a
prefix distinct from a complete `Model_selector.t` or `Provider_model_id.t`:

```ocaml
let* model_filter =
  Llm_temporal.Query.Filter.model_inventory
    ~model_prefix:(Llm_temporal.Model_prefix.of_string "gpt-") ()
in
Llm_temporal.Query.execute ~task_queue ~id ~operation_key ~context
  (Llm_temporal.Query.Model_inventory model_filter)
```

The wrapper preserves arbitrary provider model naming and is encoded as the
same JSON string expected by the Go query workflow.  Use `None` when no prefix
filter is desired.

Model inventory results expose `display_name` as the nominal
`Model_display_name.t option`, distinct from `Provider_model_id.t`. Both remain
JSON strings at the workflow boundary, but the wrappers prevent accidental
identifier interchange in OCaml code.

Inventory capability labels use `Model_capability.t`. The Go control plane
intentionally permits its capability vocabulary to grow, so this is a nominal
string wrapper rather than a hard-coded OCaml enum: callers can retain an
unknown future label while the type system prevents passing it as a model ID,
diagnostic code, or arbitrary string.

Provider and credit query results expose safe diagnostic values as
`Safe_code.t option`, matching the Go control-plane `SafeCode` type. The wrapper
is distinct from `Diagnostic_code.t` and from arbitrary strings; it is still
encoded as the unchanged JSON string at the workflow boundary.

Model inventory lifecycle filters use the Go wire values `available`,
`deprecated`, `unavailable`, and `unknown`. For source compatibility the
OCaml constructors are `Active`, `Deprecated`, `Retired`, and `Unknown`; the
first and third encode as `available` and `unavailable` (never `active` or
`retired`). This lifecycle is distinct from route `availability`.

Validation failures are ordinary `validation_error` strings and happen before
a child workflow is started, which makes malformed query construction explicit in
deterministic Workflow code.

The `Provider_status`, `Model_inventory`, `Credit_status`, `Budget_status`,
and `Spend_summary` constructors are exhaustive. The facade validates that
the closed result tag matches the requested constructor and returns a codec
`Temporal.Error.t` for mismatches or unknown future tags; it never uses an
unchecked JSON cast or `Obj.magic`. `Query.start` returns a workflow-owned
Temporal future whose successful value is a typed `result` (so protocol-kind
mismatches stay on the error channel without raising in a workflow callback),
while `Query.execute_with` and `Query.start_with` are available for
deterministic synchronous and asynchronous dispatch injection in tests. Both
paths revalidate raw filter records before calling the injected dispatcher.

For paginated responses, `Query.next query response` constructs the next page
with the same GADT result type. It returns `Ok None` for the final page and
rejects a cursor attached to the wrong query kind (or to a snapshot query)
before another child workflow is started:

```ocaml
match Query.next query response with
| Ok (Some next_query) -> Query.execute ~task_queue ~id ~operation_key ~context next_query
| Ok None -> Ok_finished
| Error error -> handle_temporal_error error
```

The result association is preserved for each paginated constructor:
`Provider_status` returns a `provider_status_page`, `Model_inventory` returns
a `model_inventory_page`, and `Credit_status` returns a `credit_status_page`.
A complete page must have no cursor; an incomplete page must carry one.
The facade rejects inconsistent pagination metadata.

Response cursors retain the query kind that produced them. Reusing a cursor
returned by one paginated query with a different query kind is rejected by the
OCaml facade before a child workflow is started. `Query_cursor.of_string` remains
available for manually supplied or fixture cursors whose origin is unknown;
those cursors are still validated by the server.

The same boundary applies to injected deterministic dispatchers and to
`Query.start`: a response cursor must carry the constructor's query kind, and
budget/spend responses cannot carry a cursor. A mismatched cursor is returned
as a typed codec error and no child workflow is started for an invalid `start`
request. This keeps pagination type-associated even when a caller tests the
facade without going through the JSON codec.

Each `*_id` module is an opaque wrapper around arbitrary text—not a provider
enum or whitelist.  For example, `Operation_key.t`, `Endpoint_id.t`, and
`Provider_request_id.t` cannot be interchanged, while `of_string` and
`to_string` make construction and logging explicit.  The encoded payload
continues to use the unchanged v1 JSON strings.

`Image` and `Document` sources carrying `Bytes raw` treat `raw` as the byte
string itself. The codec emits standard padded base64 in the JSON `bytes`
field and decodes it back to the original bytes. URLs are checked for an
absolute, non-`data:`/non-`javascript:` URI; tool names are restricted to the
v1 ASCII `[A-Za-z0-9_-]{1,64}` form. Request encoding and response decoding
fail with `Temporal.Error.t` for invalid identifiers, duplicate service
fallbacks, negative token/cost limits, invalid media, duplicate open-JSON
members, and other protocol violations before a Temporal workflow is started.

`workflow ()` returns the remote Generate descriptor. The public OCaml
interface exposes workflows only: Generate, Compact, and Query. Queries use
`llm.query.workflow.v1`, whose Go implementation executes one attempt of the
existing independently authorized query activity. Query children cancel with
the parent; paid Generate/Compact children continue under `Abandon`.


Continuation handles and provider-state identifiers/media types are required
to be non-empty. `continuation.expires_at`, when present, must be an RFC3339
timestamp (the same format emitted by the Go worker). These cross-language
invariants are checked during encode and decode so malformed values fail as a
codec error before a child workflow is started or invalid data enters Temporal
history.

Use `Conversation.Cache_policy.any_age ()` to enable caching without an age limit.
`accept_up_to` retains an explicit positive maximum age. The typed model stores
`max_age_seconds` as an option; `None` omits that field on the wire.

## Hosted tools and typed results

Use one shared `code_execution` option for OpenAI Code Interpreter and Anthropic
code execution. `web_search` enables native search; `web_fetch` enables
Anthropic fetch. Each defaults off, and explicit `false` disables it.

```ocaml
let ( let* ) = Result.bind

let build_request ~context ~model =
  let* output = Output.text ~max_tokens:500 () in
  Generate.make_checked
    ~operation_key:(Operation_key.of_string "research-42") ~context ~model
    ~settings:(Settings.make ~web_search:true ~code_execution:true ~output ())
    ~input:[Item.human "Research the figures, then calculate their growth."] ()
```

| Transport | `web_search` | `web_fetch` | `code_execution` |
| --- | --- | --- | --- |
| Direct OpenAI Responses | yes | unsupported | yes |
| Direct Anthropic Messages | yes | yes | yes |
| OpenRouter OpenAI/Anthropic models | native server search | unsupported | unsupported |
| Other gateways/providers | unsupported | unsupported | unsupported |

Unsupported routes are rejected, even under best-effort portability; the worker
never silently removes a requested tool. The selected model must support the
provider's tool version. This adds no generic OpenRouter plugins or application
code sandbox. OpenRouter uses the native `openrouter:web_search` server tool,
not its separate Exa search plugin.

OpenAI requests an automatic 1 GB container and at most three hosted tool calls.
Anthropic uses `web_search_20250305` and `web_fetch_20250910` (three uses each,
fetch content capped at 10,000 tokens), and `code_execution_20250825`. These
server tools execute at the provider. Internal compaction disables them.

`Response.text` reads answer text; `Response.references` reads citations;
`Response.hosted_calls` reads typed tool observations and retained provider
payloads; `Response.artifacts` reads generated file identities. OpenAI artifact
references target its authenticated container-file API. Anthropic exposes Files
API IDs. Neither is a public or durable download link, and this library does
not download or upload files. Preserve artifacts before provider expiry.

`Response.outcome` distinguishes completed answers, application tool calls,
refusal, truncation, filtering, and `Paused_turn`. For an Anthropic pause,
continue the returned conversation/checkpoint with `~append:[]` and a new
operation key. The checkpoint retains the server blocks and container ID.
`Response.tool_calls` contains only application functions that the caller must
execute. `Response.json`/`decode_json` reject incomplete or non-JSON answers.

Hosted tools need a known model context ceiling for budget admission. The
reservation includes extra token rounds and tool fees (currently four context
rounds for OpenAI/OpenRouter, twenty for Anthropic). This is a conservative
allowance, not a provider-enforced monetary cap. Provider-reported totals take
precedence. Separately observed native searches add their per-search fee;
container session/free-tier billing and missing tool usage stay `Unknown_cost`
instead of being presented as exact token-only costs. Unknown costs retain the
reservation for reconciliation. These allowances may make hosted requests wait
for a larger available budget than ordinary generation.

`Context.make`, `Tool.function_`, `Output.*`, `Generate.make_checked`,
`Compact.make`, and `Compaction_policy.make` provide checked constructors.
`Failure.kind` classifies worker failures without hiding the original
`Temporal.Error.t`. Raw records and codecs remain available for compatibility.

`Exa.answer ~operation_key ~context ~model ~question ()` builds an Exa Answer
Generate request using your configured model selector. Its
`~include_source_text:false` option and `Exa.sources` helper cover the exposed
Answer feature; Exa Search/Crawl APIs are not workflows in this worker.

`Extra_high` serializes as `xhigh` for OpenAI models that support it.
`Maximum` continues to serialize as `maximum`, which OpenAI adapters send as `max`.
Extra High is rejected by the Anthropic adapters instead of silently lowering it.
