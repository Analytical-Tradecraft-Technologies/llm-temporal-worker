# `llm-temporal-ocaml`

Typed OCaml bindings for one durable, non-streaming `llm.generate.v1` Go
Activity. The package contains no provider credentials, token streaming,
continuation loop, polling, or application retry loop. The generated workflow
schedules exactly one activity attempt.

The public v1 API uses exact request and response records: service classes are
exactly `Economy | Standard | Priority`; request controls include portability,
instructions, items, tools, output, temperature, reasoning effort/summary, and
extensions; responses carry the v1 checkpoint, route, usage, settled cost, and
diagnostics. Only deliberately open contract leaves (schemas, tool arguments,
extension/provider metadata) use `Yojson.Safe.t`.

## Install

Pin the nested package at the deployment commit:

```sh
opam pin add --yes --kind=git llm-temporal-ocaml \
  'git+https://github.com/mfow/llm-temporal-worker.git#<commit>' \
  --subpath=ocaml/llm_temporal_worker
opam install --yes llm-temporal-ocaml
```

Its metadata pins `temporal-sdk` to immutable commit
`dbe689a0c09c5e7743e44dad48e98f52f86a4ae6`. Commit an application lock file
after `opam lock .`, then deploy with `opam install . --locked`.

Add `(libraries llm-temporal-ocaml)` to your Dune stanza.

The repository also contains a separate [downstream Dune consumer smoke
project](../consumer_smoke). CI installs this package from the Git subpath,
then builds that project against the installed public library. This catches
packaging and public-name regressions that a build from the source directory
cannot detect.

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
  ~task_queue:(Temporal_task_queue.of_string "go-activities") request
(* Handle [result] in the application Workflow. *)
```

## Immutable conversations

For a multi-turn workflow, `Llm_temporal.Conversation` keeps the v1 checkpoint
branch head as an immutable value. `fork` is a cheap persistent branch
operation: it does not schedule an Activity or mutate the parent. A successful
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
  Llm_temporal.Conversation.respond ~settings_patch:patch ?cache
    ~operation_key:(Llm_temporal.Operation_key.of_string "turn-2")
    ~append:[ Message { actor = Human; content = [ Text "Continue." ] } ] branch
in
```

The synchronous `respond_with`, `compact_with`, and `Query.execute_with`
helpers also bind an injected response to the request's `operation_key` before
exposing it to the caller. A dispatcher that returns a response for another
operation is rejected as a typed codec error; this keeps deterministic test
dispatchers subject to the same idempotency boundary as the remote Activity.

For a single non-streaming Generate Activity, `Llm_temporal.Generate` provides
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
match Llm_temporal.Generate.invoke request with
| Ok response -> handle_response response
| Error error -> handle_temporal_error error
```

`Generate.make` returns the exact `generate_request` record used by the
`llm.generate.v1` Activity. `Generate.invoke_with` accepts the same typed
dispatcher used by deterministic tests, and `Generate.start` returns a
workflow-owned Temporal future. The package-level `execute` and `workflow`
helpers now accept and schedule the same Generate v1 records and codecs. The
pre-checkpoint `Request` and `invoke_once` names remain only as deprecated
source-compatibility shims; their legacy records/codecs must not be used to
call the production Go `llm.generate.v1` Activity. New code should use
`Generate`, `Conversation`, or the migrated package-level helpers, all of
which emit the exact v1 wire shape.

The synchronous `Generate.invoke_with` and `Generate.invoke` helpers also
require the response `operation_key` to match the request key. A mismatched
key is returned as a typed codec error before the response is exposed to the
workflow, preserving the idempotency boundary used by the Conversation and
Query facades.

`Conversation.compact` creates an explicit compaction child from a checkpoint;
the following Generate restores the branch's application tools and output
configuration. The wrapper does not stream, retain a mutable implicit head, or
schedule any Activity outside the exact `llm.generate.v1` and
`llm.compact.v1` descriptors. The protocol layer also exposes exact Compact
and typed Query v1 records and Yojson codecs; those low-level records remain
separate from the ergonomic facade.

Advanced workflow code that already owns exact wire records can use the three
descriptors directly through `Llm_temporal.start_generate`,
`Llm_temporal.start_compact_v1`, and `Llm_temporal.start_query_v1`. Each returns
a workflow-owned `Temporal.Future.t`; the corresponding `invoke_generate`,
`invoke_compact_v1`, and `invoke_query_v1` helpers schedule and await one
Activity using the fixed one-attempt retry policy. These low-level helpers do
not materialize inherited conversation state or reinterpret query result tags;
use `Conversation` and `Query` when those invariants are needed.

`Conversation.of_checkpoint` intentionally treats the checkpoint's effective
settings as unknown: a handle does not materialize worker state in Workflow
code. If such an imported conversation is compacted, the wrapper leaves the
post-compaction settings patch as `Keep` rather than turning its local defaults
into an implicit `Set []`/`Clear` patch. Callers that know the desired tools or
output configuration should provide them explicitly on the next Generate;
those explicitly supplied fields are then restored after a later compaction,
while still-unknown fields remain inherited by the worker.

## Typed query facade

`Llm_temporal.Query` adds a closed GADT over the five query Activities. Each
constructor carries its filter and fixes the result type, so pagination and
result handling remain associated at the call site:

```ocaml
let query =
  Llm_temporal.Query.Budget_status {
    policy_key = None; active_at = None; include_windows = true;
  }

match Llm_temporal.Query.execute
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
    ~include_healthy:false ~page_size:100
    ~refresh_if_older_than_seconds:300L ()
in
Llm_temporal.Query.execute ~operation_key ~context
  (Llm_temporal.Query.Provider_status provider_filter)
```

Model-inventory prefixes use the nominal `Model_prefix.t` wrapper, keeping a
prefix distinct from a complete `Model_selector.t` or `Provider_model_id.t`:

```ocaml
let* model_filter =
  Llm_temporal.Query.Filter.model_inventory
    ~model_prefix:(Llm_temporal.Model_prefix.of_string "gpt-") ()
in
Llm_temporal.Query.execute ~operation_key ~context
  (Llm_temporal.Query.Model_inventory model_filter)
```

The wrapper preserves arbitrary provider model naming and is encoded as the
same JSON string expected by the Go query Activity.  Use `None` when no prefix
filter is desired.

Validation failures are ordinary `validation_error` strings and happen before
an Activity is scheduled, which makes malformed query construction explicit in
deterministic Workflow code.

The `Provider_status`, `Model_inventory`, `Credit_status`, `Budget_status`,
and `Spend_summary` constructors are exhaustive. The facade validates that
the closed result tag matches the requested constructor and returns a codec
`Temporal.Error.t` for mismatches or unknown future tags; it never uses an
unchecked JSON cast or `Obj.magic`. `Query.start` returns a workflow-owned
Temporal future whose successful value is a typed `result` (so protocol-kind
mismatches stay on the error channel without raising in a workflow callback),
while `Query.execute_with` is available for deterministic dispatch injection
in tests.

For paginated responses, `Query.next query response` constructs the next page
with the same GADT result type. It returns `Ok None` for the final page and
rejects a cursor attached to the wrong query kind (or to a snapshot query)
before another Activity is dispatched:

```ocaml
match Query.next query response with
| Ok (Some next_query) -> Query.execute ~operation_key ~context next_query
| Ok None -> Ok_finished
| Error error -> handle_temporal_error error
```

Response cursors retain the query kind that produced them. Reusing a cursor
returned by one paginated query with a different query kind is rejected by the
OCaml facade before an Activity is dispatched. `Query_cursor.of_string` remains
available for manually supplied or fixture cursors whose origin is unknown;
those cursors are still validated by the server.

The same boundary applies to injected deterministic dispatchers and to
`Query.start`: a response cursor must carry the constructor's query kind, and
budget/spend responses cannot carry a cursor. A mismatched cursor is returned
as a typed codec error and no Activity is scheduled for an invalid `start`
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
members, and other protocol violations before a Temporal activity is called.

`workflow` calls `Temporal.Activity.execute` once against the exact Go
activity name `llm.generate.v1` with the exported
`Llm_temporal.activity_retry_policy` (`maximum_attempts = 1`). The Go worker
must serve the provided task queue (or the SDK's worker queue when omitted).
Errors are returned unchanged as `Temporal.Error.t`; the wrapper does not
retry, continue, or stream after an activity result. Callers that inject a
`dispatch` through `invoke_once` can use the same one-shot/error-propagation
contract in deterministic unit tests.

Continuation handles and provider-state identifiers/media types are required
to be non-empty. `continuation.expires_at`, when present, must be an RFC3339
timestamp (the same format emitted by the Go worker). These cross-language
invariants are checked during encode and decode so malformed values fail as a
codec error before an Activity is scheduled or invalid data enters Temporal
history.
