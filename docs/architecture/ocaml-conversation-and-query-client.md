# OCaml Conversation, Compaction, and Typed Query Client

## Status and compatibility

This document specifies changes to the existing
**ocaml/llm_temporal_worker** package. It does not create another opam package
or a parallel client. The implementation extends the same **Llm_temporal**
facade, nominal identifiers, codecs, Activity descriptors, retry policy, and
test conventions.

The package currently models one **llm.generate.v1** invocation. The v1
checkpoint/delta/cache contract removes generic currency and adopts decimal
USD types. The additive `Llm_temporal.Generate` facade now constructs and
invokes that exact v1 request directly; it avoids a synthetic conversation
branch for one-shot callers. The older `Request`, `execute`, and `workflow`
names remain available as a compatibility surface until a separately approved
breaking release, but they do not add a second wire protocol or Activity.

Implementation starts from the landed OCaml validation baseline, including PR
109. Preserve those validation improvements and regenerate fixtures from the
final Go contract.

The protocol layer now contains the Task 17 Generate, Compact, and Query v1
wire records, closed Yojson codecs, exact decimal-cost representation, and
their three Temporal Activity descriptors. The public `Llm_temporal.Query`
module now adds the five-constructor GADT over those closed query records. The
companion `Llm_temporal.Conversation` facade now provides immutable v1
checkpoint roots, forks, Generate/Compact helpers, and persistent
`Settings.Patch`/`Cache_policy` builders. It remains a thin wrapper over the
descriptors below: no streaming or mutable conversation head is introduced.

Delivery follows the shared phase order: the rebuilt Generate facade is Phase
A, Compact and Redis budget materialization are Phase B, the opt-in exact cache
is Phase C, and typed Query clients are Phase D. These are implementation
checkpoints inside one unreleased API, not public protocol versions. See
[scope](../scope.md#staged-delivery-and-document-authority) for the normative
phase gates.

## Two public layers, one package

The package has two conceptual layers:

| Layer | Purpose | Public stability |
| --- | --- | --- |
| Protocol | Exact closed OCaml representation of Go JSON and Activity names | Wire-compatible and exhaustive |
| Ergonomic workflow API | Natural immutable conversations, sparse patch builders, and typed query execution | Source-friendly and hides tag matching |

Existing private modules continue to own identifier validation, model records,
JSON codecs, and invocation. New modules are included through the existing
**Llm_temporal** facade:

~~~text
lib/
  llm_temporal_identifier.ml/.mli       existing, extend
  llm_temporal_models.ml/.mli           existing protocol models, replace in place
  llm_temporal_codec.ml/.mli            existing, extend fixtures
  llm_temporal_invocation.ml/.mli       existing, add activity descriptors
  llm_temporal_conversation.ml/.mli     new ergonomic stateful API
  llm_temporal_query.ml/.mli            new typed query API
  llm_temporal.ml/.mli                  existing unified facade
~~~

The protocol modules may remain private Dune modules while their selected types
are exposed through the facade. Callers need only
**(libraries llm-temporal-ocaml)**.

## Nominal and decimal types

Add opaque wrappers for values that must not be interchanged:

~~~ocaml
module Checkpoint : sig
  type t
  val of_string : string -> (t, validation_error) result
  val of_string_exn : string -> t
  val to_string : t -> string
end

module Query_cursor : sig
  type t
  val of_string : string -> (t, validation_error) result
  val of_string_exn : string -> t
  val to_string : t -> string
end

module Query_execution_id : Identifier
module Budget_policy_key : Identifier
module Budget_generation_id : Identifier

module Budget_stream_id : sig
  type t
  val of_string : string -> (t, validation_error) result
  val of_string_exn : string -> t
  val to_string : t -> string
end

module Sha256_digest : sig
  type t
  val of_hex : string -> (t, validation_error) result
  val of_hex_exn : string -> t
  val to_hex : t -> string
end
~~~

`Budget_stream_id` validates Redis's unsigned `milliseconds-sequence` spelling;
it is not forced through a generic identifier grammar. `Sha256_digest` accepts
exactly 64 lowercase hexadecimal characters and cannot be confused with an
operation or checkpoint handle.

Money has an explicitly USD-denominated arbitrary-precision decimal type:

~~~ocaml
module Usd_decimal : sig
  type t

  val zero : t
  val of_string : string -> (t, validation_error) result
  val to_string : t -> string
  val compare : t -> t -> int
  val add : t -> t -> t
end
~~~

The accepted spelling is a non-negative canonical base-10 decimal with at most
18 fractional digits and within PostgreSQL **NUMERIC(38,18)**. JSON uses a
string. The OCaml implementation uses an exact decimal/big-integer
representation or a checked coefficient plus scale; **float** is absent from
constructors and records. There is no **currency** value, string, or enum in
the Go wire model or OCaml facade. A field named **actual_cost_usd** is known
to be USD.

For `settings_patch.temperature`, the v1 wire spelling is the same canonical
decimal string in both clients. The Go decoder retains a bounded compatibility
window for older numeric producers, but Go re-encoding always emits the string
form; new producers must never emit a JSON number.

The repository still contains the pre-Task-17 `request`/`response` compatibility
records because the unreleased legacy wrapper and Conversation tests use them.
Those records are not accepted by, or emitted from, any `llm.temporal/*/v1`
codec and are outside the public v1 boundary; new callers must use
`generate_request`, `generate_response`, `compact_request`,
`compaction_response`, and the query records below. Their old cost fields are
therefore deliberately absent from every v1 model and fixture.

Unknown is represented structurally, not by a sentinel decimal. A catalog unit
price or actual cost that the worker cannot establish is JSON null at the wire
boundary and becomes an OCaml **Unknown_cost** constructor. Exact zero remains
an ordinary **Usd_decimal.t** inside **Exact_cost**. Estimates and retained
budget bounds use different types/fields and cannot be substituted for actual
cost.

Durations use a checked non-negative integer seconds type at the wire boundary.
Cache **variant** uses **int32**, not OCaml's platform-width **int**.

## Exact protocol records

### Generate and compact

The low-level Generate request preserves omitted versus explicit patch
operations:

~~~ocaml
type 'a patch =
  | Keep
  | Set of 'a
  | Clear

type settings_patch = {
  model : Model_selector.t patch;
  service_class : service_class patch;
  service_class_fallbacks : service_class list patch;
  portability : portability patch;
  instructions : instruction list patch;
  tools : tool list patch;
  tool_policy : tool_policy patch;
  output : output_config patch;
  temperature : Usd_decimal.t patch;
  reasoning_effort : reasoning_effort patch;
  reasoning_summary : reasoning_summary patch;
  compaction_policy : compaction_policy patch;
  extensions : extensions patch;
}

type cache_policy = {
  max_age_seconds : Int64.t;
  variant : Int32.t;
}

type generate_request = {
  api_version : string;
  operation_key : Operation_key.t;
  context : request_context;
  parent : Checkpoint.t option;
  append : item list;
  settings_patch : settings_patch;
  cache : cache_policy option;
}
~~~

The encoder omits every **Keep** leaf and omits **settings_patch** when all
leaves are Keep. **Set []** emits an empty list and differs from **Clear**.
**Clear** emits **{"clear":true}**. It never serializes inherited settings into
the Activity payload.

Effective temperature is validated by the Go worker after inheritance. The
OCaml builder rejects immediately when it can prove temperature zero with a
positive variant, but it does not guess an inherited value. Server validation
remains authoritative and returns a typed Activity error.

Responses use new-turn output and a checkpoint:

~~~ocaml
type settled_cost =
  | Exact_cost of {
      actual_cost_usd : Usd_decimal.t;
      method_ : cost_method;
      catalog_version : string option;
    }
  | Unknown_cost of { reason : cost_unknown_reason }

type cost = {
  reserved_cost_usd : Usd_decimal.t;
  settled : settled_cost;
}

type generate_response = {
  operation_key : Operation_key.t;
  operation_id : Operation_id.t;
  status : response_status;
  output : item list;
  checkpoint : checkpoint_metadata;
  cache : cache_disposition;
  route : route option;
  usage : usage;
  cost : cost;
  diagnostics : diagnostic list;
}
~~~

There is no transcript field. The Compact protocol has its own request/response
types and Activity name. Its result identifies a compaction checkpoint,
provenance, cache disposition, usage, and USD cost. Its request accepts the
same optional `cache_policy`; omission disables both read and population, and
the protocol rejects a nonzero variant. Compaction protocol records contain no
application tool list, tool policy, or structured-output field. Those are
inherited checkpoint settings restored only by a later Generate.

### Tagged Query wire contract

The exact JSON union is closed:

~~~ocaml
type query_request =
  | Provider_status_request of provider_status_filter
  | Model_inventory_request of model_inventory_filter
  | Credit_status_request of credit_status_filter
  | Budget_status_request of budget_status_filter
  | Spend_summary_request of spend_summary_filter

type query_result =
  | Provider_status_result of provider_status_page
  | Model_inventory_result of model_inventory_page
  | Credit_status_result of credit_status_page
  | Budget_status_result of budget_status
  | Spend_summary_result of spend_summary

type query_response = {
  operation_key : Operation_key.t;
  query_execution_id : Query_execution_id.t;
  observed_at : Ptime.t;
  source : query_source;
  freshness : freshness;
  complete : bool;
  next_cursor : Query_cursor.t option;
  result : query_result;
  cost : settled_cost;
}
~~~

The filters and returned pages are ordinary closed records. Their proposed
public shape is explicit so implementation does not collapse them to a shared
JSON object:

~~~ocaml
type provider_status_filter = {
  provider : Provider.t option;
  endpoint : Endpoint.t option;
  availability : availability option;
  include_healthy : bool;
  refresh_if_older_than_seconds : Int64.t option;
  page_size : int;
  cursor : Query_cursor.t option;
}

type model_inventory_filter = {
  provider : Provider.t option;
  endpoint : Endpoint.t option;
  model_prefix : Model_prefix.t option;
  lifecycle : model_lifecycle option;
  refresh_if_older_than_seconds : Int64.t option;
  page_size : int;
  cursor : Query_cursor.t option;
}

`Model_prefix.t` is a nominal wrapper around the provider's arbitrary model
prefix. It is intentionally distinct from both `Model_selector.t` (the
requested model in Generate) and `Provider_model_id.t` (a complete inventory
entry), while retaining the unchanged JSON string on the wire. Use `None` to
omit the prefix filter.

type credit_status_filter = {
  provider : Provider.t option;
  endpoint : Endpoint.t option;
  include_ok : bool;
  refresh_if_older_than_seconds : Int64.t option;
  page_size : int;
  cursor : Query_cursor.t option;
}

type budget_status_filter = {
  policy_key : Budget_policy_key.t option;
  active_at : Ptime.t option;
  include_windows : bool;
}

type spend_summary_filter = {
  start_time : Ptime.t;
  end_time : Ptime.t;
  group_by : spend_group_by list;
  operation_kinds : operation_kind list;
}

type provider_route_status = {
  route_id : Route_id.t;
  provider : Provider.t;
  endpoint : Endpoint.t;
  availability : availability;
  credit_state : credit_state;
  billing_state : billing_state;
  circuit_state : circuit_state;
  observed_at : Ptime.t;
  stale_after : Ptime.t;
  safe_code : string option;
}

type provider_status_page = { routes : provider_route_status list }

type model_inventory_entry = {
  provider : Provider.t;
  endpoint : Endpoint.t;
  provider_model_id : string;
  display_name : string option;
  lifecycle : model_lifecycle;
  capabilities : model_capability list;
  source : inventory_source;
  complete_snapshot : bool;
  safe_metadata : Safe_metadata.t;
}

type model_inventory_page = { models : model_inventory_entry list }

type credit_status_entry = {
  provider : Provider.t;
  endpoint : Endpoint.t;
  credit_state : credit_state;
  billing_state : billing_state;
  confirmed_at : Ptime.t option;
  evidence_source : credit_evidence_source;
  safe_evidence_code : string option;
}

type credit_status_page = { endpoints : credit_status_entry list }

type budget_window_status = {
  policy_key : Budget_policy_key.t;
  window_key : string;
  coverage_start : Ptime.t;
  coverage_end : Ptime.t;
  limit_usd : Usd_decimal.t;
  reserved_cost_usd : Usd_decimal.t;
  accounted_cost_usd : Usd_decimal.t;
  available_usd : Usd_decimal.t;
  retry_after_seconds : Int64.t option;
}

type budget_status = {
  active_at : Ptime.t;
  generation_id : Budget_generation_id.t;
  manifest_digest : Sha256_digest.t;
  stream_high_water_mark : Budget_stream_id.t;
  windows : budget_window_status list;
}

type spend_group_key = {
  operation_kind : operation_kind option;
  provider : Provider.t option;
  model : Model_selector.t option;
}

type spend_bucket = {
  group : spend_group_key;
  known_actual_cost_usd : Usd_decimal.t;
  exact_operation_count : Int64.t;
  unknown_operation_count : Int64.t;
  completeness : cost_completeness;
}

type spend_summary = {
  start_time : Ptime.t;
  end_time : Ptime.t;
  buckets : spend_bucket list;
}
~~~

The `Query` member of `operation_kind` is a spend-reporting dimension across
the dedicated query audit ledger; it does not imply that Query uses the paid
inference operation state machine. Accordingly, Query responses expose
`Query_execution_id.t`, while Generate and Compact expose `Operation_id.t`.

`Safe_metadata.t` is the package's bounded, redacted open-metadata wrapper; it
is not a general escape hatch for the surrounding records. Page bounds and
half-open spend intervals are validated by both the OCaml constructor and Go
worker. Empty `group_by` means one aggregate bucket. Duplicate dimensions and
an end time not strictly after the start time fail before Activity scheduling.

Each codec validates both outer **kind** and inner result constructor. The
decoder rejects an unknown tag, duplicate/unknown member, malformed cursor,
unbounded page, invalid timestamp/decimal, or a response whose tag and result
shape disagree. It never returns **Yojson.Safe.t** for a known query result.
Open JSON remains limited to deliberately open provider metadata already
allowed by the package.

The JSON codec enforces the exact cost-state invariant. **cost_status=exact**
requires a decimal-string **actual_cost_usd** and method. **unknown** requires
JSON null plus a closed safe reason and forbids a method. This is decoded
directly to **settled_cost** so ordinary OCaml callers cannot accidentally sum
an unknown price as zero.

Result records are closed and typed:

- provider status has route ID, availability, credit/billing state, circuit
  state, observation/staleness timestamps, and safe code;
- model inventory has provider model ID, display/lifecycle values, typed known
  capabilities, safe open metadata, source, and completeness;
- credit status has confirmed state/time/source and safe evidence code;
- budget status has policy/window IDs, exact USD limit/reserved/finalized/
  available values, retry-after, **Budget_generation_id.t**,
  **Budget_stream_id.t** high-water mark, and **Sha256_digest.t** manifest
  identity;
- spend summary has a half-open interval, closed grouping keys, exact USD total,
  and completed-operation count.

Unknown future enum values are decoding errors until the OCaml library is
updated. This is intentional for Workflow decisions: silently treating a new
credit state as healthy would be unsafe.

The `Budget_status` result source is a closed `Redis_budget_generation`
constructor rather than a generic persisted-state string. It proves the Go
Activity read the current Redis working set. The OCaml API offers no option to
request a PostgreSQL budget fallback. `Spend_summary` remains a PostgreSQL
operation-cost query and does not expose budget-journal rows.

## Ergonomic immutable Conversation API

A conversation value is a pure branch head plus any effective-settings
knowledge available to the caller. Values built from `root` or returned by
`respond`/`compact` carry the complete effective settings, enabling local
variant validation. `of_checkpoint` deliberately starts with an unknown
settings hint because a handle alone must not materialize server state inside a
Workflow; after import, the Go worker remains authoritative for inherited
validation. Compaction of such an imported value therefore keeps the settings
patch omitted; the facade never turns its local default hint into a destructive
tool/output clear. Callers that need to restore known application settings
must provide those fields explicitly on the subsequent Generate; those fields
are then restored after compaction while still-unknown fields remain inherited
by the worker. The value contains no network client, mutable reference, or
process-global state:

~~~ocaml
module Conversation : sig
  type t

  type turn = {
    response : generate_response;
    conversation : t;
  }

  val root :
    context:request_context ->
    model:Model_selector.t ->
    ?service_class:service_class ->
    ?settings:Settings.t ->
    unit ->
    t

  val of_checkpoint :
    context:request_context ->
    checkpoint:Checkpoint.t ->
    t

  val checkpoint : t -> Checkpoint.t option

  val fork : t -> t

  val respond :
    ?task_queue:Temporal_task_queue.t ->
    operation_key:Operation_key.t ->
    ?settings_patch:Settings.Patch.t ->
    ?cache:Cache_policy.t ->
    append:item list ->
    t ->
    (turn, Temporal.Error.t) result

  val start_respond :
    ?task_queue:Temporal_task_queue.t ->
    operation_key:Operation_key.t ->
    ?settings_patch:Settings.Patch.t ->
    ?cache:Cache_policy.t ->
    append:item list ->
    t ->
    ((turn, Temporal.Error.t) result, Temporal.Error.t) Temporal.Future.t

  val compact :
    ?task_queue:Temporal_task_queue.t ->
    operation_key:Operation_key.t ->
    ?policy:Compaction_policy.t ->
    ?cache:Cache_policy.t ->
    t ->
    (compaction_response * t, Temporal.Error.t) result

  val start_compact :
    ?task_queue:Temporal_task_queue.t ->
    operation_key:Operation_key.t ->
    ?policy:Compaction_policy.t ->
    ?cache:Cache_policy.t ->
    t ->
    (compaction_response * t, Temporal.Error.t) Temporal.Future.t
end
~~~

**fork** is intentionally trivial: it returns another immutable value naming
the same checkpoint. It exists to make application intent clear, not to allocate
server state. The same value may also be passed directly to three **respond**
calls with different operation keys:

~~~ocaml
let parent = turn.conversation in
let branch_a = Conversation.fork parent in
let branch_b = Conversation.fork parent in
let branch_c = Conversation.fork parent in

let a =
  Conversation.respond
    ~operation_key:(Operation_key.of_string "case-a")
    ~append:[Message { actor = Human; content = [Text "Try A"] }]
    branch_a
in
let b =
  Conversation.respond
    ~operation_key:(Operation_key.of_string "case-b")
    ~append:[Message { actor = Human; content = [Text "Try B"] }]
    branch_b
in
let c =
  Conversation.respond
    ~operation_key:(Operation_key.of_string "case-c")
    ~append:[Message { actor = Human; content = [Text "Try C"] }]
    branch_c
in
...
~~~

The library does not mutate **parent** when one branch completes. Each success
returns its own child conversation. Temporal Workflow code stores the desired
child in its deterministic Workflow state.

### Natural settings and cache builders

The ergonomic settings patch module uses the same tri-state explicitly:

~~~ocaml
module Settings : sig
  type t

  val make :
    ?service_class:service_class ->
    ?temperature:Decimal.t ->
    ?reasoning_effort:reasoning_effort ->
    ?reasoning_summary:reasoning_summary ->
    ?tools:tool list ->
    ?tool_policy:tool_policy ->
    ?output:output_config ->
    unit ->
    t

  module Patch : sig
    type t
    val keep : t
    val set_temperature : Decimal.t -> t -> t
    val clear_temperature : t -> t
    val set_reasoning_effort : reasoning_effort -> t -> t
    val replace_tools : tool list -> t -> t
    val clear_tools : t -> t
    val replace_output : output_config -> t -> t
    val clear_output : t -> t
  end
end

module Cache_policy : sig
  type t
  val accept_up_to :
    max_age_seconds:Int64.t ->
    ?variant:Int32.t ->
    unit ->
    (t, validation_error) result
end
~~~

There is no Boolean **use_cache** that leaves freshness undefined. Omitting
**cache** means no read and no population. Variant defaults to **Int32.zero**.
No random key is generated automatically; callers deliberately choose stable
variants for smoke-test fixtures.

The optional reasoning controls are part of the same typed sparse settings
builder as service class, temperature, tools, and output.  An omitted control
is encoded as **Keep** on a resumed checkpoint; an explicit value emits the
closed v1 enum.  The facade never accepts a free-form reasoning string or
silently clears an inherited value.

Compact accepts the same `Cache_policy.t`, but its effective compaction
sampling contract permits only variant zero. Automatic compaction receives the
outer Generate cache age and variant zero; a final-answer variant of one or two
is not propagated into the summary cache key.

The one-shot convenience **`Generate.make`/`Generate.invoke`/`Generate.start`**
constructs and schedules one v1 Generate directly. It does not maintain a loop,
stream tokens, or hide a checkpoint. `Generate.invoke_with` accepts the same
typed dispatcher used by deterministic tests. The older
**`Request.make`/`execute`/`workflow`** names remain recognizable for existing
callers; new code should prefer `Generate` or `Conversation` so it cannot
accidentally construct the pre-checkpoint wire shape.

## GADT-typed Query API

The ergonomic query layer associates each filter with exactly one result type:

~~~ocaml
module Query : sig
  type _ t =
    | Provider_status :
        provider_status_filter -> provider_status_page t
    | Model_inventory :
        model_inventory_filter -> model_inventory_page t
    | Credit_status :
        credit_status_filter -> credit_status_page t
    | Budget_status :
        budget_status_filter -> budget_status t
    | Spend_summary :
        spend_summary_filter -> spend_summary t

  type 'a response = {
    value : 'a;
    query_execution_id : Query_execution_id.t;
    observed_at : Ptime.t;
    source : query_source;
    freshness : freshness;
    complete : bool;
    next_cursor : Query_cursor.t option;
    cost : settled_cost;
  }

  val execute :
    ?task_queue:Temporal_task_queue.t ->
    operation_key:Operation_key.t ->
    context:request_context ->
    'a t ->
    ('a response, Temporal.Error.t) result

  val start :
    ?task_queue:Temporal_task_queue.t ->
    operation_key:Operation_key.t ->
    context:request_context ->
    'a t ->
    (('a response, Temporal.Error.t) result, Temporal.Error.t) Temporal.Future.t
end
~~~

The same module exposes `Query.Filter` builders for natural call sites. They
return `(filter, validation_error) result` before a filter is wrapped in a
GADT constructor. A builder applies the protocol's page-size (`1..1000`) and
refresh-age (`1..86400` seconds) bounds, rejects a tagged cursor belonging to
another query kind, and checks that a spend interval is strictly increasing
with unique grouping dimensions and operation kinds. The low-level filter
records remain available for codec fixtures and generated protocol values;
application Workflow code should use the builders so invalid queries cannot
reach Activity scheduling.

~~~ocaml
let* provider_filter =
  Query.Filter.provider_status ~include_healthy:false ~page_size:100 ()
in
Query.execute ~operation_key ~context (Query.Provider_status provider_filter)
~~~

`start` keeps the Activity's Temporal error as the Future error and returns
the protocol-kind matcher result as its successful value. This preserves a
typed error for a mismatched closed result without raising from a workflow
callback; the current Temporal SDK intentionally exposes no public operation
for converting a successful Future value into a Future error.

Call-site type inference prevents asking a provider-status query and treating
the answer as a spend summary:

~~~ocaml
let result =
  Query.execute
    ~operation_key:(Operation_key.of_string "budget-check-481")
    ~context
    (Query.Budget_status {
       policy_key = None;
       active_at = None;
       include_windows = true;
     })
in
match result with
| Ok { value = budget; cost = Exact_cost { actual_cost_usd; _ }; _ } ->
    assert (Usd_decimal.compare actual_cost_usd Usd_decimal.zero = 0);
    inspect_budget budget
| Ok { cost = Unknown_cost { reason }; _ } -> report_unknown_cost reason
| Error error -> handle_temporal_error error
~~~

Internally, **execute** existentially packages the wire request, schedules the
single Activity, decodes the closed response, and pattern-matches the matching
result constructor. An impossible mismatch is returned as a protocol
**Temporal.Error.t** with safe details. It never uses **Obj.magic**, polymorphic
variants with catch-all values, or an unchecked JSON cast.

The facade also enforces the pagination association after an injected
dispatcher returns. A `next_cursor` from a paginated response must carry the
same query kind as the GADT constructor; budget and spend responses must not
carry one. This check is deliberately duplicated at the ergonomic boundary so
tests and custom dispatchers cannot bypass the wire codec's cursor invariant.
`Query.start` performs the corresponding input check before scheduling an
Activity and returns the validation error in its typed result value.

Pagination remains typed: the cursor can be supplied only to the same query
constructor/filter digest. The server is authoritative for cursor binding; the
OCaml client also retains query kind in the cursor wrapper to reject obvious
cross-kind reuse before dispatch. **Budget_status** and **Spend_summary** are
bounded snapshots rather than pages, so their filters and responses must not
carry a cursor.

## Activity descriptors and Workflow determinism

The invocation module exposes three exact names:

~~~ocaml
val generate_v1_activity :
  (generate_request, generate_response) Temporal.Activity.t

val compact_v1_activity :
  (compact_request, compaction_response) Temporal.Activity.t

val query_v1_activity :
  (query_envelope, query_response) Temporal.Activity.t
~~~

The direct-style helpers call **Temporal.Activity.execute**. Their **start_***
forms call **Temporal.Activity.start** and return workflow-owned futures so a
caller can record sibling Activity commands before awaiting any result. A
direct helper is exactly **Temporal.Future.await (start_... request)**; it does
not use an OCaml thread, Lwt promise, or process-global scheduler.

## End-to-end mock OCaml Workflow

The following is the required shape of the downstream compile example. The
checked-in `ocaml/consumer_workflow_smoke` Dune project compiles this sample
against the installed package, importing only **Llm_temporal**. Names in this
sample are the public API, not private helper aliases. Application-owned
Workflow input/output codec definitions are omitted; the **Llm_temporal**
calls and Temporal scheduling primitives are concrete. The fixture is
compile-only: it does not contact a Temporal server or provider.

The example exercises all three Activities and all five typed Query variants:

- query credit and budget state before spending;
- create a cached root Generate;
- fork one checkpoint into three concurrent cached variants;
- compact one chosen branch explicitly;
- continue from the compaction checkpoint with application tools and structured
  output restored by Generate; and
- query provider status, model inventory, and exact spend afterward.

~~~ocaml
open Llm_temporal

let ( let* ) = Result.bind

type workflow_input = {
  run_key : string;
  context : request_context;
  model : Model_selector.t;
  question : string;
  branch_instruction : string;
  tools : tool list;
  output : output_config;
  spend_from : Ptime.t;
  spend_until : Ptime.t;
}

type workflow_output = {
  final_turn : generate_response;
  branch_checkpoints : checkpoint_metadata list;
  compaction : compaction_response;
  provider_status : provider_status_page;
  model_inventory : model_inventory_page;
  credit_status : credit_status_page;
  budget_status : budget_status;
  spend_summary : spend_summary;
}

let operation_key input suffix =
  Operation_key.of_string (input.run_key ^ ":" ^ suffix)

let decimal_constant value =
  match Decimal.of_string value with
  | Ok value -> value
  | Error _ -> invalid_arg "invalid source-code decimal constant"

let cache_constant variant =
  match Cache_policy.accept_up_to
          ~max_age_seconds:15_552_000L (* 180 days *)
          ~variant () with
  | Ok value -> value
  | Error _ -> invalid_arg "invalid source-code cache policy"

let cache_0 = cache_constant Int32.zero
let cache_1 = cache_constant Int32.one
let cache_2 = cache_constant (Int32.of_int 2)
let branch_temperature = decimal_constant "0.7"

let message text =
  Message { actor = Human; content = [ Text text ] }

let exactly_three_results = function
  | [ branch_0; branch_1; branch_2 ] ->
      let* branch_0 = branch_0 in
      let* branch_1 = branch_1 in
      let* branch_2 = branch_2 in
      Ok (branch_0, branch_1, branch_2)
  | _ ->
      invalid_arg "Temporal.Future.all changed result cardinality"

let claim_workflow ~input_codec ~output_codec ~task_queue =
  Temporal.Workflow.define
    ~name:"claims.cached-branching.v1"
    ~input:input_codec
    ~output:output_codec
    (fun input ->
      (* Queries are Activities too. No database/provider read occurs in
         Workflow code. A refresh request remains inside the Go Activity. *)
      let* credit =
        Query.execute ~task_queue
          ~operation_key:(operation_key input "credit-before")
          ~context:input.context
          (Query.Credit_status {
             provider = None;
             endpoint = None;
             include_ok = false;
             refresh_if_older_than_seconds = Some 300L;
             page_size = 100;
             cursor = None;
           })
      in
      let* budget =
        Query.execute ~task_queue
          ~operation_key:(operation_key input "budget-before")
          ~context:input.context
          (Query.Budget_status {
             policy_key = None;
             active_at = None;
             include_windows = true;
           })
      in

      let root =
        Conversation.root
          ~context:input.context
          ~model:input.model
          ~settings:(Settings.make
            ~temperature:(decimal_constant "0")
            ~tools:input.tools
            ~tool_policy:{ choice = Auto; parallel = false }
            ~output:input.output
            ())
          ()
      in
      let* first =
        Conversation.respond ~task_queue
          ~operation_key:(operation_key input "turn-1")
          ~cache:cache_0
          ~append:[ message input.question ]
          root
      in

      (* All children name the same immutable parent. Non-zero temperature
         permits explicit variants 0, 1, and 2. Starting every Activity before
         awaiting enables deterministic Temporal fan-out. *)
      let branch_patch =
        Settings.Patch.keep
        |> Settings.Patch.set_temperature branch_temperature
        |> Settings.Patch.set_reasoning_effort High
      in
      let start_branch suffix cache =
        Conversation.start_respond ~task_queue
          ~operation_key:(operation_key input suffix)
          ~settings_patch:branch_patch
          ~cache
          ~append:[ message input.branch_instruction ]
          (Conversation.fork first.conversation)
      in
      let branch_0 = start_branch "branch-0" cache_0 in
      let branch_1 = start_branch "branch-1" cache_1 in
      let branch_2 = start_branch "branch-2" cache_2 in
      let* branch_results =
        Temporal.Future.await
          (Temporal.Future.all [ branch_0; branch_1; branch_2 ])
      in
      let* (branch_0, branch_1, branch_2) =
        exactly_three_results branch_results
      in
      let branches = [ branch_0; branch_1; branch_2 ] in
      let chosen = branch_0 in

      (* Compact accepts no application tool or structured-output arguments.
         The worker disables both for summarization while retaining the
         application settings on the returned checkpoint. *)
      let* (compaction, compacted) =
        Conversation.compact ~task_queue
          ~operation_key:(operation_key input "compact-chosen")
          ~cache:cache_0
          chosen.conversation
      in
      let* final =
        Conversation.respond ~task_queue
          ~operation_key:(operation_key input "after-compaction")
          ~cache:cache_0
          ~append:[ message "Return the final structured answer." ]
          compacted
      in

      let* provider_status =
        Query.execute ~task_queue
          ~operation_key:(operation_key input "provider-status-after")
          ~context:input.context
          (Query.Provider_status {
             provider = None;
             endpoint = None;
             availability = None;
             include_healthy = false;
             refresh_if_older_than_seconds = None;
             page_size = 100;
             cursor = None;
           })
      in
      let* model_inventory =
        Query.execute ~task_queue
          ~operation_key:(operation_key input "model-inventory-after")
          ~context:input.context
          (Query.Model_inventory {
             provider = None;
             endpoint = None;
             model_prefix = None;
             lifecycle = None;
             refresh_if_older_than_seconds = None;
             page_size = 100;
             cursor = None;
           })
      in
      let* spend_summary =
        Query.execute ~task_queue
          ~operation_key:(operation_key input "spend-after")
          ~context:input.context
          (Query.Spend_summary {
             start_time = input.spend_from;
             end_time = input.spend_until;
             group_by = [ By_operation_kind; By_provider; By_model ];
             operation_kinds = [ Generate; Compact; Query ];
           })
      in
      Ok {
        final_turn = final.response;
        branch_checkpoints =
          List.map (fun branch -> branch.response.checkpoint) branches;
        compaction;
        provider_status = provider_status.value;
        model_inventory = model_inventory.value;
        credit_status = credit.value;
        budget_status = budget.value;
        spend_summary = spend_summary.value;
      })
~~~

The result matcher is exhaustive over the Activity Future's typed error channel
and treats a changed future cardinality as a source-code invariant violation.
The chosen branch is
selected by stable input order. Choosing from recorded model output is also
deterministic, but application policy should make that choice explicit and test
replay.

The positive-variant branches set temperature in the same request, so the
client can validate them locally. If the patch kept an inherited temperature,
the Go worker would still validate the materialized value. A zero-temperature
request with variant 1 or 2 is a typed validation failure and must not reach a
provider. Omitting **cache** entirely disables both exact-cache reads and
population for that operation.

### Low-level protocol access

The ergonomic modules are thin wrappers over the exported descriptors.
Advanced callers and codec tests may schedule exact wire records directly:

~~~ocaml
let invoke_generate ~task_queue request =
  Temporal.Activity.execute
    ~task_queue:(Temporal_task_queue.to_string task_queue)
    ~retry_policy:activity_retry_policy
    generate_v1_activity request

let invoke_compact_v1 ~task_queue request =
  Temporal.Activity.execute
    ~task_queue:(Temporal_task_queue.to_string task_queue)
    ~retry_policy:activity_retry_policy
    compact_v1_activity request

let invoke_query_v1 ~task_queue envelope =
  Temporal.Activity.execute
    ~task_queue:(Temporal_task_queue.to_string task_queue)
    ~retry_policy:activity_retry_policy
    query_v1_activity envelope
~~~

Application Workflows should normally prefer **Conversation** and **Query**.
Direct protocol access must not materialize inherited history/settings into the
Activity payload, poll a provider ID, interpret query JSON with an open cast,
or add Workflow-level retries. The same operation key is reused when Temporal
replays one logical call; every fork and distinct query gets a new stable key.

The package reuses validated Activity options and the current one-attempt SDK
policy; the Go operation ledger owns durable retry/recovery. It does not poll a
provider from Workflow code. A Go Activity retry sees **provider_pending** and
continues polling the persisted provider ID.

Every builder is pure. Current time, randomness, environment variables, model
discovery, status refresh, and cache selection occur in the Go Activity, not
OCaml Workflow code. Callers supply stable operation keys and explicit
variants. Decoding a response is deterministic for its payload. A future
non-USD provider would require the separately approved worker-owned FX design;
no FX input or currency value is exposed to Workflow code now.

## Error surface

Keep **Temporal.Error.t** as the invocation error surface, with helpers that
recognize new safe application types:

- invalid checkpoint/patch/variant;
- operation conflict;
- cache/state unavailable;
- cache fill wait expired;
- checkpoint pinned/corrupt/expired;
- compaction failed;
- provider pending/ambiguous;
- unsupported query refresh/inventory;
- invalid query cursor;
- budget wait; and
- protocol tag mismatch.

Errors never embed raw prompt, output, provider error body, poll ID, cache
fingerprint, or database value.

## OCaml implementation order

1. Regenerate/freeze Generate v1, Compact v1, Query v1, and decimal JSON
   fixtures from
   Go before changing public OCaml types.
2. Add nominal checkpoint/cursor/query-execution IDs and exact **Usd_decimal** with
   property tests.
3. Replace generic currency/microUSD response fields with USD decimal fields in
   protocol models, codecs, README, and fixtures.
4. Add patch/cache/checkpoint/compaction wire records and exhaustive validation.
5. Add the three Activity descriptors and low-level invoke functions.
6. Implement immutable Conversation and settings/cache builders.
7. Implement every closed query filter/result record and codec.
8. Implement the Query GADT and safe internal tag matcher.
9. Add the typed one-shot `Generate` facade on the v1 Activity without a second
   package; preserve legacy names until a breaking-release decision.
10. Update Dune module lists/interfaces, opam metadata if a decimal dependency is
    selected, examples, and downstream compile fixtures.

## OCaml acceptance gates

- Go and OCaml golden JSON is byte-equivalent after canonicalization for every
  request/result kind and error.
- Omitted/Set/Clear survive round trips as three distinct values.
- Inherited temperature zero plus positive variant is rejected by the server;
  locally known zero is rejected by the builder.
- No float or currency field exists in public money types.
- Decimal values with 18 fractional digits round-trip exactly.
- The same immutable parent can produce three distinct child conversations.
- Cache omission emits no cache object; variant zero is omitted or encoded
  consistently with the canonical fixture.
- Compact requests contain neither application tools nor structured output;
  the next Generate restores them exactly.
- Every Query GADT constructor decodes only its associated result; mismatch and
  unknown tags fail safely.
- Query pagination preserves its static result type.
- Activity names and payload codecs match the Go worker constants exactly.
- The one-shot `Generate` sample uses exact v1 types and no second OCaml
  package/import path is introduced; legacy names remain covered by a
  compatibility smoke assertion.
- Dune builds libraries, interfaces, examples, unit tests, and an external
  downstream package under the pinned Temporal SDK version.
