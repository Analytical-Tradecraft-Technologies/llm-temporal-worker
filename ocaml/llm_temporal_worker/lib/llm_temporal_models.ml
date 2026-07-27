module Identifier = Llm_temporal_identifier

module type Opaque_identifier = sig
  type t = private string
  val of_string : string -> t
  val to_string : t -> string
end

module Operation_key = Identifier.Operation_key
module Operation_id = Identifier.Operation_id
module Model_selector = Identifier.Model_selector
module Resolved_model_id = Identifier.Resolved_model_id
module Endpoint_id = Identifier.Endpoint_id
module Route_id = Identifier.Route_id
module Continuation_handle = Identifier.Continuation_handle
module Provider_id = Identifier.Provider_id
module Endpoint_family = Identifier.Endpoint_family
module Api_family = Identifier.Api_family
module Provider_response_id = Identifier.Provider_response_id
module Provider_request_id = Identifier.Provider_request_id
module Provider_generation_id = Identifier.Provider_generation_id
module Tool_name = Identifier.Tool_name
module Tool_call_id = Identifier.Tool_call_id
module Tenant_id = Identifier.Tenant_id
module Project_id = Identifier.Project_id
module Actor_id = Identifier.Actor_id
module Blob_digest = Identifier.Blob_digest
module Diagnostic_code = Identifier.Diagnostic_code
module Cost_catalog_version = Identifier.Cost_catalog_version
module Temporal_task_queue = Identifier.Temporal_task_queue
module Query_execution_id = Identifier.Query_execution_id
module Budget_policy_key = Identifier.Budget_policy_key
module Budget_generation_id = Identifier.Budget_generation_id
module Provider_model_id = Identifier.Provider_model_id
module Model_prefix = Identifier.Model_prefix
module Window_key = Identifier.Window_key
module Checkpoint = Identifier.Checkpoint
module Query_cursor = Identifier.Query_cursor
module Budget_stream_id = Identifier.Budget_stream_id
module Sha256_digest = Identifier.Sha256_digest
module Usd_decimal = Llm_temporal_usd_decimal

type service_class = Economy | Standard | Priority
type validation_error = string
type portability = Strict | Best_effort
type actor = Human | Model
type instruction_level = Application | Policy
type response_status = Completed | Tool_calls | Refused | Length | Content_filtered
type output_kind = Output_text | Output_json | Output_json_schema
type reasoning_mode = Provider_default | Reasoning_disabled | Adaptive | Reasoning_enabled
type reasoning_effort = Effort_default | Minimal | Low | Medium | High | Maximum
type reasoning_summary = Summary_default | Summary_none | Summary_auto | Concise | Detailed
type cost_status = Cost_known | Cost_unknown

type request_context = {
  tenant : Tenant_id.t option;
  project : Project_id.t option;
  actor : Actor_id.t option;
  tags : (string * string) list;
}

type provider_state = {
  provider : Provider_id.t;
  endpoint_family : Endpoint_family.t;
  media_type : string;
  opaque : string;
}

type media_source =
  | Url of string
  | Bytes of string
  | Blob of {
      locator : string;
      digest : Blob_digest.t;
      byte_length : int64;
      media_type : string;
    }

type content =
  | Text of string
  | Image of { media_type : string; source : media_source; detail : string option }
  | Document of { media_type : string; source : media_source; title : string option }
  | Json of Yojson.Safe.t
  | Refusal of { message : string; provider_code : string option }
  | Content_provider_state of provider_state

type message = { actor : actor; content : content list }
type instruction =
  | Text_instruction of { level : instruction_level; text : string }
  | Parts_instruction of { level : instruction_level; content : content list }
type reference = { uri : string; metadata : (string * Yojson.Safe.t) list }
type item =
  | Message of message
  | Tool_call of { id : Tool_call_id.t; name : Tool_name.t; arguments : Yojson.Safe.t }
  | Tool_result of { call_id : Tool_call_id.t; name : Tool_name.t option; content : content list; is_error : bool }
  | Provider_state of provider_state
  | Reference of reference

type tool_kind = Function | Provider | Remote_mcp
type function_tool = {
  kind : tool_kind;
  name : Tool_name.t;
  description : string;
  input_schema : Yojson.Safe.t;
  output_schema : Yojson.Safe.t option;
}

type tool_choice = Auto | None_allowed | Required | Named of Tool_name.t
type tool_policy = { choice : tool_choice; parallel : bool }
type output_format = Text_format | Json_format | Json_schema_format of { name : string; description : string option; schema : Yojson.Safe.t; strict : bool }
type output_spec = { max_tokens : int option; format : output_format }
type sampling = { temperature : float option; top_p : float option; top_k : int option; seed : int64 option; presence_penalty : float option; frequency_penalty : float option; stop_sequences : string list option }
type reasoning = { mode : reasoning_mode; effort : reasoning_effort; token_budget : int option; summary : reasoning_summary }
type continuation = {
  handle : Continuation_handle.t;
  endpoint_id : Endpoint_id.t option;
  model : Resolved_model_id.t option;
  expires_at : string option;
  pinned : bool;
  provider_state : provider_state list option;
}

type request = {
  operation_key : Operation_key.t;
  context : request_context option;
  model : Model_selector.t;
  service_class : service_class;
  service_class_fallbacks : service_class list;
  portability : portability;
  instructions : instruction list;
  input : item list;
  tools : function_tool list;
  tool_policy : tool_policy;
  output : output_spec option;
  sampling : sampling option;
  reasoning : reasoning option;
  continuation : continuation option;
  extensions : (string * Yojson.Safe.t) list;
}

module Request = struct
  type t = request

  let make
      ~operation_key
      ~model
      ~service_class
      ~input
      ?context
      ?(service_class_fallbacks = [])
      ?(portability = Strict)
      ?(instructions = [])
      ?(tools = [])
      ?(tool_policy = { choice = Auto; parallel = false })
      ?output
      ?sampling
      ?reasoning
      ?continuation
      ?(extensions = [])
      () =
    { operation_key;
      context;
      model;
      service_class;
      service_class_fallbacks;
      portability;
      instructions;
      input;
      tools;
      tool_policy;
      output;
      sampling;
      reasoning;
      continuation;
      extensions }
end

type route = {
  route_id : Route_id.t option;
  endpoint_id : Endpoint_id.t option;
  api_family : Api_family.t option;
  requested_model : Model_selector.t option;
  resolved_model : Resolved_model_id.t option;
}
type service = { requested : service_class; attempted : service_class; actual : service_class option; provider_value : string option; fallback_index : int }
type usage = { input_tokens : int64; output_tokens : int64; reasoning_tokens : int64; cache_read_tokens : int64; cache_write_tokens : int64; provider_raw : (string * Yojson.Safe.t) list option }
type cost = { status : cost_status option; currency : string; reserved_microusd : int64; actual_microusd : int64; method_ : string; catalog_version : Cost_catalog_version.t }
type provider = { response_id : Provider_response_id.t option; request_id : Provider_request_id.t option; generation_id : Provider_generation_id.t option; finish_reason : string option; raw : (string * Yojson.Safe.t) list }
type diagnostic_severity = Info | Warning | Diagnostic_error
type diagnostic = { code : Diagnostic_code.t; message : string; severity : diagnostic_severity; path : string option; details : (string * string) list option }
type response_metadata = { operation_id : Operation_id.t option }
type response = {
  operation_key : Operation_key.t;
  operation_id : Operation_id.t option;
  status : response_status;
  output : item list;
  route : route;
  service : service;
  usage : usage;
  cost : cost;
  provider : provider;
  continuation : continuation option;
  diagnostics : diagnostic list;
  metadata : response_metadata;
}

(* -------------------------------------------------------------------------- *)
(* Checkpoint/query protocol (llm.temporal/v1).                               *)

type 'a patch = Keep | Set of 'a | Clear

type checkpoint_kind = Generation_checkpoint | Compaction_checkpoint | Cache_replay_checkpoint

type checkpoint_metadata = {
  handle : Checkpoint.t;
  parent : Checkpoint.t option;
  kind : checkpoint_kind;
  depth : int32;
}

type cache_policy = {
  max_age_seconds : int64;
  variant : int32;
}

type cache_disposition_kind = Cache_disabled | Cache_miss_populated | Cache_hit | Cache_miss_not_populated

type cache_disposition = {
  disposition : cache_disposition_kind;
  variant : int32;
  entry_age_seconds : int64 option;
}

type settled_cost =
  | Exact_cost of {
      actual_cost_usd : Usd_decimal.t;
      method_ : cost_method;
      catalog_version : Cost_catalog_version.t option;
    }
  | Unknown_cost of { reason : cost_unknown_reason }

and cost_method = Provider_reported | Catalog_usage | Control_query_zero

and cost_unknown_reason =
  | Provider_did_not_report_cost
  | Catalog_incomplete
  | State_unavailable
  | Ambiguous_dispatch

and checkpoint_provenance = Provider_provenance | Worker_cache_provenance

type provenance = {
  source : checkpoint_provenance;
  origin_operation_id : Operation_id.t option;
  policy : string option;
}

type settings_patch = {
  model : Model_selector.t patch;
  service_class : service_class patch;
  service_class_fallbacks : service_class list patch;
  portability : portability patch;
  instructions : instruction list patch;
  tools : function_tool list patch;
  tool_policy : tool_policy patch;
  output : output_spec patch;
  temperature : Usd_decimal.t patch;
  reasoning_effort : reasoning_effort patch;
  reasoning_summary : reasoning_summary patch;
  compaction_policy : Yojson.Safe.t patch;
  extensions : (string * Yojson.Safe.t) list patch;
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

type generate_response = {
  api_version : string;
  operation_key : Operation_key.t;
  operation_id : Operation_id.t;
  status : response_status;
  output : item list;
  checkpoint : checkpoint_metadata;
  cache : cache_disposition;
  route : route option;
  usage : usage option;
  cost : settled_cost;
  diagnostics : diagnostic list;
}

type compaction_policy = {
  target_tokens : int64 option;
  summary_style : summary_style option;
}

and summary_style = Concise | Balanced | Detailed

type compact_request = {
  api_version : string;
  operation_key : Operation_key.t;
  context : request_context;
  parent : Checkpoint.t;
  policy : compaction_policy option;
  cache : cache_policy option;
}

type compaction_response = {
  api_version : string;
  operation_key : Operation_key.t;
  operation_id : Operation_id.t;
  checkpoint : checkpoint_metadata;
  cache : cache_disposition;
  provenance : provenance option;
  usage : usage option;
  cost : settled_cost;
  diagnostics : diagnostic list;
}

type availability = Available | Degraded | Unavailable
(* The legacy constructor names remain source-compatible, but their wire
   spellings follow the Go query contract: [Active] means an available model
   and [Retired] means an unavailable model. [Unknown] is an explicit
   provider-reported lifecycle state, not an open-world fallback. *)
type model_lifecycle = Active | Deprecated | Retired | Unknown
type model_capability = string
type inventory_source = Provider_api_inventory | Operator_inventory | Unknown_inventory_source
type credit_state = Credit_ok | Credit_low | Credit_exhausted | Credit_unknown
type billing_state = Billing_ok | Billing_blocked | Billing_unknown
type circuit_state = Circuit_closed | Circuit_open | Circuit_half_open
type credit_evidence_source = Provider_api_evidence | Operator_evidence | Unknown_evidence
type query_source = Persisted | Persisted_and_refreshed | Redis_budget_generation
type freshness = Current | Stale | Unknown_freshness
type operation_kind = Generate | Compact | Query
type spend_group_by = By_operation_kind | By_provider | By_model
type cost_completeness = Complete_cost | Partial_cost

module Safe_metadata = struct
  type t = (string * Yojson.Safe.t) list
  let empty = []
  let of_list value = value
  let to_list value = value
end

type provider_status_filter = {
  provider : Provider_id.t option;
  endpoint : Endpoint_id.t option;
  availability : availability option;
  include_healthy : bool;
  refresh_if_older_than_seconds : int64 option;
  page_size : int;
  cursor : Query_cursor.t option;
}

type model_inventory_filter = {
  provider : Provider_id.t option;
  endpoint : Endpoint_id.t option;
  model_prefix : Model_prefix.t option;
  lifecycle : model_lifecycle option;
  refresh_if_older_than_seconds : int64 option;
  page_size : int;
  cursor : Query_cursor.t option;
}

type credit_status_filter = {
  provider : Provider_id.t option;
  endpoint : Endpoint_id.t option;
  include_ok : bool;
  refresh_if_older_than_seconds : int64 option;
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
  provider : Provider_id.t;
  endpoint : Endpoint_id.t;
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
  provider : Provider_id.t;
  endpoint : Endpoint_id.t;
  provider_model_id : Provider_model_id.t;
  display_name : string option;
  lifecycle : model_lifecycle;
  capabilities : model_capability list;
  source : inventory_source;
  complete_snapshot : bool;
  safe_metadata : Safe_metadata.t;
}

type model_inventory_page = { models : model_inventory_entry list }

type credit_status_entry = {
  provider : Provider_id.t;
  endpoint : Endpoint_id.t;
  credit_state : credit_state;
  billing_state : billing_state;
  confirmed_at : Ptime.t option;
  evidence_source : credit_evidence_source;
  safe_evidence_code : string option;
}

type credit_status_page = { endpoints : credit_status_entry list }

type budget_window_status = {
  policy_key : Budget_policy_key.t;
  window_key : Window_key.t;
  coverage_start : Ptime.t;
  coverage_end : Ptime.t;
  limit_usd : Usd_decimal.t;
  reserved_cost_usd : Usd_decimal.t;
  accounted_cost_usd : Usd_decimal.t;
  available_usd : Usd_decimal.t;
  retry_after_seconds : int64 option;
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
  provider : Provider_id.t option;
  model : Model_selector.t option;
}

type spend_bucket = {
  group : spend_group_key;
  known_actual_cost_usd : Usd_decimal.t;
  exact_operation_count : int64;
  unknown_operation_count : int64;
  completeness : cost_completeness;
}

type spend_summary = {
  start_time : Ptime.t;
  end_time : Ptime.t;
  buckets : spend_bucket list;
}

type query_request =
  | Provider_status_request of provider_status_filter
  | Model_inventory_request of model_inventory_filter
  | Credit_status_request of credit_status_filter
  | Budget_status_request of budget_status_filter
  | Spend_summary_request of spend_summary_filter

type query_envelope = {
  api_version : string;
  operation_key : Operation_key.t;
  context : request_context;
  query : query_request;
}

type query_result =
  | Provider_status_result of provider_status_page
  | Model_inventory_result of model_inventory_page
  | Credit_status_result of credit_status_page
  | Budget_status_result of budget_status
  | Spend_summary_result of spend_summary

type query_response = {
  api_version : string;
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
