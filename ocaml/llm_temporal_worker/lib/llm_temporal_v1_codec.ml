open Llm_temporal_models

let generate_api_version = "llm.temporal/v1"
let compact_api_version = "llm.temporal/compact/v1"
let query_api_version = "llm.temporal/query/v1"

let errorf format = Printf.ksprintf (fun message -> Temporal.Error.codec ~message) format
let ( let* ) = Result.bind
let ( >>= ) value f = Result.bind value f

let assoc context = function
  | `Assoc fields -> Ok fields
  | _ -> Error (errorf "%s must be an object" context)

let required context name fields =
  match List.assoc_opt name fields with
  | Some value -> Ok value
  | None -> Error (errorf "%s is missing required field %S" context name)

let optional name fields = List.assoc_opt name fields

let closed context allowed value =
  let* fields = assoc context value in
  let seen = Hashtbl.create (List.length fields) in
  let rec check = function
    | [] -> Ok fields
    | (name, _) :: _rest when Hashtbl.mem seen name -> Error (errorf "%s contains duplicate field %S" context name)
    | (name, _) :: _rest when not (List.mem name allowed) -> Error (errorf "%s contains unknown field %S" context name)
    | (name, _) :: rest -> Hashtbl.add seen name (); check rest
  in
  check fields

let string context = function
  | `String value -> Ok value
  | _ -> Error (errorf "%s must be a string" context)

let validated context parse value =
  match parse value with
  | Ok value -> Ok value
  | Error message -> Error (errorf "%s: %s" context message)

let bool context = function
  | `Bool value -> Ok value
  | _ -> Error (errorf "%s must be a boolean" context)

let int64 context = function
  | `Int value -> Ok (Int64.of_int value)
  | `Intlit value -> (try Ok (Int64.of_string value) with Failure _ -> Error (errorf "%s must be an integer" context))
  | _ -> Error (errorf "%s must be an integer" context)

let int32 context value =
  let* value = int64 context value in
  if value < Int64.of_int32 Int32.min_int || value > Int64.of_int32 Int32.max_int then Error (errorf "%s is outside int32 range" context)
  else Ok (Int64.to_int32 value)

let nonempty context value = if value = "" then Error (errorf "%s must not be empty" context) else Ok value
let nonnegative context value = if value < 0L then Error (errorf "%s must not be negative" context) else Ok ()

let option_field name encode = function None -> [] | Some value -> [name, encode value]
let list context = function `List values -> Ok values | _ -> Error (errorf "%s must be an array" context)

let parse_json decoder bytes =
  try
    let value = Yojson.Safe.from_string (Bytes.to_string bytes) in
    decoder value
  with
  | Yojson.Json_error message -> Error (errorf "invalid JSON: %s" message)
  | Failure message -> Error (errorf "invalid JSON: %s" message)

let to_bytes value =
  try Ok (Bytes.of_string (Yojson.Safe.to_string value)) with
  | Yojson.Json_error message -> Error (errorf "failed to encode JSON: %s" message)

let map_result f values =
  let rec loop acc = function
    | [] -> Ok (List.rev acc)
    | value :: rest -> let* value = f value in loop (value :: acc) rest
  in
  loop [] values

let service_class_to_json = function Economy -> `String "economy" | Standard -> `String "standard" | Priority -> `String "priority"
let service_class_of_json context = function
  | `String "economy" -> Ok Economy
  | `String "standard" -> Ok Standard
  | `String "priority" -> Ok Priority
  | _ -> Error (errorf "%s has an invalid service class" context)

let portability_to_json = function Strict -> `String "strict" | Best_effort -> `String "best_effort"
let portability_of_json context = function
  | `String "strict" -> Ok Strict
  | `String "best_effort" -> Ok Best_effort
  | _ -> Error (errorf "%s has an invalid portability" context)

let response_status_to_json = function
  | Completed -> `String "completed" | Tool_calls -> `String "tool_calls" | Refused -> `String "refused" | Length -> `String "length" | Content_filtered -> `String "content_filtered"
let response_status_of_json context = function
  | `String "completed" -> Ok Completed
  | `String "tool_calls" -> Ok Tool_calls
  | `String "refused" -> Ok Refused
  | `String "length" -> Ok Length
  | `String "content_filtered" -> Ok Content_filtered
  | _ -> Error (errorf "%s has an invalid response status" context)

let checkpoint_kind_to_json = function
  | Generation_checkpoint -> `String "generation"
  | Compaction_checkpoint -> `String "compaction"
  | Cache_replay_checkpoint -> `String "cache_replay"
let checkpoint_kind_of_json context = function
  | `String "generation" -> Ok Generation_checkpoint
  | `String "compaction" -> Ok Compaction_checkpoint
  | `String "cache_replay" -> Ok Cache_replay_checkpoint
  | _ -> Error (errorf "%s has an invalid checkpoint kind" context)

let cache_disposition_to_json value =
  let disposition = match value.disposition with
    | Cache_disabled -> "disabled" | Cache_miss_populated -> "miss_populated" | Cache_hit -> "hit" | Cache_miss_not_populated -> "miss_not_populated"
  in
  `Assoc (["disposition", `String disposition; "variant", `Intlit (Int32.to_string value.variant)] @ option_field "entry_age_seconds" (fun value -> `Intlit (Int64.to_string value)) value.entry_age_seconds)

let cache_disposition_of_json context value =
  let* fields = closed context ["disposition"; "variant"; "entry_age_seconds"] value in
  let* disposition = required context "disposition" fields in
  let* disposition = match disposition with
    | `String "disabled" -> Ok Cache_disabled | `String "miss_populated" -> Ok Cache_miss_populated | `String "hit" -> Ok Cache_hit | `String "miss_not_populated" -> Ok Cache_miss_not_populated
    | _ -> Error (errorf "%s has an invalid disposition" context)
  in
  let* variant = required context "variant" fields in
  let* variant = int32 (context ^ ".variant") variant in
  let* () = nonnegative (context ^ ".variant") (Int64.of_int32 variant) in
  let* entry_age_seconds = match optional "entry_age_seconds" fields with None | Some `Null -> Ok None | Some value -> let* age = int64 (context ^ ".entry_age_seconds") value in let* () = nonnegative (context ^ ".entry_age_seconds") age in Ok (Some age) in
  Ok { disposition; variant; entry_age_seconds }

let cache_policy_to_json value =
  `Assoc (["max_age_seconds", `Intlit (Int64.to_string value.max_age_seconds)] @ if value.variant = 0l then [] else ["variant", `Intlit (Int32.to_string value.variant)])

let cache_policy_of_json context value =
  let* fields = closed context ["max_age_seconds"; "variant"] value in
  let* age = required context "max_age_seconds" fields >>= int64 (context ^ ".max_age_seconds") in
  let* () = if age < 1L || age > 31536000L then Error (errorf "%s must be between 1 and 31536000" (context ^ ".max_age_seconds")) else Ok () in
  let* variant = match optional "variant" fields with None -> Ok 0l | Some value -> int32 (context ^ ".variant") value in
  let* () = nonnegative (context ^ ".variant") (Int64.of_int32 variant) in
  Ok { max_age_seconds = age; variant }

let usd_to_json value = `String (Usd_decimal.to_string value)
let usd_of_json context value =
  let* value = string context value in
  match Usd_decimal.of_string value with Ok value -> Ok value | Error message -> Error (errorf "%s: %s" context message)

let cost_method_to_string = function Provider_reported -> "provider_reported" | Catalog_usage -> "catalog_usage" | Control_query_zero -> "control_query_zero"
let cost_method_of_string context = function
  | "provider_reported" -> Ok Provider_reported | "catalog_usage" -> Ok Catalog_usage | "control_query_zero" -> Ok Control_query_zero
  | _ -> Error (errorf "%s has an invalid cost method" context)
let unknown_reason_to_string = function Provider_did_not_report_cost -> "provider_did_not_report_cost" | Catalog_incomplete -> "catalog_incomplete" | State_unavailable -> "state_unavailable" | Ambiguous_dispatch -> "ambiguous_dispatch"
let unknown_reason_of_string context = function
  | "provider_did_not_report_cost" -> Ok Provider_did_not_report_cost | "catalog_incomplete" -> Ok Catalog_incomplete | "state_unavailable" -> Ok State_unavailable | "ambiguous_dispatch" -> Ok Ambiguous_dispatch
  | _ -> Error (errorf "%s has an invalid unknown reason" context)

let settled_cost_to_json = function
  | Exact_cost { actual_cost_usd; method_; catalog_version } ->
      `Assoc (["status", `String "exact"; "actual_cost_usd", usd_to_json actual_cost_usd; "method", `String (cost_method_to_string method_)] @ option_field "catalog_version" (fun value -> `String (Cost_catalog_version.to_string value)) catalog_version)
  | Unknown_cost { reason } -> `Assoc ["status", `String "unknown"; "actual_cost_usd", `Null; "unknown_reason", `String (unknown_reason_to_string reason)]

let settled_cost_of_json context value =
  let* fields = closed context ["status"; "actual_cost_usd"; "method"; "catalog_version"; "unknown_reason"] value in
  let* status = required context "status" fields >>= string (context ^ ".status") in
  match status with
  | "exact" ->
      let* actual_cost_usd = required context "actual_cost_usd" fields >>= usd_of_json (context ^ ".actual_cost_usd") in
      let* method_ = required context "method" fields >>= string (context ^ ".method") >>= cost_method_of_string (context ^ ".method") in
      let* catalog_version = match optional "catalog_version" fields with None | Some `Null -> Ok None | Some value -> let* value = string (context ^ ".catalog_version") value in Ok (Some (Cost_catalog_version.of_string value)) in
      Ok (Exact_cost { actual_cost_usd; method_; catalog_version })
  | "unknown" ->
      let* () = match optional "actual_cost_usd" fields with None | Some `Null -> Ok () | Some _ -> Error (errorf "%s unknown cost must have null actual_cost_usd" context) in
      let* reason = required context "unknown_reason" fields >>= string (context ^ ".unknown_reason") >>= unknown_reason_of_string (context ^ ".unknown_reason") in
      let* () = match optional "method" fields with None | Some `Null -> Ok () | Some _ -> Error (errorf "%s unknown cost must not have method" context) in
      Ok (Unknown_cost { reason })
  | _ -> Error (errorf "%s has an invalid cost status" context)

let context_to_v1_json context =
  let one name = function Some value -> Ok (name, `String value) | None -> Error (errorf "context.%s is required" name) in
  let* tenant = one "tenant" (Option.map Tenant_id.to_string context.tenant) in
  let* project = one "project" (Option.map Project_id.to_string context.project) in
  let* actor = one "actor" (Option.map Actor_id.to_string context.actor) in
  Ok (`Assoc [tenant; project; actor])

let context_of_v1_json value =
  let* fields = closed "context" ["tenant"; "project"; "actor"] value in
  let* tenant = required "context" "tenant" fields >>= string "context.tenant" >>= fun value -> nonempty "context.tenant" value in
  let* project = required "context" "project" fields >>= string "context.project" >>= fun value -> nonempty "context.project" value in
  let* actor = required "context" "actor" fields >>= string "context.actor" >>= fun value -> nonempty "context.actor" value in
  Ok { tenant = Some (Tenant_id.of_string tenant); project = Some (Project_id.of_string project); actor = Some (Actor_id.of_string actor); tags = [] }

let patch_to_json encode = function Keep -> None | Set value -> Some (`Assoc ["set", encode value]) | Clear -> Some (`Assoc ["clear", `Bool true])
let patch_of_json context decode value =
  let* fields = closed context ["set"; "clear"] value in
  match optional "set" fields, optional "clear" fields with
  | Some value, None -> let* value = decode (context ^ ".set") value in Ok (Set value)
  | None, Some (`Bool true) -> Ok Clear
  | None, Some _ -> Error (errorf "%s.clear must be true" context)
  | None, None -> Error (errorf "%s must contain set or clear" context)
  | Some _, Some _ -> Error (errorf "%s must not contain set and clear" context)

let empty_settings_patch = { model = Keep; service_class = Keep; service_class_fallbacks = Keep; portability = Keep; instructions = Keep; tools = Keep; tool_policy = Keep; output = Keep; temperature = Keep; reasoning_effort = Keep; reasoning_summary = Keep; compaction_policy = Keep; extensions = Keep }

let output_to_json = Llm_temporal_codec.output_to_json
let instruction_to_json = Llm_temporal_codec.instruction_to_json
let item_to_json = Llm_temporal_codec.item_to_json
let tool_to_json = Llm_temporal_codec.tool_to_json

let settings_patch_to_json (value : settings_patch) =
  let fields = [] in
  let add name encode patch fields = match patch_to_json encode patch with None -> fields | Some value -> fields @ [name, value] in
  let fields = add "model" (fun value -> `String (Model_selector.to_string value)) value.model fields in
  let fields = add "service_class" service_class_to_json value.service_class fields in
  let fields = add "service_class_fallbacks" (fun values -> `List (List.map service_class_to_json values)) value.service_class_fallbacks fields in
  let fields = add "portability" portability_to_json value.portability fields in
  let fields = add "instructions" (fun values -> `List (List.map instruction_to_json values)) value.instructions fields in
  let fields = add "tools" (fun values -> `List (List.map tool_to_json values)) value.tools fields in
  let fields = add "tool_policy" Llm_temporal_codec.policy_to_json value.tool_policy fields in
  let fields = add "output" output_to_json value.output fields in
  let fields = add "temperature" usd_to_json value.temperature fields in
  let fields = add "reasoning_effort" (fun value -> `String (match value with Effort_default -> "provider_default" | Minimal -> "minimal" | Low -> "low" | Medium -> "medium" | High -> "high" | Maximum -> "maximum")) value.reasoning_effort fields in
  let fields = add "reasoning_summary" (fun value -> `String (match value with Summary_default -> "provider_default" | Summary_none -> "none" | Summary_auto -> "auto" | Concise -> "concise" | Detailed -> "detailed")) value.reasoning_summary fields in
  let fields = add "compaction_policy" (fun value -> value) value.compaction_policy fields in
  let fields = add "extensions" (fun value -> `Assoc value) value.extensions fields in
  `Assoc fields

let settings_patch_of_json value =
  let* fields = closed "settings_patch" ["model"; "service_class"; "service_class_fallbacks"; "portability"; "instructions"; "tools"; "tool_policy"; "output"; "temperature"; "reasoning_effort"; "reasoning_summary"; "compaction_policy"; "extensions"] value in
  let get name decode = match optional name fields with None -> Ok Keep | Some value -> patch_of_json ("settings_patch." ^ name) decode value in
  let* model = get "model" (fun context value -> string context value >>= fun value -> nonempty context value >>= fun value -> Ok (Model_selector.of_string value)) in
  let* service_class = get "service_class" service_class_of_json in
  let* service_class_fallbacks = get "service_class_fallbacks" (fun context value -> list context value >>= map_result (service_class_of_json context)) in
  let* portability = get "portability" portability_of_json in
  let* instructions = get "instructions" (fun context value -> list context value >>= map_result (fun value -> Llm_temporal_codec.instruction_of_json value)) in
  let* tools = get "tools" (fun context value -> list context value >>= map_result (fun value -> Llm_temporal_codec.tool_of_json value)) in
  let* tool_policy = get "tool_policy" (fun _ value -> Llm_temporal_codec.policy_of_json value) in
  let* output = get "output" (fun _ value -> Llm_temporal_codec.output_of_json value) in
  let* temperature = get "temperature" (fun context value -> usd_of_json context value) in
  let effort context = function `String "provider_default" -> Ok Effort_default | `String "minimal" -> Ok Minimal | `String "low" -> Ok Low | `String "medium" -> Ok Medium | `String "high" -> Ok High | `String "maximum" -> Ok Maximum | _ -> Error (errorf "%s has an invalid reasoning effort" context) in
  let summary context = function `String "provider_default" -> Ok Summary_default | `String "none" -> Ok Summary_none | `String "auto" -> Ok Summary_auto | `String "concise" -> Ok Concise | `String "detailed" -> Ok Detailed | _ -> Error (errorf "%s has an invalid reasoning summary" context) in
  let* reasoning_effort = get "reasoning_effort" effort in
  let* reasoning_summary = get "reasoning_summary" summary in
  let* compaction_policy = get "compaction_policy" (fun context value -> let* fields = assoc context value in Ok (`Assoc fields)) in
  let* extensions = get "extensions" (fun context value -> assoc context value) in
  Ok { model; service_class; service_class_fallbacks; portability; instructions; tools; tool_policy; output; temperature; reasoning_effort; reasoning_summary; compaction_policy; extensions }

let settings_patch_is_empty value = value = empty_settings_patch

let checkpoint_to_json value =
  `Assoc (["handle", `String (Checkpoint.to_string value.handle); "kind", checkpoint_kind_to_json value.kind; "depth", `Intlit (Int32.to_string value.depth)] @ option_field "parent" (fun parent -> `String (Checkpoint.to_string parent)) value.parent)

let checkpoint_of_json context value =
  let* fields = closed context ["handle"; "parent"; "kind"; "depth"] value in
  let* handle = required context "handle" fields >>= string (context ^ ".handle") >>= fun value -> nonempty (context ^ ".handle") value in
  let* kind = required context "kind" fields >>= checkpoint_kind_of_json (context ^ ".kind") in
  let* depth = required context "depth" fields >>= int32 (context ^ ".depth") in
  let* () = nonnegative (context ^ ".depth") (Int64.of_int32 depth) in
  let* parent = match optional "parent" fields with None | Some `Null -> Ok None | Some value -> let* value = string (context ^ ".parent") value in let* value = validated (context ^ ".parent") Checkpoint.of_string value in Ok (Some value) in
  let* handle = validated (context ^ ".handle") Checkpoint.of_string handle in
  Ok { handle; parent; kind; depth }

let usage_to_json (value : usage) =
  `Assoc ["input_tokens", `Intlit (Int64.to_string value.input_tokens); "output_tokens", `Intlit (Int64.to_string value.output_tokens); "reasoning_tokens", `Intlit (Int64.to_string value.reasoning_tokens); "cache_read_tokens", `Intlit (Int64.to_string value.cache_read_tokens); "cache_write_tokens", `Intlit (Int64.to_string value.cache_write_tokens)]

let usage_of_json context value =
  let* fields = closed context ["input_tokens"; "output_tokens"; "reasoning_tokens"; "cache_read_tokens"; "cache_write_tokens"] value in
  let get name = required context name fields >>= int64 (context ^ "." ^ name) >>= fun value -> let* () = nonnegative (context ^ "." ^ name) value in Ok value in
  let* input_tokens = get "input_tokens" in let* output_tokens = get "output_tokens" in let* reasoning_tokens = get "reasoning_tokens" in let* cache_read_tokens = get "cache_read_tokens" in let* cache_write_tokens = get "cache_write_tokens" in
  Ok { input_tokens; output_tokens; reasoning_tokens; cache_read_tokens; cache_write_tokens; provider_raw = None }

let diagnostic_to_json (value : diagnostic) =
  let severity = match value.severity with Info -> "info" | Warning -> "warning" | Diagnostic_error -> "error" in
  `Assoc (["code", `String (Diagnostic_code.to_string value.code); "severity", `String severity; "message", `String value.message] @ option_field "path" (fun value -> `String value) value.path)

let diagnostic_of_json context value =
  let* fields = closed context ["code"; "severity"; "message"; "path"] value in
  let* code = required context "code" fields >>= string (context ^ ".code") >>= fun value -> nonempty (context ^ ".code") value in
  let* severity = required context "severity" fields >>= string (context ^ ".severity") >>= fun value -> match value with "info" -> Ok Info | "warning" -> Ok Warning | "error" -> Ok Diagnostic_error | _ -> Error (errorf "%s.severity is invalid" context) in
  let* message = required context "message" fields >>= string (context ^ ".message") >>= fun value -> nonempty (context ^ ".message") value in
  let* path = match optional "path" fields with None | Some `Null -> Ok None | Some value -> let* value = string (context ^ ".path") value in Ok (Some value) in
  Ok { code = Diagnostic_code.of_string code; severity; message; path; details = None }

let route_to_v1_json (value : route) =
  `Assoc (option_field "route_id" (fun value -> `String (Route_id.to_string value)) value.route_id @ option_field "endpoint_id" (fun value -> `String (Endpoint_id.to_string value)) value.endpoint_id @ option_field "api_family" (fun value -> `String (Api_family.to_string value)) value.api_family @ option_field "requested_model" (fun value -> `String (Model_selector.to_string value)) value.requested_model @ option_field "resolved_model" (fun value -> `String (Resolved_model_id.to_string value)) value.resolved_model)

let route_of_v1_json context value =
  let* fields = closed context ["route_id"; "endpoint_id"; "api_family"; "requested_model"; "resolved_model"] value in
  let optional_id name module_of = match optional name fields with None | Some `Null -> Ok None | Some value -> let* value = string (context ^ "." ^ name) value in let* value = nonempty (context ^ "." ^ name) value in Ok (Some (module_of value)) in
  let* route_id = optional_id "route_id" Route_id.of_string in let* endpoint_id = optional_id "endpoint_id" Endpoint_id.of_string in let* api_family = optional_id "api_family" Api_family.of_string in let* requested_model = optional_id "requested_model" Model_selector.of_string in let* resolved_model = optional_id "resolved_model" Resolved_model_id.of_string in
  Ok { route_id; endpoint_id; api_family; requested_model; resolved_model }

let generate_request_to_json (value : generate_request) =
  let* context = context_to_v1_json value.context in
  let fields = ["api_version", `String generate_api_version; "operation_key", `String (Operation_key.to_string value.operation_key); "context", context; "append", `List (List.map item_to_json value.append)] in
  let fields = match value.parent with None -> fields | Some parent -> fields @ ["parent", `String (Checkpoint.to_string parent)] in
  let fields = if settings_patch_is_empty value.settings_patch then fields else fields @ ["settings_patch", settings_patch_to_json value.settings_patch] in
  Ok (`Assoc (fields @ option_field "cache" cache_policy_to_json value.cache))

let generate_request_of_json value =
  let* fields = closed "generate request" ["api_version"; "operation_key"; "context"; "parent"; "append"; "settings_patch"; "cache"] value in
  let* version = required "generate request" "api_version" fields >>= string "generate request.api_version" in
  let* () = if version = generate_api_version then Ok () else Error (errorf "unsupported generate api_version %S" version) in
  let* operation_key = required "generate request" "operation_key" fields >>= string "generate request.operation_key" >>= fun value -> nonempty "generate request.operation_key" value in
  let* context = required "generate request" "context" fields >>= context_of_v1_json in
  let* parent = match optional "parent" fields with None | Some `Null -> Ok None | Some value -> let* value = string "generate request.parent" value in let* value = validated "generate request.parent" Checkpoint.of_string value in Ok (Some value) in
  let* append = required "generate request" "append" fields >>= list "generate request.append" >>= map_result (fun value -> Llm_temporal_codec.item_of_json value) in
  let* settings_patch = match optional "settings_patch" fields with None -> Ok empty_settings_patch | Some value -> settings_patch_of_json value in
  let* cache = match optional "cache" fields with None | Some `Null -> Ok None | Some value -> let* value = cache_policy_of_json "generate request.cache" value in Ok (Some value) in
  Ok { api_version = version; operation_key = Operation_key.of_string operation_key; context; parent; append; settings_patch; cache }

let generate_response_to_json (value : generate_response) =
  let* () = Llm_temporal_response_validation.validate_generate_response value in
  let fields = ["api_version", `String generate_api_version; "operation_key", `String (Operation_key.to_string value.operation_key); "operation_id", `String (Operation_id.to_string value.operation_id); "status", response_status_to_json value.status; "output", `List (List.map item_to_json value.output); "checkpoint", checkpoint_to_json value.checkpoint; "cache", cache_disposition_to_json value.cache; "cost", settled_cost_to_json value.cost] in
  let fields = fields @ option_field "route" route_to_v1_json value.route @ option_field "usage" usage_to_json value.usage in
  Ok (`Assoc (fields @ ["diagnostics", `List (List.map diagnostic_to_json value.diagnostics)]))

let generate_response_of_json value =
  let* fields = closed "generate response" ["api_version"; "operation_key"; "operation_id"; "status"; "output"; "checkpoint"; "cache"; "route"; "usage"; "cost"; "diagnostics"] value in
  let* version = required "generate response" "api_version" fields >>= string "generate response.api_version" in
  let* () = if version = generate_api_version then Ok () else Error (errorf "unsupported generate api_version %S" version) in
  let* operation_key = required "generate response" "operation_key" fields >>= string "generate response.operation_key" >>= fun value -> nonempty "generate response.operation_key" value in
  let* operation_id = required "generate response" "operation_id" fields >>= string "generate response.operation_id" >>= fun value -> nonempty "generate response.operation_id" value in
  let* status = required "generate response" "status" fields >>= response_status_of_json "generate response.status" in
  let* output = required "generate response" "output" fields >>= list "generate response.output" >>= map_result Llm_temporal_codec.item_of_json in
  let* checkpoint = required "generate response" "checkpoint" fields >>= checkpoint_of_json "generate response.checkpoint" in
  let* cache = required "generate response" "cache" fields >>= cache_disposition_of_json "generate response.cache" in
  let* route = match optional "route" fields with None | Some `Null -> Ok None | Some value -> let* value = route_of_v1_json "generate response.route" value in Ok (Some value) in
  let* usage = match optional "usage" fields with None | Some `Null -> Ok None | Some value -> let* value = usage_of_json "generate response.usage" value in Ok (Some value) in
  let* cost = required "generate response" "cost" fields >>= settled_cost_of_json "generate response.cost" in
  let* diagnostics = match optional "diagnostics" fields with None -> Ok [] | Some value -> list "generate response.diagnostics" value >>= map_result (diagnostic_of_json "generate response.diagnostic") in
  let response = { api_version = version; operation_key = Operation_key.of_string operation_key; operation_id = Operation_id.of_string operation_id; status; output; checkpoint; cache; route; usage; cost; diagnostics } in
  let* () = Llm_temporal_response_validation.validate_generate_response response in
  Ok response

let encode_generate_request value = let* value = generate_request_to_json value in to_bytes value
let decode_generate_request bytes = parse_json generate_request_of_json bytes
let encode_generate_response value = let* value = generate_response_to_json value in to_bytes value
let decode_generate_response bytes = parse_json generate_response_of_json bytes

let summary_style_to_json = function Concise -> `String "concise" | Balanced -> `String "balanced" | Detailed -> `String "detailed"
let summary_style_of_json context = function `String "concise" -> Ok Concise | `String "balanced" -> Ok Balanced | `String "detailed" -> Ok Detailed | _ -> Error (errorf "%s has an invalid summary style" context)

let policy_to_json value =
  `Assoc (option_field "target_tokens" (fun value -> `Intlit (Int64.to_string value)) value.target_tokens @ option_field "summary_style" summary_style_to_json value.summary_style)

let policy_of_json context value =
  let* fields = closed context ["target_tokens"; "summary_style"] value in
  let* target_tokens = match optional "target_tokens" fields with None | Some `Null -> Ok None | Some value -> let* value = int64 (context ^ ".target_tokens") value in let* () = if value < 1L || value > 10000000L then Error (errorf "%s.target_tokens is out of bounds" context) else Ok () in Ok (Some value) in
  let* summary_style = match optional "summary_style" fields with None | Some `Null -> Ok None | Some value -> let* value = summary_style_of_json (context ^ ".summary_style") value in Ok (Some value) in
  Ok { target_tokens; summary_style }

let compact_request_to_json (value : compact_request) =
  let* context = context_to_v1_json value.context in
  let fields = ["api_version", `String compact_api_version; "operation_key", `String (Operation_key.to_string value.operation_key); "context", context; "parent", `String (Checkpoint.to_string value.parent)] in
  let fields = fields @ option_field "policy" policy_to_json value.policy in
  Ok (`Assoc (fields @ option_field "cache" cache_policy_to_json value.cache))

let compact_request_of_json value =
  let* fields = closed "compact request" ["api_version"; "operation_key"; "context"; "parent"; "policy"; "cache"] value in
  let* version = required "compact request" "api_version" fields >>= string "compact request.api_version" in
  let* () = if version = compact_api_version then Ok () else Error (errorf "unsupported compact api_version %S" version) in
  let* operation_key = required "compact request" "operation_key" fields >>= string "compact request.operation_key" >>= fun value -> nonempty "compact request.operation_key" value in
  let* context = required "compact request" "context" fields >>= context_of_v1_json in
  let* parent = required "compact request" "parent" fields >>= string "compact request.parent" >>= fun value -> nonempty "compact request.parent" value in
  let* policy = match optional "policy" fields with None | Some `Null -> Ok None | Some value -> let* value = policy_of_json "compact request.policy" value in Ok (Some value) in
  let* cache = match optional "cache" fields with None | Some `Null -> Ok None | Some value -> let* value = cache_policy_of_json "compact request.cache" value in let* () = if value.variant = 0l then Ok () else Error (errorf "compact cache variant must be zero") in Ok (Some value) in
  let* parent = validated "compact request.parent" Checkpoint.of_string parent in
  Ok { api_version = version; operation_key = Operation_key.of_string operation_key; context; parent; policy; cache }

let provenance_to_json (value : provenance) =
  `Assoc (["source", `String (match value.source with Provider_provenance -> "provider" | Worker_cache_provenance -> "worker_cache")] @ option_field "origin_operation_id" (fun value -> `String (Operation_id.to_string value)) value.origin_operation_id @ option_field "policy" (fun value -> `String value) value.policy)

let provenance_of_json context value =
  let* fields = closed context ["source"; "origin_operation_id"; "policy"] value in
  let* source = required context "source" fields >>= string (context ^ ".source") >>= fun value -> match value with "provider" -> Ok Provider_provenance | "worker_cache" -> Ok Worker_cache_provenance | _ -> Error (errorf "%s.source is invalid" context) in
  let* origin_operation_id = match optional "origin_operation_id" fields with None | Some `Null -> Ok None | Some value -> let* value = string (context ^ ".origin_operation_id") value in Ok (Some (Operation_id.of_string value)) in
  let* policy = match optional "policy" fields with None | Some `Null -> Ok None | Some value -> let* value = string (context ^ ".policy") value in Ok (Some value) in
  Ok { source; origin_operation_id; policy }

let compaction_response_to_json (value : compaction_response) =
  let* () = Llm_temporal_response_validation.validate_compaction_response value in
  let fields = ["api_version", `String compact_api_version; "operation_key", `String (Operation_key.to_string value.operation_key); "operation_id", `String (Operation_id.to_string value.operation_id); "status", `String "completed"; "checkpoint", checkpoint_to_json value.checkpoint; "cache", cache_disposition_to_json value.cache; "cost", settled_cost_to_json value.cost] in
  let fields = fields @ option_field "provenance" provenance_to_json value.provenance @ option_field "usage" usage_to_json value.usage in
  Ok (`Assoc (fields @ ["diagnostics", `List (List.map diagnostic_to_json value.diagnostics)]))

let compaction_response_of_json value =
  let* fields = closed "compact response" ["api_version"; "operation_key"; "operation_id"; "status"; "checkpoint"; "cache"; "provenance"; "usage"; "cost"; "diagnostics"] value in
  let* version = required "compact response" "api_version" fields >>= string "compact response.api_version" in
  let* () = if version = compact_api_version then Ok () else Error (errorf "unsupported compact api_version %S" version) in
  let* operation_key = required "compact response" "operation_key" fields >>= string "compact response.operation_key" >>= fun value -> nonempty "compact response.operation_key" value in
  let* operation_id = required "compact response" "operation_id" fields >>= string "compact response.operation_id" >>= fun value -> nonempty "compact response.operation_id" value in
  let* status = required "compact response" "status" fields >>= string "compact response.status" in
  let* () = if status = "completed" then Ok () else Error (errorf "compact response.status must be completed") in
  let* checkpoint = required "compact response" "checkpoint" fields >>= checkpoint_of_json "compact response.checkpoint" in
  let* cache = required "compact response" "cache" fields >>= cache_disposition_of_json "compact response.cache" in
  let* () = if cache.variant = 0l then Ok () else Error (errorf "compact response cache variant must be zero") in
  let* provenance = match optional "provenance" fields with None | Some `Null -> Ok None | Some value -> let* value = provenance_of_json "compact response.provenance" value in Ok (Some value) in
  let* usage = match optional "usage" fields with None | Some `Null -> Ok None | Some value -> let* value = usage_of_json "compact response.usage" value in Ok (Some value) in
  let* cost = required "compact response" "cost" fields >>= settled_cost_of_json "compact response.cost" in
  let* diagnostics = match optional "diagnostics" fields with None -> Ok [] | Some value -> list "compact response.diagnostics" value >>= map_result (diagnostic_of_json "compact response.diagnostic") in
  let response = { api_version = version; operation_key = Operation_key.of_string operation_key; operation_id = Operation_id.of_string operation_id; checkpoint; cache; provenance; usage; cost; diagnostics } in
  let* () = Llm_temporal_response_validation.validate_compaction_response response in
  Ok response

let encode_compact_request value = let* value = compact_request_to_json value in to_bytes value
let decode_compact_request bytes = parse_json compact_request_of_json bytes
let encode_compaction_response value = let* value = compaction_response_to_json value in to_bytes value
let decode_compaction_response bytes = parse_json compaction_response_of_json bytes

let time_to_json value = `String (Ptime.to_rfc3339 value)
let time_of_json context value =
  let* value = string context value in
  match Ptime.of_rfc3339 value with Ok (time, _, _) -> Ok time | Error _ -> Error (errorf "%s is not a valid RFC3339 timestamp" context)

let availability_to_string = function Available -> "available" | Degraded -> "degraded" | Unavailable -> "unavailable"
let availability_of_string context = function "available" -> Ok Available | "degraded" -> Ok Degraded | "unavailable" -> Ok Unavailable | _ -> Error (errorf "%s has invalid availability" context)
let lifecycle_to_string = function
  | Active -> "available"
  | Deprecated -> "deprecated"
  | Retired -> "unavailable"
  | Unknown -> "unknown"

let lifecycle_of_string context = function
  | "available" -> Ok Active
  | "deprecated" -> Ok Deprecated
  | "unavailable" -> Ok Retired
  | "unknown" -> Ok Unknown
  | _ -> Error (errorf "%s has invalid lifecycle" context)
let credit_to_string = function Credit_ok -> "ok" | Credit_low -> "low" | Credit_exhausted -> "exhausted" | Credit_unknown -> "unknown"
let credit_of_string context = function "ok" -> Ok Credit_ok | "low" -> Ok Credit_low | "exhausted" -> Ok Credit_exhausted | "unknown" -> Ok Credit_unknown | _ -> Error (errorf "%s has invalid credit state" context)
let billing_to_string = function Billing_ok -> "ok" | Billing_blocked -> "blocked" | Billing_unknown -> "unknown"
let billing_of_string context = function "ok" -> Ok Billing_ok | "blocked" -> Ok Billing_blocked | "unknown" -> Ok Billing_unknown | _ -> Error (errorf "%s has invalid billing state" context)
let circuit_to_string = function Circuit_closed -> "closed" | Circuit_open -> "open" | Circuit_half_open -> "half_open"
let circuit_of_string context = function "closed" -> Ok Circuit_closed | "open" -> Ok Circuit_open | "half_open" -> Ok Circuit_half_open | _ -> Error (errorf "%s has invalid circuit state" context)
let evidence_to_string = function Provider_api_evidence -> "provider_api" | Operator_evidence -> "operator" | Unknown_evidence -> "unknown"
let evidence_of_string context = function "provider_api" -> Ok Provider_api_evidence | "operator" -> Ok Operator_evidence | "unknown" -> Ok Unknown_evidence | _ -> Error (errorf "%s has invalid evidence source" context)
let source_to_string = function Persisted -> "persisted" | Persisted_and_refreshed -> "persisted_and_refreshed" | Redis_budget_generation -> "redis_budget_generation"
let source_of_string context = function "persisted" -> Ok Persisted | "persisted_and_refreshed" -> Ok Persisted_and_refreshed | "redis_budget_generation" -> Ok Redis_budget_generation | _ -> Error (errorf "%s has invalid source" context)
let freshness_to_string = function Current -> "current" | Stale -> "stale" | Unknown_freshness -> "unknown"
let freshness_of_string context = function "current" -> Ok Current | "stale" -> Ok Stale | "unknown" -> Ok Unknown_freshness | _ -> Error (errorf "%s has invalid freshness" context)
let operation_kind_to_string = function Generate -> "generate" | Compact -> "compact" | Query -> "query"
let operation_kind_of_string context = function "generate" -> Ok Generate | "compact" -> Ok Compact | "query" -> Ok Query | _ -> Error (errorf "%s has invalid operation kind" context)
let group_by_to_string = function By_operation_kind -> "operation_kind" | By_provider -> "provider" | By_model -> "model"
let group_by_of_string context = function "operation_kind" -> Ok By_operation_kind | "provider" -> Ok By_provider | "model" -> Ok By_model | _ -> Error (errorf "%s has invalid group_by" context)
let completeness_to_string = function Complete_cost -> "complete" | Partial_cost -> "partial"
let optional_id_field name module_to_string fields = match optional name fields with None | Some `Null -> Ok None | Some value -> let* value = string name value in Ok (Some (module_to_string value))

let provider_filter_to_json (value : provider_status_filter) =
  `Assoc (option_field "provider" (fun value -> `String (Provider_id.to_string value)) value.provider @ option_field "endpoint" (fun value -> `String (Endpoint_id.to_string value)) value.endpoint @ option_field "availability" (fun value -> `String (availability_to_string value)) value.availability @ ["include_healthy", `Bool value.include_healthy] @ option_field "refresh_if_older_than_seconds" (fun value -> `Intlit (Int64.to_string value)) value.refresh_if_older_than_seconds @ ["page_size", `Int value.page_size] @ ["cursor", match value.cursor with None -> `Null | Some value -> `String (Query_cursor.to_string value)])

let model_filter_to_json (value : model_inventory_filter) =
  `Assoc (option_field "provider" (fun value -> `String (Provider_id.to_string value)) value.provider @ option_field "endpoint" (fun value -> `String (Endpoint_id.to_string value)) value.endpoint @ option_field "model_prefix" (fun value -> `String (Model_prefix.to_string value)) value.model_prefix @ option_field "lifecycle" (fun value -> `String (lifecycle_to_string value)) value.lifecycle @ option_field "refresh_if_older_than_seconds" (fun value -> `Intlit (Int64.to_string value)) value.refresh_if_older_than_seconds @ ["page_size", `Int value.page_size] @ ["cursor", match value.cursor with None -> `Null | Some value -> `String (Query_cursor.to_string value)])

let credit_filter_to_json (value : credit_status_filter) =
  `Assoc (option_field "provider" (fun value -> `String (Provider_id.to_string value)) value.provider @ option_field "endpoint" (fun value -> `String (Endpoint_id.to_string value)) value.endpoint @ ["include_ok", `Bool value.include_ok] @ option_field "refresh_if_older_than_seconds" (fun value -> `Intlit (Int64.to_string value)) value.refresh_if_older_than_seconds @ ["page_size", `Int value.page_size] @ ["cursor", match value.cursor with None -> `Null | Some value -> `String (Query_cursor.to_string value)])

let budget_filter_to_json (value : budget_status_filter) = `Assoc (option_field "policy_key" (fun value -> `String (Budget_policy_key.to_string value)) value.policy_key @ option_field "active_at" time_to_json value.active_at @ ["include_windows", `Bool value.include_windows])
let spend_filter_to_json (value : spend_summary_filter) = `Assoc (["start_time", time_to_json value.start_time; "end_time", time_to_json value.end_time; "group_by", `List (List.map (fun value -> `String (group_by_to_string value)) value.group_by); "operation_kinds", `List (List.map (fun value -> `String (operation_kind_to_string value)) value.operation_kinds)])

let query_request_to_json value =
  let kind, query = match value with
    | Provider_status_request value -> "provider_status", provider_filter_to_json value
    | Model_inventory_request value -> "model_inventory", model_filter_to_json value
    | Credit_status_request value -> "credit_status", credit_filter_to_json value
    | Budget_status_request value -> "budget_status", budget_filter_to_json value
    | Spend_summary_request value -> "spend_summary", spend_filter_to_json value
  in
  Ok (`Assoc ["api_version", `String query_api_version; "operation_key", `String "query"; "context", `Assoc ["tenant", `String "query"; "project", `String "query"; "actor", `String "query"]; "kind", `String kind; "query", query])

let query_request_of_json value =
  let* fields = closed "query request" ["api_version"; "operation_key"; "context"; "kind"; "query"] value in
  let* version = required "query request" "api_version" fields >>= string "query request.api_version" in
  let* () = if version = query_api_version then Ok () else Error (errorf "unsupported query api_version %S" version) in
  let* operation_key = required "query request" "operation_key" fields >>= string "query request.operation_key" >>= fun value -> nonempty "query request.operation_key" value in
  let* _context = required "query request" "context" fields >>= context_of_v1_json in
  let* kind = required "query request" "kind" fields >>= string "query request.kind" in
  let* query = required "query request" "query" fields in
  let page context kind fields =
    let* page_size = match optional "page_size" fields with None -> Ok 100 | Some value -> int64 (context ^ ".page_size") value >>= fun value -> if value < 1L || value > 1000L then Error (errorf "%s.page_size is out of bounds" context) else Ok (Int64.to_int value) in
    let* cursor = match optional "cursor" fields with None | Some `Null -> Ok None | Some value -> let* value = string (context ^ ".cursor") value in let* value = validated (context ^ ".cursor") (Query_cursor.of_string_for_kind kind) value in Ok (Some value) in
    Ok (page_size, cursor)
  in
  let optional_provider _context fields = optional_id_field "provider" Provider_id.of_string fields in
  let optional_endpoint _context fields = optional_id_field "endpoint" Endpoint_id.of_string fields in
  let* query = match kind with
    | "provider_status" ->
        let* fields = closed "provider_status query" ["provider"; "endpoint"; "availability"; "include_healthy"; "refresh_if_older_than_seconds"; "page_size"; "cursor"] query in
        let* provider = optional_provider "provider" fields in let* endpoint = optional_endpoint "endpoint" fields in
        let* availability = match optional "availability" fields with None | Some `Null -> Ok None | Some (`String value) -> let* value = availability_of_string "provider_status.availability" value in Ok (Some value) | Some _ -> Error (errorf "provider_status.availability must be a string") in
        let* include_healthy = match optional "include_healthy" fields with None -> Ok true | Some value -> bool "provider_status.include_healthy" value in
        let* refresh_if_older_than_seconds = match optional "refresh_if_older_than_seconds" fields with None | Some `Null -> Ok None | Some value -> let* value = int64 "provider_status.refresh_if_older_than_seconds" value in let* () = if value < 1L || value > 86400L then Error (errorf "provider_status refresh age out of bounds") else Ok () in Ok (Some value) in
        let* page_size, cursor = page "provider_status" Query_cursor.Provider_status fields in Ok (Provider_status_request { provider; endpoint; availability; include_healthy; refresh_if_older_than_seconds; page_size; cursor })
    | "model_inventory" ->
        let* fields = closed "model_inventory query" ["provider"; "endpoint"; "model_prefix"; "lifecycle"; "refresh_if_older_than_seconds"; "page_size"; "cursor"] query in
        let* provider = optional_provider "provider" fields in let* endpoint = optional_endpoint "endpoint" fields in
        let* model_prefix = match optional "model_prefix" fields with None | Some `Null -> Ok None | Some value -> let* value = string "model_inventory.model_prefix" value in Ok (Some (Model_prefix.of_string value)) in
        let* lifecycle = match optional "lifecycle" fields with None | Some `Null -> Ok None | Some (`String value) -> let* value = lifecycle_of_string "model_inventory.lifecycle" value in Ok (Some value) | Some _ -> Error (errorf "model_inventory.lifecycle must be a string") in
        let* refresh_if_older_than_seconds = match optional "refresh_if_older_than_seconds" fields with None | Some `Null -> Ok None | Some value -> let* value = int64 "model_inventory.refresh_if_older_than_seconds" value in let* () = if value < 1L || value > 86400L then Error (errorf "model inventory refresh age out of bounds") else Ok () in Ok (Some value) in
        let* page_size, cursor = page "model_inventory" Query_cursor.Model_inventory fields in Ok (Model_inventory_request { provider; endpoint; model_prefix; lifecycle; refresh_if_older_than_seconds; page_size; cursor })
    | "credit_status" ->
        let* fields = closed "credit_status query" ["provider"; "endpoint"; "include_ok"; "refresh_if_older_than_seconds"; "page_size"; "cursor"] query in
        let* provider = optional_provider "provider" fields in let* endpoint = optional_endpoint "endpoint" fields in
        let* include_ok = match optional "include_ok" fields with None -> Ok true | Some value -> bool "credit_status.include_ok" value in
        let* refresh_if_older_than_seconds = match optional "refresh_if_older_than_seconds" fields with None | Some `Null -> Ok None | Some value -> let* value = int64 "credit_status.refresh_if_older_than_seconds" value in let* () = if value < 1L || value > 86400L then Error (errorf "credit status refresh age out of bounds") else Ok () in Ok (Some value) in
        let* page_size, cursor = page "credit_status" Query_cursor.Credit_status fields in Ok (Credit_status_request { provider; endpoint; include_ok; refresh_if_older_than_seconds; page_size; cursor })
    | "budget_status" ->
        let* fields = closed "budget_status query" ["policy_key"; "active_at"; "include_windows"] query in
        let* policy_key = match optional "policy_key" fields with None | Some `Null -> Ok None | Some value -> let* value = string "budget_status.policy_key" value in Ok (Some (Budget_policy_key.of_string value)) in
        let* active_at = match optional "active_at" fields with None | Some `Null -> Ok None | Some value -> let* value = time_of_json "budget_status.active_at" value in Ok (Some value) in
        let* include_windows = match optional "include_windows" fields with None -> Ok true | Some value -> bool "budget_status.include_windows" value in
        Ok (Budget_status_request { policy_key; active_at; include_windows })
    | "spend_summary" ->
        let* fields = closed "spend_summary query" ["start_time"; "end_time"; "group_by"; "operation_kinds"] query in
        let* start_time = required "spend_summary query" "start_time" fields >>= time_of_json "spend_summary.start_time" in
        let* end_time = required "spend_summary query" "end_time" fields >>= time_of_json "spend_summary.end_time" in
        let* () = if Ptime.compare end_time start_time > 0 then Ok () else Error (errorf "spend_summary end_time must be after start_time") in
        let* group_by = match optional "group_by" fields with None -> Ok [] | Some value -> list "spend_summary.group_by" value >>= map_result (fun value -> string "spend_summary.group_by" value >>= group_by_of_string "spend_summary.group_by") in
        let* operation_kinds = match optional "operation_kinds" fields with None -> Ok [] | Some value -> list "spend_summary.operation_kinds" value >>= map_result (fun value -> string "spend_summary.operation_kinds" value >>= operation_kind_of_string "spend_summary.operation_kinds") in
        Ok (Spend_summary_request { start_time; end_time; group_by; operation_kinds })
    | _ -> Error (errorf "query kind %S is unsupported" kind)
  in
  (* The request codec intentionally carries the caller's key/context.  The
     query union itself is returned here; invocation wraps these values with
     the common envelope before dispatch. *)
  let _ = operation_key in
  Ok query

let encode_query_request value = let* value = query_request_to_json value in to_bytes value
let decode_query_request bytes = parse_json query_request_of_json bytes

let query_envelope_to_json (value : query_envelope) =
  let kind, query = match value.query with
    | Provider_status_request value -> "provider_status", provider_filter_to_json value
    | Model_inventory_request value -> "model_inventory", model_filter_to_json value
    | Credit_status_request value -> "credit_status", credit_filter_to_json value
    | Budget_status_request value -> "budget_status", budget_filter_to_json value
    | Spend_summary_request value -> "spend_summary", spend_filter_to_json value
  in
  let* context = context_to_v1_json value.context in
  Ok (`Assoc ["api_version", `String query_api_version; "operation_key", `String (Operation_key.to_string value.operation_key); "context", context; "kind", `String kind; "query", query])

let query_envelope_of_json value =
  let* fields = closed "query envelope" ["api_version"; "operation_key"; "context"; "kind"; "query"] value in
  let* version = required "query envelope" "api_version" fields >>= string "query envelope.api_version" in
  let* () = if version = query_api_version then Ok () else Error (errorf "unsupported query api_version %S" version) in
  let* operation_key = required "query envelope" "operation_key" fields >>= string "query envelope.operation_key" >>= nonempty "query envelope.operation_key" in
  let* context = required "query envelope" "context" fields >>= context_of_v1_json in
  let* query = query_request_of_json value in
  Ok { api_version = version; operation_key = Operation_key.of_string operation_key; context; query }

let encode_query_envelope value = let* value = query_envelope_to_json value in to_bytes value
let decode_query_envelope bytes = parse_json query_envelope_of_json bytes

(* Query result codecs deliberately mirror the closed Go wire schema.  The
   fields carrying money are always decimal strings, never JSON numbers. *)
let string_option name = function None -> [name, `Null] | Some value -> [name, `String value]
let id_option name f = function None -> [name, `Null] | Some value -> [name, `String (f value)]

let route_status_to_json (value : provider_route_status) =
  `Assoc (["route_id", `String (Route_id.to_string value.route_id); "provider", `String (Provider_id.to_string value.provider); "endpoint", `String (Endpoint_id.to_string value.endpoint); "availability", `String (availability_to_string value.availability); "credit_state", `String (credit_to_string value.credit_state); "billing_state", `String (billing_to_string value.billing_state); "circuit_state", `String (circuit_to_string value.circuit_state); "observed_at", time_to_json value.observed_at; "stale_after", time_to_json value.stale_after] @ string_option "safe_code" (Option.map Safe_code.to_string value.safe_code))

let route_status_of_json context value =
  let* fields = closed context ["route_id"; "provider"; "endpoint"; "availability"; "credit_state"; "billing_state"; "circuit_state"; "observed_at"; "stale_after"; "safe_code"] value in
  let* route_id = required context "route_id" fields >>= string (context ^ ".route_id") in
  let* provider = required context "provider" fields >>= string (context ^ ".provider") in
  let* endpoint = required context "endpoint" fields >>= string (context ^ ".endpoint") in
  let* availability = required context "availability" fields >>= string (context ^ ".availability") >>= availability_of_string context in
  let* credit_state = match optional "credit_state" fields with None -> Ok Credit_unknown | Some value -> string (context ^ ".credit_state") value >>= credit_of_string context in
  let* billing_state = match optional "billing_state" fields with None -> Ok Billing_unknown | Some value -> string (context ^ ".billing_state") value >>= billing_of_string context in
  let* circuit_state = match optional "circuit_state" fields with None -> Ok Circuit_closed | Some value -> string (context ^ ".circuit_state") value >>= circuit_of_string context in
  let* observed_at = required context "observed_at" fields >>= time_of_json (context ^ ".observed_at") in
  let* stale_after = required context "stale_after" fields >>= time_of_json (context ^ ".stale_after") in
  let* safe_code = match optional "safe_code" fields with None | Some `Null -> Ok None | Some value -> string (context ^ ".safe_code") value >>= fun value -> Ok (Some (Safe_code.of_string value)) in
  Ok { route_id = Route_id.of_string route_id; provider = Provider_id.of_string provider; endpoint = Endpoint_id.of_string endpoint; availability; credit_state; billing_state; circuit_state; observed_at; stale_after; safe_code }

let provider_page_to_json (value : provider_status_page) = `Assoc ["routes", `List (List.map route_status_to_json value.routes)]
let provider_page_of_json context value =
  let* fields = closed context ["routes"] value in
  let* routes = required context "routes" fields >>= list (context ^ ".routes") >>= map_result (route_status_of_json (context ^ ".route")) in
  Ok { routes }

let inventory_to_json (value : model_inventory_entry) =
  `Assoc (["provider", `String (Provider_id.to_string value.provider); "endpoint", `String (Endpoint_id.to_string value.endpoint); "provider_model_id", `String (Provider_model_id.to_string value.provider_model_id); "lifecycle", `String (lifecycle_to_string value.lifecycle); "capabilities", `List (List.map (fun value -> `String (Model_capability.to_string value)) value.capabilities); "complete_snapshot", `Bool value.complete_snapshot] @ string_option "display_name" (Option.map Model_display_name.to_string value.display_name))

let inventory_of_json context value =
  let* fields = closed context ["provider"; "endpoint"; "provider_model_id"; "display_name"; "lifecycle"; "capabilities"; "complete_snapshot"] value in
  let* provider = required context "provider" fields >>= string (context ^ ".provider") in
  let* endpoint = required context "endpoint" fields >>= string (context ^ ".endpoint") in
  let* provider_model_id = required context "provider_model_id" fields >>= string (context ^ ".provider_model_id") >>= nonempty (context ^ ".provider_model_id") in
  let* display_name = match optional "display_name" fields with None | Some `Null -> Ok None | Some value -> string (context ^ ".display_name") value >>= fun value -> Ok (Some (Model_display_name.of_string value)) in
  let* lifecycle = required context "lifecycle" fields >>= string (context ^ ".lifecycle") >>= lifecycle_of_string context in
  let* capabilities = required context "capabilities" fields >>= list (context ^ ".capabilities") >>= map_result (fun value -> string (context ^ ".capability") value >>= fun value -> Ok (Model_capability.of_string value)) in
  let* complete_snapshot = required context "complete_snapshot" fields >>= bool (context ^ ".complete_snapshot") in
  Ok { provider = Provider_id.of_string provider; endpoint = Endpoint_id.of_string endpoint; provider_model_id = Provider_model_id.of_string provider_model_id; display_name; lifecycle; capabilities; source = Unknown_inventory_source; complete_snapshot; safe_metadata = Safe_metadata.empty }

let model_page_to_json (value : model_inventory_page) = `Assoc ["models", `List (List.map inventory_to_json value.models)]
let model_page_of_json context value = let* fields = closed context ["models"] value in let* models = required context "models" fields >>= list (context ^ ".models") >>= map_result (inventory_of_json (context ^ ".model")) in Ok { models }

let credit_entry_to_json (value : credit_status_entry) =
  `Assoc (["provider", `String (Provider_id.to_string value.provider); "endpoint", `String (Endpoint_id.to_string value.endpoint); "credit_state", `String (credit_to_string value.credit_state); "billing_state", `String (billing_to_string value.billing_state); "confirmed_at", (match value.confirmed_at with None -> `Null | Some value -> time_to_json value); "evidence_source", `String (evidence_to_string value.evidence_source)] @ string_option "safe_evidence_code" (Option.map Safe_code.to_string value.safe_evidence_code))

let credit_entry_of_json context value =
  let* fields = closed context ["provider"; "endpoint"; "credit_state"; "billing_state"; "confirmed_at"; "evidence_source"; "safe_evidence_code"] value in
  let* provider = required context "provider" fields >>= string (context ^ ".provider") in
  let* endpoint = required context "endpoint" fields >>= string (context ^ ".endpoint") in
  let* credit_state = required context "credit_state" fields >>= string (context ^ ".credit_state") >>= credit_of_string context in
  let* billing_state = required context "billing_state" fields >>= string (context ^ ".billing_state") >>= billing_of_string context in
  let* confirmed_at = match optional "confirmed_at" fields with None | Some `Null -> Ok None | Some value -> time_of_json (context ^ ".confirmed_at") value >>= fun value -> Ok (Some value) in
  let* evidence_source = required context "evidence_source" fields >>= string (context ^ ".evidence_source") >>= evidence_of_string context in
  let* safe_evidence_code = match optional "safe_evidence_code" fields with None | Some `Null -> Ok None | Some value -> string (context ^ ".safe_evidence_code") value >>= fun value -> Ok (Some (Safe_code.of_string value)) in
  Ok { provider = Provider_id.of_string provider; endpoint = Endpoint_id.of_string endpoint; credit_state; billing_state; confirmed_at; evidence_source; safe_evidence_code }

let credit_page_to_json (value : credit_status_page) = `Assoc ["endpoints", `List (List.map credit_entry_to_json value.endpoints)]
let credit_page_of_json context value = let* fields = closed context ["endpoints"] value in let* endpoints = required context "endpoints" fields >>= list (context ^ ".endpoints") >>= map_result (credit_entry_of_json (context ^ ".endpoint")) in Ok { endpoints }

let usd_json value = `String (Usd_decimal.to_string value)
let usd_of_field context fields name = required context name fields >>= string (context ^ "." ^ name) >>= fun value -> match Usd_decimal.of_string value with Ok value -> Ok value | Error message -> Error (errorf "%s.%s: %s" context name message)

let window_to_json (value : budget_window_status) =
  `Assoc (["policy_key", `String (Budget_policy_key.to_string value.policy_key); "window_key", `String (Window_key.to_string value.window_key); "coverage_start", time_to_json value.coverage_start; "coverage_end", time_to_json value.coverage_end; "limit_usd", usd_json value.limit_usd; "reserved_cost_usd", usd_json value.reserved_cost_usd; "accounted_cost_usd", usd_json value.accounted_cost_usd; "available_usd", usd_json value.available_usd] @ option_field "retry_after_seconds" (fun value -> `Intlit (Int64.to_string value)) value.retry_after_seconds)

let window_of_json context value =
  let* fields = closed context ["policy_key"; "window_key"; "coverage_start"; "coverage_end"; "limit_usd"; "reserved_cost_usd"; "accounted_cost_usd"; "available_usd"; "retry_after_seconds"] value in
  let* policy_key = required context "policy_key" fields >>= string (context ^ ".policy_key") in
  let* window_key = required context "window_key" fields >>= string (context ^ ".window_key") in
  let* coverage_start = required context "coverage_start" fields >>= time_of_json (context ^ ".coverage_start") in
  let* coverage_end = required context "coverage_end" fields >>= time_of_json (context ^ ".coverage_end") in
  let* limit_usd = usd_of_field context fields "limit_usd" in
  let* reserved_cost_usd = usd_of_field context fields "reserved_cost_usd" in
  let* accounted_cost_usd = usd_of_field context fields "accounted_cost_usd" in
  let* available_usd = usd_of_field context fields "available_usd" in
  let* retry_after_seconds = match optional "retry_after_seconds" fields with None | Some `Null -> Ok None | Some value -> int64 (context ^ ".retry_after_seconds") value >>= fun value -> nonnegative (context ^ ".retry_after_seconds") value >>= fun () -> Ok (Some value) in
  Ok { policy_key = Budget_policy_key.of_string policy_key; window_key = Window_key.of_string window_key; coverage_start; coverage_end; limit_usd; reserved_cost_usd; accounted_cost_usd; available_usd; retry_after_seconds }

let budget_to_json (value : budget_status) = `Assoc ["active_at", time_to_json value.active_at; "generation_id", `String (Budget_generation_id.to_string value.generation_id); "manifest_digest", `String (Sha256_digest.to_hex value.manifest_digest); "stream_high_water_mark", `String (Budget_stream_id.to_string value.stream_high_water_mark); "windows", `List (List.map window_to_json value.windows)]
let budget_of_json context value =
  let* fields = closed context ["active_at"; "generation_id"; "manifest_digest"; "stream_high_water_mark"; "windows"] value in
  let* active_at = required context "active_at" fields >>= time_of_json (context ^ ".active_at") in
  let* generation_id = required context "generation_id" fields >>= string (context ^ ".generation_id") in
  let* manifest_digest = required context "manifest_digest" fields >>= string (context ^ ".manifest_digest") in
  let* stream_high_water_mark = required context "stream_high_water_mark" fields >>= string (context ^ ".stream_high_water_mark") in
  let* windows = required context "windows" fields >>= list (context ^ ".windows") >>= map_result (window_of_json (context ^ ".window")) in
  let* manifest_digest = validated "query response.manifest_digest" Sha256_digest.of_hex manifest_digest in
  let* stream_high_water_mark = validated "query response.stream_high_water_mark" Budget_stream_id.of_string stream_high_water_mark in
  Ok { active_at; generation_id = Budget_generation_id.of_string generation_id; manifest_digest; stream_high_water_mark; windows }

let spend_group_to_json (value : spend_group_key) = `Assoc (id_option "operation_kind" (fun value -> operation_kind_to_string value) value.operation_kind @ id_option "provider" Provider_id.to_string value.provider @ id_option "model" Model_selector.to_string value.model)
let spend_bucket_to_json (value : spend_bucket) = `Assoc (["group", spend_group_to_json value.group; "known_actual_cost_usd", usd_json value.known_actual_cost_usd; "exact_operation_count", `Intlit (Int64.to_string value.exact_operation_count); "unknown_operation_count", `Intlit (Int64.to_string value.unknown_operation_count); "completeness", `String (completeness_to_string value.completeness)])
let spend_to_json (value : spend_summary) = `Assoc ["start_time", time_to_json value.start_time; "end_time", time_to_json value.end_time; "buckets", `List (List.map spend_bucket_to_json value.buckets)]

let spend_group_of_json context value =
  let* fields = closed context ["operation_kind"; "provider"; "model"] value in
  let* operation_kind = match optional "operation_kind" fields with None | Some `Null -> Ok None | Some value -> string (context ^ ".operation_kind") value >>= operation_kind_of_string context >>= fun value -> Ok (Some value) in
  let* provider = match optional "provider" fields with None | Some `Null -> Ok None | Some value -> string (context ^ ".provider") value >>= fun value -> Ok (Some (Provider_id.of_string value)) in
  let* model = match optional "model" fields with None | Some `Null -> Ok None | Some value -> string (context ^ ".model") value >>= fun value -> Ok (Some (Model_selector.of_string value)) in
  Ok { operation_kind; provider; model }

let spend_bucket_of_json context value =
  let* fields = closed context ["group"; "known_actual_cost_usd"; "exact_operation_count"; "unknown_operation_count"; "completeness"] value in
  let* group = match optional "group" fields with None | Some `Null -> Ok { operation_kind = None; provider = None; model = None } | Some value -> spend_group_of_json (context ^ ".group") value in
  let* known_actual_cost_usd = usd_of_field context fields "known_actual_cost_usd" in
  let* exact_operation_count = required context "exact_operation_count" fields >>= int64 (context ^ ".exact_operation_count") >>= fun value -> nonnegative (context ^ ".exact_operation_count") value >>= fun () -> Ok value in
  let* unknown_operation_count = required context "unknown_operation_count" fields >>= int64 (context ^ ".unknown_operation_count") >>= fun value -> nonnegative (context ^ ".unknown_operation_count") value >>= fun () -> Ok value in
  let* completeness = required context "completeness" fields >>= string (context ^ ".completeness") >>= function "complete" -> Ok Complete_cost | "partial" -> Ok Partial_cost | _ -> Error (errorf "%s.completeness is invalid" context) in
  Ok { group; known_actual_cost_usd; exact_operation_count; unknown_operation_count; completeness }

let spend_of_json context value =
  let* fields = closed context ["start_time"; "end_time"; "buckets"] value in
  let* start_time = required context "start_time" fields >>= time_of_json (context ^ ".start_time") in
  let* end_time = required context "end_time" fields >>= time_of_json (context ^ ".end_time") in
  let* () = if Ptime.compare end_time start_time > 0 then Ok () else Error (errorf "%s.end_time must be after start_time" context) in
  let* buckets = required context "buckets" fields >>= list (context ^ ".buckets") >>= map_result (spend_bucket_of_json (context ^ ".bucket")) in
  Ok { start_time; end_time; buckets }

let query_result_to_json = function
  | Provider_status_result value -> "provider_status", provider_page_to_json value
  | Model_inventory_result value -> "model_inventory", model_page_to_json value
  | Credit_status_result value -> "credit_status", credit_page_to_json value
  | Budget_status_result value -> "budget_status", budget_to_json value
  | Spend_summary_result value -> "spend_summary", spend_to_json value

let validate_query_pagination ~kind ~complete ~next_cursor =
  match kind, complete, next_cursor with
  | ("provider_status" | "model_inventory" | "credit_status"), true, Some _ ->
      Error
        (errorf "query response.%s complete response must not include next_cursor"
           kind)
  | ("provider_status" | "model_inventory" | "credit_status"), false, None ->
      Error
        (errorf "query response.%s incomplete response requires next_cursor" kind)
  | ("budget_status" | "spend_summary"), _, Some _ ->
      Error (errorf "query response.%s must not include next_cursor" kind)
  | ("budget_status" | "spend_summary"), false, None ->
      Error (errorf "query response.%s must be complete" kind)
  | _ -> Ok ()

let query_response_to_json (value : query_response) =
  let kind, result = query_result_to_json value.result in
  let* () =
    validate_query_pagination ~kind ~complete:value.complete
      ~next_cursor:value.next_cursor
  in
  let cost_fields = match value.cost with
    | Exact_cost { actual_cost_usd; method_; catalog_version = _ } -> ["cost_status", `String "exact"; "actual_cost_usd", usd_json actual_cost_usd; "cost_method", `String (match method_ with Provider_reported -> "provider_reported" | Catalog_usage -> "catalog_usage" | Control_query_zero -> "control_query_zero")]
    | Unknown_cost { reason } -> ["cost_status", `String "unknown"; "actual_cost_usd", `Null; "cost_unknown_reason_code", `String (match reason with Provider_did_not_report_cost -> "provider_did_not_report_cost" | Catalog_incomplete -> "catalog_incomplete" | State_unavailable -> "state_unavailable" | Ambiguous_dispatch -> "ambiguous_dispatch")]
  in
  Ok (`Assoc (["api_version", `String query_api_version; "operation_key", `String (Operation_key.to_string value.operation_key); "query_execution_id", `String (Query_execution_id.to_string value.query_execution_id); "kind", `String kind; "observed_at", time_to_json value.observed_at; "source", `String (source_to_string value.source); "freshness", `String (freshness_to_string value.freshness); "complete", `Bool value.complete; "next_cursor", (match value.next_cursor with None -> `Null | Some value -> `String (Query_cursor.to_string value)); "result", result] @ cost_fields))

let query_response_of_json value =
  let* fields = closed "query response" ["api_version"; "operation_key"; "query_execution_id"; "kind"; "observed_at"; "source"; "freshness"; "complete"; "next_cursor"; "result"; "cost_status"; "actual_cost_usd"; "cost_method"; "cost_unknown_reason_code"] value in
  let* version = required "query response" "api_version" fields >>= string "query response.api_version" in
  let* () = if version = query_api_version then Ok () else Error (errorf "unsupported query api_version %S" version) in
  let* operation_key = required "query response" "operation_key" fields >>= string "query response.operation_key" >>= nonempty "query response.operation_key" in
  let* query_execution_id = required "query response" "query_execution_id" fields >>= string "query response.query_execution_id" >>= nonempty "query response.query_execution_id" in
  let* kind = required "query response" "kind" fields >>= string "query response.kind" in
  let* observed_at = required "query response" "observed_at" fields >>= time_of_json "query response.observed_at" in
  let* source = required "query response" "source" fields >>= string "query response.source" >>= source_of_string "query response.source" in
  let* freshness = required "query response" "freshness" fields >>= string "query response.freshness" >>= freshness_of_string "query response.freshness" in
  let* complete = required "query response" "complete" fields >>= bool "query response.complete" in
  let* cursor_kind = validated "query response.kind" Query_cursor.kind_of_string kind in
  let* next_cursor = match optional "next_cursor" fields with
    | None | Some `Null -> Ok None
    | Some _ when kind = "budget_status" || kind = "spend_summary" ->
        Error (errorf "query response.%s must not include next_cursor" kind)
    | Some value -> string "query response.next_cursor" value >>= fun value -> validated "query response.next_cursor" (Query_cursor.of_string_for_kind cursor_kind) value >>= fun value -> Ok (Some value)
  in
  let* () = validate_query_pagination ~kind ~complete ~next_cursor in
  let* result_value = required "query response" "result" fields in
  let* result = match kind with
    | "provider_status" -> provider_page_of_json "query response.provider_status" result_value >>= fun value -> Ok (Provider_status_result value)
    | "model_inventory" -> model_page_of_json "query response.model_inventory" result_value >>= fun value -> Ok (Model_inventory_result value)
    | "credit_status" -> credit_page_of_json "query response.credit_status" result_value >>= fun value -> Ok (Credit_status_result value)
    | "budget_status" -> budget_of_json "query response.budget_status" result_value >>= fun value -> Ok (Budget_status_result value)
    | "spend_summary" -> spend_of_json "query response.spend_summary" result_value >>= fun value -> Ok (Spend_summary_result value)
    | _ -> Error (errorf "query response kind %S is unsupported" kind)
  in
  let* cost_status = required "query response" "cost_status" fields >>= string "query response.cost_status" in
  let* cost = match cost_status with
    | "exact" -> let* actual = required "query response" "actual_cost_usd" fields in let* actual = string "query response.actual_cost_usd" actual in let* actual = match Usd_decimal.of_string actual with Ok value -> Ok value | Error message -> Error (errorf "query response.actual_cost_usd: %s" message) in let* method_ = required "query response" "cost_method" fields >>= string "query response.cost_method" >>= function "provider_reported" -> Ok Provider_reported | "catalog_usage" -> Ok Catalog_usage | "control_query_zero" -> Ok Control_query_zero | _ -> Error (errorf "query response.cost_method is invalid") in Ok (Exact_cost { actual_cost_usd = actual; method_; catalog_version = None })
    | "unknown" -> let* reason = required "query response" "cost_unknown_reason_code" fields >>= string "query response.cost_unknown_reason_code" >>= function "provider_did_not_report_cost" -> Ok Provider_did_not_report_cost | "catalog_incomplete" -> Ok Catalog_incomplete | "state_unavailable" -> Ok State_unavailable | "ambiguous_dispatch" -> Ok Ambiguous_dispatch | _ -> Error (errorf "query response.cost_unknown_reason_code is invalid") in Ok (Unknown_cost { reason })
    | _ -> Error (errorf "query response.cost_status is invalid")
  in
  Ok { api_version = version; operation_key = Operation_key.of_string operation_key; query_execution_id = Query_execution_id.of_string query_execution_id; observed_at; source; freshness; complete; next_cursor; result; cost }

let encode_query_response value = let* value = query_response_to_json value in to_bytes value
let decode_query_response bytes = parse_json query_response_of_json bytes
