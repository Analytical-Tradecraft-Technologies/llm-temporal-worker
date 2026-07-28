open Llm_temporal_models

module Settings = struct
  type t = {
    service_class : service_class;
    service_class_fallbacks : service_class list;
    portability : portability;
    instructions : instruction list;
    tools : function_tool list;
    tool_policy : tool_policy;
    output : output_spec option;
    temperature : Usd_decimal.t option;
    reasoning_effort : reasoning_effort option;
    reasoning_summary : reasoning_summary option;
    extensions : (string * Yojson.Safe.t) list;
  }

  let make
      ?(service_class = Standard)
      ?(service_class_fallbacks = [])
      ?(portability = Strict)
      ?(instructions = [])
      ?(tools = [])
      ?(tool_policy = { choice = Auto; parallel = false })
      ?output
      ?temperature
      ?reasoning_effort
      ?reasoning_summary
      ?(extensions = [])
      () =
    { service_class; service_class_fallbacks; portability; instructions; tools;
      tool_policy; output; temperature; reasoning_effort; reasoning_summary;
      extensions }

  let default = make ()

  module Patch = struct
    type t = settings_patch

    let keep =
      { model = Keep; service_class = Keep; service_class_fallbacks = Keep;
        portability = Keep; instructions = Keep; tools = Keep;
        tool_policy = Keep; output = Keep; temperature = Keep;
        reasoning_effort = Keep; reasoning_summary = Keep;
        compaction_policy = Keep; extensions = Keep }

    let set_model value (patch : t) = { patch with model = (Set value : Model_selector.t patch) }
    let clear_model (patch : t) = { patch with model = Clear }
    let set_service_class value (patch : t) = { patch with service_class = (Set value : service_class patch) }
    let clear_service_class (patch : t) = { patch with service_class = Clear }
    let set_service_class_fallbacks value (patch : t) = { patch with service_class_fallbacks = (Set value : service_class list patch) }
    let clear_service_class_fallbacks (patch : t) = { patch with service_class_fallbacks = Clear }
    let set_portability value (patch : t) = { patch with portability = (Set value : portability patch) }
    let clear_portability (patch : t) = { patch with portability = Clear }
    let set_instructions value (patch : t) = { patch with instructions = (Set value : instruction list patch) }
    let clear_instructions (patch : t) = { patch with instructions = Clear }
    let replace_tools value (patch : t) = { patch with tools = (Set value : function_tool list patch) }
    let clear_tools (patch : t) = { patch with tools = Clear }
    let set_tool_policy value (patch : t) = { patch with tool_policy = (Set value : tool_policy patch) }
    let clear_tool_policy (patch : t) = { patch with tool_policy = Clear }
    let replace_output value (patch : t) = { patch with output = (Set value : output_spec patch) }
    let clear_output (patch : t) = { patch with output = Clear }
    let set_temperature value (patch : t) = { patch with temperature = (Set value : Usd_decimal.t patch) }
    let clear_temperature (patch : t) = { patch with temperature = Clear }
    let set_reasoning_effort value (patch : t) = { patch with reasoning_effort = (Set value : reasoning_effort patch) }
    let clear_reasoning_effort (patch : t) = { patch with reasoning_effort = Clear }
    let set_reasoning_summary value (patch : t) = { patch with reasoning_summary = (Set value : reasoning_summary patch) }
    let clear_reasoning_summary (patch : t) = { patch with reasoning_summary = Clear }
    let set_compaction_policy value (patch : t) = { patch with compaction_policy = (Set value : Yojson.Safe.t patch) }
    let clear_compaction_policy (patch : t) = { patch with compaction_policy = Clear }
    let replace_extensions value (patch : t) = { patch with extensions = (Set value : (string * Yojson.Safe.t) list patch) }
    let clear_extensions (patch : t) = { patch with extensions = Clear }
  end
end

module Cache_policy = struct
  type t = cache_policy

  let accept_up_to ~max_age_seconds ?(variant = 0l) () =
    if max_age_seconds < 1L || max_age_seconds > 31_536_000L then
      Error "cache max_age_seconds must be between 1 and 31536000"
    else if Int32.compare variant 0l < 0 then Error "cache variant must not be negative"
    else Ok { max_age_seconds; variant }

  let max_age_seconds (value : t) = value.max_age_seconds
  let variant (value : t) = value.variant
end

type t = {
  context : request_context;
  model : Model_selector.t option;
  settings : Settings.t;
  (* A checkpoint handle does not carry the caller's effective settings.  Keep
     only fields explicitly known to this facade for post-compaction restore;
     an imported handle therefore starts with [Keep] for every field. *)
  restore_patch : settings_patch;
  checkpoint : Checkpoint.t option;
  restore_after_compact : bool;
}

type turn = {
  response : generate_response;
  conversation : t;
}

let operation_key_mismatch ~expected ~actual =
  Temporal.Error.codec
    ~message:(Printf.sprintf
                "activity response operation key mismatch: expected %s, got %s"
                (Operation_key.to_string expected)
                (Operation_key.to_string actual))

let root ~context ~model ?service_class ?(settings = Settings.default) () =
  let settings = match service_class with
    | None -> settings
    | Some service_class -> { settings with Settings.service_class }
  in
  let restore_patch =
    { Settings.Patch.keep with
      tools = Set settings.tools;
      tool_policy = Set settings.tool_policy;
      output = (match settings.output with None -> Clear | Some value -> Set value) }
  in
  { context; model = Some model; settings; restore_patch;
    checkpoint = None; restore_after_compact = false }

let of_checkpoint ~context ~checkpoint =
  { context; model = None; settings = Settings.default;
    restore_patch = Settings.Patch.keep;
    checkpoint = Some checkpoint; restore_after_compact = false }

let context conversation = conversation.context
let model conversation = conversation.model
let checkpoint conversation = conversation.checkpoint
let fork conversation = conversation

let patch_override (base : settings_patch) (override : settings_patch) : settings_patch =
  let choose left right = match right with Keep -> left | _ -> right in
  { model = choose base.model override.model;
    service_class = choose base.service_class override.service_class;
    service_class_fallbacks = choose base.service_class_fallbacks override.service_class_fallbacks;
    portability = choose base.portability override.portability;
    instructions = choose base.instructions override.instructions;
    tools = choose base.tools override.tools;
    tool_policy = choose base.tool_policy override.tool_policy;
    output = choose base.output override.output;
    temperature = choose base.temperature override.temperature;
    reasoning_effort = choose base.reasoning_effort override.reasoning_effort;
    reasoning_summary = choose base.reasoning_summary override.reasoning_summary;
    compaction_policy = choose base.compaction_policy override.compaction_policy;
    extensions = choose base.extensions override.extensions }

let initial_patch conversation : settings_patch =
  let settings = conversation.settings in
  { model = (match conversation.model with None -> Keep | Some value -> Set value);
    service_class = Set settings.service_class;
    service_class_fallbacks = Set settings.service_class_fallbacks;
    portability = Set settings.portability;
    instructions = Set settings.instructions;
    tools = Set settings.tools;
    tool_policy = Set settings.tool_policy;
    output = (match settings.output with None -> Clear | Some value -> Set value);
    temperature = (match settings.temperature with None -> Keep | Some value -> Set value);
    reasoning_effort = (match settings.reasoning_effort with None -> Keep | Some value -> Set value);
    reasoning_summary = (match settings.reasoning_summary with None -> Keep | Some value -> Set value);
    compaction_policy = Keep;
    extensions = Set settings.extensions }

let restore_patch conversation : settings_patch =
  conversation.restore_patch

let restore_patch_override (base : settings_patch) (override : settings_patch) : settings_patch =
  let choose_value ~cleared left right =
    match right with
    | Keep -> left
    | Set value -> Set value
    | Clear -> Set cleared
  in
  let choose_output left right = match right with Keep -> left | _ -> right in
  { base with
    tools = choose_value ~cleared:[] base.tools override.tools;
    tool_policy = choose_value ~cleared:{ choice = Auto; parallel = false }
        base.tool_policy override.tool_policy;
    output = choose_output base.output override.output }

let apply_patch (settings : Settings.t) (patch : settings_patch) =
  let value_or ~cleared current = function Keep -> current | Set value -> value | Clear -> cleared in
  let option_or current = function Keep -> current | Set value -> Some value | Clear -> None in
  ({ service_class = value_or ~cleared:Standard settings.service_class patch.service_class;
    service_class_fallbacks = value_or ~cleared:[] settings.service_class_fallbacks patch.service_class_fallbacks;
    portability = value_or ~cleared:Strict settings.portability patch.portability;
    instructions = value_or ~cleared:[] settings.instructions patch.instructions;
    tools = value_or ~cleared:[] settings.tools patch.tools;
    tool_policy = value_or ~cleared:{ choice = Auto; parallel = false } settings.tool_policy patch.tool_policy;
    output = option_or settings.output patch.output;
    temperature = option_or settings.temperature patch.temperature;
    reasoning_effort = option_or settings.reasoning_effort patch.reasoning_effort;
    reasoning_summary = option_or settings.reasoning_summary patch.reasoning_summary;
    extensions = value_or ~cleared:[] settings.extensions patch.extensions } : Settings.t)

let request_patch conversation (explicit : settings_patch option) : settings_patch =
  let base =
    if Option.is_none conversation.checkpoint then initial_patch conversation
    else if conversation.restore_after_compact then restore_patch conversation
    else Settings.Patch.keep
  in
  patch_override base (Option.value ~default:Settings.Patch.keep explicit)

let to_request ?settings_patch ?cache ~operation_key ~append conversation =
  { api_version = Llm_temporal_v1_codec.generate_api_version;
    operation_key; context = conversation.context; parent = conversation.checkpoint;
    append; settings_patch = request_patch conversation settings_patch; cache }

let validate_cache_temperature (cache : cache_policy option) settings_patch =
  match cache, settings_patch.temperature with
  | Some { variant; _ }, Set temperature
    when Int32.compare variant 0l > 0
         && Usd_decimal.compare temperature Usd_decimal.zero <= 0 ->
      Error "positive cache variant requires an explicitly positive temperature"
  | _ -> Ok ()

let child conversation (patch : settings_patch) checkpoint =
  { conversation with
    model = (match patch.model with Keep -> conversation.model | Set value -> Some value | Clear -> None);
    settings = apply_patch conversation.settings patch;
    restore_patch = restore_patch_override conversation.restore_patch patch;
    checkpoint = Some checkpoint;
    restore_after_compact = false }

type dispatcher =
  ?task_queue:Temporal_task_queue.t ->
  (generate_request, generate_response) Temporal.Activity.t ->
  generate_request -> (generate_response, Temporal.Error.t) result

let respond_with ?task_queue ~dispatch ?settings_patch ?cache ~operation_key ~append conversation =
  let request = to_request ?settings_patch ?cache ~operation_key ~append conversation in
  match validate_cache_temperature request.cache request.settings_patch with
  | Error message -> Error (Temporal.Error.codec ~message)
  | Ok () ->
      match Llm_temporal_invocation.invoke_generate_once ?task_queue ~dispatch request with
      | Error error -> Error error
      | Ok response when
          not (String.equal
                 (Operation_key.to_string response.operation_key)
                 (Operation_key.to_string request.operation_key)) ->
          Error (operation_key_mismatch ~expected:request.operation_key
                   ~actual:response.operation_key)
      | Ok response -> Ok { response; conversation = child conversation request.settings_patch response.checkpoint.handle }

let activity_dispatch ?task_queue activity input =
  Temporal.Activity.execute
    ?task_queue:(Option.map Temporal_task_queue.to_string task_queue)
    ~retry_policy:Llm_temporal_invocation.activity_retry_policy activity input

let respond ?task_queue ?settings_patch ?cache ~operation_key ~append conversation =
  respond_with ?task_queue ~dispatch:activity_dispatch ?settings_patch ?cache ~operation_key ~append conversation

let start_respond ?task_queue ?settings_patch ?cache ~operation_key ~append conversation =
  let request = to_request ?settings_patch ?cache ~operation_key ~append conversation in
  match validate_cache_temperature request.cache request.settings_patch with
  | Error message ->
      let error = Temporal.Error.codec ~message in
      Temporal.Future.map (fun _ -> Error error) (Temporal.Future.all [])
  | Ok () ->
      let future =
        Temporal.Activity.start
          ?task_queue:(Option.map Temporal_task_queue.to_string task_queue)
          ~retry_policy:Llm_temporal_invocation.activity_retry_policy
          Llm_temporal_invocation.generate_v1_activity request
      in
      Temporal.Future.map
        (fun (response : generate_response) ->
          if String.equal
               (Operation_key.to_string response.operation_key)
               (Operation_key.to_string request.operation_key)
          then Ok { response; conversation = child conversation request.settings_patch response.checkpoint.handle }
          else Error (operation_key_mismatch ~expected:request.operation_key
                        ~actual:response.operation_key))
        future

type compact_dispatcher =
  ?task_queue:Temporal_task_queue.t ->
  (compact_request, compaction_response) Temporal.Activity.t ->
  compact_request -> (compaction_response, Temporal.Error.t) result

let compact_request ?policy ?cache ~operation_key conversation =
  match conversation.checkpoint with
  | None -> Error (Temporal.Error.codec ~message:"cannot compact a conversation without a checkpoint")
  | Some parent ->
      (match cache with
       | Some cache when Cache_policy.variant cache <> 0l ->
           Error (Temporal.Error.codec ~message:"compact cache variant must be zero")
       | _ ->
           Ok { api_version = Llm_temporal_v1_codec.compact_api_version; operation_key;
                context = conversation.context; parent; policy; cache })

let compact_with ?task_queue ~dispatch ?policy ?cache ~operation_key conversation =
  match compact_request ?policy ?cache ~operation_key conversation with
  | Error error -> Error error
  | Ok request ->
      match Llm_temporal_invocation.invoke_compact_once ?task_queue ~dispatch request with
      | Error error -> Error error
      | Ok response when
          not (String.equal
                 (Operation_key.to_string response.operation_key)
                 (Operation_key.to_string request.operation_key)) ->
          Error (operation_key_mismatch ~expected:request.operation_key
                   ~actual:response.operation_key)
      | Ok response ->
          let conversation = { conversation with checkpoint = Some response.checkpoint.handle; restore_after_compact = true } in
          Ok (response, conversation)

let compact_dispatch ?task_queue activity input =
  Temporal.Activity.execute
    ?task_queue:(Option.map Temporal_task_queue.to_string task_queue)
    ~retry_policy:Llm_temporal_invocation.activity_retry_policy activity input

let compact ?task_queue ?policy ?cache ~operation_key conversation =
  compact_with ?task_queue ~dispatch:compact_dispatch ?policy ?cache ~operation_key conversation

let start_compact ?task_queue ?policy ?cache ~operation_key conversation =
  match compact_request ?policy ?cache ~operation_key conversation with
  | Error error ->
      Temporal.Future.map (fun _ -> Error error) (Temporal.Future.all [])
  | Ok request ->
      let future =
        Temporal.Activity.start
          ?task_queue:(Option.map Temporal_task_queue.to_string task_queue)
          ~retry_policy:Llm_temporal_invocation.activity_retry_policy
          Llm_temporal_invocation.compact_v1_activity request
      in
      Temporal.Future.map
        (fun (response : compaction_response) ->
          if String.equal
               (Operation_key.to_string response.operation_key)
               (Operation_key.to_string request.operation_key)
          then
            let conversation = { conversation with checkpoint = Some response.checkpoint.handle; restore_after_compact = true } in
            Ok (response, conversation)
          else Error (operation_key_mismatch ~expected:request.operation_key
                        ~actual:response.operation_key))
        future
