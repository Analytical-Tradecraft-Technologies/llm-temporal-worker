open Llm_temporal_models

let ( let* ) = Result.bind

let api_version = Llm_temporal_codec.api_version
let workflow_name = "llm.generate.workflow.v1"

let request_codec =
  Temporal.Codec.make ~encoding:"json/plain"
    ~encode:Llm_temporal_codec.encode_request
    ~decode:Llm_temporal_codec.decode_request

let response_codec =
  Temporal.Codec.make ~encoding:"json/plain"
    ~encode:Llm_temporal_codec.encode_response
    ~decode:Llm_temporal_codec.decode_response

type dispatcher =
  ?task_queue:Temporal_task_queue.t ->
  (generate_request, generate_response) Temporal.Workflow.t ->
  generate_request ->
  (generate_response, Temporal.Error.t) result

(* Validate raw records before a child command can be emitted. Encoders may
   normalize the API version, so check the supplied version before encoding. *)
let validated_encode ~version ~actual ~encode ~decode request =
  if actual <> version then Error (Temporal.Error.codec ~message:"unsupported API version")
  else
    let* payload = encode request in
    let* _ = decode payload in
    Ok payload

let encode_generate (request : generate_request) =
  validated_encode ~version:Llm_temporal_v1_codec.generate_api_version ~actual:request.api_version
    ~encode:Llm_temporal_v1_codec.encode_generate_request ~decode:Llm_temporal_v1_codec.decode_generate_request request
let encode_compact (request : compact_request) =
  validated_encode ~version:Llm_temporal_v1_codec.compact_api_version ~actual:request.api_version
    ~encode:Llm_temporal_v1_codec.encode_compact_request ~decode:Llm_temporal_v1_codec.decode_compact_request request

let generate_v1_request_codec =
  Temporal.Codec.make ~encoding:"json/plain" ~encode:encode_generate ~decode:Llm_temporal_v1_codec.decode_generate_request
let generate_v1_response_codec =
  Temporal.Codec.make ~encoding:"json/plain" ~encode:Llm_temporal_v1_codec.encode_generate_response ~decode:Llm_temporal_v1_codec.decode_generate_response
let compact_v1_request_codec =
  Temporal.Codec.make ~encoding:"json/plain" ~encode:encode_compact ~decode:Llm_temporal_v1_codec.decode_compact_request
let compact_v1_response_codec =
  Temporal.Codec.make ~encoding:"json/plain" ~encode:Llm_temporal_v1_codec.encode_compaction_response ~decode:Llm_temporal_v1_codec.decode_compaction_response
let encode_query (request : query_envelope) =
  validated_encode ~version:Llm_temporal_v1_codec.query_api_version ~actual:request.api_version
    ~encode:Llm_temporal_v1_codec.encode_query_envelope ~decode:Llm_temporal_v1_codec.decode_query_envelope request

let query_v1_request_codec =
  Temporal.Codec.make ~encoding:"json/plain" ~encode:encode_query ~decode:Llm_temporal_v1_codec.decode_query_envelope
let query_v1_response_codec =
  Temporal.Codec.make ~encoding:"json/plain" ~encode:Llm_temporal_v1_codec.encode_query_response ~decode:Llm_temporal_v1_codec.decode_query_response

let generate_v1_workflow = Temporal.Workflow.remote ~name:workflow_name ~input:generate_v1_request_codec ~output:generate_v1_response_codec
let compact_v1_workflow = Temporal.Workflow.remote ~name:"llm.compact.workflow.v1" ~input:compact_v1_request_codec ~output:compact_v1_response_codec
let query_v1_workflow = Temporal.Workflow.remote ~name:"llm.query.workflow.v1" ~input:query_v1_request_codec ~output:query_v1_response_codec

let conversion_error message = Error (Temporal.Error.codec ~message)

let require_nonempty context module_name value =
  if String.equal value "" then conversion_error (Printf.sprintf
    "legacy Request %s.%s must not be empty" context module_name)
  else Ok ()

let legacy_context_to_v1 = function
  | None -> conversion_error
      "legacy Request.context must include tenant, project, and actor for Generate v1"
  | Some ({ tenant = Some tenant; project = Some project; actor = Some actor; tags } as context) ->
      if tags <> [] then conversion_error
        "legacy Request.context.tags is not representable by Generate v1"
      else
        let* () = require_nonempty "context" "tenant" (Tenant_id.to_string tenant) in
        let* () = require_nonempty "context" "project" (Project_id.to_string project) in
        let* () = require_nonempty "context" "actor" (Actor_id.to_string actor) in
        Ok context
  | Some _ -> conversion_error
      "legacy Request.context must include tenant, project, and actor for Generate v1"

let fixed_decimal_of_float value =
  (* [Float.to_string] is the shortest round-tripping representation, but it
     may use an exponent (for example [1e-10]).  Expand that representation
     before feeding it to the exact fixed-point USD decimal parser. *)
  if value = 0.0 then Ok "0"
  else
    let repr = Float.to_string value in
    let negative = repr.[0] = '-' in
    let unsigned = if negative then String.sub repr 1 (String.length repr - 1) else repr in
    let exponent_index =
      match String.index_opt unsigned 'e', String.index_opt unsigned 'E' with
      | Some index, None | None, Some index -> Some index
      | Some left, Some right -> Some (min left right)
      | None, None -> None
    in
    let mantissa, exponent =
      match exponent_index with
      | None -> unsigned, 0
      | Some index ->
          (String.sub unsigned 0 index,
           int_of_string (String.sub unsigned (index + 1)
             (String.length unsigned - index - 1)))
    in
    let decimal_index = String.index_opt mantissa '.' in
    let whole_length = Option.value ~default:(String.length mantissa)
      (Option.map Fun.id decimal_index) in
    let digits =
      match decimal_index with
      | None -> mantissa
      | Some index -> String.sub mantissa 0 index ^
                      String.sub mantissa (index + 1)
                        (String.length mantissa - index - 1)
    in
    let point = whole_length + exponent in
    let integer, fraction =
      if point <= 0 then "0", (String.make (-point) '0' ^ digits)
      else if point >= String.length digits then
        digits ^ String.make (point - String.length digits) '0', ""
      else String.sub digits 0 point,
           String.sub digits point (String.length digits - point)
    in
    let integer =
      let rec first_nonzero index =
        if index + 1 < String.length integer && integer.[index] = '0' then
          first_nonzero (index + 1)
        else index
      in
      String.sub integer (first_nonzero 0)
        (String.length integer - first_nonzero 0)
    in
    let rec trim_fraction_end value =
      if String.length value > 0 && value.[String.length value - 1] = '0' then
        trim_fraction_end (String.sub value 0 (String.length value - 1))
      else value
    in
    let fraction = trim_fraction_end fraction in
    let result = if fraction = "" then integer else integer ^ "." ^ fraction in
    Ok (if negative then "-" ^ result else result)

let legacy_sampling_temperature = function
  | None -> Ok Keep
  | Some { temperature; top_p; top_k; seed; presence_penalty; frequency_penalty; stop_sequences } ->
      if Option.is_some top_p || Option.is_some top_k || Option.is_some seed
         || Option.is_some presence_penalty || Option.is_some frequency_penalty
         || Option.is_some stop_sequences then
        conversion_error
          "legacy Request.sampling contains controls not representable by Generate v1"
      else
        match temperature with
        | None -> Ok Keep
        | Some value when not (Float.is_finite value) ->
            conversion_error "legacy Request.sampling.temperature must be finite"
        | Some value ->
            (match fixed_decimal_of_float value with
             | Error message -> conversion_error
                 ("legacy Request.sampling.temperature: " ^ message)
             | Ok value ->
               match Usd_decimal.of_string value with
             | Ok value -> Ok (Set value)
             | Error message -> conversion_error
                 ("legacy Request.sampling.temperature: " ^ message))

let legacy_reasoning_patch = function
  | None -> Ok (Keep, Keep)
  | Some { mode; effort; token_budget; summary } ->
      (match mode with
       | Reasoning_disabled -> conversion_error
           "legacy Request.reasoning.mode=Reasoning_disabled is not representable by Generate v1"
       | Adaptive | Reasoning_enabled -> conversion_error
           "legacy Request.reasoning.mode is not representable by Generate v1"
       | Provider_default ->
           match token_budget with
           | Some _ -> conversion_error
               "legacy Request.reasoning.token_budget is not representable by Generate v1"
           | None -> Ok (Set effort, Set summary))

let legacy_request_to_generate (request : request) =
  let* context = legacy_context_to_v1 request.context in
  let* temperature = legacy_sampling_temperature request.sampling in
  let* reasoning_effort, reasoning_summary = legacy_reasoning_patch request.reasoning in
  (match request.continuation with
   | Some _ -> conversion_error
       "legacy Request.continuation is not representable by Generate v1"
   | None ->
       Ok { api_version = Llm_temporal_v1_codec.generate_api_version;
            operation_key = request.operation_key;
            context;
            parent = None;
            append = request.input;
            settings_patch = {
              web_search = Keep; web_fetch = Keep; code_execution = Keep;
              model = Set request.model;
              service_class = Set request.service_class;
              service_class_fallbacks = Set request.service_class_fallbacks;
              portability = Set request.portability;
              instructions = Set request.instructions;
              tools = Set request.tools;
              tool_policy = Set request.tool_policy;
              output = (match request.output with None -> Clear | Some value -> Set value);
              temperature;
              (* The legacy converter still rejects these controls above;
                 it maps only what it carried before they reached v1. *)
              top_p = Keep; stop_sequences = Keep; seed = Keep;
              reasoning_mode = Keep; reasoning_token_budget = Keep;
              reasoning_effort;
              reasoning_summary;
              compaction_policy = Keep;
              extensions = Set request.extensions };
            cache = None })

(* [Request.make] remains source-compatible for callers that still construct
   the pre-checkpoint record.  This deprecated helper is the only path from
   that record into Temporal: it validates the fields that cannot be carried
   by v1, then dispatches the canonical [llm.generate.workflow.v1] descriptor. *)
let invoke_once ?task_queue ~(dispatch : dispatcher) (input : request) =
  match legacy_request_to_generate input with
  | Error error -> Error error
  | Ok request ->
      (match dispatch ?task_queue generate_v1_workflow request with
       | Error error -> Error error
       | Ok response when
           not (String.equal
                  (Operation_key.to_string response.operation_key)
                  (Operation_key.to_string request.operation_key)) ->
           Error (Temporal.Error.codec
             ~message:(Printf.sprintf
               "generate response operation key mismatch: expected %s, got %s"
               (Operation_key.to_string request.operation_key)
               (Operation_key.to_string response.operation_key)))
       | Ok response -> Ok response)

(* Generation and compaction run as child workflows. The caller supplies an
   identity unique to this child start in the namespace, derived deterministically
   from its own durable request identity. Paid work survives parent closure. *)
let generate_workflow = generate_v1_workflow
let workflow () = generate_v1_workflow

let start_child ~task_queue ~id definition request =
  Temporal.Child_workflow.start
    ~task_queue:(Temporal_task_queue.to_string task_queue) ~id
    ~parent_close_policy:Temporal.Child_workflow.Parent_close_policy.Abandon
    ~cancellation_type:Temporal.Child_workflow.Abandon
    definition request

let start_generate ~task_queue ~id request =
  start_child ~task_queue ~id generate_v1_workflow request
let invoke_generate ~task_queue ~id request =
  Temporal.Future.await (start_generate ~task_queue ~id request)
let execute = invoke_generate

let start_compact_v1 ~task_queue ~id request =
  start_child ~task_queue ~id compact_v1_workflow request
let invoke_compact_v1 ~task_queue ~id request =
  Temporal.Future.await (start_compact_v1 ~task_queue ~id request)

let start_query_v1 ~task_queue ~id envelope =
  Temporal.Child_workflow.start
    ~task_queue:(Temporal_task_queue.to_string task_queue) ~id
    ~parent_close_policy:Temporal.Child_workflow.Parent_close_policy.Request_cancel
    ~cancellation_type:Temporal.Child_workflow.Wait_cancellation_completed
    query_v1_workflow envelope

let invoke_query_v1 ~task_queue ~id envelope =
  Temporal.Future.await (start_query_v1 ~task_queue ~id envelope)

let invoke_generate_once ?task_queue ~(dispatch : ?task_queue:Temporal_task_queue.t -> (generate_request, generate_response) Temporal.Workflow.t -> generate_request -> (generate_response, Temporal.Error.t) result) input =
  let* _ = encode_generate input in
  let* response = dispatch ?task_queue generate_v1_workflow input in
  let* _ = validated_encode ~version:Llm_temporal_v1_codec.generate_api_version ~actual:response.api_version
    ~encode:Llm_temporal_v1_codec.encode_generate_response ~decode:Llm_temporal_v1_codec.decode_generate_response response in
  Ok response
let invoke_compact_once ?task_queue ~(dispatch : ?task_queue:Temporal_task_queue.t -> (compact_request, compaction_response) Temporal.Workflow.t -> compact_request -> (compaction_response, Temporal.Error.t) result) input =
  let* _ = encode_compact input in
  let* response = dispatch ?task_queue compact_v1_workflow input in
  let* _ = validated_encode ~version:Llm_temporal_v1_codec.compact_api_version ~actual:response.api_version
    ~encode:Llm_temporal_v1_codec.encode_compaction_response ~decode:Llm_temporal_v1_codec.decode_compaction_response response in
  Ok response
let invoke_query_once ?task_queue ~(dispatch : ?task_queue:Temporal_task_queue.t -> (query_envelope, query_response) Temporal.Workflow.t -> query_envelope -> (query_response, Temporal.Error.t) result) input =
  let* _ = encode_query input in
  dispatch ?task_queue query_v1_workflow input
