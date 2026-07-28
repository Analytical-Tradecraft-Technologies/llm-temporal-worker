open Llm_temporal_models

let ( let* ) = Result.bind

let api_version = Llm_temporal_codec.api_version
let activity_name = "llm.generate.v1"
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
  (generate_request, generate_response) Temporal.Activity.t ->
  generate_request ->
  (generate_response, Temporal.Error.t) result

let activity_retry_policy =
  match Temporal.Activity.Retry_policy.make ~initial_interval:(Temporal.Duration.of_ms 1L)
          ~backoff_coefficient:1.0 ~maximum_interval:(Temporal.Duration.of_ms 1L)
          ~maximum_attempts:1 () with
  | Ok policy -> policy
  | Error error -> invalid_arg (Temporal.Error.message error)

let generate_v1_request_codec =
  Temporal.Codec.make ~encoding:"json/plain" ~encode:Llm_temporal_v1_codec.encode_generate_request ~decode:Llm_temporal_v1_codec.decode_generate_request
let generate_v1_response_codec =
  Temporal.Codec.make ~encoding:"json/plain" ~encode:Llm_temporal_v1_codec.encode_generate_response ~decode:Llm_temporal_v1_codec.decode_generate_response
let compact_v1_request_codec =
  Temporal.Codec.make ~encoding:"json/plain" ~encode:Llm_temporal_v1_codec.encode_compact_request ~decode:Llm_temporal_v1_codec.decode_compact_request
let compact_v1_response_codec =
  Temporal.Codec.make ~encoding:"json/plain" ~encode:Llm_temporal_v1_codec.encode_compaction_response ~decode:Llm_temporal_v1_codec.decode_compaction_response
let query_v1_request_codec =
  Temporal.Codec.make ~encoding:"json/plain" ~encode:Llm_temporal_v1_codec.encode_query_envelope ~decode:Llm_temporal_v1_codec.decode_query_envelope
let query_v1_response_codec =
  Temporal.Codec.make ~encoding:"json/plain" ~encode:Llm_temporal_v1_codec.encode_query_response ~decode:Llm_temporal_v1_codec.decode_query_response

let generate_v1_activity = Temporal.Activity.remote ~name:activity_name ~input:generate_v1_request_codec ~output:generate_v1_response_codec
let compact_v1_activity = Temporal.Activity.remote ~name:"llm.compact.v1" ~input:compact_v1_request_codec ~output:compact_v1_response_codec
let query_v1_activity = Temporal.Activity.remote ~name:"llm.query.v1" ~input:query_v1_request_codec ~output:query_v1_response_codec

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
            (match Usd_decimal.of_string (Float.to_string value) with
             | Ok value -> Ok (Set value)
             | Error message -> conversion_error
                 ("legacy Request.sampling.temperature: " ^ message))

let legacy_reasoning_patch = function
  | None -> Ok (Keep, Keep)
  | Some { mode; effort; token_budget; summary } ->
      (match mode with
       | Reasoning_disabled -> conversion_error
           "legacy Request.reasoning.mode=Reasoning_disabled is not representable by Generate v1"
       | Provider_default | Adaptive | Reasoning_enabled ->
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
              model = Set request.model;
              service_class = Set request.service_class;
              service_class_fallbacks = Set request.service_class_fallbacks;
              portability = Set request.portability;
              instructions = Set request.instructions;
              tools = Set request.tools;
              tool_policy = Set request.tool_policy;
              output = (match request.output with None -> Clear | Some value -> Set value);
              temperature;
              reasoning_effort;
              reasoning_summary;
              compaction_policy = Keep;
              extensions = Set request.extensions };
            cache = None })

(* [Request.make] remains source-compatible for callers that still construct
   the pre-checkpoint record.  This deprecated helper is the only path from
   that record into Temporal: it validates the fields that cannot be carried
   by v1, then dispatches the canonical [llm.generate.v1] descriptor. *)
let invoke_once ?task_queue ~(dispatch : dispatcher) (input : request) =
  match legacy_request_to_generate input with
  | Error error -> Error error
  | Ok request ->
      (match dispatch ?task_queue generate_v1_activity request with
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

(* The facade's canonical one-shot descriptor is v1.  The deprecated
   [invoke_once] function below converts its old constructor shape before
   dispatch, so no public helper can pair the legacy wire codec with this
   Activity name. *)
let generate_activity = generate_v1_activity

let execute ?task_queue input =
  Temporal.Activity.execute
    ?task_queue:(Option.map Temporal_task_queue.to_string task_queue)
    ~retry_policy:activity_retry_policy generate_v1_activity input

let workflow ?task_queue () =
  Temporal.Workflow.define ~name:workflow_name ~input:generate_v1_request_codec
    ~output:generate_v1_response_codec
    (fun input -> execute ?task_queue input)

(* The low-level v1 helpers intentionally keep the wire records visible.  A
   caller that needs the Conversation or Query invariants should use those
   facades instead; these functions are useful for protocol tests and for
   workflows that already own the exact request records. *)
let task_queue_string = Option.map Temporal_task_queue.to_string

let start_generate ?task_queue request =
  Temporal.Activity.start
    ?task_queue:(task_queue_string task_queue)
    ~retry_policy:activity_retry_policy generate_v1_activity request

let invoke_generate ?task_queue request =
  Temporal.Activity.execute
    ?task_queue:(task_queue_string task_queue)
    ~retry_policy:activity_retry_policy generate_v1_activity request

let start_compact_v1 ?task_queue request =
  Temporal.Activity.start
    ?task_queue:(task_queue_string task_queue)
    ~retry_policy:activity_retry_policy compact_v1_activity request

let invoke_compact_v1 ?task_queue request =
  Temporal.Activity.execute
    ?task_queue:(task_queue_string task_queue)
    ~retry_policy:activity_retry_policy compact_v1_activity request

let start_query_v1 ?task_queue envelope =
  Temporal.Activity.start
    ?task_queue:(task_queue_string task_queue)
    ~retry_policy:activity_retry_policy query_v1_activity envelope

let invoke_query_v1 ?task_queue envelope =
  Temporal.Activity.execute
    ?task_queue:(task_queue_string task_queue)
    ~retry_policy:activity_retry_policy query_v1_activity envelope

let invoke_generate_once ?task_queue ~(dispatch : ?task_queue:Temporal_task_queue.t -> (generate_request, generate_response) Temporal.Activity.t -> generate_request -> (generate_response, Temporal.Error.t) result) input =
  dispatch ?task_queue generate_v1_activity input
let invoke_compact_once ?task_queue ~(dispatch : ?task_queue:Temporal_task_queue.t -> (compact_request, compaction_response) Temporal.Activity.t -> compact_request -> (compaction_response, Temporal.Error.t) result) input =
  dispatch ?task_queue compact_v1_activity input
let invoke_query_once ?task_queue ~(dispatch : ?task_queue:Temporal_task_queue.t -> (query_envelope, query_response) Temporal.Activity.t -> query_envelope -> (query_response, Temporal.Error.t) result) input =
  dispatch ?task_queue query_v1_activity input
