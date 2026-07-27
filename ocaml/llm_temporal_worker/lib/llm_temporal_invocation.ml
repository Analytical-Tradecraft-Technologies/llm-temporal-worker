open Llm_temporal_models

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

(* Keep the pre-checkpoint descriptor private to the compatibility helper
   below.  The public [generate_activity] descriptor is defined after the v1
   codecs and therefore cannot accidentally pair the production activity name
   with the legacy payload codec. *)
let legacy_generate_activity =
  Temporal.Activity.remote ~name:activity_name ~input:request_codec ~output:response_codec

type dispatcher =
  ?task_queue:Temporal_task_queue.t ->
  (request, response) Temporal.Activity.t ->
  request ->
  (response, Temporal.Error.t) result

let invoke_once ?task_queue ~(dispatch : dispatcher) input =
  dispatch ?task_queue legacy_generate_activity input

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

(* The facade's canonical one-shot descriptor is v1.  Keep the old
   [invoke_once] function available for source compatibility, but make it
   impossible for [execute] or [workflow] to dispatch the legacy wire shape. *)
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
