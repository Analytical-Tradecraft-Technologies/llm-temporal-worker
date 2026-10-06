open Llm_temporal_models

val api_version : string
val workflow_name : string
(* Deprecated pre-checkpoint codec. It remains exported only for source
   compatibility and must not be paired with the public Generate
   workflow; [invoke_once] converts its request to the canonical v1 codec. *)
val request_codec : request Temporal.Codec.t
(* Deprecated pre-checkpoint response codec retained for source compatibility.
   Production callers should use [generate_v1_response_codec]. *)
val response_codec : response Temporal.Codec.t
val generate_workflow : (generate_request, generate_response) Temporal.Workflow.t

(** The one-attempt Temporal retry policy used only by Query activities.
    Generation and compaction retries belong to the Go workflows. *)
val activity_retry_policy : Temporal.Activity.Retry_policy.t

(** Deprecated compatibility helper.  It accepts the old [Request.make]
    record, rejects fields which have no v1 representation, and dispatches the
    canonical [llm.generate.workflow.v1] workflow.  The returned response is the typed
    v1 response; use [Generate.invoke] for new code. *)
val invoke_once :
  ?task_queue:Temporal_task_queue.t ->
  dispatch:(?task_queue:Temporal_task_queue.t -> (generate_request, generate_response) Temporal.Workflow.t -> generate_request -> (generate_response, Temporal.Error.t) result) ->
  request ->
  (generate_response, Temporal.Error.t) result

val execute : task_queue:Temporal_task_queue.t -> id:string -> generate_request -> (generate_response, Temporal.Error.t) result
val workflow : unit -> (generate_request, generate_response) Temporal.Workflow.t

val generate_v1_request_codec : generate_request Temporal.Codec.t
val generate_v1_response_codec : generate_response Temporal.Codec.t
val compact_v1_request_codec : compact_request Temporal.Codec.t
val compact_v1_response_codec : compaction_response Temporal.Codec.t
val query_v1_request_codec : query_envelope Temporal.Codec.t
val query_v1_response_codec : query_response Temporal.Codec.t
val generate_v1_workflow : (generate_request, generate_response) Temporal.Workflow.t
val compact_v1_workflow : (compact_request, compaction_response) Temporal.Workflow.t
val query_v1_activity : (query_envelope, query_response) Temporal.Activity.t

(** Low-level child workflow calls return exact wire responses. Supply the Go
    worker queue and a deterministic child ID unique within the namespace.
    No cancellation handle or scope is exposed; paid children survive parent
    closure. Prefer [Generate] and [Conversation] for response validation.
    Query remains a one-attempt Activity. *)
val start_generate :
  task_queue:Temporal_task_queue.t -> id:string ->
  generate_request ->
  (generate_response, Temporal.Error.t) Temporal.Future.t

val invoke_generate :
  task_queue:Temporal_task_queue.t -> id:string ->
  generate_request -> (generate_response, Temporal.Error.t) result

val start_compact_v1 :
  task_queue:Temporal_task_queue.t -> id:string ->
  compact_request ->
  (compaction_response, Temporal.Error.t) Temporal.Future.t

val invoke_compact_v1 :
  task_queue:Temporal_task_queue.t -> id:string ->
  compact_request -> (compaction_response, Temporal.Error.t) result

val start_query_v1 :
  task_queue:Temporal_task_queue.t ->
  query_envelope ->
  (query_response, Temporal.Error.t) Temporal.Future.t

val invoke_query_v1 :
  task_queue:Temporal_task_queue.t ->
  query_envelope -> (query_response, Temporal.Error.t) result

val invoke_generate_once :
  ?task_queue:Temporal_task_queue.t ->
  dispatch:(?task_queue:Temporal_task_queue.t -> (generate_request, generate_response) Temporal.Workflow.t -> generate_request -> (generate_response, Temporal.Error.t) result) ->
  generate_request -> (generate_response, Temporal.Error.t) result
val invoke_compact_once :
  ?task_queue:Temporal_task_queue.t ->
  dispatch:(?task_queue:Temporal_task_queue.t -> (compact_request, compaction_response) Temporal.Workflow.t -> compact_request -> (compaction_response, Temporal.Error.t) result) ->
  compact_request -> (compaction_response, Temporal.Error.t) result
val invoke_query_once :
  ?task_queue:Temporal_task_queue.t ->
  dispatch:(?task_queue:Temporal_task_queue.t -> (query_envelope, query_response) Temporal.Activity.t -> query_envelope -> (query_response, Temporal.Error.t) result) ->
  query_envelope -> (query_response, Temporal.Error.t) result
