open Llm_temporal_models

val api_version : string
val activity_name : string
val workflow_name : string
val request_codec : request Temporal.Codec.t
val response_codec : response Temporal.Codec.t
val generate_activity : (generate_request, generate_response) Temporal.Activity.t

(** The fixed Temporal retry policy used by [execute].  It permits exactly one
    attempt; Temporal does not retry a failed provider activity. *)
val activity_retry_policy : Temporal.Activity.Retry_policy.t

val invoke_once :
  ?task_queue:Temporal_task_queue.t ->
  dispatch:(?task_queue:Temporal_task_queue.t -> (request, response) Temporal.Activity.t -> request -> (response, Temporal.Error.t) result) ->
  request ->
  (response, Temporal.Error.t) result

val execute : ?task_queue:Temporal_task_queue.t -> generate_request -> (generate_response, Temporal.Error.t) result
val workflow : ?task_queue:Temporal_task_queue.t -> unit -> (generate_request, generate_response) Temporal.Workflow.t

val generate_v1_request_codec : generate_request Temporal.Codec.t
val generate_v1_response_codec : generate_response Temporal.Codec.t
val compact_v1_request_codec : compact_request Temporal.Codec.t
val compact_v1_response_codec : compaction_response Temporal.Codec.t
val query_v1_request_codec : query_envelope Temporal.Codec.t
val query_v1_response_codec : query_response Temporal.Codec.t
val generate_v1_activity : (generate_request, generate_response) Temporal.Activity.t
val compact_v1_activity : (compact_request, compaction_response) Temporal.Activity.t
val query_v1_activity : (query_envelope, query_response) Temporal.Activity.t

(** Workflow-native low-level v1 Activity access.  These helpers schedule the
    exact wire records and apply the one-attempt policy used by the provider
    worker.  Prefer [Conversation] and [Query] for validation and typed
    result matching. *)
val start_generate :
  ?task_queue:Temporal_task_queue.t ->
  generate_request ->
  (generate_response, Temporal.Error.t) Temporal.Future.t

val invoke_generate :
  ?task_queue:Temporal_task_queue.t ->
  generate_request -> (generate_response, Temporal.Error.t) result

val start_compact_v1 :
  ?task_queue:Temporal_task_queue.t ->
  compact_request ->
  (compaction_response, Temporal.Error.t) Temporal.Future.t

val invoke_compact_v1 :
  ?task_queue:Temporal_task_queue.t ->
  compact_request -> (compaction_response, Temporal.Error.t) result

val start_query_v1 :
  ?task_queue:Temporal_task_queue.t ->
  query_envelope ->
  (query_response, Temporal.Error.t) Temporal.Future.t

val invoke_query_v1 :
  ?task_queue:Temporal_task_queue.t ->
  query_envelope -> (query_response, Temporal.Error.t) result

val invoke_generate_once :
  ?task_queue:Temporal_task_queue.t ->
  dispatch:(?task_queue:Temporal_task_queue.t -> (generate_request, generate_response) Temporal.Activity.t -> generate_request -> (generate_response, Temporal.Error.t) result) ->
  generate_request -> (generate_response, Temporal.Error.t) result
val invoke_compact_once :
  ?task_queue:Temporal_task_queue.t ->
  dispatch:(?task_queue:Temporal_task_queue.t -> (compact_request, compaction_response) Temporal.Activity.t -> compact_request -> (compaction_response, Temporal.Error.t) result) ->
  compact_request -> (compaction_response, Temporal.Error.t) result
val invoke_query_once :
  ?task_queue:Temporal_task_queue.t ->
  dispatch:(?task_queue:Temporal_task_queue.t -> (query_envelope, query_response) Temporal.Activity.t -> query_envelope -> (query_response, Temporal.Error.t) result) ->
  query_envelope -> (query_response, Temporal.Error.t) result
