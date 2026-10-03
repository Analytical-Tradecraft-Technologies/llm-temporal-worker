(** Typed calls to the public [llm.generate.workflow.v1] child workflow. *)

open Llm_temporal_models

type request = generate_request
type response = generate_response

module Settings : module type of Llm_temporal_conversation.Settings
module Cache_policy : module type of Llm_temporal_conversation.Cache_policy

val make :
  operation_key:Operation_key.t ->
  context:request_context ->
  model:Model_selector.t ->
  ?settings:Settings.t ->
  ?cache:Cache_policy.t ->
  input:item list ->
  unit -> request

type dispatcher =
  ?task_queue:Temporal_task_queue.t ->
  (request, response) Temporal.Workflow.t ->
  request -> (response, Temporal.Error.t) result

val invoke_with :
  ?task_queue:Temporal_task_queue.t ->
  dispatch:dispatcher ->
  request -> (response, Temporal.Error.t) result

(** Supply the Go queue and a deterministic child ID unique in the namespace.
    Paid work survives parent closure; no cancellation API is exposed. *)
val invoke :
  task_queue:Temporal_task_queue.t -> id:string ->
  request -> (response, Temporal.Error.t) result

(** Starts without waiting. The inner result reports response validation
    errors; the outer future error reports child execution or codec failures. *)
val start :
  task_queue:Temporal_task_queue.t -> id:string ->
  request -> ((response, Temporal.Error.t) result, Temporal.Error.t) Temporal.Future.t
