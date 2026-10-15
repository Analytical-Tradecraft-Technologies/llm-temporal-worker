open Llm_temporal_models
type request = compact_request
type response = compaction_response
val make : operation_key:Operation_key.t -> context:request_context -> parent:Checkpoint.t -> ?policy:compaction_policy -> ?summarizer_model:Model_selector.t -> ?summarizer_service_class:service_class -> ?cache:Llm_temporal_conversation.Cache_policy.t -> unit -> (request, Temporal.Error.t) result
type dispatcher = ?task_queue:Temporal_task_queue.t -> (request, response) Temporal.Workflow.t -> request -> (response, Temporal.Error.t) result
val invoke_with : ?task_queue:Temporal_task_queue.t -> dispatch:dispatcher -> request -> (response, Temporal.Error.t) result
val invoke : task_queue:Temporal_task_queue.t -> id:string -> request -> (response, Temporal.Error.t) result
val start : task_queue:Temporal_task_queue.t -> id:string -> request -> ((response, Temporal.Error.t) result, Temporal.Error.t) Temporal.Future.t
