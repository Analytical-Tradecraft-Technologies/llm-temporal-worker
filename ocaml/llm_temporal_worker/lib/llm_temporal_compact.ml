open Llm_temporal_models
let ( let* ) = Result.bind
type request = compact_request
type response = compaction_response
let workflow = Llm_temporal_invocation.compact_v1_workflow
let make ~operation_key ~context ~parent ?policy ?cache () =
  let request = { api_version = Llm_temporal_v1_codec.compact_api_version; operation_key; context; parent; policy; cache } in
  let* _ = Temporal.Codec.encode (Temporal.Workflow.input workflow) request in
  Ok request
type dispatcher = ?task_queue:Temporal_task_queue.t -> (request, response) Temporal.Workflow.t -> request -> (response, Temporal.Error.t) result
let invoke_with ?task_queue ~dispatch request =
  let* response = Llm_temporal_invocation.invoke_compact_once ?task_queue ~dispatch request in
  let* () = Llm_temporal_response_validation.validate_compaction_response_for_request request response in
  Ok response
let invoke ~task_queue ~id request =
  invoke_with ~task_queue ~dispatch:(fun ?task_queue:_ _ request -> Llm_temporal_invocation.invoke_compact_v1 ~task_queue ~id request) request
let start ~task_queue ~id request =
  Temporal.Future.map (fun response ->
    let* () = Llm_temporal_response_validation.validate_compaction_response_for_request request response in Ok response)
    (Llm_temporal_invocation.start_compact_v1 ~task_queue ~id request)
