open Llm_temporal_models

type request = generate_request
type response = generate_response

module Settings = Llm_temporal_conversation.Settings
module Cache_policy = Llm_temporal_conversation.Cache_policy

let make ~operation_key ~context ~model ?(settings = Settings.default) ?cache ~input () =
  let conversation = Llm_temporal_conversation.root ~context ~model ~settings () in
  Llm_temporal_conversation.to_request ?cache ~operation_key ~append:input conversation

type dispatcher =
  ?task_queue:Temporal_task_queue.t ->
  (request, response) Temporal.Activity.t ->
  request -> (response, Temporal.Error.t) result

let operation_key_mismatch ~expected ~actual =
  Temporal.Error.codec
    ~message:(Printf.sprintf
                "generate response operation key mismatch: expected %s, got %s"
                (Operation_key.to_string expected)
                (Operation_key.to_string actual))

let invoke_with ?task_queue ~dispatch request =
  match Llm_temporal_invocation.invoke_generate_once ?task_queue ~dispatch request with
  | Error error -> Error error
  | Ok response when
      not (String.equal
             (Operation_key.to_string response.operation_key)
             (Operation_key.to_string request.operation_key)) ->
      Error (operation_key_mismatch ~expected:request.operation_key
               ~actual:response.operation_key)
  | Ok response -> Ok response

let invoke_dispatch ?task_queue activity request =
  Temporal.Activity.execute
    ?task_queue:(Option.map Temporal_task_queue.to_string task_queue)
    ~retry_policy:Llm_temporal_invocation.activity_retry_policy activity request

let invoke ?task_queue request =
  invoke_with ?task_queue ~dispatch:invoke_dispatch request

let start ?task_queue request =
  Temporal.Activity.start
    ?task_queue:(Option.map Temporal_task_queue.to_string task_queue)
    ~retry_policy:Llm_temporal_invocation.activity_retry_policy
    Llm_temporal_invocation.generate_v1_activity request
