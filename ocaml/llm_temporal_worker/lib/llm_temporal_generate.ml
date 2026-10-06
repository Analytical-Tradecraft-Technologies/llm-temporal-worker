open Llm_temporal_models

type request = generate_request
type response = generate_response

module Settings = Llm_temporal_conversation.Settings
module Cache_policy = Llm_temporal_conversation.Cache_policy

let make ~operation_key ~context ~model ?(settings = Settings.default) ?cache ~input () =
  let conversation = Llm_temporal_conversation.root ~context ~model ~settings () in
  let request =
    Llm_temporal_conversation.to_request ?cache ~operation_key ~append:input conversation
  in
  match Llm_temporal_conversation.validate_cache_temperature
          request.cache request.settings_patch with
  | Ok () -> request
  | Error message -> invalid_arg message

let make_checked ~operation_key ~context ~model ?settings ?cache ~input () =
  try
    let request = make ~operation_key ~context ~model ?settings ?cache ~input () in
    Result.map (fun _ -> request)
      (Temporal.Codec.encode (Temporal.Workflow.input Llm_temporal_invocation.generate_v1_workflow) request)
  with Invalid_argument message -> Error (Temporal.Error.codec ~message)

type dispatcher =
  ?task_queue:Temporal_task_queue.t ->
  (request, response) Temporal.Workflow.t ->
  request -> (response, Temporal.Error.t) result

let operation_key_mismatch ~expected ~actual =
  Temporal.Error.codec
    ~message:(Printf.sprintf
                "generate response operation key mismatch: expected %s, got %s"
                (Operation_key.to_string expected)
                (Operation_key.to_string actual))

let invoke_with ?task_queue ~dispatch (request : request) =
  match Llm_temporal_conversation.validate_cache_temperature
          request.cache request.settings_patch with
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
      | Ok response ->
          (match Llm_temporal_response_validation.validate_generate_response_for_request
                   request response with
           | Error error -> Error error
           | Ok () -> Ok response)

let invoke ~task_queue ~id request =
  let dispatch ?task_queue:_ _workflow input =
    Llm_temporal_invocation.invoke_generate ~task_queue ~id input
  in
  invoke_with ~task_queue ~dispatch request

let start ~task_queue ~id request =
  Temporal.Future.map
    (fun response ->
       match Llm_temporal_response_validation.validate_generate_response_for_request request response with
       | Error error -> Error error
       | Ok () -> Ok response)
    (Llm_temporal_invocation.start_generate ~task_queue ~id request)
