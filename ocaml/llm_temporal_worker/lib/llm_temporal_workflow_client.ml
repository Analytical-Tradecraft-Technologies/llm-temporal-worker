open Llm_temporal_models

let ( let* ) = Result.bind

type t = { client : Temporal.Client.t; namespace : string }

type 'response handle = Handle : {
    owner : t;
    workflow : ('request, 'response) Temporal.Workflow.t;
    request : 'request;
    run : ('request, 'response) Temporal.Client.handle;
    validate : 'request -> 'response -> (unit, Temporal.Error.t) result;
  } -> 'response handle

let generate_workflow = Temporal.Workflow.remote
    ~name:"llm.generate.workflow.v1"
    ~input:Llm_temporal_invocation.generate_v1_request_codec
    ~output:Llm_temporal_invocation.generate_v1_response_codec
let compact_workflow = Temporal.Workflow.remote
    ~name:"llm.compact.workflow.v1"
    ~input:Llm_temporal_invocation.compact_v1_request_codec
    ~output:Llm_temporal_invocation.compact_v1_response_codec

let create ?identity ~target_url ~namespace () =
  let* client = Temporal.Client.create ?identity ~target_url ~namespace () in
  Ok { client; namespace }
let shutdown owner = Temporal.Client.shutdown owner.client

let validate_identity expected_key actual_key (cache : cache_policy option) variant =
  if expected_key <> actual_key then
    Error (Temporal.Error.codec ~message:"workflow response operation key does not match request")
  else if variant <> (match cache with None -> 0l | Some policy -> policy.variant) then
    Error (Temporal.Error.codec ~message:"workflow response sample index does not match request")
  else Ok ()

let validate_generate (request : generate_request) (response : generate_response) =
  let* () = validate_identity request.operation_key response.operation_key request.cache response.cache.variant in
  Llm_temporal_response_validation.validate_generate_response_for_request request response
let validate_compact (request : compact_request) (response : compaction_response) =
  let* () = validate_identity request.operation_key response.operation_key request.cache response.cache.variant in
  Llm_temporal_response_validation.validate_compaction_response_for_request request response

let validate_request workflow request =
  let* encoded = Temporal.Codec.encode (Temporal.Workflow.input workflow) request in
  let* _ = Temporal.Codec.decode (Temporal.Workflow.input workflow) encoded in
  Ok ()

let start owner workflow validate ~task_queue ~id ~request_id request =
  let* () = validate_request workflow request in
  let* run = Temporal.Client.start owner.client ~workflow ~request_id
      ~task_queue:(Temporal_task_queue.to_string task_queue) ~id ~input:request () in
  Ok (Handle { owner; workflow; request; run; validate })
let validate_version expected actual =
  if expected = actual then Ok ()
  else Error (Temporal.Error.codec ~message:"unsupported request API version")

let start_generate owner ~task_queue ~id ~request_id (request : generate_request) =
  let* () = validate_version Llm_temporal_v1_codec.generate_api_version request.api_version in
  start owner generate_workflow validate_generate ~task_queue ~id ~request_id request
let start_compact owner ~task_queue ~id ~request_id (request : compact_request) =
  let* () = validate_version Llm_temporal_v1_codec.compact_api_version request.api_version in
  start owner compact_workflow validate_compact ~task_queue ~id ~request_id request

let execution (Handle handle) : Temporal.Client.execution =
  { namespace = handle.owner.namespace;
    workflow_id = Temporal.Client.workflow_id handle.run;
    run_id = Temporal.Client.run_id handle.run }

let resume owner workflow validate ~execution request =
  let* () = validate_request workflow request in
  let* run = Temporal.Client.follow owner.client ~workflow execution in
  Ok (Handle { owner; workflow; request; run; validate })
let resume_generate owner ~execution (request : generate_request) =
  let* () = validate_version Llm_temporal_v1_codec.generate_api_version request.api_version in
  resume owner generate_workflow validate_generate ~execution request
let resume_compact owner ~execution (request : compact_request) =
  let* () = validate_version Llm_temporal_v1_codec.compact_api_version request.api_version in
  resume owner compact_workflow validate_compact ~execution request

let wait (Handle handle) =
  let* result = Temporal.Client.wait handle.run in
  match result with
  | Temporal.Client.Completed response ->
      let* () = handle.validate handle.request response in
      Ok (Temporal.Client.Completed response)
  | result -> Ok result

module Runs = Set.Make(String)

let await first =
  let rec next seen ((Handle handle) as current) =
    let identity = execution current in
    if Runs.mem identity.run_id seen then
      Error (Temporal.Error.codec ~message:"workflow continuation repeated a run")
    else
      let seen = Runs.add identity.run_id seen in
      let* result = wait current in
      match result with
      | Temporal.Client.Completed response -> Ok response
      | Temporal.Client.Failed error | Temporal.Client.Cancelled error
      | Temporal.Client.Terminated error | Temporal.Client.Timed_out error -> Error error
      | Temporal.Client.Continued_as_new successor ->
          if successor.workflow_id <> identity.workflow_id then
            Error (Temporal.Error.codec ~message:"workflow continuation changed workflow identity")
          else
            let* run = Temporal.Client.follow handle.owner.client ~workflow:handle.workflow successor in
            next seen (Handle { handle with run })
  in
  next Runs.empty first
