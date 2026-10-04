(* Invoked only by the isolated Temporal/Redis Go harness. Separate invocations
   prove that a saved execution can be resumed without process-local state. *)
open Llm_temporal

let get = function
  | Ok value -> value
  | Error error -> failwith (Temporal.Error.message error)

let read_json path = Yojson.Safe.from_file path
let field_string name json = Yojson.Safe.Util.(json |> member name |> to_string)
let json_bytes json = Bytes.of_string (Yojson.Safe.to_string json)
let encoded_json encode value = get (encode value) |> Bytes.to_string |> Yojson.Safe.from_string
let request path = get (V1_codec.decode_generate_request (json_bytes (read_json path)))

let with_client address action =
  let client = get (Client.create ~target_url:address ~namespace:"default" ()) in
  Fun.protect ~finally:(fun () -> ignore (Client.shutdown client))
    (fun () -> action client)

let execution_json (execution : Temporal.Client.execution) =
  `Assoc ["namespace", `String execution.namespace;
          "workflow_id", `String execution.workflow_id;
          "run_id", `String execution.run_id]

let execution path : Temporal.Client.execution =
  let json = read_json path in
  { namespace = field_string "namespace" json;
    workflow_id = field_string "workflow_id" json;
    run_id = field_string "run_id" json }

let compact_request (request : generate_request) (response : generate_response) : compact_request =
  { api_version = V1_codec.compact_api_version;
    operation_key = Operation_key.of_string (Operation_key.to_string request.operation_key ^ "-compact");
    context = request.context; parent = response.checkpoint.handle;
    policy = None; cache = request.cache }

let results generated compacted =
  `Assoc ["generate", encoded_json V1_codec.encode_generate_response generated;
          "compact", encoded_json V1_codec.encode_compaction_response compacted]

(* Retry the uncertain Temporal start with identical caller-owned IDs. This
   must identify the same run, rather than create a second paid operation. *)
let start address queue input output =
  let request = request input in
  with_client address (fun client ->
    let submit () = get (Client.start_generate client
      ~task_queue:(Temporal_task_queue.of_string queue) ~id:(queue ^ "-ocaml")
      ~request_id:(queue ^ "-ocaml-start") request) in
    let first = Client.execution (submit ()) in
    let retried = Client.execution (submit ()) in
    if first <> retried then failwith "retried submission changed execution";
    Yojson.Safe.to_file output (execution_json first))

let resume address queue input saved output =
  let request = request input in
  with_client address (fun client ->
    let handle = get (Client.resume_generate client ~execution:(execution saved) request) in
    let generated = get (Client.await handle) in
    (match get (Client.wait handle) with
     | Temporal.Client.Completed repeated when repeated = generated -> ()
     | _ -> failwith "repeated result observation changed response");
    let compact = compact_request request generated in
    let handle = get (Client.start_compact client
      ~task_queue:(Temporal_task_queue.of_string queue) ~id:(queue ^ "-ocaml-compact")
      ~request_id:(queue ^ "-ocaml-compact-start") compact) in
    let compacted = get (Client.await handle) in
    let restored = get (Client.resume_compact client ~execution:(Client.execution handle) compact) in
    if get (Client.await restored) <> compacted then failwith "compaction resume changed response";
    Yojson.Safe.to_file output (results generated compacted))

(* The OCaml parent polls a different queue from the Go worker. Both typed
   helpers must schedule child workflows on the explicitly supplied queue. *)
let parent go_queue =
  let ( let* ) = Result.bind in
  Temporal.Workflow.define ~name:"llmtw.ocaml.integration.parent"
    ~input:(Temporal.Workflow.input Client.generate_workflow)
    ~output:Temporal.Codec.string (fun request ->
      let id = Operation_key.to_string request.operation_key in
      let task_queue = Temporal_task_queue.of_string go_queue in
      let* generated = invoke_generate ~task_queue ~id:(id ^ "-generate-child") request in
      let* compacted = invoke_compact_v1 ~task_queue ~id:(id ^ "-compact-child")
          (compact_request request generated) in
      Ok (Yojson.Safe.to_string (results generated compacted)))

let worker address parent_queue go_queue =
  let worker = get (Temporal.Worker.create ~target_url:address ~namespace:"default"
    ~task_queue:parent_queue ~activities:[]
    ~workflows:[Temporal.Worker.workflow (parent go_queue)] ()) in
  get (Temporal.Worker.run worker)

let child address parent_queue go_queue input output =
  let client = get (Temporal.Client.create ~target_url:address ~namespace:"default" ()) in
  Fun.protect ~finally:(fun () -> ignore (Temporal.Client.shutdown client)) (fun () ->
    let handle = get (Temporal.Client.start client ~workflow:(parent go_queue)
      ~task_queue:parent_queue ~id:(parent_queue ^ "-parent")
      ~request_id:(parent_queue ^ "-parent-start") ~input:(request input) ()) in
    match get (Temporal.Client.wait handle) with
    | Temporal.Client.Completed value -> Yojson.Safe.to_file output (Yojson.Safe.from_string value)
    | _ -> failwith "OCaml parent did not complete")

let () =
  if Sys.getenv_opt "LLMTW_CLOUD_TEST_PROVISION" <> Some "1" then
    failwith "live client is restricted to the isolated cloud workflow gate";
  match Array.to_list Sys.argv with
  | [_; "start"; address; queue; input; output] -> start address queue input output
  | [_; "resume"; address; queue; input; saved; output] -> resume address queue input saved output
  | [_; "worker"; address; parent_queue; go_queue] -> worker address parent_queue go_queue
  | [_; "child"; address; parent_queue; go_queue; input; output] -> child address parent_queue go_queue input output
  | _ -> failwith "invalid isolated cloud workflow gate arguments"
