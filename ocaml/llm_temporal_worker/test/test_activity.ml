(* Compile- and shape-level coverage for the public workflow and Query descriptors.
   The helpers are workflow-native, so this test deliberately does not try to
   execute remote work from a process-level test. *)

open Llm_temporal

let assert_equal expected actual =
  if not (String.equal expected actual) then
    failwith (Printf.sprintf "expected %S, got %S" expected actual)

(* Keep explicit aliases here: these declarations make a changed SDK or
   accidentally widened helper signature fail at Dune compile time. *)
let _start_generate :
    task_queue:Temporal_task_queue.t -> id:string ->
    generate_request ->
    (generate_response, Temporal.Error.t) Temporal.Future.t =
  Llm_temporal.start_generate

let _invoke_generate :
    task_queue:Temporal_task_queue.t -> id:string ->
    generate_request -> (generate_response, Temporal.Error.t) result =
  Llm_temporal.invoke_generate

let _start_compact :
    task_queue:Temporal_task_queue.t -> id:string ->
    compact_request ->
    (compaction_response, Temporal.Error.t) Temporal.Future.t =
  Llm_temporal.start_compact_v1

let _invoke_compact :
    task_queue:Temporal_task_queue.t -> id:string ->
    compact_request -> (compaction_response, Temporal.Error.t) result =
  Llm_temporal.invoke_compact_v1

let _start_query :
    task_queue:Temporal_task_queue.t -> id:string ->
    query_envelope ->
    (query_response, Temporal.Error.t) Temporal.Future.t =
  Llm_temporal.start_query_v1

let _invoke_query :
    task_queue:Temporal_task_queue.t -> id:string ->
    query_envelope -> (query_response, Temporal.Error.t) result =
  Llm_temporal.invoke_query_v1

(* The package-level one-shot names must use the exact Generate v1 payload
   types. Keeping these annotations in a downstream-facing test prevents a
   future refactor from silently routing the public workflow through the
   pre-checkpoint compatibility codec again. *)
let _execute_v1 :
    task_queue:Temporal_task_queue.t -> id:string ->
    generate_request -> (generate_response, Temporal.Error.t) result =
  Llm_temporal.execute

let _workflow_v1 :
    unit ->
    (generate_request, generate_response) Temporal.Workflow.t =
  Llm_temporal.workflow

let () =
  assert_equal "llm.generate.workflow.v1"
    (Temporal.Workflow.name Llm_temporal.generate_v1_workflow);
  assert_equal "llm.compact.workflow.v1"
    (Temporal.Workflow.name Llm_temporal.compact_v1_workflow);
  assert_equal "llm.query.workflow.v1"
    (Temporal.Workflow.name Llm_temporal.query_v1_workflow);
  if Option.is_some (Temporal.Workflow.implementation Llm_temporal.generate_v1_workflow)
  then failwith "Generate descriptor unexpectedly contains an OCaml implementation";
  if Option.is_some (Temporal.Workflow.implementation Llm_temporal.compact_v1_workflow)
  then failwith "Compact descriptor unexpectedly contains an OCaml implementation";
  if Option.is_some (Temporal.Workflow.implementation Llm_temporal.query_v1_workflow)
  then failwith "Query descriptor unexpectedly contains an OCaml implementation";
  print_endline "v1 workflow and Query descriptor tests passed"
