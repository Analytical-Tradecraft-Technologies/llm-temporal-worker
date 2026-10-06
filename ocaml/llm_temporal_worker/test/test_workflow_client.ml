open Llm_temporal

let ok = function Ok value -> value | Error error -> failwith (Temporal.Error.message error)
let rejected = function Error _ -> () | Ok _ -> failwith "invalid value accepted"
let checkpoint value = match Checkpoint.of_string value with Ok v -> v | Error e -> failwith e
let queue = Temporal_task_queue.of_string "llm-worker"
let namespace = "llm-client-tests"
let target_url = "mock://llm-workflow-client"
let context = { tenant = Some (Tenant_id.of_string "tenant"); project = Some (Project_id.of_string "project"); actor = Some (Actor_id.of_string "actor"); tags = [] }
let cache = match Cache_policy.any_age ~variant:1l () with Ok value -> value | Error message -> failwith message
let request = Generate.make ~operation_key:(Operation_key.of_string "generate-1")
    ~context ~model:(Model_selector.of_string "test") ~cache ~input:[] ()
let response : generate_response = {
  api_version = V1_codec.generate_api_version; service = None; operation_key = request.operation_key;
  operation_id = Operation_id.of_string "internal-id"; status = Completed; output = [];
  checkpoint = { handle = checkpoint "cp-result"; parent = None; kind = Generation_checkpoint; depth = 0l };
  cache = { disposition = Cache_miss_populated; variant = 1l; entry_age_seconds = None };
  route = None; usage = None; cost = Unknown_cost { reason = State_unavailable }; diagnostics = [];
}
let compact : compact_request = {
  api_version = V1_codec.compact_api_version; operation_key = Operation_key.of_string "compact-1";
  context; parent = checkpoint "cp-parent"; policy = None; cache = request.cache;
}
let compact_response : compaction_response = {
  api_version = V1_codec.compact_api_version; operation_key = compact.operation_key;
  operation_id = Operation_id.of_string "compact-id";
  checkpoint = { handle = checkpoint "cp-summary"; parent = Some compact.parent; kind = Compaction_checkpoint; depth = 1l };
  cache = response.cache; provenance = None; usage = None; cost = response.cost; diagnostics = [];
}

(* The SDK's mock transport echoes its input. Seed an exact execution with a
   response payload, then resume it using the real typed output codec. This
   exercises decoding and request/result binding without a client test seam. *)
let seed sdk ~id workflow codec value =
  let echo = Temporal.Workflow.remote ~name:(Temporal.Workflow.name workflow) ~input:codec ~output:codec in
  let handle = ok (Temporal.Client.start sdk ~workflow:echo ~task_queue:"llm-worker" ~id ~request_id:id ~input:value ()) in
  ({ Temporal.Client.namespace; workflow_id = id; run_id = Temporal.Client.run_id handle }, handle)

let () =
  if Temporal.Workflow.name Client.generate_workflow <> "llm.generate.workflow.v1"
     || Temporal.Workflow.name Client.compact_workflow <> "llm.compact.workflow.v1"
     || Temporal.Workflow.implementation Client.generate_workflow <> None
     || Temporal.Workflow.implementation Client.compact_workflow <> None then
    failwith "public definitions are not remote workflow references";
  let client = ok (Client.create ~target_url ~namespace ()) in
  let sdk = ok (Temporal.Client.create ~target_url ~namespace ()) in
  Fun.protect ~finally:(fun () -> ignore (Client.shutdown client); ignore (Temporal.Client.shutdown sdk)) (fun () ->
      let generate = ok (Client.start_generate client ~task_queue:queue ~id:"generation-start" ~request_id:"start-1" request) in
      let compaction = ok (Client.start_compact client ~task_queue:queue ~id:"compaction-start" ~request_id:"start-2" compact) in
      if (Client.execution generate).workflow_id <> "generation-start" || (Client.execution compaction).namespace <> namespace then failwith "lost execution identity";
      let visibility = ok (Temporal.Client.list_visibility sdk ~query:"" ()) in
      let expected = ["llm.generate.workflow.v1"; "llm.compact.workflow.v1"] in
      if List.sort String.compare (List.map (fun (e : Temporal.Client.visibility_execution) -> e.workflow_type) visibility.executions) <> List.sort String.compare expected
         || List.exists (fun (e : Temporal.Client.visibility_execution) -> e.task_queue <> "llm-worker") visibility.executions then failwith "wrong workflow or queue";
      (* Mock echoing a request must never become a successfully decoded result. *)
      rejected (Client.wait generate);
      rejected (Client.await compaction);
      let before = List.length visibility.executions in
      rejected (Client.start_generate client ~task_queue:queue ~id:"invalid" ~request_id:"invalid" { request with api_version = "future" });
      rejected (Client.start_compact client ~task_queue:queue ~id:"invalid" ~request_id:"invalid" { compact with api_version = "future" });
      rejected (Client.start_generate client ~task_queue:queue ~id:"" ~request_id:"start-invalid" request);
      rejected (Client.start_generate client ~task_queue:queue ~id:"invalid" ~request_id:"" request);
      rejected (Client.start_generate client ~task_queue:queue ~id:"invalid" ~request_id:"start-invalid" { request with operation_key = Operation_key.of_string "" });
      rejected (Client.start_compact client ~task_queue:queue ~id:"invalid" ~request_id:"start-invalid" { compact with cache = Some { (Option.get compact.cache) with variant = -1l } });
      if List.length (ok (Temporal.Client.list_visibility sdk ~query:"" ())).executions <> before then failwith "invalid input started a workflow";
      let execution, _ = seed sdk ~id:"generate-response" Client.generate_workflow (Temporal.Workflow.output Client.generate_workflow) response in
      let restored = ok (Client.resume_generate client ~execution request) in
      if Client.execution restored <> execution || ok (Client.await restored) <> response then failwith "resume lost typed result or identity";
      (match ok (Client.wait restored) with Temporal.Client.Completed value when value = response -> () | _ -> failwith "repeated wait changed result");
      rejected (Client.resume_generate client ~execution:{ execution with namespace = "other" } request);
      rejected (Client.resume_generate client ~execution { request with api_version = "future" });
      List.iteri (fun index altered ->
          let execution, _ = seed sdk ~id:("invalid-response-" ^ string_of_int index) Client.generate_workflow (Temporal.Workflow.output Client.generate_workflow) altered in
          rejected (Client.await (ok (Client.resume_generate client ~execution request))))
        [{ response with operation_key = Operation_key.of_string "other" };
         { response with cache = { response.cache with variant = 0l } };
         { response with checkpoint = { response.checkpoint with parent = Some (checkpoint "unexpected-parent") } }];
      let execution, _ = seed sdk ~id:"compact-response" Client.compact_workflow (Temporal.Workflow.output Client.compact_workflow) compact_response in
      if ok (Client.await (ok (Client.resume_compact client ~execution compact))) <> compact_response then failwith "compact result lost";
      List.iteri (fun index altered ->
          let execution, _ = seed sdk ~id:("invalid-compact-" ^ string_of_int index) Client.compact_workflow (Temporal.Workflow.output Client.compact_workflow) altered in
          rejected (Client.await (ok (Client.resume_compact client ~execution compact))))
        [{ compact_response with operation_key = Operation_key.of_string "other" };
         { compact_response with cache = { compact_response.cache with variant = 0l } };
         { compact_response with checkpoint = { compact_response.checkpoint with parent = Some (checkpoint "wrong-parent") } }];
      let execution, raw = seed sdk ~id:"terminated-response" Client.generate_workflow (Temporal.Workflow.output Client.generate_workflow) response in
      ok (Temporal.Client.terminate raw);
      let handle = ok (Client.resume_generate client ~execution request) in
      (match ok (Client.wait handle) with Temporal.Client.Terminated _ -> () | _ -> failwith "termination lost");
      rejected (Client.await handle);
      ok (Client.shutdown client);
      rejected (Client.start_generate client ~task_queue:queue ~id:"closed" ~request_id:"closed" request);
      rejected (Client.wait restored));
  print_endline "typed remote workflow client tests passed"
