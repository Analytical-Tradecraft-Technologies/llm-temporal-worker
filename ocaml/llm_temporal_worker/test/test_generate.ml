open Llm_temporal

let failf format = Printf.ksprintf failwith format

let expect_ok = function
  | Ok value -> value
  | Error error -> failf "unexpected Temporal error: %s" (Temporal.Error.message error)

let context = { tenant = None; project = None; actor = None; tags = [] }
let operation_key = Operation_key.of_string "generate-test"
let model = Model_selector.of_string "arbitrary-model"
let input = [ Message { actor = Human; content = [ Text "hello" ] } ]

let response (request : generate_request) =
  { api_version = V1_codec.generate_api_version;
    operation_key = request.operation_key;
    operation_id = Operation_id.of_string "generate-test-id";
    status = Completed;
    output = [];
    checkpoint = {
      handle = Checkpoint.of_string_exn "generate-test-checkpoint";
      parent = request.parent;
      kind = Generation_checkpoint;
      depth = 0l;
    };
    cache = { disposition = Cache_disabled; variant = 0l; entry_age_seconds = None };
    route = None; usage = None;
    cost = Exact_cost {
      actual_cost_usd = Decimal.zero;
      method_ = Control_query_zero;
      catalog_version = None;
    };
    diagnostics = [] }

let dispatch ?task_queue:_ activity request =
  if Temporal.Activity.name activity <> "llm.generate.v1" then
    failwith "Generate dispatched the wrong Activity";
  Ok (response request)

let () =
  let settings = Generate.Settings.make ~service_class:Priority () in
  let request = Generate.make ~operation_key ~context ~model ~settings ~input () in
  if request.parent <> None then failwith "one-shot Generate unexpectedly has a parent";
  if request.append <> input then failwith "Generate input was not preserved";
  if request.settings_patch.service_class <> Set Priority then
    failwith "Generate settings did not set service class";
  let actual = expect_ok (Generate.invoke_with ~dispatch request) in
  if actual.operation_key <> operation_key then failwith "Generate response operation key changed";
  let mismatched_dispatch ?task_queue:_ activity request =
    if Temporal.Activity.name activity <> "llm.generate.v1" then
      failwith "Generate dispatched the wrong Activity";
    Ok { (response request) with
         operation_key = Operation_key.of_string "different-operation" }
  in
  (match Generate.invoke_with ~dispatch:mismatched_dispatch request with
   | Error error when String.equal (Temporal.Error.message error)
                          "generate response operation key mismatch: expected generate-test, got different-operation" -> ()
   | Error error -> failf "unexpected operation key mismatch: %s" (Temporal.Error.message error)
   | Ok _ -> failwith "mismatched Generate operation key was accepted");
  let legacy = Request.make ~operation_key ~model ~service_class:Standard ~input () in
  if legacy.model <> model then failwith "legacy Request compatibility changed";
  print_endline "one-shot Generate facade passed"
