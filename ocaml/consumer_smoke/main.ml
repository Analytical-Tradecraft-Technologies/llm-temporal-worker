(* This project intentionally lives outside the package directory.  It models
   a downstream Dune consumer of the public library, rather than compiling
   against private implementation modules.  The dispatchers below are
   deterministic stand-ins: no Temporal server or provider is contacted. *)
open Llm_temporal

let failf format = Printf.ksprintf failwith format

let expect_ok = function
  | Ok value -> value
  | Error error -> failf "unexpected Temporal error: %s" (Temporal.Error.message error)

let expect_valid = function
  | Ok value -> value
  | Error error -> failf "unexpected validation error: %s" error

let context = { tenant = None; project = None; actor = None; tags = [] }
let model = Model_selector.of_string "consumer-smoke-model"
let model_capability = Model_capability.of_string "text_generation"
let operation suffix = Operation_key.of_string ("consumer-smoke-" ^ suffix)

let message text = Message { actor = Human; content = [ Text text ] }

let checkpoint suffix = Checkpoint.of_string_exn ("consumer-smoke-checkpoint-" ^ suffix)

let generation_response (request : generate_request) =
  { api_version = V1_codec.generate_api_version;
    operation_key = request.operation_key;
    operation_id = Operation_id.of_string (Operation_key.to_string request.operation_key ^ "-id");
    status = Completed;
    output = [ message "deterministic response" ];
    checkpoint = {
      handle = checkpoint (Operation_key.to_string request.operation_key);
      parent = request.parent;
      kind = Generation_checkpoint;
      depth = (match request.parent with None -> 0l | Some _ -> 1l);
    };
    cache = { disposition = Cache_disabled; variant = 0l; entry_age_seconds = None };
    route = None;
    usage = None;
    cost = Exact_cost {
      actual_cost_usd = Decimal.zero;
      method_ = Control_query_zero;
      catalog_version = None;
    };
    diagnostics = [] }

let generate_dispatch ?task_queue:_ activity (request : generate_request) =
  if not (String.equal (Temporal.Activity.name activity) "llm.generate.v1") then
    failwith "Conversation dispatched the wrong Activity";
  Ok (generation_response request)

let compaction_response (request : compact_request) =
  { api_version = V1_codec.compact_api_version;
    operation_key = request.operation_key;
    operation_id = Operation_id.of_string (Operation_key.to_string request.operation_key ^ "-id");
    checkpoint = {
      handle = checkpoint (Operation_key.to_string request.operation_key);
      parent = Some request.parent;
      kind = Compaction_checkpoint;
      depth = 2l;
    };
    cache = { disposition = Cache_disabled; variant = 0l; entry_age_seconds = None };
    provenance = None;
    usage = None;
    cost = Exact_cost {
      actual_cost_usd = Decimal.zero;
      method_ = Control_query_zero;
      catalog_version = None;
    };
    diagnostics = [] }

let compact_dispatch ?task_queue:_ activity (request : compact_request) =
  if not (String.equal (Temporal.Activity.name activity) "llm.compact.v1") then
    failwith "Conversation dispatched the wrong Activity";
  Ok (compaction_response request)

let query_response (envelope : query_envelope) result =
  { api_version = V1_codec.query_api_version;
    operation_key = envelope.operation_key;
    query_execution_id = Query_execution_id.of_string "consumer-smoke-query";
    observed_at = Ptime.epoch;
    source = Persisted;
    freshness = Current;
    complete = true;
    next_cursor = None;
    result;
    cost = Exact_cost {
      actual_cost_usd = Decimal.zero;
      method_ = Control_query_zero;
      catalog_version = None;
    } }

let query_dispatch ?task_queue:_ activity (envelope : query_envelope) =
  if not (String.equal (Temporal.Activity.name activity) "llm.query.v1") then
    failwith "Query dispatched the wrong Activity";
  let result =
    match envelope.query with
    | Provider_status_request _ -> Provider_status_result { routes = [] }
    | Model_inventory_request _ -> Model_inventory_result { models = [] }
    | Credit_status_request _ -> Credit_status_result { endpoints = [] }
    | Budget_status_request _ ->
        Budget_status_result {
          active_at = Ptime.epoch;
          generation_id = Budget_generation_id.of_string "consumer-smoke-generation";
          manifest_digest = Sha256_digest.of_hex_exn (String.make 64 'a');
          stream_high_water_mark = Budget_stream_id.of_string_exn "1-0";
          windows = [] }
    | Spend_summary_request filter ->
        Spend_summary_result {
          start_time = filter.start_time;
          end_time = filter.end_time;
          buckets = [] }
  in
  Ok (query_response envelope result)

let run_query query =
  ignore (expect_ok (Query.execute_with
      ~dispatch:query_dispatch
      ~operation_key:(operation "query")
      ~context
      query))

(* This is deliberately a compile-only example of Temporal fan-out.  It is
   not called by this smoke executable because Activity.start must run inside a
   Workflow activation. *)
let future_fanout (conversation : Conversation.t) =
  let start suffix =
    Conversation.start_respond
      ~operation_key:(operation suffix)
      ~append:[ message suffix ]
      (Conversation.fork conversation)
  in
  Temporal.Future.all [ start "branch-0"; start "branch-1"; start "branch-2" ]

let () =
  (* Top-level names from the documented facade are usable by a downstream
     package while the namespaced Conversation modules remain available. *)
  assert (String.equal
            (Model_capability.to_string model_capability)
            "text_generation");
  let temperature = expect_valid (Decimal.of_string "0.5") in
  let tool : tool = {
    kind = Function;
    name = Tool_name.of_string "lookup";
    description = "deterministic test tool";
    input_schema = `Assoc [];
    output_schema = None;
  } in
  let output : output_config = { max_tokens = Some 16; format = Json_format } in
  let settings =
    Settings.make ~service_class:Priority ~temperature ~tools:[ tool ] ~output ()
  in
  let cache = expect_valid (Cache_policy.accept_up_to ~max_age_seconds:60L ()) in
  let compaction_policy : Compaction_policy.t = {
    target_tokens = Some 64L;
    summary_style = Some Concise;
  } in
  let root = Conversation.root ~context ~model ~settings () in
  let first = expect_ok (Conversation.respond_with
      ~dispatch:generate_dispatch
      ~operation_key:(operation "root")
      ~cache
      ~append:[ message "root" ]
      root) in
  let sibling suffix =
    expect_ok (Conversation.respond_with
      ~dispatch:generate_dispatch
      ~operation_key:(operation suffix)
      ~cache
      ~append:[ message suffix ]
      (Conversation.fork first.conversation))
  in
  let branch_0 = sibling "branch-0" in
  let branch_1 = sibling "branch-1" in
  let branch_2 = sibling "branch-2" in
  assert (Conversation.checkpoint first.conversation <> None);
  assert (Conversation.checkpoint branch_0.conversation <> None);
  assert (Conversation.checkpoint branch_1.conversation <> None);
  assert (Conversation.checkpoint branch_2.conversation <> None);
  let checkpoint_string conversation =
    match Conversation.checkpoint conversation with
    | Some value -> Checkpoint.to_string value
    | None -> failwith "smoke branch lost its checkpoint"
  in
  assert (checkpoint_string branch_0.conversation <> checkpoint_string branch_1.conversation);
  assert (checkpoint_string branch_1.conversation <> checkpoint_string branch_2.conversation);

  let _, compacted = expect_ok (Conversation.compact_with
      ~dispatch:compact_dispatch
      ~operation_key:(operation "compact")
      ~policy:compaction_policy
      ~cache:(expect_valid (Cache_policy.accept_up_to ~max_age_seconds:60L ()))
      branch_0.conversation)
  in
  let final = expect_ok (Conversation.respond_with
      ~dispatch:generate_dispatch
      ~operation_key:(operation "after-compact")
      ~append:[ message "after compact" ]
      compacted)
  in
  assert (Conversation.checkpoint final.conversation <> None);

  let one_shot = Generate.make
      ~operation_key:(operation "one-shot")
      ~context
      ~model
      ~settings:(Generate.Settings.make ~service_class:Priority ())
      ~input:[ message "one-shot" ]
      ()
  in
  let one_shot_response = expect_ok (Generate.invoke_with ~dispatch:generate_dispatch one_shot) in
  assert (one_shot_response.operation_key = operation "one-shot");

  run_query (Query.Provider_status {
    provider = None; endpoint = None; availability = None;
    include_healthy = false; refresh_if_older_than_seconds = None;
    page_size = 20; cursor = None });
  run_query (Query.Model_inventory {
    provider = None; endpoint = None; model_prefix = None; lifecycle = None;
    refresh_if_older_than_seconds = None; page_size = 20; cursor = None });
  run_query (Query.Credit_status {
    provider = None; endpoint = None; include_ok = false;
    refresh_if_older_than_seconds = None; page_size = 20; cursor = None });
  run_query (Query.Budget_status {
    policy_key = None; active_at = None; include_windows = true });
  run_query (Query.Spend_summary {
    start_time = Ptime.epoch; end_time = Ptime.epoch;
    group_by = [ By_operation_kind ]; operation_kinds = [ Generate ] });

  (* Keep the old smoke assertion too: this fixture is additive and does not
     silently change the unreleased one-shot helper in this PR. *)
  let request =
    Request.make
      ~operation_key:(operation "legacy")
      ~model
      ~service_class:Standard
      ~input:[ message "legacy" ]
      ()
  in
  assert (Operation_key.to_string request.operation_key = "consumer-smoke-legacy");
  ignore future_fanout;
  print_endline "downstream OCaml facade smoke passed"
