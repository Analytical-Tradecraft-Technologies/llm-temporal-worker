(* External-package compile fixture for the architecture's deterministic,
   one-shot Conversation sample.  This executable is intentionally not run:
   it proves that downstream code can type-check every v1 facade without
   importing private implementation modules or introducing streaming. *)

open Llm_temporal

let ( let* ) = Result.bind

type workflow_input = {
  run_key : string;
  context : request_context;
  model : Model_selector.t;
  question : string;
  branch_instruction : string;
  tools : tool list;
  output : output_config;
  spend_from : Ptime.t;
  spend_until : Ptime.t;
}

type workflow_output = {
  final_turn : generate_response;
  branch_checkpoints : checkpoint_metadata list;
  compaction : compaction_response;
  provider_status : provider_status_page;
  model_inventory : model_inventory_page;
  credit_status : credit_status_page;
  budget_status : budget_status;
  spend_summary : spend_summary;
} [@@warning "-69"]

let operation_key input suffix =
  Operation_key.of_string (input.run_key ^ ":" ^ suffix)

(* Query.Filter builders validate before scheduling.  Workflow code that
   combines their construction with Activity results can deliberately map the
   package's validation string into the same Temporal error channel. *)
let filter_result = function
  | Ok value -> Ok value
  | Error message -> Error (Temporal.Error.codec ~message)

let decimal_constant value =
  match Decimal.of_string value with
  | Ok value -> value
  | Error _ -> invalid_arg "invalid source-code decimal constant"

let cache_constant variant =
  match Cache_policy.accept_up_to ~max_age_seconds:15_552_000L ~variant () with
  | Ok value -> value
  | Error _ -> invalid_arg "invalid source-code cache policy"

let cache_0 = cache_constant Int32.zero
let cache_1 = cache_constant Int32.one
let cache_2 = cache_constant (Int32.of_int 2)

let message text = Message { actor = Human; content = [ Text text ] }

let exactly_three_results (branches : Conversation.turn list) = match branches with
  | [ branch_0; branch_1; branch_2 ] -> Ok (branch_0, branch_1, branch_2)
  | _ -> invalid_arg "Temporal.Future.all changed result cardinality"

(* Keep the exact low-level descriptor examples type-checked as well as the
   ergonomic facade below.  This function is never called: the fixture is a
   compile-only downstream consumer and must not contact Temporal or a
   provider at process startup. *)
let low_level_activity_examples ~task_queue
    (generation_request : generate_request)
    (compaction_request : compact_request)
    (query_envelope : query_envelope) =
  let task_queue = Temporal_task_queue.to_string task_queue in
  let generated =
    Temporal.Activity.execute ~task_queue
      ~retry_policy:activity_retry_policy
      generate_v1_activity generation_request
  in
  let compacted =
    Temporal.Activity.execute ~task_queue
      ~retry_policy:activity_retry_policy
      compact_v1_activity compaction_request
  in
  let queried =
    Temporal.Activity.execute ~task_queue
      ~retry_policy:activity_retry_policy
      query_v1_activity query_envelope
  in
  generated, compacted, queried

let claim_workflow ~input_codec ~output_codec ~task_queue =
  Temporal.Workflow.define
    ~name:"claims.cached-branching.v1"
    ~input:input_codec ~output:output_codec
    (fun input ->
      let* credit_filter =
        filter_result (Query.Filter.credit_status ~include_ok:false
          ~refresh_if_older_than_seconds:300L ~page_size:100 ())
      in
      let* credit =
        Query.execute ~task_queue
          ~operation_key:(operation_key input "credit-before")
          ~context:input.context
          (Query.Credit_status credit_filter)
      in
      let* budget_filter = filter_result (Query.Filter.budget_status ()) in
      let* budget =
        Query.execute ~task_queue
          ~operation_key:(operation_key input "budget-before")
          ~context:input.context
          (Query.Budget_status budget_filter)
      in
      let root =
        Conversation.root ~context:input.context ~model:input.model
          ~settings:(Settings.make ~temperature:(decimal_constant "0")
            ~reasoning_effort:Medium ~reasoning_summary:Concise
            ~tools:input.tools ~tool_policy:{ choice = Auto; parallel = false }
            ~output:input.output ()) ()
      in
      let* first =
        Conversation.respond ~task_queue
          ~operation_key:(operation_key input "turn-1") ~cache:cache_0
          ~append:[ message input.question ] root
      in
      let branch_patch =
        Settings.Patch.keep
        |> Settings.Patch.set_temperature (decimal_constant "0.7")
        |> Settings.Patch.set_reasoning_effort High
      in
      let start_branch suffix cache =
        Conversation.start_respond ~task_queue
          ~operation_key:(operation_key input suffix)
          ~settings_patch:branch_patch ~cache
          ~append:[ message input.branch_instruction ]
          (Conversation.fork first.conversation)
      in
      let branch_0 = start_branch "branch-0" cache_0 in
      let branch_1 = start_branch "branch-1" cache_1 in
      let branch_2 = start_branch "branch-2" cache_2 in
      (* Future.await exposes the Future's Activity error channel as a result;
         successful values are the typed Conversation turns. *)
      let* branch_results =
        Temporal.Future.await (Temporal.Future.all [ branch_0; branch_1; branch_2 ])
      in
      let* (branch_0, branch_1, branch_2) = exactly_three_results branch_results in
      let branches : Conversation.turn list = [ branch_0; branch_1; branch_2 ] in
      let chosen = branch_0 in
      let* (compaction, compacted) =
        Conversation.compact ~task_queue ~operation_key:(operation_key input "compact")
          ~cache:cache_0 chosen.conversation
      in
      let* final =
        Conversation.respond ~task_queue
          ~operation_key:(operation_key input "after-compaction") ~cache:cache_0
          ~append:[ message "Return the final structured answer." ] compacted
      in
      let* provider_filter =
        filter_result
          (Query.Filter.provider_status ~include_healthy:false ~page_size:100 ())
      in
      let* provider_status =
        Query.execute ~task_queue
          ~operation_key:(operation_key input "provider-status-after")
          ~context:input.context
          (Query.Provider_status provider_filter)
      in
      let* model_filter =
        filter_result (Query.Filter.model_inventory ~page_size:100 ())
      in
      let* model_inventory =
        Query.execute ~task_queue
          ~operation_key:(operation_key input "model-inventory-after")
          ~context:input.context
          (Query.Model_inventory model_filter)
      in
      let* spend_filter =
        filter_result
          (Query.Filter.spend_summary ~start_time:input.spend_from
             ~end_time:input.spend_until
             ~group_by:[ By_operation_kind; By_provider; By_model ]
             ~operation_kinds:[ Generate; Compact; Query ] ())
      in
      let* spend_summary =
        Query.execute ~task_queue
          ~operation_key:(operation_key input "spend-after")
          ~context:input.context
          (Query.Spend_summary spend_filter)
      in
      Ok {
        final_turn = final.response;
        branch_checkpoints =
          List.map (fun (branch : Conversation.turn) -> branch.response.checkpoint) branches;
        compaction;
        provider_status = provider_status.value;
        model_inventory = model_inventory.value;
        credit_status = credit.value;
        budget_status = budget.value;
        spend_summary = spend_summary.value;
      })

(* Keep the definition reachable so Dune type-checks its full inferred type,
   while avoiding a workflow registration or Activity execution at process
   startup. *)
let () = ignore (claim_workflow, low_level_activity_examples)
