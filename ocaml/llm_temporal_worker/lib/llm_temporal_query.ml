open Llm_temporal_models

let ( let* ) = Result.bind

module Filter = struct
  (** The wire records remain available for protocol fixtures, but callers
      should prefer these constructors.  They validate values before an
      Activity is scheduled, so a malformed page or spend interval cannot
      become a workflow-side protocol failure. *)

  let validate_page_size ~kind value =
    if value < 1 || value > 1000 then
      Error (Printf.sprintf "%s.page_size must be between 1 and 1000" kind)
    else Ok value

  let refresh_age ~kind value =
    match value with
    | None -> Ok None
    | Some value when Int64.compare value 1L >= 0
                    && Int64.compare value 86400L <= 0 -> Ok (Some value)
    | Some _ ->
        Error (Printf.sprintf
                 "%s.refresh_if_older_than_seconds must be between 1 and 86400"
                 kind)

  let validate_cursor ~kind expected value =
    match value with
    | None -> Ok None
    | Some cursor ->
        (match Query_cursor.kind cursor with
         | None -> Ok (Some cursor)
         | Some actual when actual = expected -> Ok (Some cursor)
         | Some actual ->
             Error (Printf.sprintf "%s.cursor kind mismatch: expected %s, got %s"
                      kind (Query_cursor.kind_to_string expected)
                      (Query_cursor.kind_to_string actual)))

  let paginated ~kind expected ?page_size ?cursor () f =
    let page_size = Option.value ~default:100 page_size in
    match validate_page_size ~kind page_size with
    | Error error -> Error error
    | Ok page_size ->
        (match validate_cursor ~kind expected cursor with
         | Error error -> Error error
         | Ok cursor -> Ok (f page_size cursor))

  let provider_status
      ?provider ?endpoint ?availability ?(include_healthy = true)
      ?refresh_if_older_than_seconds ?page_size ?cursor () =
    let* refresh_if_older_than_seconds =
      refresh_age ~kind:"provider_status" refresh_if_older_than_seconds
    in
    paginated ~kind:"provider_status" Query_cursor.Provider_status
      ?page_size ?cursor ()
      (fun page_size cursor ->
         { provider; endpoint; availability; include_healthy;
           refresh_if_older_than_seconds; page_size; cursor })

  let model_inventory
      ?provider ?endpoint ?model_prefix ?lifecycle
      ?refresh_if_older_than_seconds ?page_size ?cursor () =
    let* refresh_if_older_than_seconds =
      refresh_age ~kind:"model_inventory" refresh_if_older_than_seconds
    in
    paginated ~kind:"model_inventory" Query_cursor.Model_inventory
      ?page_size ?cursor ()
      (fun page_size cursor ->
         { provider; endpoint; model_prefix; lifecycle;
           refresh_if_older_than_seconds; page_size; cursor })

  let credit_status
      ?provider ?endpoint ?(include_ok = true)
      ?refresh_if_older_than_seconds ?page_size ?cursor () =
    let* refresh_if_older_than_seconds =
      refresh_age ~kind:"credit_status" refresh_if_older_than_seconds
    in
    paginated ~kind:"credit_status" Query_cursor.Credit_status
      ?page_size ?cursor ()
      (fun page_size cursor ->
         { provider; endpoint; include_ok; refresh_if_older_than_seconds;
           page_size; cursor })

  let budget_status ?policy_key ?active_at ?(include_windows = true) () =
    Ok { policy_key; active_at; include_windows }

  let duplicate ~field values equal =
    let rec loop seen = function
      | [] -> Ok ()
      | value :: _rest when List.exists (equal value) seen ->
          Error (Printf.sprintf "%s contains duplicate value" field)
      | value :: rest -> loop (value :: seen) rest
    in
    loop [] values

  let spend_summary ~start_time ~end_time ?(group_by = [])
      ?(operation_kinds = []) () =
    if Ptime.compare end_time start_time <= 0 then
      Error "spend_summary.end_time must be after start_time"
    else
      let* () = duplicate ~field:"spend_summary.group_by" group_by ( = ) in
      let* () = duplicate ~field:"spend_summary.operation_kinds"
          operation_kinds ( = ) in
      Ok { start_time; end_time; group_by; operation_kinds }
end

type _ t =
  | Provider_status : provider_status_filter -> provider_status_page t
  | Model_inventory : model_inventory_filter -> model_inventory_page t
  | Credit_status : credit_status_filter -> credit_status_page t
  | Budget_status : budget_status_filter -> budget_status t
  | Spend_summary : spend_summary_filter -> spend_summary t

type 'a response = {
  value : 'a;
  query_execution_id : Query_execution_id.t;
  observed_at : Ptime.t;
  source : query_source;
  freshness : freshness;
  complete : bool;
  next_cursor : Query_cursor.t option;
  cost : settled_cost;
}

let query_request : type a. a t -> query_request = function
  | Provider_status filter -> Provider_status_request filter
  | Model_inventory filter -> Model_inventory_request filter
  | Credit_status filter -> Credit_status_request filter
  | Budget_status filter -> Budget_status_request filter
  | Spend_summary filter -> Spend_summary_request filter

let to_envelope ~operation_key ~context query =
  { api_version = Llm_temporal_v1_codec.query_api_version;
    operation_key;
    context;
    query = query_request query }

let expected_cursor_kind : type a. a t -> Query_cursor.kind = function
  | Provider_status _ -> Query_cursor.Provider_status
  | Model_inventory _ -> Query_cursor.Model_inventory
  | Credit_status _ -> Query_cursor.Credit_status
  | Budget_status _ -> Query_cursor.Budget_status
  | Spend_summary _ -> Query_cursor.Spend_summary

let query_cursor : type a. a t -> Query_cursor.t option = function
  | Provider_status { cursor; _ }
  | Model_inventory { cursor; _ }
  | Credit_status { cursor; _ } -> cursor
  | Budget_status _
  | Spend_summary _ -> None

let validate_cursor query =
  match query_cursor query with
  | None -> Ok ()
  | Some cursor ->
      (match Query_cursor.kind cursor with
       | None -> Ok ()
       | Some actual when actual = expected_cursor_kind query -> Ok ()
       | Some actual ->
           Error
             (Temporal.Error.codec
                ~message:(Printf.sprintf "query cursor kind mismatch: expected %s, got %s"
                            (Query_cursor.kind_to_string (expected_cursor_kind query))
                            (Query_cursor.kind_to_string actual))))

let mismatch expected actual =
  Temporal.Error.codec
    ~message:(Printf.sprintf "query result kind mismatch: expected %s, got %s" expected actual)

let operation_key_mismatch ~expected ~actual =
  Temporal.Error.codec
    ~message:(Printf.sprintf
                "query response operation key mismatch: expected %s, got %s"
                (Operation_key.to_string expected)
                (Operation_key.to_string actual))

let cursor_mismatch expected actual =
  Temporal.Error.codec
    ~message:(Printf.sprintf "query response cursor kind mismatch: expected %s, got %s"
                (Query_cursor.kind_to_string expected)
                (Query_cursor.kind_to_string actual))

let cursor_missing_kind expected =
  Temporal.Error.codec
    ~message:(Printf.sprintf "query response cursor is missing its %s kind"
                (Query_cursor.kind_to_string expected))

let cursor_forbidden kind =
  Temporal.Error.codec
    ~message:(Printf.sprintf "query response.%s must not include next_cursor"
                (Query_cursor.kind_to_string kind))

let result_kind = function
  | Provider_status_result _ -> "provider_status"
  | Model_inventory_result _ -> "model_inventory"
  | Credit_status_result _ -> "credit_status"
  | Budget_status_result _ -> "budget_status"
  | Spend_summary_result _ -> "spend_summary"

let validate_next_cursor : type a. a t -> Query_cursor.t option -> (unit, Temporal.Error.t) result =
  fun query next_cursor ->
    let expected = expected_cursor_kind query in
    match query, next_cursor with
    | (Budget_status _ | Spend_summary _), Some _ -> Error (cursor_forbidden expected)
    | (_, None) -> Ok ()
    | (_, Some cursor) ->
        (match Query_cursor.kind cursor with
         | Some actual when actual = expected -> Ok ()
         | Some actual -> Error (cursor_mismatch expected actual)
         | None -> Error (cursor_missing_kind expected))

let validate_response_cursor : type a. a t -> query_response -> (unit, Temporal.Error.t) result =
  fun query response -> validate_next_cursor query response.next_cursor

let response_metadata (response : query_response) value =
  { value;
    query_execution_id = response.query_execution_id;
    observed_at = response.observed_at;
    source = response.source;
    freshness = response.freshness;
    complete = response.complete;
    next_cursor = response.next_cursor;
    cost = response.cost }

let next : type a. a t -> a response -> (a t option, Temporal.Error.t) result =
  fun query response ->
    match validate_next_cursor query response.next_cursor with
    | Error error -> Error error
    | Ok () ->
        match response.next_cursor with
        | None -> Ok None
        | Some cursor ->
            match query with
            | Provider_status filter -> Ok (Some (Provider_status { filter with cursor = Some cursor }))
            | Model_inventory filter -> Ok (Some (Model_inventory { filter with cursor = Some cursor }))
            | Credit_status filter -> Ok (Some (Credit_status { filter with cursor = Some cursor }))
            | Budget_status _ | Spend_summary _ -> Ok None

let of_response : type a. a t -> query_response -> (a response, Temporal.Error.t) result =
  fun query response ->
    match validate_response_cursor query response with
    | Error error -> Error error
    | Ok () ->
        match query, response.result with
        | Provider_status _, Provider_status_result value -> Ok (response_metadata response value)
        | Model_inventory _, Model_inventory_result value -> Ok (response_metadata response value)
        | Credit_status _, Credit_status_result value -> Ok (response_metadata response value)
        | Budget_status _, Budget_status_result value -> Ok (response_metadata response value)
        | Spend_summary _, Spend_summary_result value -> Ok (response_metadata response value)
        | Provider_status _, result -> Error (mismatch "provider_status" (result_kind result))
        | Model_inventory _, result -> Error (mismatch "model_inventory" (result_kind result))
        | Credit_status _, result -> Error (mismatch "credit_status" (result_kind result))
        | Budget_status _, result -> Error (mismatch "budget_status" (result_kind result))
        | Spend_summary _, result -> Error (mismatch "spend_summary" (result_kind result))

type dispatcher =
  ?task_queue:Temporal_task_queue.t ->
  (query_envelope, query_response) Temporal.Activity.t ->
  query_envelope -> (query_response, Temporal.Error.t) result

let execute_with ?task_queue ~dispatch ~operation_key ~context query =
  match validate_cursor query with
  | Error error -> Error error
  | Ok () ->
      let envelope = to_envelope ~operation_key ~context query in
      match Llm_temporal_invocation.invoke_query_once ?task_queue ~dispatch envelope with
      | Error error -> Error error
      | Ok response when
          not (String.equal
                 (Operation_key.to_string response.operation_key)
                 (Operation_key.to_string operation_key)) ->
          Error (operation_key_mismatch ~expected:operation_key
                   ~actual:response.operation_key)
      | Ok response -> of_response query response

let activity_dispatch ?task_queue activity input =
  Temporal.Activity.execute
    ?task_queue:(Option.map Temporal_task_queue.to_string task_queue)
    ~retry_policy:Llm_temporal_invocation.activity_retry_policy
    activity input

let execute ?task_queue ~operation_key ~context query =
  execute_with ?task_queue ~dispatch:activity_dispatch ~operation_key ~context query

let start ?task_queue ~operation_key ~context query =
  match validate_cursor query with
  | Error error ->
      (* The public SDK intentionally has no constructor for turning a
         successful Future value into a Future error.  Preserve the same
         value-channel validation contract as [execute_with] without
         dispatching an Activity.  [Future.all []] is an owner-aware ready
         future inside a Workflow and remains a safe ready value in tests. *)
      Temporal.Future.map (fun _ -> Error error) (Temporal.Future.all [])
  | Ok () ->
      let envelope = to_envelope ~operation_key ~context query in
      let future =
        Temporal.Activity.start
          ?task_queue:(Option.map Temporal_task_queue.to_string task_queue)
          ~retry_policy:Llm_temporal_invocation.activity_retry_policy
          Llm_temporal_invocation.query_v1_activity envelope
      in
      (* [Temporal.Future.map] preserves the Activity's error channel and
         keeps protocol-kind mismatches in the successful value channel
         rather than raising from a workflow callback. *)
      Temporal.Future.map
        (fun response ->
          if String.equal
               (Operation_key.to_string response.operation_key)
               (Operation_key.to_string operation_key)
          then of_response query response
          else Error (operation_key_mismatch ~expected:operation_key
                        ~actual:response.operation_key))
        future
