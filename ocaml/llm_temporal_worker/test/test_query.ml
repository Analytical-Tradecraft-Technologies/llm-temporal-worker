open Llm_temporal

let failf format = Printf.ksprintf failwith format
let ok = function Ok value -> value | Error error -> failf "%s" (Temporal.Error.message error)
let cursor value = match Query_cursor.of_string value with Ok value -> value | Error message -> failwith message
let tagged_cursor kind value =
  match Query_cursor.of_string_for_kind kind value with
  | Ok value -> value
  | Error message -> failwith message
let stream_id value = match Budget_stream_id.of_string value with Ok value -> value | Error message -> failwith message
let digest value = match Sha256_digest.of_hex value with Ok value -> value | Error message -> failwith message
let time value =
  match Ptime.of_rfc3339 value with
  | Ok (value, _, _) -> value
  | Error _ -> failwith "invalid test timestamp"

let context = { tenant = None; project = None; actor = None; tags = [] }
let operation_key = Operation_key.of_string "query-test"

let provider_filter ?cursor () =
  { provider = None; endpoint = None; availability = None; include_healthy = true;
    refresh_if_older_than_seconds = None; page_size = 20; cursor }

let model_filter () =
  { provider = None; endpoint = None; model_prefix = None; lifecycle = None;
    refresh_if_older_than_seconds = None; page_size = 20; cursor = None }

let credit_filter () =
  { provider = None; endpoint = None; include_ok = false;
    refresh_if_older_than_seconds = None; page_size = 20; cursor = None }

let budget_filter () = { policy_key = None; active_at = None; include_windows = true }

let spend_filter () =
  { start_time = time "2026-01-01T00:00:00Z";
    end_time = time "2026-01-02T00:00:00Z";
    group_by = [ By_operation_kind ]; operation_kinds = [ Generate ] }

let response result =
  { api_version = V1_codec.query_api_version;
    operation_key;
    query_execution_id = Query_execution_id.of_string "execution-1";
    observed_at = time "2026-01-01T00:00:00Z";
    source = Persisted; freshness = Current; complete = true; next_cursor = None;
    result;
    cost = Exact_cost { actual_cost_usd = Usd_decimal.zero;
                        method_ = Control_query_zero; catalog_version = None } }

let response_for = function
  | Provider_status_request _ -> response (Provider_status_result { routes = [] })
  | Model_inventory_request _ -> response (Model_inventory_result { models = [] })
  | Credit_status_request _ -> response (Credit_status_result { endpoints = [] })
  | Budget_status_request _ ->
      response (Budget_status_result {
        active_at = time "2026-01-01T00:00:00Z";
        generation_id = Budget_generation_id.of_string "generation-1";
        manifest_digest = digest (String.make 64 'a');
        stream_high_water_mark = stream_id "1-0";
        windows = [] })
  | Spend_summary_request filter ->
      response (Spend_summary_result {
        start_time = filter.start_time; end_time = filter.end_time; buckets = [] })

let dispatch ?task_queue:_ activity envelope =
  if Temporal.Activity.name activity <> "llm.query.v1" then
    failwith "Query used the wrong Activity descriptor";
  Ok (response_for envelope.query)

let run query = ok (Query.execute_with ~dispatch ~operation_key ~context query)

let expect_filter_error label expected = function
  | Error error when String.equal error expected -> ()
  | Error error -> failf "%s returned unexpected validation error: %s" label error
  | Ok _ -> failwith (label ^ " accepted an invalid filter")

let filter_ok = function
  | Ok value -> value
  | Error error -> failwith ("unexpected filter validation error: " ^ error)

let () =
  (match Budget_stream_id.of_string "not-a-stream-id" with
   | Error _ -> ()
   | Ok _ -> failwith "invalid budget stream id was accepted");
  let provider : provider_status_page Query.t = Query.Provider_status (provider_filter ()) in
  let model : model_inventory_page Query.t = Query.Model_inventory (model_filter ()) in
  let credit : credit_status_page Query.t = Query.Credit_status (credit_filter ()) in
  let budget : budget_status Query.t = Query.Budget_status (budget_filter ()) in
  let spend : spend_summary Query.t = Query.Spend_summary (spend_filter ()) in
  if (run provider).value.routes <> [] then failwith "provider result changed";
  if (run model).value.models <> [] then failwith "model result changed";
  if (run credit).value.endpoints <> [] then failwith "credit result changed";
  ignore (run budget);
  ignore (run spend);

  let cursor = cursor "provider:page-2" in
  let paged = Query.Provider_status (provider_filter ~cursor ()) in
  let envelope = Query.to_envelope ~operation_key ~context paged in
  (match envelope.query with
   | Provider_status_request { cursor = Some value; _ } when value = cursor -> ()
   | _ -> failwith "query cursor was not retained");

  let provider_cursor = tagged_cursor Query_cursor.Provider_status "provider:page-3" in
  let first_page = { (run provider) with complete = false; next_cursor = Some provider_cursor } in
  (match Query.next provider first_page with
   | Ok (Some (Query.Provider_status { cursor = Some next; _ })) when next = provider_cursor -> ()
   | Ok _ -> failwith "typed pagination dropped the provider cursor"
   | Error error -> failf "unexpected pagination error: %s" (Temporal.Error.message error));
  (match Query.next provider { first_page with complete = true } with
   | Ok (Some (Query.Provider_status { cursor = Some next; _ })) when next = provider_cursor -> ()
   | Ok _ -> failwith "complete page dropped its worker-provided cursor"
   | Error error -> failf "unexpected complete-page pagination error: %s" (Temporal.Error.message error));
  (match Query.next budget { (run budget) with complete = false; next_cursor = Some provider_cursor } with
   | Error error when String.equal (Temporal.Error.message error)
                         "query response.budget_status must not include next_cursor" -> ()
   | Error error -> failf "unexpected snapshot pagination error: %s" (Temporal.Error.message error)
   | Ok _ -> failwith "snapshot query unexpectedly exposed a next page");

  let wrong_kind =
    Query.Model_inventory
      { (model_filter ()) with cursor = Some provider_cursor }
  in
  let dispatch_called = ref false in
  let should_not_dispatch ?task_queue:_ _activity _envelope =
    dispatch_called := true;
    failwith "dispatch should not be called for a mismatched cursor"
  in
  (match Query.execute_with ~dispatch:should_not_dispatch ~operation_key ~context wrong_kind with
   | Error error when String.equal (Temporal.Error.message error)
                          "query cursor kind mismatch: expected model_inventory, got provider_status" -> ()
   | Error error -> failf "unexpected cursor mismatch error: %s" (Temporal.Error.message error)
   | Ok _ -> failwith "mismatched query cursor was accepted");
  if !dispatch_called then failwith "mismatched cursor was dispatched";

  let encoded =
    ok (V1_codec.encode_query_response
          { (response (Provider_status_result { routes = [] })) with
            next_cursor = Some provider_cursor })
  in
  (match V1_codec.decode_query_response encoded with
   | Ok { next_cursor = Some value; _ }
     when Query_cursor.kind value = Some Query_cursor.Provider_status -> ()
   | Ok _ -> failwith "decoded response cursor lost its query kind"
   | Error error -> failf "response cursor failed to round-trip: %s" (Temporal.Error.message error));

  let reject_non_paginated_cursor kind result =
    let candidate =
      { (response result) with
        next_cursor = Some (tagged_cursor kind "snapshot-page-2") }
    in
    match V1_codec.encode_query_response candidate with
    | Error _ -> ()
    | Ok _ -> failwith "non-paginated query response encoded a cursor"
  in
  reject_non_paginated_cursor Query_cursor.Budget_status
    (Budget_status_result {
       active_at = time "2026-01-01T00:00:00Z";
       generation_id = Budget_generation_id.of_string "generation-1";
       manifest_digest = digest (String.make 64 'a');
       stream_high_water_mark = stream_id "1-0";
       windows = [] });
  reject_non_paginated_cursor Query_cursor.Spend_summary
    (Spend_summary_result {
       start_time = time "2026-01-01T00:00:00Z";
       end_time = time "2026-01-02T00:00:00Z";
       buckets = [] });

  let mismatched =
    { (response (Provider_status_result { routes = [] })) with
      operation_key = Operation_key.of_string "mismatch" }
  in
  (match Query.of_response budget mismatched with
   | Error error when String.equal (Temporal.Error.message error)
                          "query result kind mismatch: expected budget_status, got provider_status" -> ()
   | Error error -> failf "unexpected mismatch error: %s" (Temporal.Error.message error)
   | Ok _ -> failwith "mismatched query result was accepted");

  let mismatched_operation_dispatch ?task_queue:_ _activity envelope =
    Ok { (response_for envelope.query) with
         operation_key = Operation_key.of_string "other-query" }
  in
  (match Query.execute_with ~dispatch:mismatched_operation_dispatch
      ~operation_key ~context provider with
   | Error error when String.equal (Temporal.Error.message error)
                          "query response operation key mismatch: expected query-test, got other-query" -> ()
   | Error error -> failf "unexpected query operation key mismatch: %s" (Temporal.Error.message error)
   | Ok _ -> failwith "mismatched query operation key was accepted");

  (* A typed response must carry a cursor associated with the same query
     constructor.  Dispatchers injected by deterministic tests bypass the
     JSON codec, so the ergonomic facade validates this boundary as well. *)
  let wrong_response_cursor =
    { (response (Provider_status_result { routes = [] })) with
      next_cursor = Some (tagged_cursor Query_cursor.Model_inventory "model-page-2") }
  in
  (match Query.of_response provider wrong_response_cursor with
   | Error error when String.equal (Temporal.Error.message error)
                          "query response cursor kind mismatch: expected provider_status, got model_inventory" -> ()
   | Error error -> failf "unexpected response cursor error: %s" (Temporal.Error.message error)
   | Ok _ -> failwith "mismatched response cursor was accepted");

  let untagged_response_cursor =
    { (response (Provider_status_result { routes = [] })) with
      next_cursor = Some (Query_cursor.of_string_exn "provider:untagged-page-2") }
  in
  (match Query.of_response provider untagged_response_cursor with
   | Error error when String.equal (Temporal.Error.message error)
                          "query response cursor is missing its provider_status kind" -> ()
   | Error error -> failf "unexpected untagged response cursor error: %s" (Temporal.Error.message error)
   | Ok _ -> failwith "untagged response cursor was accepted");

  let non_paginated_response_cursor =
    { (response (Budget_status_result {
          active_at = time "2026-01-01T00:00:00Z";
          generation_id = Budget_generation_id.of_string "generation-1";
          manifest_digest = digest (String.make 64 'a');
          stream_high_water_mark = stream_id "1-0";
          windows = [] })) with
      next_cursor = Some (tagged_cursor Query_cursor.Budget_status "unexpected-page-2") }
  in
  (match Query.of_response budget non_paginated_response_cursor with
   | Error error when String.equal (Temporal.Error.message error)
                          "query response.budget_status must not include next_cursor" -> ()
   | Error error -> failf "unexpected non-paginated cursor error: %s" (Temporal.Error.message error)
   | Ok _ -> failwith "non-paginated response cursor was accepted");

  (* [start] performs the same cursor validation before scheduling an
     Activity.  Its error is kept in the successful result channel, matching
     the existing Temporal.Future contract for protocol mismatches. *)
  let invalid_start = Query.start ~operation_key ~context wrong_kind in
  (match Temporal.Future.peek invalid_start with
   | Some (Ok (Error error)) when String.equal (Temporal.Error.message error)
                                      "query cursor kind mismatch: expected model_inventory, got provider_status" -> ()
   | Some (Ok (Error error)) -> failf "unexpected start cursor error: %s" (Temporal.Error.message error)
   | Some (Ok (Ok _)) -> failwith "start accepted a mismatched cursor"
   | Some (Error error) -> failf "start returned a Temporal error: %s" (Temporal.Error.message error)
   | None -> failwith "invalid start did not produce a ready validation result");

  let activity_error = Temporal.Error.make ~category:`Activity ~message:"query failed" () in
  let failing_dispatch ?task_queue:_ _activity _envelope = Error activity_error in
  (match Query.execute_with ~dispatch:failing_dispatch ~operation_key ~context provider with
   | Error error when String.equal (Temporal.Error.message error) "query failed" -> ()
   | Error error -> failf "unexpected Activity error: %s" (Temporal.Error.message error)
   | Ok _ -> failwith "Activity error was swallowed");

  (* Natural builders validate query invariants before callers wrap the
     filter in a GADT constructor.  Tagged cursors from another query kind
     are rejected locally; untagged cursors remain available for externally
     supplied continuation tokens. *)
  expect_filter_error "provider page size"
    "provider_status.page_size must be between 1 and 1000"
    (Query.Filter.provider_status ~page_size:0 ());
  expect_filter_error "provider refresh age"
    "provider_status.refresh_if_older_than_seconds must be between 1 and 86400"
    (Query.Filter.provider_status ~refresh_if_older_than_seconds:0L ());
  expect_filter_error "provider cursor kind"
    "provider_status.cursor kind mismatch: expected provider_status, got model_inventory"
    (Query.Filter.provider_status
       ~cursor:(tagged_cursor Query_cursor.Model_inventory "model-page-2") ());
  let built_provider =
    filter_ok (Query.Filter.provider_status ~include_healthy:false ~page_size:25
          ~refresh_if_older_than_seconds:300L ())
  in
  if built_provider.page_size <> 25 || built_provider.include_healthy
     || built_provider.refresh_if_older_than_seconds <> Some 300L then
    failwith "validated provider filter lost its fields";
  let built_model = filter_ok (Query.Filter.model_inventory ()) in
  if built_model.page_size <> 100 then
    failwith "validated model filter did not apply the default page size";
  let prefix = Model_prefix.of_string "gpt-" in
  let built_model_with_prefix =
    filter_ok (Query.Filter.model_inventory ~model_prefix:prefix ())
  in
  (match built_model_with_prefix.model_prefix with
   | Some value when String.equal (Model_prefix.to_string value) "gpt-" -> ()
   | _ -> failwith "typed model prefix was not retained");
  let prefix_envelope =
    Query.to_envelope ~operation_key ~context
      (Query.Model_inventory built_model_with_prefix)
  in
  let prefix_payload =
    ok (V1_codec.encode_query_request prefix_envelope.query)
  in
  let prefix_json = Yojson.Safe.from_string (Bytes.to_string prefix_payload) in
  (match prefix_json with
   | `Assoc fields ->
       (match List.assoc_opt "query" fields with
        | Some (`Assoc query_fields) ->
            (match List.assoc_opt "model_prefix" query_fields with
             | Some (`String value) when String.equal value "gpt-" -> ()
             | _ -> failwith "typed model prefix was not encoded as a string")
        | _ -> failwith "model inventory query payload is missing its query object")
   | _ -> failwith "query request payload is not an object");
  (* Model display names are presentation metadata, not provider model IDs.
     Keep the nominal wrapper through the closed response codec so callers
     cannot accidentally interchange the two string-shaped values. *)
  let model_display_name = Model_display_name.of_string "GPT-4o" in
  let inventory = {
    provider = Provider_id.of_string "openai";
    endpoint = Endpoint_id.of_string "responses";
    provider_model_id = Provider_model_id.of_string "gpt-4o";
    display_name = Some model_display_name;
    lifecycle = Active;
    capabilities = [ "text_generation" ];
    source = Unknown_inventory_source;
    complete_snapshot = true;
    safe_metadata = Safe_metadata.empty;
  } in
  let inventory_response = response (Model_inventory_result { models = [ inventory ] }) in
  let inventory_round_trip =
    ok (V1_codec.decode_query_response (ok (V1_codec.encode_query_response inventory_response)))
  in
  (match inventory_round_trip.result with
   | Model_inventory_result { models = [ { display_name = Some actual; provider_model_id; _ } ] }
     when String.equal (Model_display_name.to_string actual) "GPT-4o"
       && String.equal (Provider_model_id.to_string provider_model_id) "gpt-4o" -> ()
   | _ -> failwith "model inventory display name lost its nominal type through the codec");
  (* Safe diagnostic values are nominal query metadata, not arbitrary strings
     or Activity diagnostic codes.  Both query result positions use the same
     wrapper and retain it through the closed wire codec. *)
  let provider_status = {
    route_id = Route_id.of_string "route-1";
    provider = Provider_id.of_string "openai";
    endpoint = Endpoint_id.of_string "responses";
    availability = Degraded;
    credit_state = Credit_low;
    billing_state = Billing_ok;
    circuit_state = Circuit_closed;
    observed_at = time "2026-01-01T00:00:00Z";
    stale_after = time "2026-01-01T01:00:00Z";
    safe_code = Some (Safe_code.of_string "provider-degraded");
  } in
  let provider_status_round_trip =
    ok (V1_codec.decode_query_response
          (ok (V1_codec.encode_query_response
                 (response (Provider_status_result { routes = [ provider_status ] })))))
  in
  (match provider_status_round_trip.result with
   | Provider_status_result { routes = [ { safe_code = Some value; _ } ] }
     when String.equal (Safe_code.to_string value) "provider-degraded" -> ()
   | _ -> failwith "provider safe code lost its nominal type through the codec");
  let credit_status = {
    provider = Provider_id.of_string "openai";
    endpoint = Endpoint_id.of_string "responses";
    credit_state = Credit_unknown;
    billing_state = Billing_unknown;
    confirmed_at = None;
    evidence_source = Unknown_evidence;
    safe_evidence_code = Some (Safe_code.of_string "credit-unknown");
  } in
  let credit_status_round_trip =
    ok (V1_codec.decode_query_response
          (ok (V1_codec.encode_query_response
                 (response (Credit_status_result { endpoints = [ credit_status ] })))))
  in
  (match credit_status_round_trip.result with
   | Credit_status_result { endpoints = [ { safe_evidence_code = Some value; _ } ] }
     when String.equal (Safe_code.to_string value) "credit-unknown" -> ()
   | _ -> failwith "credit safe evidence code lost its nominal type through the codec");
  (* Model inventory lifecycle values must use the exact Go wire vocabulary.
     In particular, [available]/[unavailable]/[unknown] are not the route
     availability or the legacy active/retired spellings. *)
  let assert_lifecycle_wire lifecycle expected =
    let filter =
      filter_ok (Query.Filter.model_inventory ~lifecycle ())
    in
    let request = (Query.to_envelope ~operation_key ~context
                     (Query.Model_inventory filter)).query
    in
    let payload = Yojson.Safe.from_string
      (Bytes.to_string (ok (V1_codec.encode_query_request request)))
    in
    (match payload with
     | `Assoc fields ->
         (match List.assoc_opt "query" fields with
          | Some (`Assoc query_fields) ->
              (match List.assoc_opt "lifecycle" query_fields with
               | Some (`String value) when String.equal value expected -> ()
               | _ -> failwith ("unexpected lifecycle wire value for " ^ expected))
          | _ -> failwith "model inventory lifecycle payload is missing its query object")
     | _ -> failwith "model inventory lifecycle payload is not an object");
    (match V1_codec.decode_query_request
             (ok (V1_codec.encode_query_request request)) with
     | Ok (Model_inventory_request { lifecycle = Some actual; _ })
       when actual = lifecycle -> ()
     | Ok _ -> failwith ("model inventory lifecycle did not round-trip: " ^ expected)
     | Error error -> failf "model inventory lifecycle decode failed: %s"
                       (Temporal.Error.message error))
  in
  assert_lifecycle_wire Active "available";
  assert_lifecycle_wire Deprecated "deprecated";
  assert_lifecycle_wire Retired "unavailable";
  assert_lifecycle_wire Unknown "unknown";
  let invalid_lifecycle = Bytes.of_string
    {|{"api_version":"llm.temporal/query/v1","operation_key":"query-1","context":{"tenant":"tenant","project":"project","actor":"actor"},"kind":"model_inventory","query":{"lifecycle":"active"}}|}
  in
  (match V1_codec.decode_query_request invalid_lifecycle with
   | Error _ -> ()
   | Ok _ -> failwith "legacy active lifecycle spelling was accepted");
  (match (Query.to_envelope ~operation_key ~context
            (Query.Provider_status built_provider)).query with
   | Provider_status_request { page_size = 25; include_healthy = false;
                               refresh_if_older_than_seconds = Some 300L; _ } -> ()
   | _ -> failwith "validated provider filter did not build the expected query");
  expect_filter_error "spend interval"
    "spend_summary.end_time must be after start_time"
    (Query.Filter.spend_summary
       ~start_time:(time "2026-01-02T00:00:00Z")
       ~end_time:(time "2026-01-01T00:00:00Z") ());
  expect_filter_error "duplicate spend dimension"
    "spend_summary.group_by contains duplicate value"
    (Query.Filter.spend_summary ~start_time:(time "2026-01-01T00:00:00Z")
       ~end_time:(time "2026-01-02T00:00:00Z")
       ~group_by:[ By_provider; By_provider ] ());
  expect_filter_error "duplicate spend operation kind"
    "spend_summary.operation_kinds contains duplicate value"
    (Query.Filter.spend_summary ~start_time:(time "2026-01-01T00:00:00Z")
       ~end_time:(time "2026-01-02T00:00:00Z")
       ~operation_kinds:[ Generate; Generate ] ());

  let unknown = Bytes.of_string
    {|{"api_version":"llm.temporal/query/v1","operation_key":"query-1","query_execution_id":"execution-1","kind":"future_kind","observed_at":"2026-01-01T00:00:00Z","source":"persisted","freshness":"current","complete":true,"next_cursor":null,"result":{},"cost_status":"unknown","actual_cost_usd":null,"cost_unknown_reason_code":"state_unavailable"}|}
  in
  (match V1_codec.decode_query_response unknown with
   | Error _ -> ()
   | Ok _ -> failwith "unknown query result tag was accepted");
  print_endline "typed query tests passed"
