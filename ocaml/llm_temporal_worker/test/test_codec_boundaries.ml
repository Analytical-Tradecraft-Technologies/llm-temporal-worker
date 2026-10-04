open Llm_temporal
let ok = function Ok value -> value | Error error -> failwith (Temporal.Error.message error)
let time value = match Ptime.of_rfc3339 value with Ok (value, _, _) -> value | Error _ -> failwith "invalid test timestamp"
let value = function Ok value -> value | Error message -> failwith message
let spend_filter () = { start_time = time "2026-01-01T00:00:00Z"; end_time = time "2026-01-02T00:00:00Z"; group_by = []; operation_kinds = [] }
let budget_filter () = { policy_key = None; active_at = None; include_windows = true }
let response_for _ = {
 api_version = V1_codec.query_api_version;
 operation_key = Operation_key.of_string "query-1";
 query_execution_id = Query_execution_id.of_string "execution-1";
 observed_at = time "2026-01-01T00:00:00Z";
 source = Persisted; freshness = Current; complete = true; next_cursor = None;
 result = Budget_status_result {
  active_at = time "2026-01-01T00:00:00Z";
  generation_id = Budget_generation_id.of_string "generation-1";
  manifest_digest = value (Sha256_digest.of_hex (String.make 64 'a'));
  stream_high_water_mark = value (Budget_stream_id.of_string "1-0"); windows = [] };
 cost = Exact_cost { actual_cost_usd = Usd_decimal.zero; method_ = Control_query_zero; catalog_version = None }
}

let () =
  let start_time = time "2026-09-20T12:00:00.100000001Z" in
  let end_time = time "2026-09-20T12:00:00.900000009Z" in
  let filter = { (spend_filter ()) with start_time; end_time } in
  let request = Spend_summary_request filter in
  (match ok (V1_codec.decode_query_request (ok (V1_codec.encode_query_request request))) with
   | Spend_summary_request actual when Ptime.equal actual.start_time start_time && Ptime.equal actual.end_time end_time -> ()
   | _ -> failwith "fractional spend boundaries changed on the wire");
  let request = Budget_status_request { (budget_filter ()) with active_at = Some start_time } in
  (match ok (V1_codec.decode_query_request (ok (V1_codec.encode_query_request request))) with
   | Budget_status_request { active_at = Some actual; _ } when Ptime.equal actual start_time -> ()
   | _ -> failwith "fractional budget timestamp changed on the wire");
  let original = { (response_for request) with observed_at = start_time } in
  let original = match original.result with
    | Budget_status_result status -> { original with result = Budget_status_result { status with active_at = end_time } }
    | _ -> failwith "unexpected response kind" in
  let decoded = ok (V1_codec.decode_query_response (ok (V1_codec.encode_query_response original))) in
  if not (Ptime.equal decoded.observed_at start_time) then failwith "fractional observed_at changed";
  (match decoded.result with
   | Budget_status_result status when Ptime.equal status.active_at end_time -> ()
   | _ -> failwith "fractional result timestamp changed");
  let empty_policy = Bytes.of_string
    {|{"api_version":"llm.temporal/query/v1","operation_key":"query-1","context":{"tenant":"tenant","project":"project","actor":"actor"},"kind":"budget_status","query":{"policy_key":""}}|} in
  (match V1_codec.decode_query_request empty_policy with
   | Error _ -> () | Ok _ -> failwith "empty policy identifier accepted");
  (match V1_codec.decode_query_envelope empty_policy with
   | Error _ -> () | Ok _ -> failwith "empty policy identifier accepted in envelope");
  let encoded = Yojson.Safe.from_string (Bytes.to_string (ok (V1_codec.encode_query_response original))) in
  let invalid_response = match encoded with
    | `Assoc fields -> `Assoc (List.map (function
      | "result", `Assoc fields -> "result", `Assoc (List.map (function "generation_id", _ -> "generation_id", `String "" | field -> field) fields)
      | field -> field) fields)
    | _ -> failwith "expected response object" in
  match V1_codec.decode_query_response (Bytes.of_string (Yojson.Safe.to_string invalid_response)) with
  | Error _ -> () | Ok _ -> failwith "empty response generation identifier accepted"
