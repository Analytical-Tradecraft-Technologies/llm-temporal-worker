open Llm_temporal

let failf format = Printf.ksprintf failwith format
let ok = function Ok value -> value | Error error -> failf "codec error: %s" (Temporal.Error.message error)
let error = function Ok _ -> failwith "expected codec error" | Error _ -> ()
let checkpoint value = match Checkpoint.of_string value with Ok value -> value | Error message -> failwith message
let time value = match Ptime.of_rfc3339 value with Ok (value, _, _) -> value | Error _ -> failwith "invalid test time"
let fixture name =
  let candidates =
    [ Filename.concat (Sys.getcwd ()) ("fixtures/task17/" ^ name);
      (match Sys.getenv_opt "DUNE_SOURCEROOT" with
       | None -> ""
       | Some root -> Filename.concat root ("ocaml/llm_temporal_worker/test/fixtures/task17/" ^ name)) ]
  in
  match List.find_opt Sys.file_exists candidates with
  | Some path -> In_channel.with_open_bin path In_channel.input_all
  | None -> failf "fixture not found: %s" name
let canonical json = Yojson.Safe.to_string (Yojson.Safe.from_string json)
let assert_fixture name decode encode =
  let input = fixture name in
  let value = ok (decode (Bytes.of_string input)) in
  let output = ok (encode value) |> Bytes.to_string in
  if canonical input <> canonical output then failf "fixture %s changed on canonical round trip" name
let assert_rejected name decode =
  if Result.is_ok (decode (Bytes.of_string (fixture name))) then failf "fixture %s was accepted" name
let omit field bytes =
  let json = Yojson.Safe.from_string (Bytes.to_string bytes) in
  let json = match json with `Assoc fields -> `Assoc (List.remove_assoc field fields) | value -> value in
  Bytes.of_string (Yojson.Safe.to_string json)

let map_checkpoint transform bytes =
  let json = Yojson.Safe.from_string (Bytes.to_string bytes) in
  let json = match json with
    | `Assoc fields ->
        let checkpoint = List.assoc "checkpoint" fields in
        `Assoc (("checkpoint", transform checkpoint) :: List.remove_assoc "checkpoint" fields)
    | value -> value
  in
  Bytes.of_string (Yojson.Safe.to_string json)

let set_checkpoint_kind kind = function
  | `Assoc fields -> `Assoc (("kind", `String kind) :: List.remove_assoc "kind" fields)
  | value -> value

let omit_checkpoint_parent = function
  | `Assoc fields -> `Assoc (List.remove_assoc "parent" fields)
  | value -> value

let () =
  let () = match Usd_decimal.of_string "000.0100" with Ok _ -> failwith "leading zero accepted" | Error _ -> () in
  let decimal = match Usd_decimal.of_string "1.2300" with Ok value -> value | Error message -> failwith message in
  if Usd_decimal.to_string decimal <> "1.23" then failwith "decimal was not canonicalized";
  if Usd_decimal.of_string "1.0000000000000000001" |> Result.is_ok then failwith "19 fractional digits accepted";
  let context = { tenant = Some (Tenant_id.of_string "tenant"); project = Some (Project_id.of_string "project"); actor = Some (Actor_id.of_string "actor"); tags = [] } in
  let keep_patch = { model = Keep; service_class = Keep; service_class_fallbacks = Keep; portability = Keep; instructions = Keep; tools = Keep; tool_policy = Keep; output = Keep; temperature = Keep; reasoning_effort = Keep; reasoning_summary = Keep; compaction_policy = Keep; extensions = Keep } in
  let request = {
    api_version = V1_codec.generate_api_version; operation_key = Operation_key.of_string "op-1"; context;
    parent = Some (checkpoint "cp-0"); append = [Message { actor = Human; content = [Text "hello"] }];
    settings_patch = keep_patch; cache = Some { max_age_seconds = 60L; variant = 0l };
  } in
  let request' = ok (V1_codec.decode_generate_request (ok (V1_codec.encode_generate_request request))) in
  if request'.operation_key <> request.operation_key || request'.append <> request.append then failwith "generate round trip";
  let generate_response = { api_version = V1_codec.generate_api_version; operation_key = Operation_key.of_string "op-1"; operation_id = Operation_id.of_string "id-1"; status = Completed; output = []; checkpoint = { handle = checkpoint "cp-1"; parent = None; kind = Generation_checkpoint; depth = 0l }; cache = { disposition = Cache_miss_populated; variant = 0l; entry_age_seconds = None }; route = None; usage = None; cost = Unknown_cost { reason = State_unavailable }; diagnostics = [] } in
  let generate_bytes = ok (V1_codec.encode_generate_response generate_response) in
  let generate_without_diagnostics = ok (V1_codec.decode_generate_response (omit "diagnostics" generate_bytes)) in
  if generate_without_diagnostics.diagnostics <> [] then failwith "omitted generate diagnostics";
  error (V1_codec.encode_generate_response
           { generate_response with
             checkpoint = { generate_response.checkpoint with kind = Compaction_checkpoint } });
  error (V1_codec.decode_generate_response
           (map_checkpoint (set_checkpoint_kind "compaction") generate_bytes));
  let cache_replay_bytes = ok (V1_codec.encode_generate_response
      { generate_response with
        checkpoint = { generate_response.checkpoint with kind = Cache_replay_checkpoint } }) in
  ignore (ok (V1_codec.decode_generate_response cache_replay_bytes));
  let compact = { api_version = V1_codec.compact_api_version; operation_key = Operation_key.of_string "compact-1"; context; parent = checkpoint "cp-1"; policy = Some { target_tokens = Some 100L; summary_style = Some Concise }; cache = None } in
  ignore (ok (V1_codec.decode_compact_request (ok (V1_codec.encode_compact_request compact))));
  let compact_response = { api_version = V1_codec.compact_api_version; operation_key = Operation_key.of_string "compact-1"; operation_id = Operation_id.of_string "id-2"; checkpoint = { handle = checkpoint "cp-2"; parent = Some (checkpoint "cp-1"); kind = Compaction_checkpoint; depth = 1l }; cache = { disposition = Cache_miss_populated; variant = 0l; entry_age_seconds = None }; provenance = None; usage = None; cost = Unknown_cost { reason = State_unavailable }; diagnostics = [] } in
  let compact_bytes = ok (V1_codec.encode_compaction_response compact_response) in
  let compact_without_diagnostics = ok (V1_codec.decode_compaction_response (omit "diagnostics" compact_bytes)) in
  if compact_without_diagnostics.diagnostics <> [] then failwith "omitted compact diagnostics";
  error (V1_codec.encode_compaction_response
           { compact_response with
             checkpoint = { compact_response.checkpoint with parent = None } });
  error (V1_codec.decode_compaction_response
           (map_checkpoint omit_checkpoint_parent compact_bytes));
  let query = Provider_status_request { provider = Some (Provider_id.of_string "openai"); endpoint = None; availability = None; include_healthy = true; refresh_if_older_than_seconds = None; page_size = 20; cursor = None } in
  let envelope = { api_version = V1_codec.query_api_version; operation_key = Operation_key.of_string "query-1"; context; query } in
  let envelope' = ok (V1_codec.decode_query_envelope (ok (V1_codec.encode_query_envelope envelope))) in
  if envelope'.operation_key <> envelope.operation_key then failwith "query envelope round trip";
  let query_response = {
    api_version = V1_codec.query_api_version; operation_key = Operation_key.of_string "query-1"; query_execution_id = Query_execution_id.of_string "qx-1";
    observed_at = time "2026-01-01T00:00:00Z"; source = Persisted; freshness = Current; complete = true; next_cursor = None;
    result = Provider_status_result { routes = [] }; cost = Exact_cost { actual_cost_usd = Usd_decimal.zero; method_ = Control_query_zero; catalog_version = None };
  } in
  ignore (ok (V1_codec.decode_query_response (ok (V1_codec.encode_query_response query_response))));
  error (V1_codec.decode_generate_request (Bytes.of_string "{\"api_version\":\"llm.temporal/v1\",\"operation_key\":\"x\",\"context\":{\"tenant\":\"t\",\"project\":\"p\",\"actor\":\"a\"},\"parent\":null,\"append\":[],\"settings_patch\":{},\"cache\":null,\"extra\":true}"));
  assert_fixture "generate-request.json" V1_codec.decode_generate_request V1_codec.encode_generate_request;
  assert_rejected "generate-request.invalid-temperature.json" V1_codec.decode_generate_request;
  assert_fixture "compact-request.json" V1_codec.decode_compact_request V1_codec.encode_compact_request;
  assert_rejected "compact-request.invalid-parent.json" V1_codec.decode_compact_request;
  assert_fixture "query-envelope.json" V1_codec.decode_query_envelope V1_codec.encode_query_envelope;
  assert_rejected "query-envelope.invalid-kind.json" V1_codec.decode_query_envelope;
  print_endline "v1 protocol tests passed"
