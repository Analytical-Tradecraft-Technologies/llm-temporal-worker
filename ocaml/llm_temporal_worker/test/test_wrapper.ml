open Llm_temporal

let expect_ok = function Ok value -> value | Error error -> failwith (Temporal.Error.message error)
let expect_error = function Ok _ -> failwith "expected codec error" | Error _ -> ()
let assert_equal expected actual = if expected <> actual then failwith (Printf.sprintf "expected %S, got %S" expected actual)
let replace_field name value fields =
  List.map (fun (field, current) -> if field = name then (field, value) else (field, current)) fields

let operation_key = Operation_key.of_string
let operation_id = Operation_id.of_string
let model_selector = Model_selector.of_string
let resolved_model_id = Resolved_model_id.of_string
let endpoint_id = Endpoint_id.of_string
let route_id = Route_id.of_string
let continuation_handle = Continuation_handle.of_string
let provider_id = Provider_id.of_string
let endpoint_family = Endpoint_family.of_string
let api_family = Api_family.of_string
let provider_response_id = Provider_response_id.of_string
let provider_request_id = Provider_request_id.of_string
let tool_name = Tool_name.of_string
let tenant_id = Tenant_id.of_string
let project_id = Project_id.of_string
let cost_catalog_version = Cost_catalog_version.of_string
let diagnostic_code = Diagnostic_code.of_string
let task_queue = Temporal_task_queue.of_string

(* This mirrors the README and deliberately uses only the public facade. *)
let _facade_example : Llm_temporal.request = {
  operation_key = Llm_temporal.Operation_key.of_string "facade-example";
  context = None;
  model = Llm_temporal.Model_selector.of_string "arbitrary-model-name";
  service_class = Llm_temporal.Standard;
  service_class_fallbacks = [];
  portability = Llm_temporal.Strict;
  instructions = [];
  input = [];
  tools = [];
  tool_policy = { choice = Llm_temporal.Auto; parallel = false };
  output = None;
  sampling = None;
  reasoning = None;
  continuation = None;
  extensions = [];
}

let request_value = {
  operation_key = operation_key "order-42";
  context = Some { tenant = Some (tenant_id "tenant"); project = Some (project_id "project"); actor = None; tags = [ ("region", "au") ] };
  model = model_selector "gpt-test";
  service_class = Priority;
  service_class_fallbacks = [ Standard ];
  portability = Strict;
  instructions = [ Text_instruction { level = Application; text = "Be concise." } ];
  input = [ Message { actor = Human; content = [ Text "Hello"; Image { media_type = "image/png"; source = Url "https://example.invalid/image.png"; detail = Some "high" } ] } ];
  tools = [];
  tool_policy = { choice = Auto; parallel = false };
  output = Some { max_tokens = Some 32; format = Text_format };
  sampling = None;
  reasoning = None;
  continuation =
    Some {
      handle = continuation_handle "continuation-42";
      endpoint_id = None;
      model = None;
      expires_at = None;
      pinned = true;
      provider_state =
        Some [{
          provider = provider_id "openai";
          endpoint_family = endpoint_family "responses";
          media_type = "application/json";
          opaque = "c3RhdGU=";
        }];
    };
  extensions = [ ("test.extension", `Assoc [ ("enabled", `Bool true) ]) ];
}

let make_value =
  Request.make
    ~operation_key:(operation_key "make-example")
    ~model:(model_selector "arbitrary-model")
    ~service_class:Economy
    ~input:[ Message { actor = Human; content = [ Text "hello" ] } ]
    ()

let service_value = { requested = Priority; attempted = Priority; actual = Some Priority; provider_value = Some "priority"; fallback_index = 0 }
let usage_value = {
  input_tokens = 2L; output_tokens = 1L; reasoning_tokens = 0L;
  cache_read_tokens = 0L; cache_write_tokens = 0L;
  provider_raw = Some [ ("provider_usage", `Int 3) ];
}

let response_value = {
  operation_key = operation_key "order-42";
  operation_id = Some (operation_id "operation-42");
  status = Completed;
  output = [ Message { actor = Model; content = [ Refusal { message = "No."; provider_code = Some "policy" } ] } ];
  route = { route_id = Some (route_id "route-1"); endpoint_id = Some (endpoint_id "openai"); api_family = Some (api_family "responses"); requested_model = Some (model_selector "gpt-test"); resolved_model = Some (resolved_model_id "gpt-test") };
  service = service_value;
  usage = usage_value;
  cost = {
    status = Some Cost_known;
    currency = "USD"; reserved_microusd = 10L; actual_microusd = 8L;
    method_ = "catalog"; catalog_version = cost_catalog_version "v1";
  };
  provider = { response_id = Some (provider_response_id "resp-1"); request_id = Some (provider_request_id "req-1"); generation_id = None; finish_reason = Some "stop"; raw = [ ("provider_field", `String "value") ] };
  continuation = None;
  diagnostics = [ { code = diagnostic_code "translated"; message = "typed"; severity = Info; path = Some "request"; details = Some [ ("source", "test") ] } ];
  metadata = { operation_id = Some (operation_id "operation-42") };
}

(* The deprecated [Request]/[invoke_once] compatibility path still accepts
   the old constructor shape, but it now dispatches this canonical v1
   envelope.  Keep this fixture separate from [request_value], which exercises
   the legacy codec's validation rules. *)
let legacy_v1_request =
  Request.make
    ~operation_key:(operation_key "legacy-v1")
    ~context:{ tenant = Some (tenant_id "tenant");
               project = Some (project_id "project");
               actor = Some (Actor_id.of_string "actor"); tags = [] }
    ~model:(model_selector "gpt-test")
    ~service_class:Priority
    ~input:[ Message { actor = Human; content = [ Text "legacy v1" ] } ]
    ()

let generate_response_value (request : generate_request) = {
  api_version = V1_codec.generate_api_version;
  operation_key = request.operation_key;
  operation_id = operation_id "legacy-v1-operation";
  status = Completed;
  output = [];
  checkpoint = { handle = Checkpoint.of_string_exn "legacy-v1-checkpoint";
                 parent = request.parent; kind = Generation_checkpoint; depth = 0l };
  cache = { disposition = Cache_disabled; variant = 0l; entry_age_seconds = None };
  route = None; usage = None;
  cost = Exact_cost { actual_cost_usd = Decimal.zero;
                       method_ = Control_query_zero; catalog_version = None };
  diagnostics = [];
}

let () =
  if make_value.context <> None then failwith "Request.make context default";
  if make_value.service_class_fallbacks <> [] then failwith "Request.make fallback default";
  if make_value.portability <> Strict then failwith "Request.make portability default";
  if make_value.instructions <> [] then failwith "Request.make instructions default";
  if make_value.tools <> [] then failwith "Request.make tools default";
  if make_value.tool_policy <> { choice = Auto; parallel = false } then failwith "Request.make tool policy default";
  if make_value.output <> None || make_value.sampling <> None || make_value.reasoning <> None || make_value.continuation <> None then failwith "Request.make optional defaults";
  if make_value.extensions <> [] then failwith "Request.make extensions default";
  assert_equal "llm.generate.v1" (Temporal.Activity.name generate_activity);
  assert_equal "llm.generate.workflow.v1" (Temporal.Workflow.name (workflow ()));
  if Temporal.Activity.implementation generate_activity <> None then failwith "remote Go activity has an OCaml implementation";
  if Temporal.Activity.Retry_policy.maximum_attempts activity_retry_policy <> 1 then
    failwith "activity retry policy must permit exactly one attempt";
  if Temporal.Duration.to_ms (Temporal.Activity.Retry_policy.initial_interval activity_retry_policy) <> 1L then
    failwith "activity retry policy initial interval changed";
  if Temporal.Duration.to_ms (Temporal.Activity.Retry_policy.maximum_interval activity_retry_policy) <> 1L then
    failwith "activity retry policy maximum interval changed";
  let valid_tool = {
    kind = Function; name = tool_name "lookup"; description = "lookup";
    input_schema = `Assoc []; output_schema = None;
  } in
  expect_error
    (Temporal.Codec.encode request_codec
       { request_value with tools = [{ valid_tool with input_schema = `String "invalid" }] });
  expect_error
    (Temporal.Codec.encode request_codec
       { request_value with tools = [{ valid_tool with output_schema = Some (`List []) }] });
  expect_error
    (Temporal.Codec.encode request_codec
       { request_value with
         output = Some { max_tokens = None;
                         format = Json_schema_format {
                           name = "result"; description = None; schema = `Bool true;
                           strict = true; } } });
  expect_error
    (Temporal.Codec.encode request_codec
       { request_value with
         continuation = Some {
           handle = continuation_handle "";
           endpoint_id = None; model = None; expires_at = None; pinned = true;
           provider_state = None;
         } });
  expect_error
    (Temporal.Codec.encode request_codec
       { request_value with
         continuation = Some {
           handle = continuation_handle "continuation-42";
           endpoint_id = None; model = None; expires_at = Some "tomorrow"; pinned = true;
           provider_state = None;
         } });
  expect_error
    (Temporal.Codec.encode request_codec
       { request_value with
         continuation = Some {
           handle = continuation_handle "continuation-42";
           endpoint_id = None; model = None; expires_at = None; pinned = true;
           provider_state = Some [{
             provider = provider_id "";
             endpoint_family = endpoint_family "responses";
             media_type = "application/json";
             opaque = "state";
           }];
         } });
  let valid_expiry_request = {
    request_value with
    continuation = Some {
      handle = continuation_handle "continuation-42";
      endpoint_id = None; model = None;
      expires_at = Some "2026-07-18T13:45:00.123+10:00";
      pinned = true; provider_state = None;
    };
  } in
  ignore (expect_ok (Temporal.Codec.encode request_codec valid_expiry_request));
  let request_payload = expect_ok (Temporal.Codec.encode request_codec request_value) in
  assert_equal "json/plain" (List.assoc "encoding" request_payload.metadata);
  (* The pre-checkpoint compatibility codec has an outer [request] object.
     The production v1 Activity accepts the flat Generate envelope instead;
     keep this boundary explicit so a legacy helper cannot silently send an
     incompatible payload to the same Activity name. *)
  let flat_v1_payload = {
    request_payload with
    data = Bytes.of_string
      {|{"api_version":"llm.temporal/v1","operation_key":"v1-boundary","context":{"tenant":"tenant","project":"project","actor":"actor"},"append":[]}|};
  } in
  expect_error (Temporal.Codec.decode request_codec flat_v1_payload);
  (match Temporal.Codec.decode generate_v1_request_codec flat_v1_payload with
   | Ok request when Operation_key.to_string request.operation_key = "v1-boundary" -> ()
   | Ok _ -> failwith "v1 boundary decoded the wrong operation key"
   | Error error -> failwith ("v1 boundary rejected its canonical payload: " ^ Temporal.Error.message error));
  let request_envelope = Yojson.Safe.from_string (Bytes.to_string request_payload.data) in
  let request_fields = match request_envelope with `Assoc fields -> fields | _ -> failwith "request envelope is not an object" in
  assert_equal api_version (match List.assoc "api_version" request_fields with `String value -> value | _ -> failwith "request api_version");
  let encoded_request = match List.assoc "request" request_fields with `Assoc fields -> fields | _ -> failwith "request field" in
  assert_equal "priority" (match List.assoc "service_class" encoded_request with `String value -> value | _ -> failwith "service_class");
  assert_equal "strict" (match List.assoc "portability" encoded_request with `String value -> value | _ -> failwith "portability");
  ignore (List.assoc "service_class_fallbacks" encoded_request);
  ignore (List.assoc "tool_policy" encoded_request);
  let encoded_input = match List.assoc "input" encoded_request with `List [ `Assoc message ] -> message | _ -> failwith "input message" in
  let encoded_content = match List.assoc "content" encoded_input with `List [ _; `Assoc image ] -> image | _ -> failwith "image content" in
  assert_equal "https://example.invalid/image.png" (match List.assoc "url" encoded_content with `String value -> value | _ -> failwith "flat image url");
  assert_equal "high" (match List.assoc "detail" encoded_content with `String value -> value | _ -> failwith "image detail");
  if List.mem_assoc "source" encoded_content then failwith "media source must be flat";
  let binary_request = {
    request_value with
    input = [ Message { actor = Human; content = [ Image { media_type = "application/octet-stream"; source = Bytes "\000\001\255"; detail = None } ] } ];
  } in
  let binary_payload = expect_ok (Temporal.Codec.encode request_codec binary_request) in
  let binary_envelope = Yojson.Safe.from_string (Bytes.to_string binary_payload.data) in
  let binary_request_json = match binary_envelope with
    | `Assoc fields -> (match List.assoc "request" fields with `Assoc request -> request | _ -> failwith "binary request")
    | _ -> failwith "binary envelope"
  in
  let binary_content = match List.assoc "input" binary_request_json with
    | `List [ `Assoc message ] -> (match List.assoc "content" message with `List [ `Assoc content ] -> content | _ -> failwith "binary content")
    | _ -> failwith "binary input"
  in
  assert_equal "AAH/" (match List.assoc "bytes" binary_content with `String value -> value | _ -> failwith "binary base64");
  (match (expect_ok (Temporal.Codec.decode request_codec binary_payload)).input with
   | [ Message { actor = _; content = [ Image { source = Bytes value; _ } ] } ] when value = "\000\001\255" -> ()
   | _ -> failwith "binary media did not round trip");
  let invalid_tool = { valid_tool with name = tool_name "bad name" } in
  expect_error (Temporal.Codec.encode request_codec { request_value with tools = [ invalid_tool ] });
  expect_error (Temporal.Codec.encode request_codec { request_value with service_class_fallbacks = [ Standard; Standard ] });
  expect_error (Temporal.Codec.encode request_codec { request_value with output = Some { max_tokens = Some (-1); format = Text_format } });
  expect_error (Temporal.Codec.encode request_codec { request_value with input = [ Message { actor = Human; content = [ Image { media_type = "text/plain"; source = Url "javascript:alert(1)"; detail = None } ] } ] });
  let request_payload_with request =
    { request_payload with
      data = Bytes.of_string (Yojson.Safe.to_string (`Assoc [
        ("api_version", `String api_version); ("request", `Assoc request);
      ])) }
  in
  let duplicate_outer =
    Printf.sprintf
      "{\"api_version\":\"%s\",\"api_version\":\"%s\",\"request\":%s}"
      api_version api_version (Yojson.Safe.to_string (`Assoc encoded_request))
  in
  expect_error
    (Temporal.Codec.decode request_codec
       { request_payload with data = Bytes.of_string duplicate_outer });
  expect_error
    (Temporal.Codec.decode request_codec
       { request_payload with
         data = Bytes.of_string (Yojson.Safe.to_string (`Assoc [
           ("api_version", `String api_version); ("request", `Assoc encoded_request);
           ("unexpected", `Bool true);
         ])) });
  let unknown_part_input =
    match List.assoc "input" encoded_request with
    | `List [ `Assoc message ] ->
        (match List.assoc "content" message with
         | `List (`Assoc text :: rest) ->
             `List [ `Assoc (replace_field "content"
               (`List (`Assoc (("unexpected", `Bool true) :: text) :: rest)) message) ]
         | _ -> failwith "request text content")
    | _ -> failwith "request input"
  in
  expect_error
    (Temporal.Codec.decode request_codec
       (request_payload_with (replace_field "input" unknown_part_input encoded_request)));
  let unknown_blob_input =
    `List [ `Assoc [
      ("kind", `String "message"); ("actor", `String "human");
      ("content", `List [ `Assoc [
        ("kind", `String "image"); ("media_type", `String "image/png");
        ("blob", `Assoc [ ("locator", `String "blob://test");
                            ("digest", `String "sha256:test");
                            ("byte_length", `Int 1); ("media_type", `String "image/png");
                            ("unexpected", `Bool true) ]);
      ] ]);
    ] ]
  in
  expect_error
    (Temporal.Codec.decode request_codec
       (request_payload_with (replace_field "input" unknown_blob_input encoded_request)));
  let request_without_continuation = { request_value with continuation = None } in
  let without_continuation_payload =
    expect_ok (Temporal.Codec.encode request_codec request_without_continuation)
  in
  let without_continuation_envelope =
    Yojson.Safe.from_string (Bytes.to_string without_continuation_payload.data)
  in
  let without_continuation_request =
    match without_continuation_envelope with
    | `Assoc fields -> (match List.assoc "request" fields with `Assoc request -> request | _ -> failwith "request envelope")
    | _ -> failwith "request envelope"
  in
  (match List.assoc "continuation" without_continuation_request with
   | `Null -> () | _ -> failwith "continuation must be encoded as explicit null");
  (match (expect_ok (Temporal.Codec.decode request_codec without_continuation_payload)).continuation with
   | None -> () | Some _ -> failwith "explicit null continuation did not decode");
  let tool_without_kind =
    `Assoc [ ("name", `String "lookup"); ("description", `String "lookup");
             ("input_schema", `Assoc []) ]
  in
  let request_with_default_tool =
    request_payload_with
      (replace_field "tools" (`List [ tool_without_kind ]) encoded_request)
  in
  (match (expect_ok (Temporal.Codec.decode request_codec request_with_default_tool)).tools with
   | [ { kind = Function; _ } ] -> ()
   | _ -> failwith "omitted tool kind did not default to function");
  let invalid_tool field value =
    let fields = [
      ("kind", `String "function"); ("name", `String "lookup");
      ("description", `String "lookup"); ("input_schema", `Assoc []);
    ] in
    `Assoc
      (if field = "output_schema" then (field, value) :: fields
       else replace_field field value fields)
  in
  let request_with_tool tool =
    request_payload_with (replace_field "tools" (`List [ tool ]) encoded_request)
  in
  expect_error
    (Temporal.Codec.decode request_codec
       (request_with_tool (invalid_tool "input_schema" (`String "invalid"))));
  expect_error
    (Temporal.Codec.decode request_codec
       (request_with_tool
          (invalid_tool "output_schema" (`List []))));
  let invalid_output =
    `Assoc [
      ("format", `Assoc [ ("kind", `String "json_schema");
                            ("name", `String "result");
                            ("schema", `String "invalid"); ("strict", `Bool true) ]);
    ]
  in
  expect_error
    (Temporal.Codec.decode request_codec
       (request_payload_with (replace_field "output" invalid_output encoded_request)));
  let decoded_request = expect_ok (Temporal.Codec.decode request_codec request_payload) in
  assert_equal "order-42" (Operation_key.to_string decoded_request.operation_key);
  assert_equal "priority" (match decoded_request.service_class with Economy -> "economy" | Standard -> "standard" | Priority -> "priority");
  (match decoded_request.continuation with
   | Some { endpoint_id = None; model = None; provider_state = Some [ _ ]; _ } -> ()
   | _ -> failwith "request continuation did not preserve typed optional fields");
  let encoded_continuation =
    match List.assoc "continuation" encoded_request with
    | `Assoc fields -> fields
    | _ -> failwith "request continuation"
  in
  let invalid_continuation_payload continuation =
    request_payload_with
      (replace_field "continuation" (`Assoc continuation) encoded_request)
  in
  expect_error
    (Temporal.Codec.decode request_codec
       (invalid_continuation_payload
          (replace_field "handle" (`String "") encoded_continuation)));
  expect_error
    (Temporal.Codec.decode request_codec
       (invalid_continuation_payload
          (("expires_at", `String "tomorrow") :: encoded_continuation)));
  expect_error
    (Temporal.Codec.decode request_codec
       (invalid_continuation_payload
          (("expires_at", `String "2026-02-31T13:45:00Z") :: encoded_continuation)));
  let response_payload = expect_ok (Temporal.Codec.encode response_codec response_value) in
  let response_envelope = Yojson.Safe.from_string (Bytes.to_string response_payload.data) in
  let response_fields = match response_envelope with `Assoc fields -> fields | _ -> failwith "response envelope is not an object" in
  assert_equal api_version (match List.assoc "api_version" response_fields with `String value -> value | _ -> failwith "response api_version");
  ignore (List.assoc "response" response_fields);
  ignore (List.assoc "metadata" response_fields);
  let encoded_response = match List.assoc "response" response_fields with `Assoc fields -> fields | _ -> failwith "response field" in
  let output_item = match List.assoc "output" encoded_response with `List [ `Assoc item ] -> item | _ -> failwith "response item" in
  let refusal = match List.assoc "content" output_item with `List [ `Assoc item ] -> item | _ -> failwith "refusal content" in
  assert_equal "No." (match List.assoc "text" refusal with `String value -> value | _ -> failwith "refusal text");
  let response_payload_with response =
    { response_payload with
      data = Bytes.of_string (Yojson.Safe.to_string (`Assoc [
        ("api_version", `String api_version);
        ("response", `Assoc response);
        ("metadata", `Assoc [ ("operation_id", `String "operation-42") ]);
      ])) }
  in
  let negative_usage = `Assoc [
    ("input_tokens", `Intlit "-1"); ("output_tokens", `Intlit "0");
    ("reasoning_tokens", `Intlit "0"); ("cache_read_tokens", `Intlit "0");
    ("cache_write_tokens", `Intlit "0")
  ] in
  expect_error (Temporal.Codec.decode response_codec (response_payload_with (replace_field "usage" negative_usage encoded_response)));
  expect_error (Temporal.Codec.encode response_codec { response_value with usage = { usage_value with input_tokens = -1L } });
  let empty_diagnostic = `List [ `Assoc [
    ("code", `String ""); ("message", `String ""); ("severity", `String "error")
  ] ] in
  expect_error (Temporal.Codec.decode response_codec (response_payload_with (replace_field "diagnostics" empty_diagnostic encoded_response)));
  let decoded_response = expect_ok (Temporal.Codec.decode response_codec response_payload) in
  assert_equal "operation-42" (Operation_id.to_string (Option.get decoded_response.operation_id));
  assert_equal "operation-42" (Operation_id.to_string (Option.get decoded_response.metadata.operation_id));
  let derived_metadata_payload =
    expect_ok
      (Temporal.Codec.encode response_codec
         { response_value with metadata = { operation_id = None } })
  in
  assert_equal "operation-42"
    (Operation_id.to_string
       (Option.get
          (expect_ok (Temporal.Codec.decode response_codec derived_metadata_payload)).metadata.operation_id));
  expect_error
    (Temporal.Codec.encode response_codec
       { response_value with metadata = { operation_id = Some (operation_id "different-operation") } });
  let mismatched_metadata_payload =
    { response_payload with
      data = Bytes.of_string (Yojson.Safe.to_string (`Assoc [
        ("api_version", `String api_version);
        ("response", `Assoc encoded_response);
        ("metadata", `Assoc [ ("operation_id", `String "different-operation") ]);
      ])) }
  in
  expect_error (Temporal.Codec.decode response_codec mismatched_metadata_payload);
  let response_without_optional_fields = {
    response_value with
    usage = { usage_value with provider_raw = None };
    cost = { response_value.cost with status = None };
    continuation = Some {
      handle = continuation_handle "continuation-response-42";
      endpoint_id = None;
      model = None;
      expires_at = None;
      pinned = false;
      provider_state = None;
    };
  } in
  let response_without_optional_payload =
    expect_ok (Temporal.Codec.encode response_codec response_without_optional_fields)
  in
  let decoded_without_optional =
    expect_ok (Temporal.Codec.decode response_codec response_without_optional_payload)
  in
  (match decoded_without_optional.usage.provider_raw with
   | None -> ()
   | Some _ -> failwith "optional provider_raw should remain absent");
  (match decoded_without_optional.cost.status with
   | None -> ()
   | Some _ -> failwith "optional cost_status should remain absent");
  (match decoded_without_optional.continuation with
   | Some { handle; endpoint_id = None; model = None;
            provider_state = None; pinned = false; _ }
     when Continuation_handle.to_string handle = "continuation-response-42" -> ()
   | _ -> failwith "response continuation optional fields did not decode");
  let unknown_cost_response = {
    response_value with cost = { response_value.cost with status = Some Cost_unknown }
  } in
  let unknown_cost_payload =
    expect_ok (Temporal.Codec.encode response_codec unknown_cost_response)
  in
  (match (expect_ok (Temporal.Codec.decode response_codec unknown_cost_payload)).cost.status with
   | Some Cost_unknown -> ()
   | _ -> failwith "unknown cost_status did not round trip");
  let calls = ref 0 in
  let dispatch ?task_queue activity (request : generate_request) =
    incr calls;
    assert_equal "go-activities" (Temporal_task_queue.to_string (Option.get task_queue));
    assert_equal activity_name (Temporal.Activity.name activity);
    assert_equal "legacy-v1" (Operation_key.to_string request.operation_key);
    if request.parent <> None then failwith "legacy compatibility request unexpectedly has a parent";
    let payload = expect_ok (Temporal.Codec.encode generate_v1_request_codec request) in
    let json = Yojson.Safe.from_string (Bytes.to_string payload.data) in
    (match json with
     | `Assoc fields ->
         if List.mem_assoc "request" fields then
           failwith "legacy compatibility encoded the pre-v1 nested request envelope";
         (match List.assoc "api_version" fields with
          | `String value -> assert_equal "llm.temporal/v1" value
          | _ -> failwith "legacy compatibility v1 api_version")
     | _ -> failwith "legacy compatibility v1 payload is not an object");
    if [ "legacy v1" ] <>
       (List.filter_map (function
          | Message { content = [ Text value ]; _ } -> Some value
          | _ -> None) request.append)
    then failwith "legacy compatibility changed the Generate append payload";
    Ok (generate_response_value request)
  in
  ignore (expect_ok (invoke_once ~task_queue:(task_queue "go-activities") ~dispatch legacy_v1_request));
  if !calls <> 1 then failwith "wrapper dispatched more than once";
  let default_queue_calls = ref 0 in
  let default_queue_dispatch ?task_queue activity (request : generate_request) =
    incr default_queue_calls;
    if task_queue <> None then failwith "omitted task queue must remain omitted";
    assert_equal activity_name (Temporal.Activity.name activity);
    assert_equal "legacy-v1" (Operation_key.to_string request.operation_key);
    Ok (generate_response_value request)
  in
  ignore (expect_ok (invoke_once ~dispatch:default_queue_dispatch legacy_v1_request));
  if !default_queue_calls <> 1 then failwith "default queue dispatch count";
  let rejected_calls = ref 0 in
  let rejected_dispatch ?task_queue:_ _activity _request =
    incr rejected_calls;
    Error (Temporal.Error.codec ~message:"unexpected dispatch")
  in
  let incomplete_context =
    { legacy_v1_request with
      context = Some { tenant = Some (tenant_id "tenant"); project = None;
                       actor = Some (Actor_id.of_string "actor"); tags = [] } }
  in
  (match invoke_once ~dispatch:rejected_dispatch incomplete_context with
   | Error error when String.equal (Temporal.Error.message error)
                          "legacy Request.context must include tenant, project, and actor for Generate v1" -> ()
   | Error error -> failwith ("unexpected legacy conversion error: " ^ Temporal.Error.message error)
   | Ok _ -> failwith "legacy compatibility dispatched an incomplete context");
  if !rejected_calls <> 0 then failwith "legacy conversion failure dispatched an Activity";
  let unsupported_sampling =
    { legacy_v1_request with
      sampling = Some { temperature = None; top_p = Some 0.5; top_k = None;
                        seed = None; presence_penalty = None;
                        frequency_penalty = None; stop_sequences = None } }
  in
  (match invoke_once ~dispatch:rejected_dispatch unsupported_sampling with
   | Error error when String.equal (Temporal.Error.message error)
                          "legacy Request.sampling contains controls not representable by Generate v1" -> ()
   | Error error -> failwith ("unexpected legacy sampling error: " ^ Temporal.Error.message error)
   | Ok _ -> failwith "legacy compatibility silently dropped unsupported sampling");
  if !rejected_calls <> 0 then failwith "unsupported legacy sampling dispatched an Activity";
  let small_temperature_request =
    { legacy_v1_request with
      sampling = Some { temperature = Some 1e-10; top_p = None; top_k = None;
                        seed = None; presence_penalty = None;
                        frequency_penalty = None; stop_sequences = None } }
  in
  let decimal_dispatch ?task_queue:_ activity request =
    if Temporal.Activity.name activity <> activity_name then
      failwith "legacy temperature dispatched the wrong Activity";
    (match request.settings_patch.temperature with
     | Set value when String.equal (Decimal.to_string value) "0.0000000001" -> ()
     | Set value -> failwith ("legacy temperature was not canonicalized: " ^ Decimal.to_string value)
     | _ -> failwith "legacy temperature was not carried into the v1 patch");
    Ok (generate_response_value request)
  in
  ignore (expect_ok (invoke_once ~dispatch:decimal_dispatch small_temperature_request));
  List.iter (fun mode ->
    let unsupported_reasoning =
      { legacy_v1_request with
        reasoning = Some { mode; effort = High; token_budget = None; summary = Concise } }
    in
    match invoke_once ~dispatch:rejected_dispatch unsupported_reasoning with
    | Error error when String.equal (Temporal.Error.message error)
                           "legacy Request.reasoning.mode is not representable by Generate v1" -> ()
    | Error error -> failwith ("unexpected legacy reasoning error: " ^ Temporal.Error.message error)
    | Ok _ -> failwith "legacy compatibility silently changed an unsupported reasoning mode"
  ) [ Adaptive; Reasoning_enabled ];
  if !rejected_calls <> 0 then failwith "unsupported legacy reasoning dispatched an Activity";
  let mismatched_dispatch ?task_queue:_ activity request =
    if Temporal.Activity.name activity <> activity_name then
      failwith "legacy compatibility dispatched the wrong Activity for mismatch test";
    Ok { (generate_response_value request) with
         operation_key = operation_key "different-operation" }
  in
  (match invoke_once ~dispatch:mismatched_dispatch legacy_v1_request with
   | Error error when String.equal (Temporal.Error.message error)
                          "generate response operation key mismatch: expected legacy-v1, got different-operation" -> ()
   | Error error -> failwith ("unexpected legacy mismatch error: " ^ Temporal.Error.message error)
   | Ok _ -> failwith "legacy compatibility accepted a mismatched operation key");
  let failure = Temporal.Error.make ~category:`Activity ~message:"provider failed" () in
  let failed_calls = ref 0 in
  let failing_dispatch ?task_queue:_ _activity _request =
    incr failed_calls;
    Error failure
  in
  (match invoke_once ~dispatch:failing_dispatch legacy_v1_request with
   | Error error when Temporal.Error.message error = "provider failed" -> ()
   | Error _ -> failwith "wrapper changed activity failure"
   | Ok _ -> failwith "wrapper swallowed activity failure");
  if !failed_calls <> 1 then failwith "failed activity was retried by invoke_once";
  print_endline "llm_temporal typed wrapper tests passed"
