open Llm_temporal_models

let ( let* ) = Result.bind
let error message = Error (Temporal.Error.codec ~message)

type handle = {
  operation_id : string;
  kind : string;
  provider : string;
  endpoint_id : string;
  provider_operation_id : string;
}

type 'a submission =
  | Ready of 'a
  | Pending of { operation_key : string; handle : handle }

type request = { context : request_context; pending : handle }

type response =
  | Still_pending of handle
  | Generated of handle * generate_response
  | Compacted of handle * compaction_response
  | Failed of handle * string

let closed allowed = function
  | `Assoc fields
    when List.for_all (fun (k, _) -> List.mem k allowed) fields
         && List.length fields
            = List.length (List.sort_uniq String.compare (List.map fst fields))
    ->
      Ok fields
  | _ -> error "invalid polling object"

let field key fields =
  match List.assoc_opt key fields with
  | Some v -> Ok v
  | None -> error ("missing " ^ key)

let text = function
  | `String s when s <> "" -> Ok s
  | _ -> error "nonempty string required"

let get key fields =
  let* v = field key fields in
  text v

let handle_json h =
  `Assoc
    [
      ("operation_id", `String h.operation_id);
      ("kind", `String h.kind);
      ("provider", `String h.provider);
      ("endpoint_id", `String h.endpoint_id);
      ("provider_operation_id", `String h.provider_operation_id);
    ]

let handle_of_json j =
  let* f =
    closed
      [
        "operation_id";
        "kind";
        "provider";
        "endpoint_id";
        "provider_operation_id";
      ]
      j
  in
  let* operation_id = get "operation_id" f in
  let* kind = get "kind" f in
  let* provider = get "provider" f in
  let* endpoint_id = get "endpoint_id" f in
  let* provider_operation_id = get "provider_operation_id" f in
  let valid s =
    String.length s <= 512
    && not (String.exists (fun c -> List.mem c [ '\000'; '\r'; '\n'; '\t' ]) s)
  in
  if
    (not (List.mem kind [ "generate"; "compact" ]))
    || not
         (List.for_all valid
            [ operation_id; provider; endpoint_id; provider_operation_id ])
  then error "invalid polling handle"
  else Ok { operation_id; kind; provider; endpoint_id; provider_operation_id }

let parse f b =
  try f (Yojson.Safe.from_string (Bytes.to_string b))
  with Yojson.Json_error _ -> error "invalid polling JSON"

let bytes j = Bytes.of_string (Yojson.Safe.to_string j)
let decode_json codec j = codec (bytes j)

let encoded codec v =
  let* b = codec v in
  try Ok (Yojson.Safe.from_string (Bytes.to_string b))
  with Yojson.Json_error _ -> error "invalid encoded JSON"

let version f expected =
  let* v = get "api_version" f in
  if v = expected then Ok () else error "unsupported api_version"

let submission_of_json kind api decode j =
  match j with
  | `Assoc f when List.assoc_opt "status" f = Some (`String "pending") ->
      let* f =
        closed
          [
            "api_version"; "operation_key"; "operation_id"; "status"; "pending";
          ]
          j
      in
      let* () = version f api in
      let* operation_key = get "operation_key" f in
      let* id = get "operation_id" f in
      let* p = field "pending" f in
      let* handle = handle_of_json p in
      if handle.kind <> kind || handle.operation_id <> id then
        error "pending identity mismatch"
      else Ok (Pending { operation_key; handle })
  | _ ->
      let* v = decode_json decode j in
      Ok (Ready v)

let submission_json kind api encode = function
  | Ready v -> encoded encode v
  | Pending { operation_key; handle } ->
      let* _ = handle_of_json (handle_json handle) in
      if operation_key = "" || handle.kind <> kind then
        error "pending identity mismatch"
      else
        Ok
          (`Assoc
             [
               ("api_version", `String api);
               ("operation_key", `String operation_key);
               ("operation_id", `String handle.operation_id);
               ("status", `String "pending");
               ("pending", handle_json handle);
             ])

let decode_generate =
  parse
    (submission_of_json "generate" Llm_temporal_v1_codec.generate_api_version
       Llm_temporal_v1_codec.decode_generate_response)

let encode_generate v =
  let* j =
    submission_json "generate" Llm_temporal_v1_codec.generate_api_version
      Llm_temporal_v1_codec.encode_generate_response v
  in
  Ok (bytes j)

let decode_compact =
  parse
    (submission_of_json "compact" Llm_temporal_v1_codec.compact_api_version
       Llm_temporal_v1_codec.decode_compaction_response)

let encode_compact v =
  let* j =
    submission_json "compact" Llm_temporal_v1_codec.compact_api_version
      Llm_temporal_v1_codec.encode_compaction_response v
  in
  Ok (bytes j)

let api = "llm.temporal/poll/v1"

let context_json context =
  if context.tags <> [] then error "poll context tags are unsupported"
  else
    let* j = Llm_temporal_v1_codec.context_to_v1_json context in
    let* _ = Llm_temporal_v1_codec.context_of_v1_json j in
    Ok j

let request_json (r : request) =
  let* _ = handle_of_json (handle_json r.pending) in
  let* c = context_json r.context in
  Ok
    (`Assoc
       [
         ("api_version", `String api);
         ("context", c);
         ("pending", handle_json r.pending);
       ])

let encode_request r =
  let* j = request_json r in
  Ok (bytes j)

let decode_request =
  parse (fun j ->
      let* f = closed [ "api_version"; "context"; "pending" ] j in
      let* () = version f api in
      let* c = field "context" f in
      let* context = Llm_temporal_v1_codec.context_of_v1_json c in
      let* h = field "pending" f in
      let* pending = handle_of_json h in
      Ok { context; pending })

let response_of_json j =
  let* f =
    closed
      [ "api_version"; "status"; "pending"; "generate"; "compact"; "failure" ]
      j
  in
  let* () = version f api in
  let* p = field "pending" f in
  let* h = handle_of_json p in
  let* status = get "status" f in
  let has k = List.mem_assoc k f in
  match status with
  | "pending" when not (has "generate" || has "compact" || has "failure") ->
      Ok (Still_pending h)
  | "failed" when not (has "generate" || has "compact") ->
      let* j = field "failure" f in
      let* f = closed [ "code"; "cost_unknown" ] j in
      let* code = get "code" f in
      let* unknown = field "cost_unknown" f in
      if
        unknown <> `Bool true
        || not
             (List.mem code
                [
                  "provider_failed"; "provider_cancelled"; "result_unavailable";
                ])
      then error "invalid poll failure"
      else Ok (Failed (h, code))
  | "completed" when h.kind = "generate" && not (has "compact" || has "failure")
    ->
      let* j = field "generate" f in
      let* r = decode_json Llm_temporal_v1_codec.decode_generate_response j in
      if Operation_id.to_string r.operation_id <> h.operation_id then
        error "poll result identity mismatch"
      else Ok (Generated (h, r))
  | "completed" when h.kind = "compact" && not (has "generate" || has "failure")
    ->
      let* j = field "compact" f in
      let* r = decode_json Llm_temporal_v1_codec.decode_compaction_response j in
      if Operation_id.to_string r.operation_id <> h.operation_id then
        error "poll result identity mismatch"
      else Ok (Compacted (h, r))
  | _ -> error "invalid poll response"

let decode_response = parse response_of_json

let encode_response r =
  let* h, status, extra =
    match r with
    | Still_pending h -> Ok (h, "pending", [])
    | Failed (h, code) ->
        Ok
          ( h,
            "failed",
            [
              ( "failure",
                `Assoc [ ("code", `String code); ("cost_unknown", `Bool true) ]
              );
            ] )
    | Generated (h, r) ->
        let* j = encoded Llm_temporal_v1_codec.encode_generate_response r in
        Ok (h, "completed", [ ("generate", j) ])
    | Compacted (h, r) ->
        let* j = encoded Llm_temporal_v1_codec.encode_compaction_response r in
        Ok (h, "completed", [ ("compact", j) ])
  in
  let j =
    `Assoc
      ([
         ("api_version", `String api);
         ("status", `String status);
         ("pending", handle_json h);
       ]
      @ extra)
  in
  let* _ = response_of_json j in
  Ok (bytes j)

let codec encode decode =
  Temporal.Codec.make ~encoding:"json/plain" ~encode ~decode

let generate_activity =
  Temporal.Activity.remote ~name:"llm.generate.v1"
    ~input:
      (codec Llm_temporal_v1_codec.encode_generate_request
         Llm_temporal_v1_codec.decode_generate_request)
    ~output:(codec encode_generate decode_generate)

let compact_activity =
  Temporal.Activity.remote ~name:"llm.compact.v1"
    ~input:
      (codec Llm_temporal_v1_codec.encode_compact_request
         Llm_temporal_v1_codec.decode_compact_request)
    ~output:(codec encode_compact decode_compact)

let activity =
  Temporal.Activity.remote ~name:"llm.poll.v1"
    ~input:(codec encode_request decode_request)
    ~output:(codec encode_response decode_response)
