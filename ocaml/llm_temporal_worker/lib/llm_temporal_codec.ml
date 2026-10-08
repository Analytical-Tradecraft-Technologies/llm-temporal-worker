open Llm_temporal_models

let api_version = "llm.temporal/v1"

let codec_error format = Printf.ksprintf (fun message -> Temporal.Error.codec ~message) format

let service_class_to_string = function Economy -> "economy" | Standard -> "standard" | Priority -> "priority"

let service_class_of_string = function
  | "economy" -> Ok Economy
  | "standard" -> Ok Standard
  | "priority" -> Ok Priority
  | value -> Error (codec_error "invalid service_class %S; expected economy, standard, or priority" value)

let actor_to_string = function Human -> "human" | Model -> "model"

let actor_of_string = function
  | "human" -> Ok Human
  | "model" -> Ok Model
  | value -> Error (codec_error "invalid message actor %S" value)

let status_to_string = function
  | Completed -> "completed"
  | Tool_calls -> "tool_calls"
  | Refused -> "refused"
  | Length -> "length"
  | Paused -> "paused"
  | Content_filtered -> "content_filtered"

let status_of_string = function
  | "completed" -> Ok Completed
  | "tool_calls" -> Ok Tool_calls
  | "refused" -> Ok Refused
  | "length" -> Ok Length
  | "paused" -> Ok Paused
  | "content_filtered" -> Ok Content_filtered
  | value -> Error (codec_error "invalid response status %S" value)

let object_fields context = function
  | `Assoc fields -> Ok fields
  | _ -> Error (codec_error "%s must be a JSON object" context)

let required context name fields =
  match List.assoc_opt name fields with
  | Some value -> Ok value
  | None -> Error (codec_error "%s is missing required field %S" context name)

let optional name fields = List.assoc_opt name fields

let validate_fields context allowed fields =
  let rec validate seen = function
        | [] -> Ok fields
        | (name, _) :: _remaining when List.mem name seen ->
            Error (codec_error "%s contains duplicate field %S" context name)
        | (name, _) :: _ when not (List.mem name allowed) ->
            Error (codec_error "%s contains unknown field %S" context name)
        | (name, _) :: remaining -> validate (name :: seen) remaining
  in
  validate [] fields

let closed_fields context allowed value =
  match object_fields context value with
  | Error _ as error -> error
  | Ok fields -> validate_fields context allowed fields

let string context = function
  | `String value -> Ok value
  | _ -> Error (codec_error "%s must be a string" context)

let float context = function
  | `Float value -> Ok value
  | `Int value -> Ok (float_of_int value)
  | `Intlit value ->
      (try Ok (float_of_string value) with Failure _ ->
         Error (codec_error "%s must be a number" context))
  | _ -> Error (codec_error "%s must be a number" context)

let bool context = function
  | `Bool value -> Ok value
  | _ -> Error (codec_error "%s must be a boolean" context)

let int64 context = function
  | `Int value -> Ok (Int64.of_int value)
  | `Intlit value -> (try Ok (Int64.of_string value) with Failure _ -> Error (codec_error "%s must be an integer" context))
  | _ -> Error (codec_error "%s must be an integer" context)

let int context value =
  match int64 context value with
  | Error _ as error -> error
  | Ok value ->
      if value < Int64.of_int min_int || value > Int64.of_int max_int then
        Error (codec_error "%s is outside the OCaml int range" context)
      else Ok (Int64.to_int value)

let list context = function
  | `List values -> Ok values
  | _ -> Error (codec_error "%s must be a JSON array" context)

let nonempty context value =
  if String.trim value = "" then Error (codec_error "%s must not be empty" context) else Ok value

let nonnegative context value =
  if value < 0L then Error (codec_error "%s must not be negative" context) else Ok value

let valid_tool_name context value =
  let length = String.length value in
  if length = 0 || length > 64 then
    Error (codec_error "%s must contain between 1 and 64 ASCII characters" context)
  else
    let valid_char c =
      (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
      (c >= '0' && c <= '9') || c = '_' || c = '-'
    in
    if String.for_all valid_char value then Ok value
    else Error (codec_error "%s must contain only ASCII letters, digits, _ or -" context)

let valid_uri context value =
  let is_alpha c = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') in
  let is_scheme_char c = is_alpha c || (c >= '0' && c <= '9') || c = '+' || c = '-' || c = '.' in
  match String.index_opt value ':' with
  | None -> Error (codec_error "%s must be an absolute URI" context)
  | Some colon when colon = 0 || not (is_alpha value.[0]) ->
      Error (codec_error "%s must have a valid URI scheme" context)
  | Some colon when not (String.for_all is_scheme_char (String.sub value 1 (colon - 1))) ->
      Error (codec_error "%s must have a valid URI scheme" context)
  | Some colon ->
      let scheme = String.lowercase_ascii (String.sub value 0 colon) in
      if scheme = "javascript" || scheme = "data" then
        Error (codec_error "%s uses a forbidden URI scheme" context)
      else Ok value

let base64_encode value = Base64.encode_string value

let base64_decode context value =
  match Base64.decode value with
  | Ok decoded -> Ok decoded
  | Error (`Msg message) -> Error (codec_error "%s is not valid base64: %s" context message)

let map_result f values =
  let rec loop reversed = function
    | [] -> Ok (List.rev reversed)
    | value :: remaining ->
        match f value with
        | Ok mapped -> loop (mapped :: reversed) remaining
        | Error _ as error -> error
  in
  loop [] values

(** Yojson preserves object members, including duplicate keys.  Walk every
    decoded value before interpreting the protocol so open JSON leaves cannot
    hide a duplicate member from the typed codecs. *)
let rec validate_unique_json context = function
  | `Assoc fields ->
      (match validate_fields context (List.map fst fields) fields with
       | Error _ as error -> error
       | Ok fields ->
           (match
              map_result
                (fun (name, value) ->
                  validate_unique_json (context ^ "." ^ name) value)
                fields
            with
            | Error _ as error -> error
            | Ok _ -> Ok ()))
  | `List values ->
      (match map_result (validate_unique_json context) values with
       | Error _ as error -> error
       | Ok _ -> Ok ())
  | _ -> Ok ()

let option_field name encode = function None -> [] | Some value -> [ (name, encode value) ]

let ( let* ) = Result.bind

let optional_value context name decode fields =
  match optional name fields with
  | None | Some `Null -> Ok None
  | Some value -> Result.map Option.some (decode (context ^ " " ^ name) value)

let optional_json context name decode =
  optional_value context name (fun _ value -> decode value)

let required_value context name decode fields =
  let* value = required context name fields in
  decode (context ^ " " ^ name) value

let unique_object context value =
  let* fields = object_fields context value in
  validate_fields context (List.map fst fields) fields

let json_object context = function
  | `Assoc _ as value -> Ok value
  | _ -> Error (codec_error "%s must be a JSON object" context)

let provider_state_payload_to_json (state : provider_state) =
  `Assoc [ ("provider", `String (Provider_id.to_string state.provider)); ("endpoint_family", `String (Endpoint_family.to_string state.endpoint_family)); ("media_type", `String state.media_type); ("opaque", `String state.opaque) ]

let media_source_to_json = function
  | Url value -> [ ("url", `String value) ]
  | Bytes value -> [ ("bytes", `String (base64_encode value)) ]
  | Blob { locator; digest; byte_length; media_type } ->
      [ ("blob", `Assoc [ ("locator", `String locator); ("digest", `String (Blob_digest.to_string digest)); ("byte_length", `Intlit (Int64.to_string byte_length)); ("media_type", `String media_type) ]) ]

let content_to_json = function
  | Text text -> `Assoc [ ("kind", `String "text"); ("text", `String text) ]
  | Image { media_type; source; detail } ->
      `Assoc
        ([ ("kind", `String "image"); ("media_type", `String media_type) ]
        @ media_source_to_json source
        @ option_field "detail" (fun value -> `String value) detail)
  | Document { media_type; source; title } ->
      `Assoc (match title with
        | None -> [ ("kind", `String "document"); ("media_type", `String media_type) ] @ media_source_to_json source
        | Some value -> ([ ("kind", `String "document"); ("media_type", `String media_type); ("title", `String value) ] @ media_source_to_json source))
  | Json value -> `Assoc [ ("kind", `String "json"); ("value", value) ]
  | Refusal { message; provider_code } -> `Assoc ([ ("kind", `String "refusal"); ("text", `String message) ] @ option_field "provider_code" (fun value -> `String value) provider_code)
  | Content_provider_state state -> `Assoc (("kind", `String "provider_state") :: match provider_state_payload_to_json state with `Assoc fields -> fields | _ -> assert false)

let provider_state_of_fields fields =
  match required "provider state" "provider" fields, required "provider state" "endpoint_family" fields,
        required "provider state" "media_type" fields, required "provider state" "opaque" fields with
  | Ok provider, Ok endpoint_family, Ok media_type, Ok opaque ->
      (match string "provider state provider" provider, string "provider state endpoint_family" endpoint_family,
             string "provider state media_type" media_type, string "provider state opaque" opaque with
       | Ok provider, Ok endpoint_family, Ok media_type, Ok opaque -> Ok { provider = Provider_id.of_string provider; endpoint_family = Endpoint_family.of_string endpoint_family; media_type; opaque }
       | (Error _ as error), _, _, _ | _, (Error _ as error), _, _ | _, _, (Error _ as error), _ | _, _, _, (Error _ as error) -> error)
  | (Error _ as error), _, _, _ | _, (Error _ as error), _, _ | _, _, (Error _ as error), _ | _, _, _, (Error _ as error) -> error

let is_ascii_digit value = value >= '0' && value <= '9'

let two_digits value offset =
  if offset + 1 < String.length value && is_ascii_digit value.[offset]
     && is_ascii_digit value.[offset + 1]
  then Some (((Char.code value.[offset] - Char.code '0') * 10)
             + Char.code value.[offset + 1] - Char.code '0')
  else None

let four_digits value offset =
  if offset + 3 < String.length value
     && String.for_all is_ascii_digit (String.sub value offset 4)
  then Some (int_of_string (String.sub value offset 4))
  else None

(* Go's [time.Parse(time.RFC3339Nano, ...)] is the canonical wire check.  Keep
   this dependency-free lexical equivalent on the OCaml side so malformed
   expiry values fail before an Activity is scheduled. *)
let valid_rfc3339 context value =
  let invalid () = Error (codec_error "%s must be an RFC3339 timestamp" context) in
  let length = String.length value in
  let separator offset expected = offset < length && value.[offset] = expected in
  let has_digits start stop =
    start < stop && stop <= length
    && String.for_all is_ascii_digit (String.sub value start (stop - start))
  in
  if length < 20 || not (separator 4 '-') || not (separator 7 '-')
     || not (separator 10 'T') || not (separator 13 ':')
     || not (separator 16 ':') || not (has_digits 0 4)
     || not (has_digits 5 7) || not (has_digits 8 10)
     || not (has_digits 11 13) || not (has_digits 14 16)
     || not (has_digits 17 19)
  then invalid ()
  else
    match four_digits value 0, two_digits value 5, two_digits value 8,
          two_digits value 11, two_digits value 14, two_digits value 17 with
    | Some year, Some month, Some day, Some hour, Some minute, Some second ->
        let leap = year mod 4 = 0 && (year mod 100 <> 0 || year mod 400 = 0) in
        let max_day =
          match month with
          | 2 -> if leap then 29 else 28
          | 4 | 6 | 9 | 11 -> 30
          | _ -> 31
        in
        if month < 1 || month > 12 || day < 1 || day > max_day
           || hour > 23 || minute > 59 || second > 59 then invalid ()
        else
          let zone_start =
            if length > 19 && value.[19] = '.' then
              let rec scan index =
                if index < length && is_ascii_digit value.[index] then scan (index + 1)
                else index
              in
              let index = scan 20 in
              if index = 20 then -1 else index
            else 19
          in
          if zone_start < 0 || zone_start >= length then invalid ()
          else (
            match value.[zone_start] with
            | 'Z' when zone_start + 1 = length -> Ok value
            | ('+' | '-') when zone_start + 6 = length
                              && value.[zone_start + 3] = ':' ->
                (match two_digits value (zone_start + 1),
                       two_digits value (zone_start + 4) with
                 | Some hours, Some minutes when hours <= 23 && minutes <= 59 -> Ok value
                 | _ -> invalid ())
            | _ -> invalid ())
    | _ -> invalid ()

let validate_provider_state context (state : provider_state) =
  let* _ = nonempty (context ^ " provider") (Provider_id.to_string state.provider) in
  let* _ = nonempty (context ^ " endpoint_family") (Endpoint_family.to_string state.endpoint_family) in
  let* _ = nonempty (context ^ " media_type") state.media_type in
  Ok ()

let validate_continuation context (continuation : continuation) =
  let* _ = nonempty (context ^ " handle") (Continuation_handle.to_string continuation.handle) in
  let* () =
    match continuation.expires_at with
    | None -> Ok ()
    | Some value -> valid_rfc3339 (context ^ " expires_at") value |> Result.map (fun _ -> ())
  in
  match continuation.provider_state with
  | None -> Ok ()
  | Some states ->
      map_result (validate_provider_state (context ^ " provider_state")) states
      |> Result.map (fun _ -> ())

let continuation_provider_state_of_json value =
  let* fields =
    closed_fields "continuation provider_state"
      [ "kind"; "provider"; "endpoint_family"; "media_type"; "opaque" ] value
  in
  let* kind = required_value "continuation provider_state" "kind" string fields in
  match kind with
  | "provider_state" -> provider_state_of_fields fields
  | value ->
      Error
        (codec_error
           "continuation provider_state kind %S is not provider_state" value)

let media_source_of_fields fields =
  match optional "url" fields, optional "bytes" fields, optional "blob" fields with
  | Some value, None, None -> Result.map (fun value -> Url value) (string "media url" value)
  | None, Some value, None ->
      let* value = string "media bytes" value in
      let* value = base64_decode "media bytes" value in
      if value = "" then Error (codec_error "media bytes must not be empty") else Ok (Bytes value)
  | None, None, Some value ->
      (match closed_fields "media blob" [ "locator"; "digest"; "byte_length"; "media_type" ] value with
       | Error _ as error -> error
       | Ok blob ->
           match required "media blob" "locator" blob, required "media blob" "digest" blob,
                 required "media blob" "byte_length" blob, required "media blob" "media_type" blob with
           | Ok locator, Ok digest, Ok byte_length, Ok media_type ->
               (match string "media blob locator" locator, string "media blob digest" digest,
                      int64 "media blob byte_length" byte_length, string "media blob media_type" media_type with
               | Ok locator, Ok digest, Ok byte_length, Ok media_type -> Ok (Blob { locator; digest = Blob_digest.of_string digest; byte_length; media_type })
                | (Error _ as error), _, _, _ | _, (Error _ as error), _, _ | _, _, (Error _ as error), _ | _, _, _, (Error _ as error) -> error)
           | (Error _ as error), _, _, _ | _, (Error _ as error), _, _ | _, _, (Error _ as error), _ | _, _, _, (Error _ as error) -> error)
  | _ -> Error (codec_error "media part must contain exactly one of url, bytes, or blob")

let content_of_json value =
  match object_fields "content part" value with
  | Error _ as error -> error
  | Ok fields ->
      match required "content part" "kind" fields with
      | Error _ as error -> error
      | Ok kind ->
          match string "content part kind" kind with
          | Ok kind ->
              let allowed =
                match kind with
                | "text" -> [ "kind"; "text" ]
                | "image" -> [ "kind"; "media_type"; "url"; "bytes"; "blob"; "detail" ]
                | "document" -> [ "kind"; "media_type"; "url"; "bytes"; "blob"; "title" ]
                | "json" -> [ "kind"; "value" ]
                | "refusal" -> [ "kind"; "text"; "provider_code" ]
                | "provider_state" -> [ "kind"; "provider"; "endpoint_family"; "media_type"; "opaque" ]
                | _ -> []
              in
              (match validate_fields "content part" allowed fields with
               | Error _ as error -> error
               | Ok _ ->
                   match kind with
          | "text" ->
              (match required "text content part" "text" fields with
               | Ok text -> Result.map (fun value -> Text value) (string "text content part text" text)
               | Error _ as error -> error)
          | "image" ->
              (match required "image content part" "media_type" fields with
               | Ok media_type ->
                   (match string "image media_type" media_type, media_source_of_fields fields with
                    | Ok media_type, Ok source ->
                        let detail =
                          match optional "detail" fields with
                          | None -> Ok None
                          | Some value ->
                              Result.map Option.some (string "image detail" value)
                        in
                        (match detail with
                         | Ok detail -> Ok (Image { media_type; source; detail })
                         | Error _ as error -> error)
                    | (Error _ as error), _ | _, (Error _ as error) -> error)
               | Error _ as error -> error)
          | "document" ->
              (match required "document content part" "media_type" fields with
               | Ok media_type ->
                   (match string "document media_type" media_type, media_source_of_fields fields with
                    | Ok media_type, Ok source ->
                        let title = match optional "title" fields with None -> Ok None | Some title -> Result.map Option.some (string "document title" title) in
                        (match title with Ok title -> Ok (Document { media_type; source; title }) | Error _ as error -> error)
                    | (Error _ as error), _ | _, (Error _ as error) -> error)
               | Error _ as error -> error)
          | "json" ->
              (match required "json content part" "value" fields with Ok value -> Ok (Json value) | Error _ as error -> error)
          | "refusal" ->
              (match required "refusal content part" "text" fields with
               | Ok text ->
                   (match string "refusal text" text with
                    | Ok message ->
                        let provider_code = match optional "provider_code" fields with None -> Ok None | Some value -> Result.map Option.some (string "refusal provider_code" value) in
                        (match provider_code with Ok provider_code -> Ok (Refusal { message; provider_code }) | Error _ as error -> error)
                    | Error _ as error -> error)
               | Error _ as error -> error)
          | "provider_state" -> Result.map (fun state -> Content_provider_state state) (provider_state_of_fields fields)
          | kind -> Error (codec_error "unsupported content part kind %S" kind))
          | Error _ as error -> error

let message_to_json message =
  `Assoc [
    ("kind", `String "message");
    ("actor", `String (actor_to_string message.actor));
    ("content", `List (List.map content_to_json message.content));
  ]

let message_of_json value =
  match closed_fields "message" [ "kind"; "actor"; "content" ] value with
  | Error _ as error -> error
  | Ok fields ->
      match required "message" "actor" fields, required "message" "content" fields with
      | Ok actor, Ok content ->
          (match string "message actor" actor, list "message content" content with
           | Ok actor, Ok content ->
               (match actor_of_string actor, map_result content_of_json content with
                | Ok actor, Ok content -> Ok { actor; content }
                | (Error _ as error), _ | _, (Error _ as error) -> error)
           | (Error _ as error), _ | _, (Error _ as error) -> error)
      | (Error _ as error), _ | _, (Error _ as error) -> error

let item_to_json = function
  | Message message -> message_to_json message
  | Tool_call { id; name; arguments } ->
      `Assoc [ ("kind", `String "tool_call"); ("id", `String (Tool_call_id.to_string id)); ("name", `String (Tool_name.to_string name)); ("arguments", arguments) ]
  | Tool_result { call_id; name; content; is_error } ->
      let fields = [
        ("kind", `String "tool_result"); ("call_id", `String (Tool_call_id.to_string call_id));
        ("content", `List (List.map content_to_json content)); ("is_error", `Bool is_error);
      ] in
      `Assoc (match name with None -> fields | Some value -> ("name", `String (Tool_name.to_string value)) :: fields)
  | Provider_state state ->
      `Assoc
        (("kind", `String "provider_state")
        :: match provider_state_payload_to_json state with
           | `Assoc fields -> fields
           | _ -> assert false)
  | Reference { uri; metadata } ->
      `Assoc
        ([ ("kind", `String "reference"); ("uri", `String uri) ]
        @ if metadata = [] then [] else [ ("metadata", `Assoc metadata) ])

let item_of_json value =
  match object_fields "item" value with
  | Error _ as error -> error
  | Ok fields ->
      match required "item" "kind" fields with
      | Error _ as error -> error
      | Ok kind ->
          match string "item kind" kind with
          | Ok kind ->
              let allowed =
                match kind with
                | "message" -> [ "kind"; "actor"; "content" ]
                | "tool_call" -> [ "kind"; "id"; "name"; "arguments" ]
                | "tool_result" -> [ "kind"; "call_id"; "name"; "content"; "is_error" ]
                | "provider_state" -> [ "kind"; "provider"; "endpoint_family"; "media_type"; "opaque" ]
                | "reference" -> [ "kind"; "uri"; "metadata" ]
                | _ -> []
              in
              (match validate_fields "item" allowed fields with
               | Error _ as error -> error
               | Ok _ ->
                   match kind with
          | "message" -> Result.map (fun message -> Message message) (message_of_json value)
          | "tool_call" ->
              (match required "tool call" "id" fields, required "tool call" "name" fields,
                     required "tool call" "arguments" fields with
               | Ok id, Ok name, Ok arguments ->
                   (match string "tool call id" id, string "tool call name" name with
                    | Ok id, Ok name ->
                        (match nonempty "tool call id" id, nonempty "tool call name" name with
                         | Ok id, Ok name -> Ok (Tool_call { id = Tool_call_id.of_string id; name = Tool_name.of_string name; arguments })
                         | (Error _ as error), _ | _, (Error _ as error) -> error)
                    | (Error _ as error), _ | _, (Error _ as error) -> error)
               | (Error _ as error), _, _ | _, (Error _ as error), _ | _, _, (Error _ as error) -> error)
          | "tool_result" ->
              (match required "tool result" "call_id" fields, required "tool result" "content" fields with
               | Ok call_id, Ok content ->
                   (match string "tool result call_id" call_id, list "tool result content" content with
                    | Ok call_id, Ok content ->
                        (match map_result content_of_json content with
                         | Error _ as error -> error
                         | Ok content ->
                             let name = match optional "name" fields with None -> Ok None | Some value -> Result.map Option.some (string "tool result name" value) in
                             let is_error = match optional "is_error" fields with None -> Ok false | Some value -> bool "tool result is_error" value in
                             (match name, is_error with
                              | Ok name, Ok is_error ->
                                  Ok (Tool_result {
                                    call_id = Tool_call_id.of_string call_id;
                                    name = Option.map Tool_name.of_string name;
                                    content;
                                    is_error;
                                  })
                              | (Error _ as error), _ | _, (Error _ as error) -> error))
                    | (Error _ as error), _ | _, (Error _ as error) -> error)
               | (Error _ as error), _ | _, (Error _ as error) -> error)
          | "provider_state" ->
              Result.map (fun state -> Provider_state state) (provider_state_of_fields fields)
          | "reference" ->
              (match required "reference" "uri" fields with
               | Error _ as error -> error
               | Ok uri ->
                   (match string "reference uri" uri with
                    | Error _ as error -> error
                    | Ok uri ->
                        let metadata =
                          match optional "metadata" fields with
                          | None -> Ok []
                          | Some (`Assoc values) -> Ok values
                          | Some _ ->
                              Error
                                (codec_error
                                   "reference metadata must be a JSON object")
                        in
                        (match metadata with
                         | Ok metadata -> Ok (Reference { uri; metadata })
                         | Error _ as error -> error)))
          | kind -> Error (codec_error "unsupported item kind %S" kind))
          | Error _ as error -> error

let instruction_to_json = function
  | Text_instruction { level; text } ->
      let level = match level with Application -> "application" | Policy -> "policy" in
      `Assoc [ ("kind", `String "text"); ("level", `String level); ("text", `String text) ]
  | Parts_instruction { level; content } ->
      let level = match level with Application -> "application" | Policy -> "policy" in
      `Assoc [ ("kind", `String "parts"); ("level", `String level); ("content", `List (List.map content_to_json content)) ]

let tool_to_json (tool : function_tool) =
  let kind = match tool.kind with Function -> "function" | Provider -> "provider" | Remote_mcp -> "remote_mcp" in
  let fields = [
    ("kind", `String kind); ("name", `String (Tool_name.to_string tool.name)); ("description", `String tool.description);
    ("input_schema", tool.input_schema);
  ] in
  `Assoc (match tool.output_schema with None -> fields | Some schema -> ("output_schema", schema) :: fields)

let validate_request_schema_objects (request : request) =
  let validate_tool tool =
    let* _ = json_object "tool input_schema" tool.input_schema in
    match tool.output_schema with
    | None -> Ok ()
    | Some schema ->
        let* _ = json_object "tool output_schema" schema in
        Ok ()
  in
  let* _ = map_result validate_tool request.tools in
  match request.output with
  | Some { format = Json_schema_format { schema; _ }; _ } ->
      let* _ = json_object "output json_schema schema" schema in
      Ok ()
  | None | Some _ -> Ok ()

let policy_to_json policy =
  match policy.choice with
  | Auto -> `Assoc [ ("mode", `String "auto"); ("parallel", `Bool policy.parallel) ]
  | None_allowed -> `Assoc [ ("mode", `String "none"); ("parallel", `Bool policy.parallel) ]
  | Required -> `Assoc [ ("mode", `String "required"); ("parallel", `Bool policy.parallel) ]
  | Named name -> `Assoc [ ("mode", `String "named"); ("name", `String (Tool_name.to_string name)); ("parallel", `Bool policy.parallel) ]

let context_to_json (context : request_context) =
  `Assoc (
    option_field "tenant" (fun value -> `String (Tenant_id.to_string value)) context.tenant
    @ option_field "project" (fun value -> `String (Project_id.to_string value)) context.project
    @ option_field "actor" (fun value -> `String (Actor_id.to_string value)) context.actor
    @ if context.tags = [] then [] else [ ("tags", `Assoc (List.map (fun (key, value) -> (key, `String value)) context.tags)) ])

let output_to_json output =
  let format = match output.format with
    | Text_format -> `Assoc [ ("kind", `String "text") ]
    | Json_format -> `Assoc [ ("kind", `String "json") ]
    | Json_schema_format { name; description; schema; strict } ->
        (* An empty name is an absent one, as in the Go codec. *)
        `Assoc ([ ("kind", `String "json_schema") ] @ (if name = "" then [] else [ ("name", `String name) ]) @ [ ("schema", schema); ("strict", `Bool strict) ] @ option_field "description" (fun value -> `String value) description) in
  `Assoc ([ ("format", format) ] @ option_field "max_tokens" (fun value -> `Int value) output.max_tokens)

let sampling_to_json (sampling : sampling) =
  `Assoc (
    option_field "temperature" (fun value -> `Float value) sampling.temperature
    @ option_field "top_p" (fun value -> `Float value) sampling.top_p
    @ option_field "top_k" (fun value -> `Int value) sampling.top_k
    @ option_field "seed" (fun value -> `Intlit (Int64.to_string value)) sampling.seed
    @ option_field "presence_penalty" (fun value -> `Float value) sampling.presence_penalty
    @ option_field "frequency_penalty" (fun value -> `Float value) sampling.frequency_penalty
    @ option_field "stop_sequences" (fun value -> `List (List.map (fun item -> `String item) value)) sampling.stop_sequences)

let reasoning_to_json (reasoning : reasoning) =
  let mode = match reasoning.mode with Provider_default -> "provider_default" | Reasoning_disabled -> "disabled" | Adaptive -> "adaptive" | Reasoning_enabled -> "enabled" in
  let effort = match reasoning.effort with Effort_default -> "provider_default" | Minimal -> "minimal" | Low -> "low" | Medium -> "medium" | High -> "high" | Extra_high -> "xhigh" | Maximum -> "maximum" in
  let summary = match reasoning.summary with Summary_default -> "provider_default" | Summary_none -> "none" | Summary_auto -> "auto" | Concise -> "concise" | Detailed -> "detailed" in
  `Assoc ([ ("mode", `String mode); ("effort", `String effort); ("summary", `String summary) ] @ option_field "token_budget" (fun value -> `Int value) reasoning.token_budget)

let provider_state_to_json (state : provider_state) =
  `Assoc [ ("kind", `String "provider_state"); ("provider", `String (Provider_id.to_string state.provider)); ("endpoint_family", `String (Endpoint_family.to_string state.endpoint_family)); ("media_type", `String state.media_type); ("opaque", `String state.opaque) ]

let continuation_to_json (continuation : continuation) =
  `Assoc (
    [ ("handle", `String (Continuation_handle.to_string continuation.handle)); ("pinned", `Bool continuation.pinned) ]
    @ option_field "endpoint_id" (fun value -> `String (Endpoint_id.to_string value)) continuation.endpoint_id
    @ option_field "model" (fun value -> `String (Resolved_model_id.to_string value)) continuation.model
    @ option_field "expires_at" (fun value -> `String value) continuation.expires_at
    @ option_field "provider_state" (fun value -> `List (List.map provider_state_to_json value)) continuation.provider_state)

let request_to_json (request : request) =
  `Assoc (
    [
    ("api_version", `String api_version); ("operation_key", `String (Operation_key.to_string request.operation_key));
    ("model", `String (Model_selector.to_string request.model)); ("service_class", `String (service_class_to_string request.service_class));
    ("service_class_fallbacks", `List (List.map (fun value -> `String (service_class_to_string value)) request.service_class_fallbacks));
    ("portability", `String (match request.portability with Strict -> "strict" | Best_effort -> "best_effort"));
    ("instructions", `List (List.map instruction_to_json request.instructions));
    ("input", `List (List.map item_to_json request.input));
    ("tools", `List (List.map tool_to_json request.tools));
    ("tool_policy", policy_to_json request.tool_policy);
    ("extensions", `Assoc request.extensions);
    ]
    @ option_field "context" context_to_json request.context
    @ option_field "output" output_to_json request.output
    @ option_field "sampling" sampling_to_json request.sampling
    @ option_field "reasoning" reasoning_to_json request.reasoning
    @ [ ("continuation", match request.continuation with None -> `Null | Some value -> continuation_to_json value) ])

let service_to_json service =
  let fields = [
    ("requested", `String (service_class_to_string service.requested));
    ("attempted", `String (service_class_to_string service.attempted));
    ("fallback_index", `Int service.fallback_index);
  ] in
  let fields = match service.actual with None -> fields | Some value -> ("actual", `String (service_class_to_string value)) :: fields in
  `Assoc (match service.provider_value with None -> fields | Some value -> ("provider_value", `String value) :: fields)

let usage_to_json (usage : usage) =
  `Assoc ([
    ("input_tokens", `Intlit (Int64.to_string usage.input_tokens));
    ("output_tokens", `Intlit (Int64.to_string usage.output_tokens));
    ("reasoning_tokens", `Intlit (Int64.to_string usage.reasoning_tokens));
    ("cache_read_tokens", `Intlit (Int64.to_string usage.cache_read_tokens));
    ("cache_write_tokens", `Intlit (Int64.to_string usage.cache_write_tokens));
  ] @ option_field "provider_raw" (fun value -> `Assoc value) usage.provider_raw)

let route_to_json (route : route) = `Assoc (
  option_field "route_id" (fun value -> `String (Route_id.to_string value)) route.route_id
  @ option_field "endpoint_id" (fun value -> `String (Endpoint_id.to_string value)) route.endpoint_id
  @ option_field "api_family" (fun value -> `String (Api_family.to_string value)) route.api_family
  @ option_field "requested_model" (fun value -> `String (Model_selector.to_string value)) route.requested_model
  @ option_field "resolved_model" (fun value -> `String (Resolved_model_id.to_string value)) route.resolved_model)

let cost_to_json (cost : cost) =
  let status = function Cost_known -> "known" | Cost_unknown -> "unknown" in
  `Assoc
    ([ ("currency", `String cost.currency);
       ("reserved_microusd", `Intlit (Int64.to_string cost.reserved_microusd));
       ("actual_microusd", `Intlit (Int64.to_string cost.actual_microusd));
       ("method", `String cost.method_); ("catalog_version", `String (Cost_catalog_version.to_string cost.catalog_version)) ]
    @ option_field "cost_status" (fun value -> `String (status value)) cost.status)

let provider_to_json (provider : provider) = `Assoc (
  option_field "response_id" (fun value -> `String (Provider_response_id.to_string value)) provider.response_id
  @ option_field "request_id" (fun value -> `String (Provider_request_id.to_string value)) provider.request_id
  @ option_field "generation_id" (fun value -> `String (Provider_generation_id.to_string value)) provider.generation_id
  @ option_field "finish_reason" (fun value -> `String value) provider.finish_reason
  @ if provider.raw = [] then [] else [ ("raw", `Assoc provider.raw) ])

let diagnostic_to_json (diagnostic : diagnostic) =
  let severity = match diagnostic.severity with Info -> "info" | Warning -> "warning" | Diagnostic_error -> "error" in
  `Assoc ([ ("code", `String (Diagnostic_code.to_string diagnostic.code)); ("message", `String diagnostic.message); ("severity", `String severity) ] @ option_field "path" (fun value -> `String value) diagnostic.path @ option_field "details" (fun value -> `Assoc (List.map (fun (key, text) -> (key, `String text)) value)) diagnostic.details)

let metadata_to_json (metadata : response_metadata) =
  `Assoc (option_field "operation_id" (fun value -> `String (Operation_id.to_string value)) metadata.operation_id)

let response_metadata (response : response) =
  match response.operation_id, response.metadata.operation_id with
  | None, None -> Ok { operation_id = None }
  | Some operation_id, None -> Ok { operation_id = Some operation_id }
  | Some operation_id, Some metadata_operation_id
    when operation_id = metadata_operation_id ->
      Ok { operation_id = Some operation_id }
  | None, Some metadata_operation_id ->
      Error
        (codec_error
           "response metadata operation_id %S has no matching response operation_id"
           (Operation_id.to_string metadata_operation_id))
  | Some operation_id, Some metadata_operation_id ->
      Error
        (codec_error
           "response metadata operation_id %S does not match response operation_id %S"
           (Operation_id.to_string metadata_operation_id) (Operation_id.to_string operation_id))

let response_to_json (response : response) =
  let fields = [
    ("api_version", `String api_version); ("operation_key", `String (Operation_key.to_string response.operation_key));
    ("status", `String (status_to_string response.status));
    ("output", `List (List.map item_to_json response.output));
    ("route", route_to_json response.route); ("service", service_to_json response.service); ("usage", usage_to_json response.usage);
    ("cost", cost_to_json response.cost); ("provider", provider_to_json response.provider);
    ("diagnostics", `List (List.map diagnostic_to_json response.diagnostics));
  ] in
  `Assoc (fields @ option_field "operation_id" (fun value -> `String (Operation_id.to_string value)) response.operation_id @ option_field "continuation" continuation_to_json response.continuation)

let parse_json decoder bytes =
  try
    let value = Yojson.Safe.from_string (Bytes.to_string bytes) in
    let* () = validate_unique_json "payload" value in
    decoder value
  with Yojson.Json_error message -> Error (codec_error "invalid json/plain payload: %s" message)

let require_version fields =
  match required "payload" "api_version" fields with
  | Error _ as error -> error
  | Ok version ->
      match string "api_version" version with
      | Ok value when value = api_version -> Ok ()
      | Ok value -> Error (codec_error "unsupported api_version %S" value)
      | Error _ as error -> error

let level_of_string = function
  | "application" -> Ok Application
  | "policy" -> Ok Policy
  | value -> Error (codec_error "invalid instruction level %S" value)

let portability_of_string = function
  | "strict" -> Ok Strict
  | "best_effort" -> Ok Best_effort
  | value -> Error (codec_error "invalid portability %S" value)

let instruction_of_json value =
  let* fields = unique_object "instruction" value in
  let* kind = required_value "instruction" "kind" string fields in
  let allowed = match kind with "text" -> [ "kind"; "level"; "text" ] | "parts" -> [ "kind"; "level"; "content" ] | _ -> [] in
  let* _ = validate_fields "instruction" allowed fields in
  let* level = optional_value "instruction" "level" string fields in
  let* level = match level with None -> Ok Application | Some value -> level_of_string value in
  match kind with
  | "text" -> let* text = required_value "instruction" "text" string fields in Ok (Text_instruction { level; text })
  | "parts" -> let* values = required_value "instruction" "content" list fields in let* content = map_result content_of_json values in Ok (Parts_instruction { level; content })
  | value -> Error (codec_error "unsupported instruction kind %S" value)

let tool_of_json value =
  let* fields = closed_fields "tool" [ "kind"; "name"; "description"; "input_schema"; "output_schema" ] value in
  let* kind = optional_value "tool" "kind" string fields in
  let* kind = match kind with None -> Ok Function | Some "function" -> Ok Function | Some "provider" -> Ok Provider | Some "remote_mcp" -> Ok Remote_mcp | Some value -> Error (codec_error "invalid tool kind %S" value) in
  let* name = required_value "tool" "name" string fields in
  let* description = required_value "tool" "description" string fields in
  let* input_schema = required "tool" "input_schema" fields in
  let* input_schema = json_object "tool input_schema" input_schema in
  let* output_schema = optional_value "tool" "output_schema" (fun _ value -> Ok value) fields in
  let* output_schema =
    match output_schema with
    | None -> Ok None
    | Some schema ->
        let* schema = json_object "tool output_schema" schema in
        Ok (Some schema)
  in
  Ok { kind; name = Tool_name.of_string name; description; input_schema; output_schema }

let policy_of_json value =
  let* fields = unique_object "tool_policy" value in
  let* mode = required_value "tool_policy" "mode" string fields in
  let allowed = match mode with "named" -> [ "mode"; "name"; "parallel" ] | _ -> [ "mode"; "parallel" ] in
  let* _ = validate_fields "tool_policy" allowed fields in
  let* parallel = required_value "tool_policy" "parallel" bool fields in
  let* choice = match mode with
    | "auto" -> Ok Auto | "none" -> Ok None_allowed | "required" -> Ok Required
    | "named" -> Result.map (fun name -> Named (Tool_name.of_string name)) (required_value "tool_policy" "name" string fields)
    | value -> Error (codec_error "invalid tool policy mode %S" value)
  in
  Ok { choice; parallel }

let output_of_json value =
  let* fields = closed_fields "output" [ "format"; "max_tokens" ] value in
  let* format_value = required "output" "format" fields in
  let* format_fields = unique_object "output format" format_value in
  let* kind = required_value "output format" "kind" string format_fields in
  (* Mirror the Go codec: name and description are accepted on every kind but
     only json_schema uses them (no adapter reads them for text or json), and a
     json_schema name and strict flag are optional. *)
  let allowed = match kind with "text" | "json" -> [ "kind"; "name"; "description" ] | "json_schema" -> [ "kind"; "name"; "description"; "schema"; "strict" ] | _ -> [] in
  let* _ = validate_fields "output format" allowed format_fields in
  let* name = optional_value "output format" "name" string format_fields in
  let* () = match name with
    | Some "" -> Error (codec_error "output format name must not be empty when present")
    | Some value -> Result.map ignore (valid_tool_name "output format name" value)
    | None -> Ok () in
  let* description = optional_value "output format" "description" string format_fields in
  let* format = match kind with
    | "text" -> Ok Text_format
    | "json" -> Ok Json_format
    | "json_schema" ->
        let* schema = required "output format" "schema" format_fields in
        let* schema = json_object "output json_schema schema" schema in
        let* strict = optional_value "output format" "strict" bool format_fields in
        Ok (Json_schema_format { name = Option.value name ~default:""; description; schema; strict = Option.value strict ~default:false })
    | value -> Error (codec_error "invalid output format kind %S" value)
  in
  let* max_tokens = optional_value "output" "max_tokens" int fields in
  Ok { max_tokens; format }

let sampling_of_json value =
  let* fields = closed_fields "sampling" [ "temperature"; "top_p"; "top_k"; "seed"; "presence_penalty"; "frequency_penalty"; "stop_sequences" ] value in
  let* temperature = optional_value "sampling" "temperature" float fields in
  let* top_p = optional_value "sampling" "top_p" float fields in
  let* top_k = optional_value "sampling" "top_k" int fields in
  let* seed = optional_value "sampling" "seed" int64 fields in
  let* presence_penalty = optional_value "sampling" "presence_penalty" float fields in
  let* frequency_penalty = optional_value "sampling" "frequency_penalty" float fields in
  let* stop_sequences = optional_value "sampling" "stop_sequences" (fun context value -> let* values = list context value in map_result (string context) values) fields in
  Ok { temperature; top_p; top_k; seed; presence_penalty; frequency_penalty; stop_sequences }

let reasoning_of_json value =
  let* fields = closed_fields "reasoning" [ "mode"; "effort"; "token_budget"; "summary" ] value in
  let* mode = optional_value "reasoning" "mode" string fields in
  let* mode = match mode with None -> Ok Provider_default | Some "provider_default" -> Ok Provider_default | Some "disabled" -> Ok Reasoning_disabled | Some "adaptive" -> Ok Adaptive | Some "enabled" -> Ok Reasoning_enabled | Some value -> Error (codec_error "invalid reasoning mode %S" value) in
  let* effort = optional_value "reasoning" "effort" string fields in
  let* effort = match effort with None -> Ok Effort_default | Some "provider_default" -> Ok Effort_default | Some "minimal" -> Ok Minimal | Some "low" -> Ok Low | Some "medium" -> Ok Medium | Some "high" -> Ok High | Some "xhigh" -> Ok Extra_high | Some "maximum" -> Ok Maximum | Some value -> Error (codec_error "invalid reasoning effort %S" value) in
  let* token_budget = optional_value "reasoning" "token_budget" int fields in
  let* summary = optional_value "reasoning" "summary" string fields in
  let* summary = match summary with None -> Ok Summary_default | Some "provider_default" -> Ok Summary_default | Some "none" -> Ok Summary_none | Some "auto" -> Ok Summary_auto | Some "concise" -> Ok Concise | Some "detailed" -> Ok Detailed | Some value -> Error (codec_error "invalid reasoning summary %S" value) in
  Ok { mode; effort; token_budget; summary }

let continuation_of_json value =
  let* fields = closed_fields "continuation" [ "handle"; "endpoint_id"; "model"; "expires_at"; "pinned"; "provider_state" ] value in
  let* handle = required_value "continuation" "handle" string fields in
  let* endpoint_id = optional_value "continuation" "endpoint_id" string fields in
  let* model = optional_value "continuation" "model" string fields in
  let* expires_at = optional_value "continuation" "expires_at" string fields in
  let* pinned = required_value "continuation" "pinned" bool fields in
  let* provider_state =
    optional_value "continuation" "provider_state"
      (fun context value ->
        let* values = list context value in
        map_result continuation_provider_state_of_json values)
      fields
  in
  Ok {
    handle = Continuation_handle.of_string handle;
    endpoint_id = Option.map Endpoint_id.of_string endpoint_id;
    model = Option.map Resolved_model_id.of_string model;
    expires_at;
    pinned;
    provider_state;
  }

let context_of_json value =
  let* fields = closed_fields "request context" [ "tenant"; "project"; "actor"; "tags" ] value in
  let* tenant = optional_value "request context" "tenant" string fields in
  let* project = optional_value "request context" "project" string fields in
  let* actor = optional_value "request context" "actor" string fields in
  let* tags = match optional "tags" fields with
    | None | Some `Null -> Ok []
    | Some value ->
        let* tags = unique_object "request context tags" value in
        let* pairs = map_result (fun (name, value) -> Result.map (fun value -> (name, value)) (string "request context tag" value)) tags in
        Ok pairs
  in
  Ok {
    tenant = Option.map Tenant_id.of_string tenant;
    project = Option.map Project_id.of_string project;
    actor = Option.map Actor_id.of_string actor;
    tags;
  }

let request_of_json value =
  let* fields = closed_fields "canonical llm.Request" [ "api_version"; "operation_key"; "context"; "model"; "service_class"; "service_class_fallbacks"; "portability"; "instructions"; "input"; "tools"; "tool_policy"; "output"; "sampling"; "reasoning"; "continuation"; "extensions" ] value in
  let* () = require_version fields in
  let* operation_key = required_value "request" "operation_key" string fields in
  let* operation_key = nonempty "request operation_key" operation_key in
  let* model = required_value "request" "model" string fields in
  let* model = nonempty "request model" model in
  let* service_class = optional_value "request" "service_class" string fields in
  let* service_class = match service_class with None -> Ok Standard | Some value -> service_class_of_string value in
  let* service_class_fallbacks = required_value "request" "service_class_fallbacks" list fields in
  let* service_class_fallbacks = map_result (fun value -> let* value = string "service_class_fallback" value in service_class_of_string value) service_class_fallbacks in
  let* portability = optional_value "request" "portability" string fields in
  let* portability = match portability with None -> Ok Strict | Some value -> portability_of_string value in
  let* instructions = required_value "request" "instructions" list fields in
  let* instructions = map_result instruction_of_json instructions in
  let* input = required_value "request" "input" list fields in
  let* input = map_result item_of_json input in
  let* tools = required_value "request" "tools" list fields in
  let* tools = map_result tool_of_json tools in
  let* tool_policy_value = required "request" "tool_policy" fields in
  let* tool_policy = policy_of_json tool_policy_value in
  let* context = optional_json "request" "context" context_of_json fields in
  let* output = optional_json "request" "output" output_of_json fields in
  let* sampling = optional_json "request" "sampling" sampling_of_json fields in
  let* reasoning = optional_json "request" "reasoning" reasoning_of_json fields in
  let* continuation_value = required "request" "continuation" fields in
  let* continuation = match continuation_value with `Null -> Ok None | value -> Result.map Option.some (continuation_of_json value) in
  let* extensions_value = required "request" "extensions" fields in
  let* extensions = unique_object "request extensions" extensions_value in
  Ok { operation_key = Operation_key.of_string operation_key; context; model = Model_selector.of_string model; service_class; service_class_fallbacks; portability; instructions; input; tools; tool_policy; output; sampling; reasoning; continuation; extensions }

let route_of_json value =
  let* fields = closed_fields "response route" [ "route_id"; "endpoint_id"; "api_family"; "requested_model"; "resolved_model" ] value in
  let* route_id = optional_value "response route" "route_id" string fields in
  let* endpoint_id = optional_value "response route" "endpoint_id" string fields in
  let* api_family = optional_value "response route" "api_family" string fields in
  let* requested_model = optional_value "response route" "requested_model" string fields in
  let* resolved_model = optional_value "response route" "resolved_model" string fields in
  Ok {
    route_id = Option.map Route_id.of_string route_id;
    endpoint_id = Option.map Endpoint_id.of_string endpoint_id;
    api_family = Option.map Api_family.of_string api_family;
    requested_model = Option.map Model_selector.of_string requested_model;
    resolved_model = Option.map Resolved_model_id.of_string resolved_model;
  }

let service_of_json value =
  let* fields = closed_fields "response service" [ "requested"; "attempted"; "actual"; "provider_value"; "fallback_index" ] value in
  let* requested = required_value "response service" "requested" string fields in
  let* requested = service_class_of_string requested in
  let* attempted = required_value "response service" "attempted" string fields in
  let* attempted = service_class_of_string attempted in
  let* actual = optional_value "response service" "actual" (fun _ value -> let* value = string "response service actual" value in service_class_of_string value) fields in
  let* provider_value = optional_value "response service" "provider_value" string fields in
  let* fallback_index = required_value "response service" "fallback_index" int fields in
  Ok { requested; attempted; actual; provider_value; fallback_index }

let usage_of_json value =
  let* fields = closed_fields "response usage" [ "input_tokens"; "output_tokens"; "reasoning_tokens"; "cache_read_tokens"; "cache_write_tokens"; "provider_raw" ] value in
  let* input_tokens = required_value "response usage" "input_tokens" int64 fields in
  let* output_tokens = required_value "response usage" "output_tokens" int64 fields in
  let* reasoning_tokens = required_value "response usage" "reasoning_tokens" int64 fields in
  let* cache_read_tokens = required_value "response usage" "cache_read_tokens" int64 fields in
  let* cache_write_tokens = required_value "response usage" "cache_write_tokens" int64 fields in
  let* provider_raw =
    optional_value "response usage" "provider_raw"
      (fun _ value -> unique_object "response usage provider_raw" value)
      fields
  in
  Ok { input_tokens; output_tokens; reasoning_tokens; cache_read_tokens; cache_write_tokens; provider_raw }

let cost_of_json value =
  let* fields = closed_fields "response cost" [ "cost_status"; "currency"; "reserved_microusd"; "actual_microusd"; "method"; "catalog_version" ] value in
  let* status = optional_value "response cost" "cost_status" string fields in
  let* status =
    match status with
    | None -> Ok None
    | Some "known" -> Ok (Some Cost_known)
    | Some "unknown" -> Ok (Some Cost_unknown)
    | Some value -> Error (codec_error "invalid response cost status %S" value)
  in
  let* currency = required_value "response cost" "currency" string fields in
  let* reserved_microusd = required_value "response cost" "reserved_microusd" int64 fields in
  let* actual_microusd = required_value "response cost" "actual_microusd" int64 fields in
  let* method_ = required_value "response cost" "method" string fields in
  let* catalog_version = required_value "response cost" "catalog_version" string fields in
  Ok { status; currency; reserved_microusd; actual_microusd; method_; catalog_version = Cost_catalog_version.of_string catalog_version }

let provider_of_json value =
  let* fields = closed_fields "response provider" [ "response_id"; "request_id"; "generation_id"; "finish_reason"; "raw" ] value in
  let* response_id = optional_value "response provider" "response_id" string fields in
  let* request_id = optional_value "response provider" "request_id" string fields in
  let* generation_id = optional_value "response provider" "generation_id" string fields in
  let* finish_reason = optional_value "response provider" "finish_reason" string fields in
  let* raw = match optional "raw" fields with None | Some `Null -> Ok [] | Some value -> unique_object "response provider raw" value in
  Ok {
    response_id = Option.map Provider_response_id.of_string response_id;
    request_id = Option.map Provider_request_id.of_string request_id;
    generation_id = Option.map Provider_generation_id.of_string generation_id;
    finish_reason;
    raw;
  }

let diagnostic_of_json value =
  let* fields = closed_fields "diagnostic" [ "code"; "message"; "severity"; "path"; "details" ] value in
  let* code = required_value "diagnostic" "code" string fields in
  let* message = required_value "diagnostic" "message" string fields in
  let* severity = required_value "diagnostic" "severity" string fields in
  let* severity = match severity with "info" -> Ok Info | "warning" -> Ok Warning | "error" -> Ok Diagnostic_error | value -> Error (codec_error "invalid diagnostic severity %S" value) in
  let* path = optional_value "diagnostic" "path" string fields in
  let* details = match optional "details" fields with None | Some `Null -> Ok None | Some value -> let* values = unique_object "diagnostic details" value in let* values = map_result (fun (name, value) -> Result.map (fun value -> (name, value)) (string "diagnostic detail" value)) values in Ok (Some values) in
  Ok { code = Diagnostic_code.of_string code; message; severity; path; details }

let metadata_of_json value =
  let* fields = closed_fields "response metadata" [ "operation_id" ] value in
  let* operation_id = optional_value "response metadata" "operation_id" string fields in
  Ok { operation_id = Option.map Operation_id.of_string operation_id }

let response_of_json value =
  let* fields = closed_fields "canonical llm.Response" [ "api_version"; "operation_key"; "operation_id"; "status"; "output"; "route"; "service"; "usage"; "cost"; "provider"; "continuation"; "diagnostics" ] value in
  let* () = require_version fields in
  let* operation_key = required_value "response" "operation_key" string fields in
  let* operation_key = nonempty "response operation_key" operation_key in
  let* operation_id = optional_value "response" "operation_id" string fields in
  let* status = required_value "response" "status" string fields in
  let* status = status_of_string status in
  let* output = required_value "response" "output" list fields in
  let* output = map_result item_of_json output in
  let* route = required "response" "route" fields in let* route = route_of_json route in
  let* service = required "response" "service" fields in let* service = service_of_json service in
  let* usage = required "response" "usage" fields in let* usage = usage_of_json usage in
  let* cost = required "response" "cost" fields in let* cost = cost_of_json cost in
  let* provider = required "response" "provider" fields in let* provider = provider_of_json provider in
  let* continuation = match optional "continuation" fields with None | Some `Null -> Ok None | Some value -> Result.map Option.some (continuation_of_json value) in
  let* diagnostics = required_value "response" "diagnostics" list fields in
  let* diagnostics = map_result diagnostic_of_json diagnostics in
  Ok {
    operation_key = Operation_key.of_string operation_key;
    operation_id = Option.map Operation_id.of_string operation_id;
    status;
    output;
    route;
    service;
    usage;
    cost;
    provider;
    continuation;
    diagnostics;
    metadata = { operation_id = None };
  }

let validate_media_source context expected_media_type = function
  | Url uri ->
      let* _ = valid_uri (context ^ " url") uri in
      Ok ()
  | Bytes value -> if value = "" then Error (codec_error "%s bytes must not be empty" context) else Ok ()
  | Blob { locator; digest; byte_length; media_type } ->
      let* _ = nonempty (context ^ " locator") locator in
      let* _ = nonempty (context ^ " digest") (Blob_digest.to_string digest) in
      let* _ = nonnegative (context ^ " byte_length") byte_length in
      let* _ = nonempty (context ^ " media_type") media_type in
      if media_type <> expected_media_type then
        Error (codec_error "%s blob media_type %S does not match enclosing media_type %S" context media_type expected_media_type)
      else Ok ()

let validate_content context = function
  | Text _ -> Ok ()
  | Image { media_type; source; _ } | Document { media_type; source; _ } ->
      let* _ = nonempty (context ^ " media_type") media_type in
      validate_media_source context media_type source
  | Json value -> validate_unique_json (context ^ " json") value
  | Refusal { message; provider_code } ->
      let* _ = nonempty (context ^ " refusal") message in
      (match provider_code with None -> Ok () | Some code -> let* _ = nonempty (context ^ " provider_code") code in Ok ())
  | Content_provider_state state -> validate_provider_state context state

let validate_item context = function
  | Message message ->
      map_result (validate_content (context ^ " message")) message.content |> Result.map (fun _ -> ())
  | Tool_call { id; name; arguments } ->
      let* _ = nonempty (context ^ " tool_call id") (Tool_call_id.to_string id) in
      let* _ = valid_tool_name (context ^ " tool_call name") (Tool_name.to_string name) in
      validate_unique_json (context ^ " tool_call arguments") arguments
  | Tool_result { call_id; name; content; _ } ->
      let* _ = nonempty (context ^ " tool_result call_id") (Tool_call_id.to_string call_id) in
      let* _ = match name with None -> Ok () | Some name -> let* _ = valid_tool_name (context ^ " tool_result name") (Tool_name.to_string name) in Ok () in
      map_result (validate_content (context ^ " tool_result content")) content |> Result.map (fun _ -> ())
  | Provider_state state ->
      validate_provider_state (context ^ " provider_state") state
  | Reference { uri; metadata } ->
      let* _ = valid_uri (context ^ " reference uri") uri in
      validate_unique_json (context ^ " reference metadata") (`Assoc metadata)

let validate_request_semantics (request : request) =
  let* _ = nonempty "request operation_key" (Operation_key.to_string request.operation_key) in
  let* _ = nonempty "request model" (Model_selector.to_string request.model) in
  let requested = request.service_class in
  let rec validate_fallbacks seen = function
    | [] -> Ok ()
    | fallback :: rest ->
        if fallback = requested then Error (codec_error "request service_class_fallbacks repeats service_class")
        else if List.mem fallback seen then Error (codec_error "request service_class_fallbacks contains duplicates")
        else validate_fallbacks (fallback :: seen) rest
  in
  let* () = validate_fallbacks [] request.service_class_fallbacks in
  let* () = map_result (fun instruction -> match instruction with Text_instruction _ -> Ok () | Parts_instruction { content; _ } -> map_result (validate_content "request instruction") content |> Result.map (fun _ -> ())) request.instructions |> Result.map (fun _ -> ()) in
  let* () = map_result (validate_item "request input") request.input |> Result.map (fun _ -> ()) in
  let* () = map_result (fun tool ->
    let* _ = valid_tool_name "request tool name" (Tool_name.to_string tool.name) in
    let* _ = json_object "tool input_schema" tool.input_schema in
    let* () = validate_unique_json "tool input_schema" tool.input_schema in
    match tool.output_schema with None -> Ok () | Some schema -> let* _ = json_object "tool output_schema" schema in validate_unique_json "tool output_schema" schema)
    request.tools |> Result.map (fun _ -> ()) in
  let* () = match request.tool_policy.choice with Named name -> let* _ = valid_tool_name "request named tool policy" (Tool_name.to_string name) in Ok () | _ -> Ok () in
  let* () = match request.output with
    | None -> Ok ()
    | Some { max_tokens; format } ->
        let* () = match max_tokens with None -> Ok () | Some value when value >= 0 -> Ok () | Some _ -> Error (codec_error "request output max_tokens must not be negative") in
        (match format with Json_schema_format { name; schema; _ } -> let* _ = if name = "" then Ok "" else valid_tool_name "request output schema name" name in validate_unique_json "request output schema" schema | _ -> Ok ())
  in
  let* () = match request.reasoning with None -> Ok () | Some { token_budget; _ } -> match token_budget with None -> Ok () | Some value when value >= 0 -> Ok () | Some _ -> Error (codec_error "request reasoning token_budget must not be negative") in
  let* () = match request.sampling with
    | None -> Ok ()
    | Some sampling ->
        let values = [ sampling.temperature; sampling.top_p; sampling.presence_penalty; sampling.frequency_penalty ] in
        if List.exists (function Some value -> not (Float.is_finite value) | None -> false) values then Error (codec_error "request sampling values must be finite") else Ok ()
  in
  let* () =
    match request.continuation with
    | None -> Ok ()
    | Some continuation -> validate_continuation "request continuation" continuation
  in
  Ok ()

let validate_response_semantics (response : response) =
  let* _ = nonempty "response operation_key" (Operation_key.to_string response.operation_key) in
  let* () = map_result (validate_item "response output") response.output |> Result.map (fun _ -> ()) in
  let* () = if response.service.fallback_index < 0 then Error (codec_error "response service fallback_index must not be negative") else Ok () in
  let usage_values = [ response.usage.input_tokens; response.usage.output_tokens; response.usage.reasoning_tokens; response.usage.cache_read_tokens; response.usage.cache_write_tokens ] in
  let* () = match List.find_opt (fun value -> value < 0L) usage_values with None -> Ok () | Some _ -> Error (codec_error "response usage token counts must not be negative") in
  let* () = if response.cost.reserved_microusd < 0L || response.cost.actual_microusd < 0L then Error (codec_error "response cost values must not be negative") else Ok () in
  let* () =
    match response.continuation with
    | None -> Ok ()
    | Some continuation -> validate_continuation "response continuation" continuation
  in
  map_result (fun diagnostic ->
    let* _ = nonempty "response diagnostic code" (Diagnostic_code.to_string diagnostic.code) in
    nonempty "response diagnostic message" diagnostic.message |> Result.map (fun _ -> ())) response.diagnostics |> Result.map (fun _ -> ())

let to_bytes context value =
  try Ok (Bytes.of_string (Yojson.Safe.to_string value)) with
  | Yojson.Json_error message -> Error (codec_error "%s: %s" context message)
  | Invalid_argument message -> Error (codec_error "%s: %s" context message)
  | Failure message -> Error (codec_error "%s: %s" context message)

let encode_request request =
  let* () = validate_request_schema_objects request in
  let* () = validate_request_semantics request in
  let value = `Assoc [ ("api_version", `String api_version); ("request", request_to_json request) ] in
  let* () = validate_unique_json "request payload" value in
  to_bytes "failed to encode request"
    value
let encode_response response =
  let* metadata = response_metadata response in
  let* () = validate_response_semantics response in
  let value = `Assoc [ ("api_version", `String api_version); ("response", response_to_json response); ("metadata", metadata_to_json metadata) ] in
  let* () = validate_unique_json "response payload" value in
  to_bytes "failed to encode response" value

let decode_request bytes =
  parse_json
    (fun value ->
      let* fields =
        closed_fields "llm.generate.v1 request" [ "api_version"; "request" ] value
      in
      let* () = require_version fields in
      let* request = required "llm.generate.v1 request" "request" fields in
      let* request = request_of_json request in
      let* () = validate_request_semantics request in
      Ok request)
    bytes

let decode_response bytes =
  parse_json
    (fun value ->
      let* fields =
        closed_fields "llm.generate.v1 response"
          [ "api_version"; "response"; "metadata" ] value
      in
      let* () = require_version fields in
      let* response = required "llm.generate.v1 response" "response" fields in
      let* metadata = required "llm.generate.v1 response" "metadata" fields in
      let* response = response_of_json response in
      let* () = validate_response_semantics response in
      let* metadata = metadata_of_json metadata in
      let* metadata = response_metadata { response with metadata } in
      Ok { response with metadata })
    bytes
