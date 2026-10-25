open Llm_temporal_models
let ( let* ) = Result.bind
let error message = Error (Temporal.Error.codec ~message)

module Context = struct
  let make ~tenant ~project ~actor =
    if List.exists (fun s -> s = "" || not (String.is_valid_utf_8 s)) [tenant; project; actor] then
      Error "context identities must be nonempty UTF-8 strings"
    else Ok { tenant = Some (Tenant_id.of_string tenant); project = Some (Project_id.of_string project);
              actor = Some (Actor_id.of_string actor); tags = [] }
end

module Item = struct
  let human text = Message { actor = Human; content = [Text text] }
  let model text = Message { actor = Model; content = [Text text] }
  let checked item =
    let* () = Llm_temporal_codec.validate_item "item" item in Ok item
  let message ~actor content = checked (Message { actor; content })
  let instruction ?(level = Application) text = Text_instruction { level; text }
  let checked_content content =
    let* () = Llm_temporal_codec.validate_content "content" content in
    (* The remote-media policy the worker applies at request ingress (#1676). *)
    match content with
    | Image { source = Url url; _ } | Document { source = Url url; _ } -> (
        match Llm_temporal_media_url.check url with Ok () -> Ok content | Error reason -> error ("content url: " ^ reason))
    | _ -> Ok content
  let image ?detail ~media_type source =
    let* () = match detail with
      | None | Some ("auto" | "low" | "high" | "original") -> Ok ()
      | Some _ -> error "image detail must be auto, low, high or original" in
    checked_content (Image { media_type; source; detail })
  let document ?title ~media_type source = checked_content (Document { media_type; source; title })
  let tool_result ?name ?(is_error = false) ~call_id content =
    checked (Tool_result { call_id; name; content; is_error })
end

module Tool = struct
  let function_ ~name ?(description = "") ~input_schema ?output_schema () =
    let* _ = Llm_temporal_codec.valid_tool_name "tool name" name in
    let tool = { kind = Function; name = Tool_name.of_string name; description; input_schema; output_schema } in
    Llm_temporal_codec.tool_of_json (Llm_temporal_codec.tool_to_json tool)
  let policy ?(choice = Auto) ?(parallel = false) () = { choice; parallel }
end

module Output = struct
  let make ?max_tokens format =
    match max_tokens with
    | Some n when n < 0 -> error "max_tokens must not be negative"
    | _ -> Llm_temporal_codec.output_of_json (Llm_temporal_codec.output_to_json { max_tokens; format })
  let text ?max_tokens () = make ?max_tokens Text_format
  let json ?max_tokens () = make ?max_tokens Json_format
  let json_schema ~name ?description ?(strict = true) ?max_tokens schema =
    if name = "" then error "JSON schema name must not be empty"
    else make ?max_tokens (Json_schema_format { name; description; strict; schema })
end

module Response = struct
  type tool_call = { id : Tool_call_id.t; name : Tool_name.t; arguments : Yojson.Safe.t }
  type hosted_provider = Openai | Anthropic
  type hosted_tool = Web_search | Web_fetch | Code_execution
  type hosted_call = { provider : hosted_provider; tool : hosted_tool; id : string option; payload : Yojson.Safe.t }
  type artifact = { provider : hosted_provider; file_id : string; container_id : string option; filename : string option }
  type outcome = Paused_turn of item list | Answer of item list | Needs_tools of tool_call list
    | Refusal of item list | Truncated of item list | Filtered of item list
  let tool_calls (response : generate_response) =
    List.filter_map (function Tool_call { id; name; arguments } -> Some { id; name; arguments } | _ -> None) response.output
  let references (response : generate_response) =
    List.filter_map (function Reference r -> Some r | _ -> None) response.output
  let field key = function `Assoc fields -> List.assoc_opt key fields | _ -> None
  let string_field key json = match field key json with Some (`String s) when s <> "" -> Some s | _ -> None
  let parsed_states (response : generate_response) =
    List.filter_map (function
      | Provider_state state -> (try Some (state, Yojson.Safe.from_string (Base64.decode_exn state.opaque)) with Yojson.Json_error _ | Invalid_argument _ -> None)
      | _ -> None) response.output
  let hosted_calls response =
    parsed_states response |> List.filter_map (fun ((state : provider_state), payload) ->
      let kind = string_field "type" payload in
      let selected = match Provider_id.to_string state.provider, kind with
        | "openai", Some "web_search_call" -> Some (Openai, Web_search)
        | "openai", Some "code_interpreter_call" -> Some (Openai, Code_execution)
        | "anthropic", Some "server_tool_use" ->
            (match string_field "name" payload with
             | Some "web_search" -> Some (Anthropic, Web_search)
             | Some "web_fetch" -> Some (Anthropic, Web_fetch)
             | Some ("code_execution" | "bash_code_execution" | "text_editor_code_execution") -> Some (Anthropic, Code_execution)
             | _ -> None)
        | _ -> None in
      Option.map (fun (provider, tool) -> { provider; tool; id = string_field "id" payload; payload }) selected)
  let artifacts response =
    let openai = references response |> List.filter_map (fun (r : reference) ->
      match List.assoc_opt "artifact" r.metadata with
      | Some payload -> Option.map (fun file_id -> { provider = Openai; file_id; container_id = string_field "container_id" payload; filename = string_field "filename" payload }) (string_field "file_id" payload)
      | None -> None) in
    let states = parsed_states response in
    let container_id = List.fold_left (fun last ((state : provider_state), payload) ->
      if state.media_type = "application/vnd.anthropic.container+json" then string_field "id" payload else last) None states in
    let rec files payload =
      match payload with
      | `List values -> List.concat_map files values
      | `Assoc fields ->
          let own = match string_field "type" payload, string_field "file_id" payload with
            | Some ("bash_code_execution_output" | "code_execution_output"), Some file_id -> [{ provider = Anthropic; file_id; container_id; filename = string_field "filename" payload }]
            | _ -> [] in
          own @ (match List.assoc_opt "content" fields with Some value -> files value | None -> [])
      | _ -> [] in
    openai @ (states |> List.concat_map (fun ((state : provider_state), payload) -> if Provider_id.to_string state.provider = "anthropic" then files payload else []))
  let outcome (response : generate_response) = match response.status with
    | Completed -> Answer response.output | Tool_calls -> Needs_tools (tool_calls response)
    | Refused -> Refusal response.output | Length -> Truncated response.output
    | Paused -> Paused_turn response.output
    | Content_filtered -> Filtered response.output
  let content (response : generate_response) =
    List.concat_map (function Message { actor = Model; content } -> content | _ -> []) response.output
  let text response = content response |> List.filter_map (function Text s -> Some s | _ -> None) |> String.concat ""
  let json response =
    let* () = Llm_temporal_response_validation.validate_generate_response response in
    if response.status <> Completed then error "structured output requires a completed response"
    else
      let parts = content response in
      let values = List.filter_map (function Json j -> Some j | _ -> None) parts in
      let has_text = List.exists (function Text s -> s <> "" | _ -> false) parts in
      let forbidden = List.exists (function Text _ | Json _ | Content_provider_state _ -> false | _ -> true) parts
        || List.exists (function Tool_call _ -> true | _ -> false) response.output in
      if forbidden then error "structured output contains non-JSON answer content"
      else match values, has_text with
      | [value], false ->
          let* () = Llm_temporal_codec.validate_unique_json "structured output" value in Ok value
      | [], true ->
          (try
             let value = Yojson.Safe.from_string (text response) in
             let* () = Llm_temporal_codec.validate_unique_json "structured output" value in Ok value
           with Yojson.Json_error message -> error ("invalid structured output JSON: " ^ message))
      | _ -> error "structured output requires one JSON value or one text body"
  let decode_json ~decode response =
    let* value = json response in
    match decode value with Ok v -> Ok v | Error message -> error ("structured output: " ^ message)
end

module Failure = struct
  type kind = Invalid_argument | Authentication | Budget_wait | Provider_transient
    | Ambiguous_dispatch | Operation_conflict | State_corrupt | Internal
    | Provider_error | Provider_rejected | Incomplete_response | Other of string option
  type dispatch = Not_dispatched | Rejected | Accepted | Ambiguous
  type execution_details = { error_code : string option; dispatch : dispatch option; provider_status : int option }
  type safe_details = {
    code : string; phase : string; dispatch : dispatch; operation_id : string option;
    retry_after_ms : int option; provider_request_id : string option; reason : string option }

  let activity_kind = function
    | "llm_invalid_argument" -> Some Invalid_argument
    | "llm_authentication" -> Some Authentication
    | "llm_budget_wait" -> Some Budget_wait
    | "llm_provider_transient" -> Some Provider_transient
    | "llm_ambiguous_dispatch" -> Some Ambiguous_dispatch
    | "llm_operation_conflict" -> Some Operation_conflict
    | "llm_state_corrupt" -> Some State_corrupt
    | "llm_internal" -> Some Internal
    | _ -> None
  let is_execution_type = function
    | "provider_error" | "provider_rejected" | "incomplete_response" -> true
    | _ -> false

  let dispatch_of_string = function
    | "not_dispatched" -> Ok Not_dispatched | "rejected" -> Ok Rejected
    | "accepted" -> Ok Accepted | "ambiguous" -> Ok Ambiguous
    | _ -> error "failure details dispatch is not a known certainty"
  let optional_string name fields = match List.assoc_opt name fields with
    | None -> Ok None
    | Some (`String value) when value <> "" -> Ok (Some value)
    | Some _ -> error ("failure details " ^ name ^ " must be a nonempty string")
  let required_string name fields =
    let* value = optional_string name fields in
    match value with Some value -> Ok value | None -> error ("failure details require " ^ name)

  (* Go encodes each detail struct as one JSON object payload. The first payload
     belongs to the outermost application failure, the one [error_type]
     reports. Unknown fields are ignored so a newer worker can add facts. *)
  let decode_details failure decode =
    match (Temporal.Error.view failure).details with
    | [] -> Ok None
    | payload :: _ ->
        match Temporal.Codec.decode Temporal.Codec.json payload with
        | Ok (`Assoc fields) ->
            let names = List.map fst fields in
            if List.length (List.sort_uniq String.compare names) <> List.length names then
              error "failure details contain duplicate fields"
            else Result.map Option.some (decode fields)
        | Ok _ | Error _ -> error "failure details are not a JSON object payload"

  let execution_details failure =
    match Temporal.Error.error_type failure with
    | Some error_type when is_execution_type error_type ->
        decode_details failure (fun fields ->
          let* error_code = optional_string "error_code" fields in
          let* dispatch = optional_string "dispatch" fields in
          let* dispatch = match dispatch with
            | None -> Ok None
            | Some value -> Result.map Option.some (dispatch_of_string value) in
          let* provider_status = match List.assoc_opt "provider_status" fields with
            | None -> Ok None
            | Some (`Int value) when value >= 100 && value <= 599 -> Ok (Some value)
            | Some _ -> error "failure details provider_status must be an HTTP status" in
          Ok { error_code; dispatch; provider_status })
    | _ -> Ok None

  let safe_details failure =
    match Option.bind (Temporal.Error.error_type failure) activity_kind with
    | None -> Ok None
    | Some _ ->
        decode_details failure (fun fields ->
          let* code = required_string "code" fields in
          let* phase = required_string "phase" fields in
          let* dispatch = required_string "dispatch" fields in
          let* dispatch = dispatch_of_string dispatch in
          let* operation_id = optional_string "operation_id" fields in
          let* provider_request_id = optional_string "provider_request_id" fields in
          let* reason = optional_string "reason" fields in
          let* retry_after_ms = match List.assoc_opt "retry_after_ms" fields with
            | None -> Ok None
            | Some (`Int value) when value > 0 -> Ok (Some value)
            | Some _ -> error "failure details retry_after_ms must be a positive integer" in
          Ok { code; phase; dispatch; operation_id; retry_after_ms; provider_request_id; reason })

  let dispatch failure =
    match execution_details failure, safe_details failure with
    | Ok (Some { dispatch; _ }), _ -> dispatch
    | _, Ok (Some { dispatch; _ }) -> Some dispatch
    | _ -> None

  (* A public workflow failure keeps the provider's error code and dispatch in
     its details. Only a definitely unaccepted call is [Authentication], so a
     caller never treats a possibly charged call as safe to resubmit. *)
  let provider_kind failure fallback =
    match execution_details failure with
    | Ok (Some { dispatch = Some Ambiguous; _ }) -> Ambiguous_dispatch
    | Ok (Some { error_code = Some ("authentication" | "permission_denied");
                 dispatch = Some (Not_dispatched | Rejected); _ }) -> Authentication
    | Ok _ | Error _ -> fallback

  let kind failure = match Temporal.Error.error_type failure with
    | Some "provider_error" -> provider_kind failure Provider_error
    | Some "provider_rejected" -> provider_kind failure Provider_rejected
    | Some "incomplete_response" -> Incomplete_response
    | Some error_type as other ->
        (match activity_kind error_type with Some kind -> kind | None -> Other other)
    | None -> Other None
end
