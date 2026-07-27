module type S = sig
  type t = private string

  val of_string : string -> t
  val to_string : t -> string
end

module Make () : S = struct
  type t = string

  let of_string value = value
  let to_string value = value
end

module Operation_key = Make ()
module Operation_id = Make ()
module Model_selector = Make ()
module Resolved_model_id = Make ()
module Endpoint_id = Make ()
module Route_id = Make ()
module Continuation_handle = Make ()
module Provider_id = Make ()
module Endpoint_family = Make ()
module Api_family = Make ()
module Provider_response_id = Make ()
module Provider_request_id = Make ()
module Provider_generation_id = Make ()
module Tool_name = Make ()
module Tool_call_id = Make ()
module Tenant_id = Make ()
module Project_id = Make ()
module Actor_id = Make ()
module Blob_digest = Make ()
module Diagnostic_code = Make ()
module Cost_catalog_version = Make ()
(* Safe codes are public, bounded diagnostic values in the query contract.
   Keep them distinct from arbitrary strings and from [Diagnostic_code],
   whose domain belongs to Activity diagnostics. *)
module Safe_code = Make ()
module Temporal_task_queue = Make ()
module Make_nonempty () : S = struct
  type t = string
  let of_string value = if value = "" then invalid_arg "identifier must not be empty" else value
  let to_string value = value
end

module Query_execution_id = Make_nonempty ()
module Budget_policy_key = Make_nonempty ()
module Budget_generation_id = Make_nonempty ()
module Provider_model_id = Make_nonempty ()
(* A display name is distinct from the provider's model identifier.  It is
   still an opaque wire string, so keep it nominal to prevent accidentally
   passing an identifier where presentation metadata is expected. *)
module Model_display_name = Make ()
(* A model-inventory prefix is a distinct wire value from a complete model
   identifier.  It intentionally permits the empty string because the Go
   query contract treats an empty prefix as equivalent to omitting the
   filter; callers can still use [None] for the usual unfiltered case. *)
module Model_prefix = Make ()
(* Capability labels are an open control-plane vocabulary, but they are not
   interchangeable with model IDs, diagnostic codes, or arbitrary text.
   Keep the wire string unchanged while giving callers a nominal type. *)
module Model_capability = Make ()
module Window_key = Make_nonempty ()

module Checkpoint = struct
  type t = string
  let of_string value = if value = "" then Error "checkpoint must not be empty" else Ok value
  let of_string_exn value = match of_string value with Ok value -> value | Error message -> invalid_arg message
  let to_string value = value
end

module Query_cursor = struct
  type kind =
    | Provider_status
    | Model_inventory
    | Credit_status
    | Budget_status
    | Spend_summary

  type t = { value : string; kind : kind option }

  let kind_to_string = function
    | Provider_status -> "provider_status"
    | Model_inventory -> "model_inventory"
    | Credit_status -> "credit_status"
    | Budget_status -> "budget_status"
    | Spend_summary -> "spend_summary"

  let kind_of_string = function
    | "provider_status" -> Ok Provider_status
    | "model_inventory" -> Ok Model_inventory
    | "credit_status" -> Ok Credit_status
    | "budget_status" -> Ok Budget_status
    | "spend_summary" -> Ok Spend_summary
    | value -> Error (Printf.sprintf "unsupported query cursor kind %S" value)

  let of_string value =
    if value = "" then Error "query cursor must not be empty"
    else Ok { value; kind = None }

  let of_string_for_kind kind value =
    if value = "" then Error "query cursor must not be empty"
    else Ok { value; kind = Some kind }

  let of_string_exn value = match of_string value with Ok value -> value | Error message -> invalid_arg message
  let to_string value = value.value
  let kind value = value.kind
end

module Budget_stream_id = struct
  type t = string
  let unsigned value = value <> "" && String.for_all (fun c -> c >= '0' && c <= '9') value
  let of_string value =
    match String.split_on_char '-' value with
    | [milliseconds; sequence] when unsigned milliseconds && unsigned sequence -> Ok value
    | _ -> Error "budget stream id must use unsigned milliseconds-sequence spelling"
  let of_string_exn value = match of_string value with Ok value -> value | Error message -> invalid_arg message
  let to_string value = value
end

module Sha256_digest = struct
  type t = string
  let of_hex value =
    if String.length value <> 64 || String.exists (fun c -> not ((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) value then
      Error "sha256 digest must be 64 lowercase hexadecimal characters"
    else Ok value
  let of_hex_exn value = match of_hex value with Ok value -> value | Error message -> invalid_arg message
  let to_hex value = value
end
