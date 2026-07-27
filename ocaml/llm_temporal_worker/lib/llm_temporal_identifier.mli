(** Nominal wrappers for protocol identifiers.  Values remain JSON strings on
    the wire, but distinct OCaml types prevent unrelated identifiers from
    being interchanged accidentally. *)

module type S = sig
  type t = private string

  val of_string : string -> t
  val to_string : t -> string
end

module Operation_key : S
module Operation_id : S
module Model_selector : S
module Resolved_model_id : S
module Endpoint_id : S
module Route_id : S
module Continuation_handle : S
module Provider_id : S
module Endpoint_family : S
module Api_family : S
module Provider_response_id : S
module Provider_request_id : S
module Provider_generation_id : S
module Tool_name : S
module Tool_call_id : S
module Tenant_id : S
module Project_id : S
module Actor_id : S
module Blob_digest : S
module Diagnostic_code : S
module Cost_catalog_version : S
module Safe_code : S
module Temporal_task_queue : S

(** Identifiers introduced by the checkpoint and query protocols.  These are
    deliberately separate nominal types even though each is represented by a
    string on the wire. *)
module Query_execution_id : S
module Budget_policy_key : S
module Budget_generation_id : S
module Provider_model_id : S
module Model_display_name : S
module Model_prefix : S
module Window_key : S

module Checkpoint : sig
  type t = private string
  val of_string : string -> (t, string) result
  val of_string_exn : string -> t
  val to_string : t -> string
end

module Query_cursor : sig
  type kind =
    | Provider_status
    | Model_inventory
    | Credit_status
    | Budget_status
    | Spend_summary

  type t = private { value : string; kind : kind option }

  val kind_to_string : kind -> string
  val kind_of_string : string -> (kind, string) result
  val of_string : string -> (t, string) result
  val of_string_for_kind : kind -> string -> (t, string) result
  val of_string_exn : string -> t
  val to_string : t -> string
  val kind : t -> kind option
end

module Budget_stream_id : sig
  type t = private string
  val of_string : string -> (t, string) result
  val of_string_exn : string -> t
  val to_string : t -> string
end

module Sha256_digest : sig
  type t = private string
  val of_hex : string -> (t, string) result
  val of_hex_exn : string -> t
  val to_hex : t -> string
end
