open Llm_temporal_models

module Context : sig

  (** A complete v1 context, without constructing optional identity fields. *)
  val make : tenant:string -> project:string -> actor:string -> (request_context, validation_error) result
end

module Item : sig
  val human : string -> item
  val model : string -> item
  val message : actor:actor -> content list -> (item, Temporal.Error.t) result
  val instruction : ?level:instruction_level -> string -> instruction
  val image : ?detail:string -> media_type:string -> media_source -> (content, Temporal.Error.t) result
  val document : ?title:string -> media_type:string -> media_source -> (content, Temporal.Error.t) result
  val tool_result : ?name:Tool_name.t -> ?is_error:bool -> call_id:Tool_call_id.t -> content list -> (item, Temporal.Error.t) result
end

module Tool : sig

  (** Application functions are executed by the application. Hosted tools are
      enabled separately in [Settings]. *)
  val function_ : name:string -> ?description:string -> input_schema:Yojson.Safe.t -> ?output_schema:Yojson.Safe.t -> unit -> (function_tool, Temporal.Error.t) result
  val policy : ?choice:tool_choice -> ?parallel:bool -> unit -> tool_policy
end

module Output : sig
  val text : ?max_tokens:int -> unit -> (output_spec, Temporal.Error.t) result
  val json : ?max_tokens:int -> unit -> (output_spec, Temporal.Error.t) result
  val json_schema : name:string -> ?description:string -> ?strict:bool -> ?max_tokens:int -> Yojson.Safe.t -> (output_spec, Temporal.Error.t) result
end

module Response : sig
  type tool_call = { id : Tool_call_id.t; name : Tool_name.t; arguments : Yojson.Safe.t }
  type hosted_provider = Openai | Anthropic
  type hosted_tool = Web_search | Web_fetch | Code_execution
  type hosted_call = { provider : hosted_provider; tool : hosted_tool; id : string option; payload : Yojson.Safe.t }
  type artifact = { provider : hosted_provider; file_id : string; container_id : string option; filename : string option }

  (** Hosted calls have already run on the provider. These are observations,
      not application functions to execute. *)
  val hosted_calls : generate_response -> hosted_call list

  (** Provider file identities, not public download links. Downloading requires
      provider authentication and must happen before provider expiry. *)
  val artifacts : generate_response -> artifact list
  type outcome =
    | Paused_turn of item list
    | Answer of item list
    | Needs_tools of tool_call list
    | Refusal of item list
    | Truncated of item list
    | Filtered of item list

  val outcome : generate_response -> outcome

  (** Concatenates model text parts in order, without adding separators. *)
  val text : generate_response -> string
  val tool_calls : generate_response -> tool_call list
  val references : generate_response -> reference list

  (** Requires a completed response and exactly one structured JSON value or
      a model text body containing JSON. Refusals, truncation, tool calls,
      ambiguous mixtures and invalid JSON are errors, not decoded answers. *)
  val json : generate_response -> (Yojson.Safe.t, Temporal.Error.t) result
  val decode_json : decode:(Yojson.Safe.t -> ('a, string) result) -> generate_response -> ('a, Temporal.Error.t) result
end

module Failure : sig
  (** [Provider_error], [Provider_rejected] and [Incomplete_response] are the
      terminal failure types of the public workflows; the others before
      [Other] are the [llm_*] Activity failure types. *)
  type kind = Invalid_argument | Authentication | Budget_wait | Provider_transient
    | Ambiguous_dispatch | Operation_conflict | State_corrupt | Internal
    | Provider_error | Provider_rejected | Incomplete_response
    | Other of string option

  (** Whether the provider may have accepted, and charged for, the request.
      [Not_dispatched] and [Rejected] are definite non-acceptance. *)
  type dispatch = Not_dispatched | Rejected | Accepted | Ambiguous

  (** The details of a [provider_error], [provider_rejected] or
      [incomplete_response] public workflow failure. *)
  type execution_details = { error_code : string option; dispatch : dispatch option }

  (** The bounded details of an [llm_*] Activity failure. *)
  type safe_details = {
    code : string; phase : string; dispatch : dispatch; operation_id : string option;
    retry_after_ms : int option; provider_request_id : string option }

  (** Classifies the retained application failure type; never discards the
      SDK error or invents a retry policy for paid work. A provider failure
      whose details report [Ambiguous] dispatch is [Ambiguous_dispatch], and
      one whose error code is [authentication] or [permission_denied] with
      [Not_dispatched] or [Rejected] dispatch is [Authentication], as the
      Activity classification does. An accepted or unknown dispatch keeps the
      [Provider_*] kind. *)
  val kind : Temporal.Error.t -> kind

  (** Decodes a public workflow failure's details. [Ok None] means the failure
      has another type or carries no details (it was saved before they
      existed); malformed details are an error. *)
  val execution_details : Temporal.Error.t -> (execution_details option, Temporal.Error.t) result

  (** Decodes an [llm_*] Activity failure's details, like {!execution_details}. *)
  val safe_details : Temporal.Error.t -> (safe_details option, Temporal.Error.t) result

  (** The dispatch certainty from either detail shape. [None] means it is
      absent or malformed, which callers must treat as possibly dispatched. *)
  val dispatch : Temporal.Error.t -> dispatch option
end
