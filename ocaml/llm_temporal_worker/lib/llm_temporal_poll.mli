type handle = {
  operation_id : string;
  kind : string;
  provider : string;
  endpoint_id : string;
  provider_operation_id : string;
}
(** Low-level submit and one-check poll bindings. Workflow scheduling is left to
    callers; these helpers never loop, sleep, or retry provider submissions. *)

type 'a submission =
  | Ready of 'a
  | Pending of { operation_key : string; handle : handle }

type request = {
  context : Llm_temporal_models.request_context;
  pending : handle;
}

type response =
  | Still_pending of handle
  | Generated of handle * Llm_temporal_models.generate_response
  | Compacted of handle * Llm_temporal_models.compaction_response
  | Failed of handle * string

val encode_generate :
  Llm_temporal_models.generate_response submission ->
  (bytes, Temporal.Error.t) result

val decode_generate :
  bytes ->
  (Llm_temporal_models.generate_response submission, Temporal.Error.t) result

val encode_compact :
  Llm_temporal_models.compaction_response submission ->
  (bytes, Temporal.Error.t) result

val decode_compact :
  bytes ->
  (Llm_temporal_models.compaction_response submission, Temporal.Error.t) result

val encode_request : request -> (bytes, Temporal.Error.t) result
val decode_request : bytes -> (request, Temporal.Error.t) result
val encode_response : response -> (bytes, Temporal.Error.t) result
val decode_response : bytes -> (response, Temporal.Error.t) result

val generate_activity :
  ( Llm_temporal_models.generate_request,
    Llm_temporal_models.generate_response submission )
  Temporal.Activity.t

val compact_activity :
  ( Llm_temporal_models.compact_request,
    Llm_temporal_models.compaction_response submission )
  Temporal.Activity.t

val activity : (request, response) Temporal.Activity.t
