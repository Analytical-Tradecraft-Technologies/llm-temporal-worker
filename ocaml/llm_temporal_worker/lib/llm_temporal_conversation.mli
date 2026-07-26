(** Immutable, typed helpers over the v1 Generate and Compact Activities. *)

open Llm_temporal_models

module Settings : sig
  type t

  (** Settings are the locally known effective values used to build the first
      sparse Generate patch. Every field maps to its exact v1 wire type; an
      omitted optional value remains [Keep] so inherited worker state is not
      accidentally cleared. *)
  val make :
    ?service_class:service_class ->
    ?service_class_fallbacks:service_class list ->
    ?portability:portability ->
    ?instructions:instruction list ->
    ?tools:function_tool list ->
    ?tool_policy:tool_policy ->
    ?output:output_spec ->
    ?temperature:Usd_decimal.t ->
    ?reasoning_effort:reasoning_effort ->
    ?reasoning_summary:reasoning_summary ->
    ?extensions:(string * Yojson.Safe.t) list ->
    unit -> t

  val default : t

  module Patch : sig
    type t
    val keep : t
    val set_model : Model_selector.t -> t -> t
    val clear_model : t -> t
    val set_service_class : service_class -> t -> t
    val clear_service_class : t -> t
    val set_service_class_fallbacks : service_class list -> t -> t
    val clear_service_class_fallbacks : t -> t
    val set_portability : portability -> t -> t
    val clear_portability : t -> t
    val set_instructions : instruction list -> t -> t
    val clear_instructions : t -> t
    val replace_tools : function_tool list -> t -> t
    val clear_tools : t -> t
    val set_tool_policy : tool_policy -> t -> t
    val clear_tool_policy : t -> t
    val replace_output : output_spec -> t -> t
    val clear_output : t -> t
    val set_temperature : Usd_decimal.t -> t -> t
    val clear_temperature : t -> t
    val set_reasoning_effort : reasoning_effort -> t -> t
    val clear_reasoning_effort : t -> t
    val set_reasoning_summary : reasoning_summary -> t -> t
    val clear_reasoning_summary : t -> t
    val set_compaction_policy : Yojson.Safe.t -> t -> t
    val clear_compaction_policy : t -> t
    val replace_extensions : (string * Yojson.Safe.t) list -> t -> t
    val clear_extensions : t -> t
  end
end

module Cache_policy : sig
  type t
  val accept_up_to : max_age_seconds:Int64.t -> ?variant:Int32.t -> unit -> (t, validation_error) result
  val max_age_seconds : t -> Int64.t
  val variant : t -> Int32.t
end

type t
type turn = { response : generate_response; conversation : t }

val root :
  context:request_context -> model:Model_selector.t -> ?service_class:service_class -> ?settings:Settings.t -> unit -> t
val of_checkpoint : context:request_context -> checkpoint:Checkpoint.t -> t
val context : t -> request_context
val model : t -> Model_selector.t option
val checkpoint : t -> Checkpoint.t option
val fork : t -> t

val to_request :
  ?settings_patch:Settings.Patch.t -> ?cache:Cache_policy.t ->
  operation_key:Operation_key.t -> append:item list -> t -> generate_request

type dispatcher =
  ?task_queue:Temporal_task_queue.t ->
  (generate_request, generate_response) Temporal.Activity.t ->
  generate_request -> (generate_response, Temporal.Error.t) result

val respond_with :
  ?task_queue:Temporal_task_queue.t -> dispatch:dispatcher ->
  ?settings_patch:Settings.Patch.t -> ?cache:Cache_policy.t ->
  operation_key:Operation_key.t -> append:item list -> t -> (turn, Temporal.Error.t) result
val respond :
  ?task_queue:Temporal_task_queue.t -> ?settings_patch:Settings.Patch.t -> ?cache:Cache_policy.t ->
  operation_key:Operation_key.t -> append:item list -> t -> (turn, Temporal.Error.t) result
val start_respond :
  ?task_queue:Temporal_task_queue.t -> ?settings_patch:Settings.Patch.t -> ?cache:Cache_policy.t ->
  operation_key:Operation_key.t -> append:item list -> t -> (turn, Temporal.Error.t) Temporal.Future.t

type compact_dispatcher =
  ?task_queue:Temporal_task_queue.t ->
  (compact_request, compaction_response) Temporal.Activity.t ->
  compact_request -> (compaction_response, Temporal.Error.t) result

val compact_with :
  ?task_queue:Temporal_task_queue.t -> dispatch:compact_dispatcher ->
  ?policy:compaction_policy -> ?cache:Cache_policy.t -> operation_key:Operation_key.t -> t ->
  (compaction_response * t, Temporal.Error.t) result
val compact :
  ?task_queue:Temporal_task_queue.t -> ?policy:compaction_policy -> ?cache:Cache_policy.t ->
  operation_key:Operation_key.t -> t -> (compaction_response * t, Temporal.Error.t) result
val start_compact :
  ?task_queue:Temporal_task_queue.t -> ?policy:compaction_policy -> ?cache:Cache_policy.t ->
  operation_key:Operation_key.t -> t ->
  ((compaction_response * t, Temporal.Error.t) result, Temporal.Error.t) Temporal.Future.t
