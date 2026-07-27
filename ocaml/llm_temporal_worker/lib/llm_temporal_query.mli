(** Typed query Activities.

    The GADT associates each wire filter with exactly one result page.  This
    keeps a provider-status result from being accidentally consumed as a
    budget or spend result while retaining the closed wire representation in
    {!Llm_temporal_models}.
*)

open Llm_temporal_models

module Filter : sig
  (** Validated builders for the five query filters.  Page sizes are bounded
      to 1..1000, refresh ages to 1..86400 seconds, cursors to their query
      kind when tagged, and spend intervals/dimensions are checked before the
      returned filter is wrapped in a GADT constructor. *)
  val provider_status :
    ?provider:Provider_id.t ->
    ?endpoint:Endpoint_id.t ->
    ?availability:availability ->
    ?include_healthy:bool ->
    ?refresh_if_older_than_seconds:int64 ->
    ?page_size:int ->
    ?cursor:Query_cursor.t ->
    unit -> (provider_status_filter, validation_error) result

  val model_inventory :
    ?provider:Provider_id.t ->
    ?endpoint:Endpoint_id.t ->
    ?model_prefix:Model_prefix.t ->
    ?lifecycle:model_lifecycle ->
    ?refresh_if_older_than_seconds:int64 ->
    ?page_size:int ->
    ?cursor:Query_cursor.t ->
    unit -> (model_inventory_filter, validation_error) result

  val credit_status :
    ?provider:Provider_id.t ->
    ?endpoint:Endpoint_id.t ->
    ?include_ok:bool ->
    ?refresh_if_older_than_seconds:int64 ->
    ?page_size:int ->
    ?cursor:Query_cursor.t ->
    unit -> (credit_status_filter, validation_error) result

  val budget_status :
    ?policy_key:Budget_policy_key.t ->
    ?active_at:Ptime.t ->
    ?include_windows:bool ->
    unit -> (budget_status_filter, validation_error) result

  val spend_summary :
    start_time:Ptime.t ->
    end_time:Ptime.t ->
    ?group_by:spend_group_by list ->
    ?operation_kinds:operation_kind list ->
    unit -> (spend_summary_filter, validation_error) result
end

type _ t =
  | Provider_status : provider_status_filter -> provider_status_page t
  | Model_inventory : model_inventory_filter -> model_inventory_page t
  | Credit_status : credit_status_filter -> credit_status_page t
  | Budget_status : budget_status_filter -> budget_status t
  | Spend_summary : spend_summary_filter -> spend_summary t

type 'a response = {
  value : 'a;
  query_execution_id : Query_execution_id.t;
  observed_at : Ptime.t;
  source : query_source;
  freshness : freshness;
  complete : bool;
  next_cursor : Query_cursor.t option;
  cost : settled_cost;
}

val to_envelope :
  operation_key:Operation_key.t ->
  context:request_context ->
  'a t -> query_envelope

val of_response :
  'a t -> query_response -> ('a response, Temporal.Error.t) result

(** Build the next page from a successful response without losing the GADT's
    result type.  Snapshot queries return [Ok None]; paginated queries return
    [Ok (Some query)] when the worker supplies a cursor.  Cursor kind and
    snapshot invariants are checked again so callers using a custom
    dispatcher cannot bypass the protocol boundary.  The worker may mark a
    response complete while still returning a cursor; cursor presence, not the
    completion flag, determines whether another page is available. *)
val next : 'a t -> 'a response -> ('a t option, Temporal.Error.t) result

type dispatcher =
  ?task_queue:Temporal_task_queue.t ->
  (query_envelope, query_response) Temporal.Activity.t ->
  query_envelope -> (query_response, Temporal.Error.t) result

val execute_with :
  ?task_queue:Temporal_task_queue.t ->
  dispatch:dispatcher ->
  operation_key:Operation_key.t ->
  context:request_context ->
  'a t -> ('a response, Temporal.Error.t) result

val execute :
  ?task_queue:Temporal_task_queue.t ->
  operation_key:Operation_key.t ->
  context:request_context ->
  'a t -> ('a response, Temporal.Error.t) result

val start :
  ?task_queue:Temporal_task_queue.t ->
  operation_key:Operation_key.t ->
  context:request_context ->
  'a t ->
  (('a response, Temporal.Error.t) result, Temporal.Error.t) Temporal.Future.t

(** [start] keeps response validation in the successful value channel.  In
    addition to result-tag and cursor checks, the returned operation key must
    equal the requested key; a mismatch is returned as [Error] without
    raising in a workflow callback. *)
