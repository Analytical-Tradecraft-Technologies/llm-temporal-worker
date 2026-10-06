(** Process-level client for the Go worker's public workflows. This module owns
    its Temporal connection and exposes no cancellation operation. It must not
    be called from deterministic workflow code. *)
open Llm_temporal_models

type t
type 'response handle

val generate_workflow : (generate_request, generate_response) Temporal.Workflow.t
val compact_workflow : (compact_request, compaction_response) Temporal.Workflow.t

val create : ?identity:string -> target_url:string -> namespace:string -> unit -> (t, Temporal.Error.t) result
val shutdown : t -> (unit, Temporal.Error.t) result

(** [id] identifies the logical workflow. Persist [request_id] before starting;
    retry an uncertain start with exactly the same IDs and request. These IDs
    are distinct from [operation_key] (worker idempotency) and [cache.variant]
    (independent sample selection). *)
val start_generate : t -> task_queue:Temporal_task_queue.t -> id:string -> request_id:string -> generate_request -> (generate_response handle, Temporal.Error.t) result
val start_compact : t -> task_queue:Temporal_task_queue.t -> id:string -> request_id:string -> compact_request -> (compaction_response handle, Temporal.Error.t) result

(** Save this identity and the original request to resume waiting after a
    process restart. A resume never starts another workflow or paid request. *)
val execution : 'response handle -> Temporal.Client.execution
val resume_generate : t -> execution:Temporal.Client.execution -> generate_request -> (generate_response handle, Temporal.Error.t) result
val resume_compact : t -> execution:Temporal.Client.execution -> compact_request -> (compaction_response handle, Temporal.Error.t) result

(** Wait for the exact run. Completed payloads must match the original request's
    operation key, sample index, and checkpoint lineage. *)
val wait : 'response handle -> ('response Temporal.Client.terminal_result, Temporal.Error.t) result

(** Wait through continue-as-new successors of this workflow. Workflow failures,
    termination, timeout, and transport/codec failures are returned as errors;
    this helper never restarts the workflow after a failure. *)
val await : 'response handle -> ('response, Temporal.Error.t) result

val query_workflow : (query_envelope, query_response) Temporal.Workflow.t
val start_query : t -> task_queue:Temporal_task_queue.t -> id:string -> request_id:string -> query_envelope -> (query_response handle, Temporal.Error.t) result
val resume_query : t -> execution:Temporal.Client.execution -> query_envelope -> (query_response handle, Temporal.Error.t) result
