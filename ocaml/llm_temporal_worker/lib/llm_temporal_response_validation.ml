open Llm_temporal_models

let error message = Error (Temporal.Error.codec ~message)

let validate_generate_checkpoint (checkpoint : checkpoint_metadata) =
  match checkpoint.kind with
  | Generation_checkpoint | Cache_replay_checkpoint -> Ok ()
  | Compaction_checkpoint ->
      error "generate response checkpoint must be generation or cache_replay"

let validate_compaction_checkpoint (checkpoint : checkpoint_metadata) =
  match checkpoint.kind, checkpoint.parent with
  | Compaction_checkpoint, Some _ -> Ok ()
  | Compaction_checkpoint, None ->
      error "compact response checkpoint parent is required"
  | (Generation_checkpoint | Cache_replay_checkpoint), _ ->
      error "compact response checkpoint must be compaction"

let validate_generate_response (response : generate_response) =
  match validate_generate_checkpoint response.checkpoint, response.service with
  | Error error, _ -> Error error
  | Ok (), Some { fallback_index; _ } when fallback_index < 0 ->
      error "generate response service fallback_index must not be negative"
  | Ok (), _ -> Ok ()

let validate_compaction_response (response : compaction_response) =
  validate_compaction_checkpoint response.checkpoint

let validate_query_cost = function
  | Exact_cost { actual_cost_usd; method_ = Control_query_zero; _ }
    when Usd_decimal.compare actual_cost_usd Usd_decimal.zero <> 0 ->
      error "query response control_query_zero requires zero actual_cost_usd"
  | Exact_cost _ | Unknown_cost _ -> Ok ()

let checkpoint_equal left right =
  String.equal (Checkpoint.to_string left) (Checkpoint.to_string right)

let validate_identity expected_key actual_key (cache : cache_policy option) variant =
  if expected_key <> actual_key then
    error "workflow response operation key does not match request"
  else if variant <> (match cache with None -> 0l | Some policy -> policy.variant) then
    error "workflow response sample index does not match request"
  else Ok ()

let validate_generate_response_for_request (request : generate_request)
    (response : generate_response) =
  match Result.bind (validate_identity request.operation_key response.operation_key request.cache response.cache.variant)
          (fun () -> validate_generate_response response) with
  | Error error -> Error error
  | Ok () ->
      match request.parent, response.checkpoint.parent with
      | None, None -> Ok ()
      | None, Some _ ->
          error "root generate response checkpoint must not have a parent"
      | Some _, None ->
          error "child generate response checkpoint must have a parent"
      | Some _, Some _ ->
          (* Automatic compaction can replace the request parent with a new
             effective parent. The client can require child lineage to stay
             non-root, but only the durable worker can compare that effective
             parent exactly. *)
          Ok ()

let validate_compaction_response_for_request (request : compact_request)
    (response : compaction_response) =
  match Result.bind (validate_identity request.operation_key response.operation_key request.cache response.cache.variant)
          (fun () -> validate_compaction_response response) with
  | Error error -> Error error
  | Ok () ->
      match response.checkpoint.parent with
      | Some parent when checkpoint_equal parent request.parent -> Ok ()
      | Some _ ->
          error "compact response checkpoint parent does not match request"
      | None ->
          (* The shape validator above rejects this branch; keep the match
             exhaustive without changing that more specific error. *)
          error "compact response checkpoint parent is required"
