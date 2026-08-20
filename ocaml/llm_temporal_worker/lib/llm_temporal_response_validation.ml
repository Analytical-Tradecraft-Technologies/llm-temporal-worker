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
  validate_generate_checkpoint response.checkpoint

let validate_compaction_response (response : compaction_response) =
  validate_compaction_checkpoint response.checkpoint

let checkpoint_equal left right =
  String.equal (Checkpoint.to_string left) (Checkpoint.to_string right)

let validate_generate_response_for_request (request : generate_request)
    (response : generate_response) =
  match validate_generate_response response with
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
  match validate_compaction_response response with
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
