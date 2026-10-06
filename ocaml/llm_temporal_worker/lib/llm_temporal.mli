(** Typed clients and protocol bindings for the Go worker.
    [Client] invokes the public generation and compaction workflows from an
    application process; the other invocation helpers are workflow-native.

    Identifier modules intentionally wrap arbitrary strings nominally.  They
    preserve the wire protocol while preventing unrelated IDs from being
    accidentally interchanged in OCaml code. *)

include module type of Llm_temporal_models
include module type of Llm_temporal_invocation

module Conversation : module type of Llm_temporal_conversation
module Query : module type of Llm_temporal_query
module Generate : module type of Llm_temporal_generate
module Client : module type of Llm_temporal_workflow_client
module V1_codec : module type of Llm_temporal_v1_codec

(** The ergonomic settings and cache modules are also available at the
    package root.  Keeping these aliases next to [Conversation] lets a
    workflow open [Llm_temporal] and use the names from the public design
    without exposing the implementation module layout. *)
module Settings : module type of Conversation.Settings
  with type t = Conversation.Settings.t
module Cache_policy : module type of Conversation.Cache_policy
  with type t = Conversation.Cache_policy.t

(** Short names used by the immutable-conversation examples. *)
module Decimal : module type of Usd_decimal
  with type t = Usd_decimal.t
module Compaction_policy : module type of Llm_temporal_compaction_policy

type tool = function_tool
type output_config = output_spec

module Context : module type of Llm_temporal_helpers.Context

module Item : module type of Llm_temporal_helpers.Item

module Tool : module type of Llm_temporal_helpers.Tool

module Output : module type of Llm_temporal_helpers.Output

module Response : module type of Llm_temporal_helpers.Response

module Failure : module type of Llm_temporal_helpers.Failure

module Compact : module type of Llm_temporal_compact

module Exa : module type of Llm_temporal_exa
