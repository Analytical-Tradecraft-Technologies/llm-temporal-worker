(** Typed, one-shot bindings for the Go [llm.generate.v1] Temporal Activity.

    Identifier modules intentionally wrap arbitrary strings nominally.  They
    preserve the wire protocol while preventing unrelated IDs from being
    accidentally interchanged in OCaml code. *)

include module type of Llm_temporal_models
include module type of Llm_temporal_invocation

module Conversation : module type of Llm_temporal_conversation
module Query : module type of Llm_temporal_query
module Generate : module type of Llm_temporal_generate
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
module Compaction_policy : sig
  type t = compaction_policy
end

type tool = function_tool
type output_config = output_spec

module Poll : module type of Llm_temporal_poll
