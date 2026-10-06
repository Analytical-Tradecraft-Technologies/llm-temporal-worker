type t = Llm_temporal_models.compaction_policy

(** Checked compaction policy; omission uses worker defaults. *)
val make : ?target_tokens:int64 -> ?summary_style:Llm_temporal_models.summary_style -> unit -> (t, string) result

(** Used by the low-level settings-patch escape hatch. *)
val to_json : t -> Yojson.Safe.t
