type t = Llm_temporal_models.compaction_policy

(** Full policy for Generate settings; see {!make_settings}. *)
type settings = Llm_temporal_models.compaction_settings

(** Checked compaction policy; omission uses worker defaults.

    The worker requires [target_tokens] to be lower than the effective
    [trigger_tokens], which this two-field policy cannot set:
    - As a [Compact.make ~policy] or [Conversation.compact ~policy] override,
      the target is merged into the parent checkpoint's policy. A target at
      or above that checkpoint's trigger (12000 unless a settings policy
      raised it) is accepted here, but the worker fails the compaction with
      [invalid_argument].
    - As [Settings.make ~compaction_policy] or [Settings.Patch.set_compaction]
      it replaces the inherited policy, so the trigger is the worker default
      12000 and a target of 12000 or more is rejected when the Generate
      request is encoded. Use {!make_settings} to raise the trigger. *)
val make : ?target_tokens:int64 -> ?summary_style:Llm_temporal_models.summary_style -> unit -> (t, string) result

(** Checked full policy for [Settings.make ~compaction_settings] and
    [Settings.Patch.set_compaction_settings]. It replaces the inherited
    policy and omitted fields take the worker defaults (trigger_tokens 12000,
    target_tokens 8000), so the effective [target_tokens] must be lower than
    the effective [trigger_tokens]. The other bounds match the worker:
    trigger_tokens 1..10000000, target_tokens 1..9999999, recent_turns
    0..1000, output_reserve_tokens 1..1000000 and
    materialization_threshold_bytes 0..1073741824. *)
val make_settings :
  ?trigger_tokens:int64 -> ?target_tokens:int64 -> ?summary_style:Llm_temporal_models.summary_style ->
  ?recent_turns:int -> ?output_reserve_tokens:int64 -> ?materialization_threshold_bytes:int64 ->
  unit -> (settings, string) result

(** Checks a settings policy built as a record literal as {!make_settings} does. *)
val validate_settings : settings -> (settings, string) result

(** Used by the low-level settings-patch escape hatch. *)
val to_json : t -> Yojson.Safe.t

val settings_to_json : settings -> Yojson.Safe.t

(** Checks the effective target/trigger rule of a settings-patch
    [compaction_policy] object, filling omitted values with the worker
    defaults. Non-integer values are left to the worker. *)
val check_settings_json : Yojson.Safe.t -> (unit, string) result
