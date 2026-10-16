open Llm_temporal_models

type t = compaction_policy
type settings = compaction_settings

let make ?target_tokens ?summary_style () =
  match target_tokens with
  | Some n when n < 1L || n > 10_000_000L ->
      Error "compaction target_tokens must be between 1 and 10000000"
  | _ -> Ok { target_tokens; summary_style }

(* The worker's [compaction.DefaultPolicy] values for omitted fields. *)
let default_trigger_tokens = 12_000L
let default_target_tokens = 8_000L

let effective_order_error ~target ~trigger =
  Printf.sprintf
    "compaction target_tokens (%Ld) must be lower than trigger_tokens (%Ld); omitted fields take the worker defaults (trigger_tokens 12000, target_tokens 8000)"
    target trigger

(* Mirrors the worker's [compaction.Policy.Validate]. A settings policy
   replaces the inherited one, so once the worker defaults fill the omitted
   fields the effective trigger and target are known locally. *)
let validate_settings (policy : settings) =
  let ( let* ) = Result.bind in
  let within name low high = function
    | Some n when n < low || n > high -> Error (Printf.sprintf "compaction %s must be between %Ld and %Ld" name low high)
    | _ -> Ok () in
  let* () = within "trigger_tokens" 1L 10_000_000L policy.trigger_tokens in
  let* () = within "target_tokens" 1L 9_999_999L policy.target_tokens in
  let* () = within "recent_turns" 0L 1000L (Option.map Int64.of_int policy.recent_turns) in
  let* () = within "output_reserve_tokens" 1L 1_000_000L policy.output_reserve_tokens in
  let* () = within "materialization_threshold_bytes" 0L 1_073_741_824L policy.materialization_threshold_bytes in
  let trigger = Option.value policy.trigger_tokens ~default:default_trigger_tokens in
  let target = Option.value policy.target_tokens ~default:default_target_tokens in
  if target >= trigger then Error (effective_order_error ~target ~trigger) else Ok policy

let make_settings ?trigger_tokens ?target_tokens ?summary_style ?recent_turns ?output_reserve_tokens
    ?materialization_threshold_bytes () =
  validate_settings
    ({ trigger_tokens; target_tokens; summary_style; recent_turns; output_reserve_tokens;
       materialization_threshold_bytes } : settings)

let summary_style_to_json = function
  | Concise -> `String "concise" | Balanced -> `String "balanced" | Detailed -> `String "detailed"

let to_json (policy : t) =
  let fields = match policy.target_tokens with
    | None -> [] | Some n -> ["target_tokens", `Intlit (Int64.to_string n)] in
  let fields = match policy.summary_style with
    | None -> fields
    | Some style -> ("summary_style", summary_style_to_json style) :: fields in
  `Assoc fields

let settings_to_json (policy : settings) =
  let int64 name = Option.map (fun n -> name, `Intlit (Int64.to_string n)) in
  `Assoc (List.filter_map Fun.id [
    Option.map (fun style -> "summary_style", summary_style_to_json style) policy.summary_style;
    int64 "trigger_tokens" policy.trigger_tokens;
    int64 "target_tokens" policy.target_tokens;
    Option.map (fun n -> "recent_turns", `Int n) policy.recent_turns;
    int64 "output_reserve_tokens" policy.output_reserve_tokens;
    int64 "materialization_threshold_bytes" policy.materialization_threshold_bytes;
  ])

let check_settings_json = function
  | `Assoc fields ->
      let tokens name default = match List.assoc_opt name fields with
        | None -> Some default
        | Some (`Int n) -> Some (Int64.of_int n)
        | Some (`Intlit text) -> Int64.of_string_opt text
        | Some _ -> None in
      (match tokens "trigger_tokens" default_trigger_tokens, tokens "target_tokens" default_target_tokens with
       | Some trigger, Some target when target >= trigger -> Error (effective_order_error ~target ~trigger)
       | _ -> Ok ())
  | _ -> Ok ()
