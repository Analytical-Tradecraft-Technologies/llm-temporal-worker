open Llm_temporal

let context = match Context.make ~tenant:"tenant" ~project:"project" ~actor:"actor" with Ok v -> v | Error e -> failwith e
let operation_key = Operation_key.of_string "compaction-policy"
let model = Model_selector.of_string "model"
let parent = Checkpoint.of_string_exn "ckp_v1.parent"
let get = function Ok value -> value | Error message -> failwith message
let expect_error name = function Ok _ -> failwith (name ^ " was accepted") | Error _ -> ()

let generate ?settings ?settings_patch () =
  match settings_patch with
  | None -> Result.map_error Temporal.Error.message (Generate.make_checked ~operation_key ~context ~model ?settings ~input:[Item.human "hi"] ())
  | Some settings_patch ->
      let conversation = Conversation.of_checkpoint ~context ~checkpoint:parent in
      let request = Conversation.to_request ~settings_patch ~operation_key ~append:[Item.human "hi"] conversation in
      Result.map (fun _ -> request) (Result.map_error Temporal.Error.message (V1_codec.encode_generate_request request))

let () =
  (* A two-field policy with target_tokens >= 12000 and no trigger. *)
  let high = get (Compaction_policy.make ~target_tokens:12000L ()) in
  (* As a Compact override it merges into the checkpoint's policy, whose
     trigger is unknown here: accepted locally, decided by the worker. *)
  (match Compact.make ~operation_key ~context ~parent ~policy:high () with
   | Ok request when request.policy = Some high -> ()
   | Ok _ -> failwith "compact override changed"
   | Error error -> failwith ("compact override rejected: " ^ Temporal.Error.message error));
  (* As a settings policy it replaces the inherited one, so the trigger is
     the worker default 12000 and the request is refused before scheduling. *)
  expect_error "Settings.make ~compaction_policy target 12000"
    (generate ~settings:(Settings.make ~compaction_policy:high ()) ());
  expect_error "Patch.set_compaction target 12000"
    (generate ~settings_patch:(Settings.Patch.set_compaction high Settings.Patch.keep) ());
  expect_error "escape hatch target 12000"
    (generate ~settings_patch:(Settings.Patch.set_compaction_policy (`Assoc ["target_tokens", `Int 12000]) Settings.Patch.keep) ());
  expect_error "escape hatch trigger below default target"
    (generate ~settings_patch:(Settings.Patch.set_compaction_policy (`Assoc ["trigger_tokens", `Int 5000]) Settings.Patch.keep) ());
  ignore (get (generate ~settings_patch:(Settings.Patch.set_compaction_policy
    (`Assoc ["trigger_tokens", `Int 50000; "target_tokens", `Int 40000]) Settings.Patch.keep) ()));
  ignore (get (generate ~settings:(Settings.make ~compaction_policy:(get (Compaction_policy.make ~target_tokens:11999L ())) ()) ()));

  (* The full settings policy can raise the trigger. *)
  let full = get (Compaction_policy.make_settings ~trigger_tokens:50000L ~target_tokens:40000L ~summary_style:Detailed
    ~recent_turns:1 ~output_reserve_tokens:4000L ~materialization_threshold_bytes:1048576L ()) in
  let expected = `Assoc ["summary_style", `String "detailed"; "trigger_tokens", `Intlit "50000"; "target_tokens", `Intlit "40000";
    "recent_turns", `Int 1; "output_reserve_tokens", `Intlit "4000"; "materialization_threshold_bytes", `Intlit "1048576"] in
  if Compaction_policy.settings_to_json full <> expected then failwith "settings policy JSON changed";
  let request = get (generate ~settings:(Settings.make ~compaction_settings:full ()) ()) in
  if request.settings_patch.compaction_policy <> Set expected then failwith "Settings.make lost the settings policy";
  let wire = match V1_codec.encode_generate_request request with Ok bytes -> Yojson.Safe.from_string (Bytes.to_string bytes) | Error e -> failwith (Temporal.Error.message e) in
  (match Yojson.Safe.Util.(wire |> member "settings_patch" |> member "compaction_policy" |> member "set" |> member "trigger_tokens") with
   | `Int 50000 | `Intlit "50000" -> ()
   | _ -> failwith "trigger_tokens missing from the wire");
  let patched = get (generate ~settings_patch:(Settings.Patch.set_compaction_settings full Settings.Patch.keep) ()) in
  if patched.settings_patch.compaction_policy <> Set expected then failwith "Patch.set_compaction_settings lost the policy";
  ignore (get (Compaction_policy.make_settings ~trigger_tokens:20000L ()));

  (* The builder rejects a target at or above the effective trigger. *)
  expect_error "settings target 12000 with default trigger" (Compaction_policy.make_settings ~target_tokens:12000L ());
  expect_error "settings trigger below default target" (Compaction_policy.make_settings ~trigger_tokens:8000L ());
  expect_error "settings target equal to trigger" (Compaction_policy.make_settings ~trigger_tokens:30000L ~target_tokens:30000L ());
  expect_error "settings target above trigger" (Compaction_policy.make_settings ~trigger_tokens:30000L ~target_tokens:30001L ());
  expect_error "settings trigger out of bounds" (Compaction_policy.make_settings ~trigger_tokens:10_000_001L ());
  expect_error "settings recent_turns out of bounds" (Compaction_policy.make_settings ~recent_turns:1001 ());
  expect_error "settings output reserve out of bounds" (Compaction_policy.make_settings ~output_reserve_tokens:0L ());
  expect_error "settings materialization threshold out of bounds"
    (Compaction_policy.make_settings ~materialization_threshold_bytes:1_073_741_825L ());
  expect_error "record literal"
    (Compaction_policy.validate_settings { trigger_tokens = None; target_tokens = Some 12000L; summary_style = None;
      recent_turns = None; output_reserve_tokens = None; materialization_threshold_bytes = None });
  (match Settings.make ~compaction_policy:high ~compaction_settings:full () with
   | exception Invalid_argument _ -> ()
   | _ -> failwith "both compaction arguments accepted");

  (* Two-field record literals still resolve to the Compact policy type. *)
  let literal = { target_tokens = Some 64L; summary_style = Some Concise } in
  ignore (Compact.make ~operation_key ~context ~parent ~policy:literal ());
  print_endline "compaction policy checks passed"
