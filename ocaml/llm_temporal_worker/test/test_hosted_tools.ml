open Llm_temporal
let ok = function Ok value -> value | Error error -> failwith (Temporal.Error.message error)
let context = match Context.make ~tenant:"tenant" ~project:"project" ~actor:"actor" with Ok v -> v | Error e -> failwith e
let operation_key = Operation_key.of_string "hosted-flags"
let () =
  let request = ok (Generate.make_checked ~operation_key ~context ~model:(Model_selector.of_string "model")
    ~settings:(Settings.make ~web_search:true ~web_fetch:true ~code_execution:true ()) ~input:[Item.human "calculate"] ()) in
  if request.settings_patch.code_execution <> Set true || request.settings_patch.web_fetch <> Set true || request.settings_patch.web_search <> Set true then failwith "flags were lost";
  let roundtrip = ok (V1_codec.encode_generate_request request) |> V1_codec.decode_generate_request |> ok in
  if roundtrip.settings_patch <> request.settings_patch then failwith "wire roundtrip changed flags";
  let root = Conversation.root ~context ~model:(Model_selector.of_string "model") ~settings:(Settings.make ~code_execution:true ()) () in
  let disabled = Settings.Patch.keep |> Settings.Patch.set_code_execution false |> Settings.Patch.clear_web_fetch in
  let request = Conversation.to_request ~operation_key ~settings_patch:disabled ~append:[] root in
  if request.settings_patch.code_execution <> Set false || request.settings_patch.web_fetch <> Clear then failwith "off and clear were lost";
  let invalid = { context with actor = None } in
  (match Generate.make_checked ~operation_key ~context:invalid ~model:(Model_selector.of_string "model") ~input:[] () with Error _ -> () | Ok _ -> failwith "invalid context accepted");
  let policy = match Compaction_policy.make ~target_tokens:256L ~summary_style:Concise () with Ok v -> v | Error e -> failwith e in
  let compact = ok (Compact.make ~operation_key ~context ~parent:(Checkpoint.of_string_exn "ckp_v1.parent") ~policy ()) in
  if compact.policy <> Some { target_tokens = Some 256L; summary_style = Some Concise } then failwith "compaction policy lost";
  let exa = ok (Exa.answer ~operation_key ~context ~model:(Model_selector.of_string "exa-answer") ~include_source_text:false ~question:"What happened?" ()) in
  if exa.settings_patch.extensions <> Set ["exa", `Assoc ["text", `Bool false]] then failwith "Exa source option lost";
  let paused = ok (V1_codec.decode_generate_response (Bytes.of_string {|{"api_version":"llm.temporal/v1","operation_key":"hosted-flags","operation_id":"operation","status":"paused","output":[{"kind":"provider_state","provider":"anthropic","endpoint_family":"messages","media_type":"application/vnd.anthropic.content-block+json","opaque":"eyJ0eXBlIjoic2VydmVyX3Rvb2xfdXNlIiwiaWQiOiJzcnYtMSIsIm5hbWUiOiJiYXNoX2NvZGVfZXhlY3V0aW9uIiwiaW5wdXQiOnsiY29tbWFuZCI6ImVjaG8gNCJ9fQ=="}],"checkpoint":{"handle":"ckp_v1.result","parent":null,"kind":"generation","depth":0},"cache":{"disposition":"disabled","variant":0},"cost":{"status":"unknown","actual_cost_usd":null,"unknown_reason":"provider_did_not_report_cost"},"diagnostics":[]}|})) in
  (match Response.outcome paused with Response.Paused_turn _ -> () | _ -> failwith "pause became a completed answer");
  (match Response.hosted_calls paused with [{tool = Response.Code_execution; id = Some "srv-1"; _}] -> () | _ -> failwith "execution observation not decoded");
  if Response.tool_calls paused <> [] then failwith "hosted call exposed as application work";
  print_endline "hosted tools and checked facades passed"
