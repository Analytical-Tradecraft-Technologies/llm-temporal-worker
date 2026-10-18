open Llm_temporal

(* Detail payloads exactly as the Go SDK encodes ExecutionFailureDetails
   (golang/workflows/internal.go) and SafeErrorDetails
   (golang/activity/errors.go); TestFailureDetailsWireShape pins the Go side. *)
let go_payload json : Temporal.Payload.t =
  { metadata = [ ("encoding", "json/plain") ]; data = Bytes.of_string json }

let failure ?details error_type =
  let details = Option.to_list (Option.map go_payload details) in
  Temporal.Error.make ~non_retryable:true ~error_type ~details ~category:`Workflow
    ~message:"model request failed" ()

let expect name condition = if not condition then failwith name

let kind_name = function
  | Failure.Invalid_argument -> "Invalid_argument" | Authentication -> "Authentication"
  | Budget_wait -> "Budget_wait" | Provider_transient -> "Provider_transient"
  | Ambiguous_dispatch -> "Ambiguous_dispatch" | Operation_conflict -> "Operation_conflict"
  | State_corrupt -> "State_corrupt" | Internal -> "Internal"
  | Provider_error -> "Provider_error" | Provider_rejected -> "Provider_rejected"
  | Incomplete_response -> "Incomplete_response"
  | Other None -> "Other None" | Other (Some value) -> "Other " ^ value

let expect_kind name expected error =
  let actual = Failure.kind error in
  if actual <> expected then
    failwith (Printf.sprintf "%s: kind %s, want %s" name (kind_name actual) (kind_name expected))

let expect_execution name expected error =
  match Failure.execution_details error with
  | Ok actual when actual = expected -> ()
  | Ok _ -> failwith (name ^ ": unexpected execution details")
  | Error error -> failwith (name ^ ": " ^ Temporal.Error.message error)

let expect_safe name expected error =
  match Failure.safe_details error with
  | Ok actual when actual = expected -> ()
  | Ok _ -> failwith (name ^ ": unexpected safe details")
  | Error error -> failwith (name ^ ": " ^ Temporal.Error.message error)

let () =
  (* Public workflow failures. *)
  let rejected_auth =
    failure "provider_rejected" ~details:{|{"error_code":"permission_denied","dispatch":"rejected"}|} in
  expect_kind "permission denied" Authentication rejected_auth;
  expect_execution "permission denied"
    (Some { error_code = Some "permission_denied"; dispatch = Some Rejected; provider_status = None }) rejected_auth;
  expect "permission denied dispatch" (Failure.dispatch rejected_auth = Some Rejected);
  expect_kind "authentication" Authentication
    (failure "provider_error" ~details:{|{"error_code":"authentication","dispatch":"not_dispatched"}|});
  let ambiguous =
    failure "provider_error" ~details:{|{"error_code":"provider_rate_limited","dispatch":"ambiguous"}|} in
  expect_kind "ambiguous" Ambiguous_dispatch ambiguous;
  expect "ambiguous dispatch" (Failure.dispatch ambiguous = Some Ambiguous);
  (* Possible acceptance outranks an authentication error code. *)
  expect_kind "ambiguous auth" Ambiguous_dispatch
    (failure "provider_error" ~details:{|{"error_code":"permission_denied","dispatch":"ambiguous"}|});
  (* An accepted or unknown dispatch may have been charged, so it is not
     reported as an authentication-only failure. *)
  expect_kind "accepted auth" Provider_error
    (failure "provider_error" ~details:{|{"error_code":"authentication","dispatch":"accepted"}|});
  expect_kind "auth without dispatch" Provider_error
    (failure "provider_error" ~details:{|{"error_code":"permission_denied"}|});
  let rate_limited =
    failure "provider_rejected" ~details:{|{"error_code":"rate_limited","dispatch":"rejected"}|} in
  expect_kind "rate limited" Provider_rejected rate_limited;
  let accepted =
    failure "provider_error" ~details:{|{"error_code":"provider_invalid_response","dispatch":"accepted"}|} in
  expect_kind "accepted" Provider_error accepted;
  expect "accepted dispatch" (Failure.dispatch accepted = Some Accepted);
  let incomplete = failure "incomplete_response" in
  expect_kind "incomplete" Incomplete_response incomplete;
  expect_execution "incomplete" None incomplete;
  expect "incomplete dispatch" (Failure.dispatch incomplete = None);
  let legacy = failure "provider_error" in
  expect_kind "legacy" Provider_error legacy;
  expect_execution "legacy" None legacy;
  expect_execution "code only" (Some { error_code = Some "internal"; dispatch = None; provider_status = None })
    (failure "provider_error" ~details:{|{"error_code":"internal"}|});
  expect_execution "upstream status"
    (Some { error_code = Some "provider_unavailable"; dispatch = Some Accepted; provider_status = Some 502 })
    (failure "provider_error" ~details:{|{"error_code":"provider_unavailable","dispatch":"accepted","provider_status":502}|});
  (* Activity failures. *)
  let activity_auth =
    failure "llm_authentication"
      ~details:{|{"operation_id":"op-1","code":"permission_denied","phase":"dispatch","dispatch":"rejected","provider_request_id":"req-1"}|} in
  expect_kind "activity auth" Authentication activity_auth;
  expect_safe "activity auth"
    (Some { code = "permission_denied"; phase = "dispatch"; dispatch = Rejected;
            operation_id = Some "op-1"; retry_after_ms = None; provider_request_id = Some "req-1"; reason = None })
    activity_auth;
  expect_execution "activity has no execution details" None activity_auth;
  expect "activity auth dispatch" (Failure.dispatch activity_auth = Some Rejected);
  let transient =
    failure "llm_provider_transient"
      ~details:{|{"code":"provider_rate_limited","phase":"dispatch","dispatch":"not_dispatched","retry_after_ms":1500}|} in
  expect_kind "transient" Provider_transient transient;
  expect_safe "transient"
    (Some { code = "provider_rate_limited"; phase = "dispatch"; dispatch = Not_dispatched;
            operation_id = None; retry_after_ms = Some 1500; provider_request_id = None; reason = None })
    transient;
  expect_safe "decode"
    (Some { code = "invalid_argument"; phase = "decode"; dispatch = Not_dispatched;
            operation_id = None; retry_after_ms = None; provider_request_id = None; reason = None })
    (failure "llm_invalid_argument" ~details:{|{"code":"invalid_argument","phase":"decode","dispatch":"not_dispatched"}|});
  expect_safe "planning reason"
    (Some { code = "no_route"; phase = "plan"; dispatch = Not_dispatched;
            operation_id = None; retry_after_ms = None; provider_request_id = None; reason = Some "model_not_in_catalog" })
    (failure "llm_invalid_argument" ~details:{|{"code":"no_route","phase":"plan","dispatch":"not_dispatched","reason":"model_not_in_catalog"}|});
  expect_safe "state corrupt without details" None (failure "llm_state_corrupt");
  expect_safe "workflow failure has no safe details" None rejected_auth;
  (* Malformed details are errors and never upgrade or downgrade the kind. *)
  List.iter (fun (name, error) ->
    (match Failure.execution_details error, Failure.safe_details error with
     | Error _, _ | _, Error _ -> ()
     | Ok _, Ok _ -> failwith (name ^ ": malformed details decoded"));
    expect (name ^ " dispatch") (Failure.dispatch error = None))
    [ ("unknown dispatch", failure "provider_error" ~details:{|{"error_code":"internal","dispatch":"maybe"}|});
      ("duplicate field", failure "provider_error" ~details:{|{"dispatch":"rejected","dispatch":"ambiguous"}|});
      ("not an object", failure "provider_rejected" ~details:{|["rejected"]|});
      ("empty code", failure "provider_error" ~details:{|{"error_code":"","dispatch":"accepted"}|});
      ("missing phase", failure "llm_internal" ~details:{|{"code":"internal","dispatch":"accepted"}|});
      ("zero retry", failure "llm_provider_transient"
                       ~details:{|{"code":"provider_unavailable","phase":"poll","dispatch":"not_dispatched","retry_after_ms":0}|}) ];
  expect_kind "duplicate field kind" Provider_error
    (failure "provider_error" ~details:{|{"dispatch":"rejected","dispatch":"ambiguous"}|});
  (match Failure.execution_details
           (Temporal.Error.make ~error_type:"provider_error" ~category:`Workflow ~message:"m"
              ~details:[ { metadata = [ ("encoding", "binary/plain") ]; data = Bytes.of_string "{}" } ] ()) with
   | Error _ -> ()
   | Ok _ -> failwith "non-JSON payload decoded");
  expect_kind "unknown type" (Other (Some "custom")) (failure "custom");
  expect_kind "no type" (Other None) (Temporal.Error.codec ~message:"bad");
  print_endline "llm_temporal failure tests passed"
