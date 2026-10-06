open Llm_temporal_models

type t = compaction_policy

let make ?target_tokens ?summary_style () =
  match target_tokens with
  | Some n when n < 1L || n > 10_000_000L ->
      Error "compaction target_tokens must be between 1 and 10000000"
  | _ -> Ok { target_tokens; summary_style }

let to_json (policy : t) =
  let fields = match policy.target_tokens with
    | None -> [] | Some n -> ["target_tokens", `Intlit (Int64.to_string n)] in
  let fields = match policy.summary_style with
    | None -> fields
    | Some style ->
        let text = match style with Concise -> "concise" | Balanced -> "balanced" | Detailed -> "detailed" in
        ("summary_style", `String text) :: fields in
  `Assoc fields
