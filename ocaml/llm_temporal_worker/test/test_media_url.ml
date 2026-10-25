open Llm_temporal

(* fixtures/v1/media-url-policy.tsv is a copy of the Go case list, so the
   OCaml codec refuses exactly the image and document URLs the Go worker
   refuses at request ingress (#1676). *)
let failf format = Printf.ksprintf failwith format

let cases () =
  let path =
    List.find Sys.file_exists
      [ "fixtures/v1/media-url-policy.tsv";
        (match Sys.getenv_opt "DUNE_SOURCEROOT" with
         | None -> ""
         | Some root -> Filename.concat root "ocaml/llm_temporal_worker/test/fixtures/v1/media-url-policy.tsv") ]
  in
  In_channel.with_open_bin path In_channel.input_all
  |> String.split_on_char '\n'
  |> List.filter (fun line -> line <> "" && line.[0] <> '#')
  |> List.map (fun line ->
       match String.index_opt line '\t' with
       | Some tab -> (String.sub line 0 tab, String.sub line (tab + 1) (String.length line - tab - 1))
       | None -> failf "media-url-policy.tsv line is not blocked|allowed<TAB>url: %s" line)

let request_with url =
  Printf.sprintf
    {|{"api_version":"llm.temporal/v1","operation_key":"media-url","context":{"tenant":"acme","project":"claims","actor":"workflow:claim-1"},"append":[{"kind":"message","actor":"human","content":[{"kind":"image","media_type":"image/png","url":%s}]}]}|}
    (Yojson.Safe.to_string (`String url))

let () =
  let cases = cases () in
  if List.length (List.filter (fun (verdict, _) -> verdict = "blocked") cases) < 30 then failwith "too few blocked media URL cases";
  List.iter
    (fun (verdict, url) ->
      let helper = Result.is_ok (Item.image ~media_type:"image/png" (Url url)) in
      let decoded = V1_codec.decode_generate_request (Bytes.of_string (request_with url)) in
      match verdict with
      | "allowed" ->
          if not helper then failf "Item.image refused allowed media URL %s" url;
          (match decoded with
           | Error error -> failf "decode refused allowed media URL %s: %s" url (Temporal.Error.message error)
           | Ok request ->
               if Result.is_error (V1_codec.encode_generate_request request) then failf "encode refused allowed media URL %s" url)
      | "blocked" ->
          if helper then failf "Item.image accepted blocked media URL %s" url;
          (match decoded with
           | Ok _ -> failf "decode accepted blocked media URL %s" url
           | Error error ->
               let message = Temporal.Error.message error in
               if not (String.length message > 0) then failf "empty rejection for %s" url)
      | other -> failf "unknown media URL verdict %s" other)
    cases;
  print_endline "media URL policy tests passed"
