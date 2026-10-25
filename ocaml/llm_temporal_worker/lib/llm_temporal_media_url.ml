(* The remote-media URL policy the Go worker applies to caller-supplied image
   and document URLs at request ingress (golang/llm/media_url_policy.go and
   golang/internal/netpolicy). The worker never fetches them, but providers do,
   inside their own network, so a URL that addresses a local, private or
   metadata destination, or that carries credentials, is refused. Mirroring it
   here makes such a request fail when it is built, with the reason, instead
   of inside the Go workflow (#1676). A public DNS name that resolves to such
   an address cannot be detected by either side. *)

let ( let* ) = Result.bind

(* IPv4 literal in canonical dotted-quad form, as Go's netip.ParseAddr reads
   it: four decimal fields of 0..255 with no leading zeros. *)
let parse_ipv4 text =
  match String.split_on_char '.' text with
  | [ _; _; _; _ ] as fields ->
      let field value =
        let length = String.length value in
        if length = 0 || length > 3 || not (String.for_all (fun c -> c >= '0' && c <= '9') value)
           || (length > 1 && value.[0] = '0')
        then None
        else
          let number = int_of_string value in
          if number > 255 then None else Some number
      in
      let octets = List.filter_map field fields in
      if List.length octets = 4 then Some (Array.of_list octets) else None
  | _ -> None

let hex_digit c =
  match c with
  | '0' .. '9' -> Some (Char.code c - Char.code '0')
  | 'a' .. 'f' -> Some (Char.code c - Char.code 'a' + 10)
  | 'A' .. 'F' -> Some (Char.code c - Char.code 'A' + 10)
  | _ -> None

(* IPv6 literal as sixteen bytes: hex groups of one to four digits, at most one
   "::", and an optional embedded IPv4 tail. *)
let parse_ipv6 text =
  let group value =
    let length = String.length value in
    if length = 0 || length > 4 then None
    else
      String.fold_left
        (fun total c -> match total, hex_digit c with Some total, Some digit -> Some ((total * 16) + digit) | _ -> None)
        (Some 0) value
  in
  (* Groups of one side of "::", each two bytes; an IPv4 tail is four. *)
  let side value ~last =
    if value = "" then Some []
    else
      let fields = String.split_on_char ':' value in
      let count = List.length fields in
      let rec convert index = function
        | [] -> Some []
        | [ tail ] when last && index = count - 1 && String.contains tail '.' ->
            Option.map (fun octets -> Array.to_list octets) (parse_ipv4 tail)
        | field :: rest -> (
            match group field, convert (index + 1) rest with
            | Some number, Some bytes -> Some ((number lsr 8) :: (number land 0xff) :: bytes)
            | _ -> None)
      in
      convert 0 fields
  in
  let bytes =
    match String.split_on_char ':' text with
    | _ when String.length text < 2 -> None
    | _ -> (
        let rec find index =
          if index + 1 >= String.length text then None
          else if text.[index] = ':' && text.[index + 1] = ':' then Some index
          else find (index + 1)
        in
        match find 0 with
        | None -> Option.bind (side text ~last:true) (fun bytes -> if List.length bytes = 16 then Some bytes else None)
        | Some index ->
            let head = String.sub text 0 index and tail = String.sub text (index + 2) (String.length text - index - 2) in
            if String.length tail >= 2 && String.sub tail 0 1 = ":" then None
            else (
              match side head ~last:false, side tail ~last:true with
              | Some head, Some tail when List.length head + List.length tail <= 14 ->
                  Some (head @ List.init (16 - List.length head - List.length tail) (fun _ -> 0) @ tail)
              | _ -> None))
  in
  Option.map Array.of_list bytes

let metadata_ipv4 = [ [| 100; 100; 100; 200 |]; [| 168; 63; 129; 16 |]; [| 169; 254; 169; 254 |] ]

(* netpolicy.BlockedAddress for an IPv4 address: unspecified, loopback,
   private, link-local, multicast, reserved, carrier-grade NAT, benchmarking
   and the cloud metadata endpoints. *)
let blocked_ipv4 octets =
  let a = octets.(0) and b = octets.(1) and c = octets.(2) in
  a = 0 || a = 10 || a = 127
  || (a = 169 && b = 254)
  || (a = 172 && b land 0xf0 = 16)
  || (a = 192 && b = 168)
  || a >= 224
  || (a = 100 && b land 0xc0 = 64)
  || (a = 192 && b = 0 && c = 0)
  || (a = 198 && b land 0xfe = 18)
  || List.mem octets metadata_ipv4

let zeros bytes first last =
  let rec check index = index > last || (bytes.(index) = 0 && check (index + 1)) in
  check first

(* netpolicy.BlockedAddress for an IPv6 address, after Go's Unmap: an
   IPv4-mapped address is judged as IPv4, and the NAT64 well-known prefix by
   the IPv4 address it embeds. *)
let blocked_ipv6 bytes =
  let embedded () = Array.sub bytes 12 4 in
  if zeros bytes 0 9 && bytes.(10) = 0xff && bytes.(11) = 0xff then blocked_ipv4 (embedded ())
  else if bytes.(0) = 0 && bytes.(1) = 0x64 && bytes.(2) = 0xff && bytes.(3) = 0x9b && zeros bytes 4 11 then
    blocked_ipv4 (embedded ())
  else
    zeros bytes 0 11 (* ::/96, including :: and ::1 *)
    || bytes.(0) land 0xfe = 0xfc (* fc00::/7, including fd00:ec2::254 *)
    || (bytes.(0) = 0xfe && bytes.(1) land 0xc0 = 0x80) (* fe80::/10 *)
    || (bytes.(0) = 0xfe && bytes.(1) land 0xc0 = 0xc0) (* fec0::/10 *)
    || bytes.(0) = 0xff (* ff00::/8 *)
    || (bytes.(0) = 0 && bytes.(1) = 0x64 && bytes.(2) = 0xff && bytes.(3) = 0x9b && bytes.(4) = 0 && bytes.(5) = 1)
    || (bytes.(0) = 0x20 && bytes.(1) = 0x02) (* 2002::/16 *)
    || (bytes.(0) = 0x20 && bytes.(1) = 0x01 && bytes.(2) = 0 && bytes.(3) = 0) (* 2001::/32 *)

(* Only ASCII DNS name characters: a fetcher following the WHATWG URL rules
   maps others before resolving, which could turn an accepted name into a
   blocked address. *)
let host_name_syntax host =
  String.for_all (fun c -> (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c = '-' || c = '_' || c = '.') host

(* A last label that is decimal, octal or hexadecimal makes resolvers read the
   host as an IPv4 address, as in 127.1, 2130706433 or 0x7f000001. *)
let ends_in_number host =
  let last = match String.rindex_opt host '.' with None -> host | Some index -> String.sub host (index + 1) (String.length host - index - 1) in
  if last = "" then true
  else
    let digits, body =
      if String.length last >= 2 && String.sub last 0 2 = "0x" then ("0123456789abcdef", String.sub last 2 (String.length last - 2))
      else ("0123456789", last)
    in
    String.for_all (fun c -> String.contains digits c) body

let blocked_host_name host =
  let suffixed suffix =
    host = suffix
    || (String.length host > String.length suffix + 1
        && String.sub host (String.length host - String.length suffix - 1) (String.length suffix + 1) = "." ^ suffix)
  in
  suffixed "localhost" || suffixed "internal" || List.mem host [ "metadata"; "metadata.goog"; "instance-data" ]

let not_allowed = Error "media URL host is not allowed"

(* [check url] is [Ok ()] when [url], already accepted as an absolute URI,
   satisfies the remote-media policy; otherwise the reason. A host with a
   percent escape is refused rather than decoded. *)
let check url =
  let* rest =
    match String.index_opt url ':' with
    | None -> Error "media URL must include a scheme"
    | Some colon ->
        let scheme = String.lowercase_ascii (String.sub url 0 colon) in
        if scheme <> "https" && scheme <> "http" then Error (Printf.sprintf "media URL scheme %S is not allowed; use https or http" scheme)
        else Ok (String.sub url (colon + 1) (String.length url - colon - 1))
  in
  let* authority =
    if String.length rest >= 2 && String.sub rest 0 2 = "//" then
      let rest = String.sub rest 2 (String.length rest - 2) in
      let stop = ref (String.length rest) in
      String.iteri (fun index c -> if (c = '/' || c = '?' || c = '#') && index < !stop then stop := index) rest;
      Ok (String.sub rest 0 !stop)
    else Error "media URL must include a host"
  in
  let* () = if String.contains authority '@' then Error "media URL must not contain user information" else Ok () in
  let bracketed = String.length authority > 0 && authority.[0] = '[' in
  let* host, literal_v6 =
    if bracketed then
      match String.index_opt authority ']' with
      | None -> not_allowed
      | Some close -> Ok (String.sub authority 1 (close - 1), true)
    else
      match String.rindex_opt authority ':' with
      | Some colon when String.for_all (fun c -> c >= '0' && c <= '9') (String.sub authority (colon + 1) (String.length authority - colon - 1)) ->
          Ok (String.sub authority 0 colon, false)
      | _ -> Ok (authority, false)
  in
  let host = String.lowercase_ascii host in
  let host = if String.length host > 0 && host.[String.length host - 1] = '.' then String.sub host 0 (String.length host - 1) else host in
  if host = "" then Error "media URL must include a host"
  else if String.contains host '%' then not_allowed
  else if literal_v6 then
    match parse_ipv6 host with Some bytes when not (blocked_ipv6 bytes) -> Ok () | _ -> not_allowed
  else
    match parse_ipv4 host with
    | Some octets -> if blocked_ipv4 octets then not_allowed else Ok ()
    | None -> if (not (host_name_syntax host)) || ends_in_number host || blocked_host_name host then not_allowed else Ok ()
