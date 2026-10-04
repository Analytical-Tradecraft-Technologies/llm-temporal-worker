type t = { digits : string; scale : int }

let strip_leading_zeroes value =
  let rec first index =
    if index + 1 < String.length value && value.[index] = '0' then first (index + 1)
    else index
  in
  String.sub value (first 0) (String.length value - first 0)

let valid_digits value = String.length value > 0 && String.for_all (fun c -> c >= '0' && c <= '9') value

let normalize digits scale =
  let digits = strip_leading_zeroes digits in
  if digits = "0" then { digits = "0"; scale = 0 }
  else if scale = 0 then { digits; scale }
  else
    let trailing = ref (String.length digits) in
    let scale = ref scale in
    while !trailing > 0 && digits.[!trailing - 1] = '0' && !scale > 0 do
      decr trailing;
      decr scale
    done;
    { digits = String.sub digits 0 !trailing; scale = !scale }

let of_string value =
  let fail message = Error message in
  if value = "" then fail "USD decimal must not be empty"
  else
    match String.index_opt value '.' with
    | Some dot when String.index_from_opt value (dot + 1) '.' <> None -> fail "USD decimal has multiple decimal points"
    | Some dot ->
        let whole = String.sub value 0 dot in
        let fraction = String.sub value (dot + 1) (String.length value - dot - 1) in
        if whole = "" || not (valid_digits whole) then fail "USD decimal whole part is invalid"
        else if fraction = "" || not (valid_digits fraction) then fail "USD decimal fractional part is invalid"
        else if String.length fraction > 18 then fail "USD decimal has more than 18 fractional digits"
        else if String.length whole > 20 then fail "USD decimal exceeds NUMERIC(38,18)"
        else if String.length whole > 1 && whole.[0] = '0' then fail "USD decimal has leading zeroes"
        else
          let value = normalize (whole ^ fraction) (String.length fraction) in
          if String.length value.digits - value.scale > 20 then fail "USD decimal exceeds NUMERIC(38,18)"
          else Ok value
    | None ->
        if not (valid_digits value) then fail "USD decimal must contain only decimal digits"
        else if String.length value > 20 then fail "USD decimal exceeds NUMERIC(38,18)"
        else if String.length value > 1 && value.[0] = '0' then fail "USD decimal has leading zeroes"
        else Ok (normalize value 0)

let zero = { digits = "0"; scale = 0 }

let to_string value =
  let length = String.length value.digits in
  if value.scale = 0 then value.digits
  else if length <= value.scale then
    "0." ^ String.make (value.scale - length) '0' ^ value.digits
  else
    String.sub value.digits 0 (length - value.scale) ^ "." ^
    String.sub value.digits (length - value.scale) value.scale

let compare a b =
  let scale = max a.scale b.scale in
  (* Scaling zero must not introduce significant digits into length comparison. *)
  let pad value = strip_leading_zeroes
      (value.digits ^ String.make (scale - value.scale) '0') in
  let left = pad a and right = pad b in
  if String.length left <> String.length right then Stdlib.compare (String.length left) (String.length right)
  else String.compare left right

let add_strings a b =
  let rec loop ia ib carry acc =
    if ia < 0 && ib < 0 then
      (if carry = 0 then acc else string_of_int carry ^ acc)
    else
      let da = if ia < 0 then 0 else Char.code a.[ia] - Char.code '0' in
      let db = if ib < 0 then 0 else Char.code b.[ib] - Char.code '0' in
      let total = da + db + carry in
      loop (ia - 1) (ib - 1) (total / 10) (Char.escaped (Char.chr (Char.code '0' + (total mod 10))) ^ acc)
  in
  loop (String.length a - 1) (String.length b - 1) 0 ""

let add_checked a b =
  let scale = max a.scale b.scale in
  let pad value = value.digits ^ String.make (scale - value.scale) '0' in
  let value = normalize (add_strings (pad a) (pad b)) scale in
  if String.length value.digits - value.scale > 20 || String.length value.digits > 38 then
    Error "USD decimal exceeds NUMERIC(38,18)"
  else Ok value

let add a b =
  match add_checked a b with
  | Ok value -> value
  | Error message -> invalid_arg message
