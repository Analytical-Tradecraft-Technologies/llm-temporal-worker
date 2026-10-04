open Llm_temporal

let decimal text = match Usd_decimal.of_string text with
  | Ok value -> value
  | Error error -> failwith error

let () =
  (* Ascending exact values include zero, tiny fractions, scale changes and
     amounts beyond machine-integer precision. Check both operand orders. *)
  let values = List.map decimal
      [ "0"; "0.000000000000000001"; "0.01"; "0.25"; "0.7";
        "1"; "1.01"; "10"; "9007199254740993";
        "99999999999999999999.999999999999999999" ] in
  let sign value = Stdlib.compare value 0 in
  List.iteri (fun left a ->
      List.iteri (fun right b ->
          if sign (Usd_decimal.compare a b) <> sign (Stdlib.compare left right)
          then failwith ("incorrect decimal ordering: " ^ Usd_decimal.to_string a
                         ^ " versus " ^ Usd_decimal.to_string b)) values) values;
  List.iter (fun (a,b) ->
      if Usd_decimal.compare (decimal a) (decimal b) <> 0 then
        failwith "equivalent decimal scales differ")
    [ "0", "0.000"; "0.7", "0.7000"; "10", "10.000" ]
