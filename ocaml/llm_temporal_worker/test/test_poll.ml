open Llm_temporal

let get = function Ok v -> v | Error e -> failwith (Temporal.Error.message e)

let () =
  let h : Poll.handle =
    {
      operation_id = "op";
      kind = "generate";
      provider = "openai";
      endpoint_id = "endpoint";
      provider_operation_id = "resp_1";
    }
  in
  let pending = Poll.Pending { operation_key = "key"; handle = h } in
  let b = get (Poll.encode_generate pending) in
  assert (get (Poll.decode_generate b) = pending);
  let h = { h with kind = "compact" } in
  let pending = Poll.Pending { operation_key = "key"; handle = h } in
  assert (
    get (Poll.decode_compact (get (Poll.encode_compact pending))) = pending);
  let r = Poll.Still_pending h in
  assert (get (Poll.decode_response (get (Poll.encode_response r))) = r);
  let r = Poll.Failed (h, "result_unavailable") in
  assert (get (Poll.decode_response (get (Poll.encode_response r))) = r);
  assert (
    Result.is_error
      (Poll.encode_response (Poll.Failed (h, "raw upstream message"))));
  let bad =
    Bytes.of_string
      {|{"api_version":"llm.temporal/poll/v1","status":"pending","pending":{"operation_id":"op","kind":"generate","provider":"openai","endpoint_id":"endpoint","provider_operation_id":"resp_1"},"generate":{}}|}
  in
  assert (Result.is_error (Poll.decode_response bad))
