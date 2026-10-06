open Llm_temporal_models

(** Builds an Exa Answer Generate request using the selector configured in your
    worker. This does not pretend to expose Exa search/crawl APIs as workflows. *)
val answer : operation_key:Operation_key.t -> context:request_context -> model:Model_selector.t -> ?include_source_text:bool -> question:string -> unit -> (generate_request, Temporal.Error.t) result
val sources : generate_response -> reference list
