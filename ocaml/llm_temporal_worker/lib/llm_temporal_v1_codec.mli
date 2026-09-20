val generate_api_version : string
val compact_api_version : string
val query_api_version : string

val encode_generate_request : Llm_temporal_models.generate_request -> (bytes, Temporal.Error.t) result
val decode_generate_request : bytes -> (Llm_temporal_models.generate_request, Temporal.Error.t) result
val encode_generate_response : Llm_temporal_models.generate_response -> (bytes, Temporal.Error.t) result
val decode_generate_response : bytes -> (Llm_temporal_models.generate_response, Temporal.Error.t) result
val encode_compact_request : Llm_temporal_models.compact_request -> (bytes, Temporal.Error.t) result
val decode_compact_request : bytes -> (Llm_temporal_models.compact_request, Temporal.Error.t) result
val encode_compaction_response : Llm_temporal_models.compaction_response -> (bytes, Temporal.Error.t) result
val decode_compaction_response : bytes -> (Llm_temporal_models.compaction_response, Temporal.Error.t) result
val encode_query_request : Llm_temporal_models.query_request -> (bytes, Temporal.Error.t) result
val decode_query_request : bytes -> (Llm_temporal_models.query_request, Temporal.Error.t) result
val encode_query_envelope : Llm_temporal_models.query_envelope -> (bytes, Temporal.Error.t) result
val decode_query_envelope : bytes -> (Llm_temporal_models.query_envelope, Temporal.Error.t) result
val encode_query_response : Llm_temporal_models.query_response -> (bytes, Temporal.Error.t) result
val decode_query_response : bytes -> (Llm_temporal_models.query_response, Temporal.Error.t) result

val context_to_v1_json : Llm_temporal_models.request_context -> (Yojson.Safe.t, Temporal.Error.t) result
val context_of_v1_json : Yojson.Safe.t -> (Llm_temporal_models.request_context, Temporal.Error.t) result
