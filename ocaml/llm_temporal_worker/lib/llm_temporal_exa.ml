let answer ~operation_key ~context ~model ?(include_source_text = true) ~question () =
  Llm_temporal_generate.make_checked ~operation_key ~context ~model
    ~settings:(Llm_temporal_conversation.Settings.make ~extensions:["exa", `Assoc ["text", `Bool include_source_text]] ())
    ~input:[Llm_temporal_helpers.Item.human question] ()
let sources = Llm_temporal_helpers.Response.references
