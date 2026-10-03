# Downstream workflow-sample compile fixture

This is an external Dune project, separate from the package sources. It imports
only the installed `llm-temporal-ocaml` package and type-checks the
public workflow orchestration shape:

- all five typed `Query` constructors;
- validated `Query.Filter` builders for each query kind;
- a cached immutable root and three sibling `Conversation.start_respond`
  futures composed with `Temporal.Future.all`, including explicit handling of
  each future's `(turn, Temporal.Error.t) result` value channel;
- explicit `Conversation.compact`; and
- a post-compaction `Conversation.respond` that restores application settings;
- generation and compaction child workflows with explicit durable IDs and queues;
- the one-attempt `query_v1_activity` call.

The executable is compile-only and does not contact Temporal or an LLM
provider. Run it with:

```sh
opam exec -- dune build --root ocaml/consumer_workflow_smoke
```
