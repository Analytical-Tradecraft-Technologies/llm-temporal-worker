# ADR 0006: Forkable Conversation Checkpoints

- Status: Accepted; implemented (immutable checkpoint graph, `llm.compact.v1`
  and automatic compaction). Checkpoint retention and safe deletion remain
  deferred ([#818](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/818)).
- Date: 2026-07-18

## Context

Passing a complete transcript through every Temporal Activity repeats an
increasing amount of content in Workflow history. Tool calls, tool results,
reasoning state, and multimodal parts make that growth especially costly. A
provider continuation ID alone is not a sufficient source of truth: it may
expire, be route-specific, be unsafe to fork, or be unavailable after failover.

The current v1 continuation record is immutable and permits multiple children,
but its Activity contract still carries a full request and its engine persists
only the current request/response exchange. It therefore does not yet implement
a complete materialized conversation lineage.

## Decision

Replace the unreleased **llm.generate.v1** contract in place with an immutable,
tenant-bound checkpoint graph. Do not add a v2 name or legacy v1 adapter solely
for pre-release compatibility.
Each request names zero or one parent checkpoint, appends new semantic items,
and supplies a sparse settings patch. Omitted settings inherit from the parent;
explicit set and clear operations are distinct. A successful request creates
one child checkpoint and returns only the new turn plus a small child handle.

The same parent may be used by any number of operations. Distinct operation
keys create independent children, so branching is a normal operation rather
than a mutation race.

Canonical worker-held conversation state is the correctness source. Provider
conversation IDs, encrypted reasoning, prompt-cache affinity, and native
compaction artifacts are optional or required optimizations with explicit
provenance and pinning rules.

Add **llm.compact.v1**. Compaction creates another immutable child and never
rewrites or deletes its parent. Automatic compaction uses the same durable state
machine when a projected provider context crosses a configured threshold.

## Consequences

- Temporal history grows with per-turn deltas rather than the whole transcript.
- A checkpoint graph can represent retry-safe forks without mutable branch
  heads or distributed locks.
- Materialization, validation, canonical hashing, retention, and compaction
  provenance become first-class storage responsibilities.
- Provider-native continuation can reduce cost and latency, but cannot replace
  a portable canonical lineage unless the request explicitly accepts a hard
  provider pin.
- Go schemas/fixtures and the existing OCaml wrapper change together before the
  first release. There is no dual-contract compatibility period.

## Rejected alternatives

- Mutable conversation rows make concurrent forks contend on a branch head and
  make retry outcomes order-dependent.
- Full transcripts in every Activity preserve portability but defeat the
  Temporal-history objective.
- Provider IDs as the only state cannot guarantee retention, failover, audit,
  or safe branching.
- Destructive compaction prevents replay, audit, and recovery from a bad
  summary.
