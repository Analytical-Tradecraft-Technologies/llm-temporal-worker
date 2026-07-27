# Checkpoint graph materializer

The worker now has a pure Go checkpoint graph contract in `golang/state`.
`CheckpointGraph` publishes immutable root and child nodes, while
`Materialize` walks parent links from a tenant-scoped leaf and applies every
ancestor delta, response, and sparse settings patch in order. A parent is
never mutated, so any number of operation-keyed sibling branches can share it.

`state.Patch` keeps omitted, `Set`, and `Clear` distinct. Collection `Set`
replaces the complete collection and `Clear` restores the documented root
default. Materialization returns a defensive `MaterializedState`, including
the effective model configuration and the current tool-call frontier.

The implementation enforces bounded depth, row, item, and canonical-byte
limits. It validates tool-call/result pairing across checkpoint boundaries,
rejects reused call IDs, and refuses a child that starts a new turn while an
ancestor still has unmatched tool calls. Optional self-contained snapshots
carry the same digest as a full replay; their use does not change handles or
the logical graph.

Replay uses the append-only property of checkpoint deltas to pre-size the
materialized item buffer and perform the aggregate item/byte bound check once
after replay. Splitting one logical transcript across many deltas therefore
does not cause repeated full-prefix copies or validations, and produces the
same canonical transcript bytes as a single grouped delta. The segmentation
choice remains a storage concern; it cannot change the materialized semantic
value.

The tool-call frontier is a set belonging to one model turn, not a stack. A
turn may append multiple `ToolCall` items before any result. Once the first
`ToolResult` arrives, only results matching the remaining outstanding call IDs
are valid; those results may arrive in any order. A new message or `ToolCall`
cannot begin until the frontier is empty, after which the next item starts a
new turn. Call IDs remain unique across the complete lineage, including calls
that have already been resolved.

The storage-neutral DTO and repository/UoW ports are documented in [Durable
checkpoint repository port](../reference/checkpoint-repository-port.md). The
PostgreSQL adapter now supplies scoped reads and immutable metadata/child-row
publication through that port. Blob bytes must be uploaded first, and the
adapter verifies blob scope and metadata before entering the checkpoint row.
Operation/result publication, retention, Activity payload wiring, and
Generate/Compact runtime composition remain separate concerns; this slice does
not imply end-to-end durable runtime support.

## Durable blob prerequisite

The durable materialization prerequisite uses the versioned
`checkpoint-blob/v1` JSON envelope. Its closed `kind` values are `delta`,
`response`, `settings_patch`, and `materialized_snapshot`. Transcript values
are decoded through the provider-neutral item codec, while settings patches
use the strict v1 sparse-patch codec; unknown envelope fields, duplicate JSON
keys, unsupported versions/kinds, trailing values, invalid item values, and
resource-limit violations fail closed. Encoders normalize omitted empty item
arrays to `[]`, so retries have one canonical byte representation.

`state.ScopedBlobReader` is the only byte capability consumed by the durable
materializer. A caller supplies a scope-filtered ID-to-locator resolver, never
a raw locator. The reader checks the returned store, locator, digest, byte
length, media type, scope binding, expiry, and the SHA-256 of the bytes before
returning a copy. A locator or object from another scope is an integrity fault,
not a cache miss.

`state.DurableCheckpointMaterializer` reads the leaf-to-root metadata graph,
decodes every referenced delta/response/patch (and an optional verified
self-contained snapshot), then delegates final validation to the existing
bounded `CheckpointGraph` materializer. It can accept an opaque handle only
through the scope-bound `CheckpointHandleVerifier`; the UUID hidden inside a
verified handle is not exposed in the Activity payload. The adapter has no SQL
transaction, blob publication, retention, provider call, or Generate/Compact
dispatch path. Those runtime integrations remain explicitly unimplemented.
