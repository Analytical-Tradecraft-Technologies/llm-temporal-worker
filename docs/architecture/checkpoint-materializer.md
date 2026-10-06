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

Before the first result, the open turn may also contain model messages,
`ProviderState` and `Reference` items between or after its calls: a provider
can emit reasoning state ahead of each call and citations after them. Once
results have started, only the remaining matching results are valid.

The storage-neutral DTO and repository/UoW ports are documented in [Durable
checkpoint repository port](../reference/checkpoint-repository-port.md). The
cloud adapter supplies scoped reads and conditional immutable metadata
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

`state.DurableCheckpointMaterializer` reads metadata rows from the leaf
towards the root and stops at the newest row that carries a self-contained
snapshot. It decodes the delta/response/patch of each row newer than that
snapshot, uses the verified snapshot as the replay base, then delegates final
validation to the existing bounded `CheckpointGraph` materializer. A lineage
with no snapshot is walked to its root exactly as before. It can accept an
opaque handle only
through the scope-bound `CheckpointHandleVerifier`; the UUID hidden inside a
verified handle is not exposed in the Activity payload. The materializer itself
does not publish blobs, delete retained state or dispatch provider work. The
cloud runtime composes it with those execution and publication boundaries;
automatic production retention remains separate work.

## Stopping at the newest snapshot

A snapshot holds the complete materialized transcript, the materialized
settings, the depth and the full root-to-row lineage of its checkpoint, so the
rows and blobs older than it are not read. The materialized state is
byte-identical to a full walk: items, settings, pending tool frontier, depth
and lineage. What the full walk used to establish by reading every ancestor is
established as follows.

- **Scope and expiry.** Every row that is read, including the snapshot row, is
  checked against the caller's scope. Only the requested checkpoint is checked
  against the materializer clock: each checkpoint keeps its own retention
  deadline, and an ancestor past its deadline is retained history that stays
  readable through its live descendants. Rows older than the snapshot are not
  re-checked. Scope is fixed for a lineage because publication only accepts a
  parent from the child's own scope. Retention must therefore keep every
  ancestor that a live checkpoint still reads.
- **Snapshot integrity.** The blob reader verifies the snapshot bytes against
  the digest and length in the row, and the materializer repeats that check.
  The decoded snapshot must name the row's own depth, end its lineage with the
  row's own ID, contain exactly `depth + 1` handles, and hash to the row's
  `CanonicalLineageDigest`, which publication derives from the same handle
  list. A lineage that repeats one of the newer rows is rejected as a cycle.
- **No silent fallback.** A snapshot that is present but unreadable, corrupt
  or inconsistent with its row fails the read. It is never skipped in favour of
  older deltas, because a compaction snapshot replaces its prefix with a
  summary and cannot be reproduced from them. The snapshot row's own delta,
  response and settings-patch blobs are not read.
- **Depth and row limits.** `Depth` and `Lineage` come from the snapshot, so
  publication still sees the true depth and lineage length. The leaf row's
  depth is compared with `MaxDepth` and `MaxRows` before any ancestor or blob
  is read, and the snapshot lineage counts against `MaxRows`. Item and byte
  limits apply to the final materialized transcript as before. A leaf at
  `MaxDepth` or `MaxRows` materializes, because publication allowed it, but no
  child can be published on it; the cloud runtime rejects such a parent in
  request preparation, before budget and dispatch.

## Snapshot cadence

Compaction always writes a snapshot. Generate publication writes one when the
new checkpoint's depth is a positive multiple of
`MaterializeLimits.SnapshotInterval` (default `state.DefaultSnapshotInterval`,
8; it is a limit with a default, not a configuration key). Depth is immutable
row metadata, so retries and forks make the same choice without extra state.
A replay therefore reads at most `SnapshotInterval` rows and
`3 * SnapshotInterval` checkpoint blobs regardless of conversation depth,
while the full-transcript snapshot blob is written on one turn in eight
rather than on every turn. A cadence snapshot is only a read optimization: if
its encoding would exceed the blob byte bound while the transcript itself
still fits, Generate publishes the checkpoint without it.

Forks need no special handling. A fork from before a snapshot walks its own
rows and writes its own snapshot at the next boundary; children of a snapshot
row share that row as their base; each snapshot's lineage names only its own
branch. A lineage published before cadence snapshots existed stays readable by
the full walk and starts carrying snapshots at its next depth boundary.
