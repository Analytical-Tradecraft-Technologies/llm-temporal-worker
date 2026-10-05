# Cloud checkpoint publication

`V1RuntimeCapabilities.NewCheckpointPublication` binds the snapshot's generic
checkpoint reader, blob writer, scoped materializer and signing keyring. It
creates immutable content blobs and a `DurableCheckpoint` publication plan;
`CloudFinalizer.CommitGenerate` or `CommitCompact` saves that plan and commits
the checkpoint before completing its effects.

The caller supplies an already authorized scope and stable operation ID,
checkpoint UUID, creation time and expiry. Persist these inputs before use and
reuse them after uncertain writes. Retrying publication does not repeat provider
work. The signed continuation handle is scoped to the caller, and a child cannot
outlive its parent. Configured depth, row, item and byte limits bound publication.

Generation records the current append, settings patch and model output, keeping
exact decimal settings intact. The runtime applies the same transcript
validation to provider output when the paid response is saved, so an unusable
response fails the request there instead of reaching publication. Parent replay verifies the materializer's opaque
storage scope before attaching the authorized tenant and project. Materialized
storage scope is not itself a caller identity. A generated checkpoint whose
depth is a positive multiple of `MaterializeLimits.SnapshotInterval` (default
8) also writes a self-contained snapshot of the materialized transcript,
settings and lineage, which bounds later replays to that many rows. The choice
depends only on the immutable depth, so a retry publishes identical blobs. If
the snapshot would exceed the blob byte bound, the checkpoint is published
without it.

Compaction writes a snapshot containing the summary and the current request's
retained suffix, with unchanged application settings. Its response blob contains
only the summary so a later cache consumer can use its own suffix and lineage.
Cache callers must supply the validated origin template and corresponding model
output. A hit creates a distinct zero-cost child, with origin metadata retained
as provenance. A compact checkpoint retains the `compaction` kind, including
when the summary came from cache, as required by the cache-use receipt contract.

When no safe prefix needs compaction, publication copies the materialized state
into a new child without provider work. Use `FinalizationEffects.NoWork` for this
path: the finalizer requires compaction provenance, disabled cache, no usage and
an exact zero cost. It resumes uncertain checkpoint writes without calling Redis,
publishing a cache entry, or recording a cache-use receipt.

These helpers do not select routes, authorize caller labels, acquire budget,
schedule activities or enable the production runtime by themselves.
