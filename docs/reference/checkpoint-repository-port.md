# Durable checkpoint repository port

`golang/state` now defines the storage-neutral contract for the next durable
checkpoint slice. `DurableCheckpoint` is an immutable row-shaped DTO: it
contains scope and lineage identity, typed operation/cache/blob references,
version and compiler metadata, content digests, provider-state metadata, and
retention timestamps. Prompt, output, settings-patch, and provider-state bytes
are represented only by blob references; this contract never exposes a blob
locator or plaintext payload.

`CheckpointRepository` provides scoped reads and starts a
`CheckpointUnitOfWork`. The unit of work accepts a validated
`CheckpointWrite`, then commits or rolls back the short publication
transaction. `WithCheckpointUnitOfWork` owns that lifecycle and preserves a
callback error when rollback also fails. Repository implementations must keep
blob/provider I/O outside the transaction and must enforce scope filtering,
immutable create-if-absent semantics, parent/depth constraints, and operation
idempotency.

`CheckpointMaterializer` returns the existing `state.MaterializedState` view,
so a durable adapter and the in-memory `CheckpointGraph` share one replay
contract. The DTO's `Validate` method and `CanonicalDigest` define the common
version, digest, ordering, and timestamp checks. Expired rows remain valid
history; a materializer decides whether a caller may use one at its read time.

`golang/storage/postgres` now contains the first adapter for this port:
`DurableCheckpointRepository`. It performs scope-qualified reads, verifies blob
metadata and scope before publication, checks parent depth and origin
operation/cache scope, checks that parent and compaction-lineage references
belong to the same scope, and writes the checkpoint row plus provider-state and
affinity children through one PostgreSQL unit of work. Reads repeat the
lineage-scope check so a manually corrupted row cannot expose a cross-tenant
parent or compaction reference. A retry of the same immutable row is accepted;
a different payload for the same checkpoint ID returns `ErrCheckpointConflict`.

Because the PostgreSQL schema stores `origin_operation_id` as a UUID and has
no column for the original operation string, this adapter accepts only
canonical (lower-case, untrimmed) UUID `OperationID` values when publishing a checkpoint. The general operation
repository may still derive UUIDs for legacy string IDs, but checkpoints fail
closed instead of storing that derived value: otherwise a read would return a
different `OriginOperationID` and break canonical identity. Existing rows are
therefore expected to contain the canonical UUID returned by the operation
boundary.

The adapter deliberately does not claim the full runtime boundary. Blob bytes
must already be durable before `PutCheckpoint`, and operation/result rows are
not published by this unit of work. It does not open blob locators, call a
provider, implement retention, or wire `V1Runtime`, `llm.generate.v1`, or
`llm.compact.v1`. Those integrations still require transaction fault-injection,
schema/ACL, and Activity-level idempotency evidence before production
composition can claim end-to-end durable checkpoint support.

## Blob codec and materialization adapter

`state.CheckpointBlobCodec` defines the bounded `checkpoint-blob/v1` envelope
for the four immutable blob roles: `delta`, `response`, `settings_patch`, and
`materialized_snapshot`. The payload is canonical JSON and is decoded through
the closed item/settings codecs; callers must treat a version, kind, duplicate
key, media-type, or size/depth error as non-retryable integrity failure.

`state.CheckpointBlobReader` accepts `(context, scope ID, BlobReference)` and
returns verified bytes. `state.ScopedBlobReader` is the reusable adapter for a
content-addressed `blob.Store`: its resolver is passed the scope and typed blob
ID, and the adapter compares the resolved store reference with the durable
digest, byte length, and media type before reading. It then rechecks the byte
length and SHA-256. No caller-provided locator is accepted.

`state.DurableCheckpointMaterializer` combines the repository, reader, codec,
and optional `CheckpointHandleVerifier`. It resolves the complete parent chain,
rejects cycles/depth gaps/cross-scope rows, and delegates replay/frontier/
snapshot checks to `CheckpointGraph`. `MaterializeHandle` verifies the opaque
scope-bound handle before lookup. This is a read-side prerequisite only: it
does not publish blobs or checkpoint rows and is not wired into Generate or
Compact.

The runtime's `CheckpointCapabilities` bundle carries these ports with the
same immutable configuration snapshot as the worker clients. Its `Validate`
method permits an explicitly partial rollout, but `RequireMaterializer` is a
fail-closed gate for any builder that needs replay: repository, scoped blob
reader, and handle materializer must all be present. The production factory
can complete this bundle only when deployment supplies a checkpoint blob-ID
locator and the snapshot's continuation keyring; the locator is deliberately
separate from the result `BlobRefResolver` because checkpoint rows contain
UUID blob IDs. Runtime-owned private adapters erase concrete
PostgreSQL/blob-reader implementations before the bundle is published, so the
storage-neutral interface cannot be bypassed by a type assertion to a pool,
locator, or key binding. Without either binding, checkpoint replay remains
fail-closed.
