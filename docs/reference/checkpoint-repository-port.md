# Durable checkpoint repository port

`golang/state` defines the storage-neutral contract for durable checkpoints.
`DurableCheckpoint` is an immutable row-shaped DTO: it
contains scope and lineage identity, typed operation/cache/blob references,
version and compiler metadata, content digests, provider-state metadata, and
retention timestamps. Prompt, output, settings-patch, and provider-state bytes
are represented only by blob references; this contract never exposes a blob
locator or plaintext payload.

`CheckpointRepository` provides scoped reads and starts a
`CheckpointUnitOfWork`. The unit of work accepts a validated
`CheckpointWrite`, then commits or discards the staged publication.
`WithCheckpointUnitOfWork` owns that lifecycle and preserves a callback error
when rollback also fails. The interface does not require a multi-row database
transaction. Implementations must keep provider calls outside publication and
enforce scope filtering, immutable create-if-absent semantics, parent/depth
constraints, and operation idempotency.

`CheckpointMaterializer` returns the existing `state.MaterializedState` view,
so a durable adapter and the in-memory `CheckpointGraph` share one replay
contract. The DTO's `Validate` method and `CanonicalDigest` define the common
version, digest, ordering, and timestamp checks. Expired rows remain valid
history; a materializer decides whether a caller may use one at its read time.

`storage/cloudstate.Repository.Checkpoints()` implements `state.CheckpointStore`
over generic cloud KV/blob interfaces. The SQL adapter has been removed. The
cloud adapter verifies encrypted blob references, parent depth and scoped
lineage, then publishes immutable encrypted metadata with conditional creates.
ID and handle reservations precede the operation row that makes a checkpoint
visible. Reads require matching reservations and the committed operation row;
incomplete publication remains unreadable.

One unit stages one distinct checkpoint. Staging copies caller-owned data and
does not write storage. Rollback discards that local state, but cannot undo a
commit whose acknowledgement was lost. Retry the identical checkpoint,
including its IDs and timestamps, in a new unit. Identical completed retries
are idempotent; different contents or competing checkpoints for one operation
return a conflict. There is no portable multi-checkpoint transaction.

The finalizer writes immutable blobs before publication and binds operation and
cache provenance to authorized results. Partial failures may leave orphan
objects or reservations; automatic cleanup is deferred. See the
[cloud checkpoint contract](cloud-request-repository.md#checkpoint-persistence)
for the publication sequence and integrity checks.

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
scope-bound handle before lookup. The cloud execution runtime uses this replay
path for Generate and Compact; the materializer itself performs no publication
or provider calls.

The runtime's `CheckpointCapabilities` bundle carries these ports with the
same immutable configuration snapshot as the worker clients. Its `Validate`
method permits an explicitly partial rollout, but `RequireMaterializer` is a
fail-closed gate for any builder that needs replay: repository, scoped blob
reader, and handle materializer must all be present. The production factory
binds the cloud repository's checkpoint store, immutable blob writer and the
snapshot's continuation keyring. Runtime-owned private adapters expose the
storage-neutral ports without exposing provider clients or key material.
Missing stores or handle verification fail closed. A complete replay bundle
does not authorize a paid request; production caller authorization remains an
explicit prerequisite to activating the CLI runtime.
