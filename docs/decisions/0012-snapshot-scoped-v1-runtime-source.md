# ADR 0012: Snapshot-scoped v1 runtime source

- Status: Accepted composition seam; durable implementation pending
- Date: 2026-07-25
- Complements: ADR 0010 and ADR 0011

## Context

ADR 0010 keeps production fail-closed until the three one-shot v1 Activities
have a real durable implementation. The existing `EngineFactory` returns the
legacy `llm.Engine` and snapshot-owned clients, while `runtime.Options.V1Runtime`
is a process-level test/embedding seam. A process-level value cannot safely
survive configuration reload: an Activity could dispatch against a runtime
whose Redis, PostgreSQL, or provider clients belong to the previous snapshot
and are already draining.

The repository therefore needs a way to attach the future durable runtime to
the same client set as the engine without adapting `llm.Engine.Generate` or
inventing Compact/Query behavior.

## Decision

`internal/runtime.V1RuntimeBuilder` is an optional callback on
`ProductionFactoryOptions`. The factory invokes it once per immutable
configuration snapshot, after constructing the engine and all snapshot-owned
clients. The returned implementation is exposed through the snapshot's
`V1RuntimeSource`; a nil result is an explicit unconfigured capability.

The process registers one stable `snapshotV1Runtime` Activity implementation.
Each call acquires the current application snapshot for its entire method and
dispatches only to that snapshot's source. Reloads therefore atomically select
the new durable runtime while leases keep the old clients alive until in-flight
calls finish. If a source exists but is nil, the call fails closed rather than
falling back to a stale process-level runtime. Legacy custom `ClientSet`
implementations without the source may continue using the explicit embedding
fallback for compatibility.

The builder is a seam, not a production claim. It must eventually return a
runtime that implements the full Generate/Compact/Query contracts, owns the
typed phase ports from ADR 0011, materializes checkpoints, and preserves
redaction, heartbeats, idempotency, and cancellation. A missing builder still
causes production startup to fail before Temporal polling.

The snapshot client set also exposes an optional typed
`CheckpointCapabilities` bundle. It contains only the storage-neutral
`state.CheckpointRepository` and `state.CheckpointBlobReader` interfaces; it
does not expose a PostgreSQL pool, credentials, encryption keys, or raw object
store locators. A builder receives it by type-asserting the exported
`runtime.CheckpointCapabilitiesSource` interface on `app.ClientSet`; it does
not need to know the concrete client-set type. The PostgreSQL closer uses the
separate `runtime.PostgresCheckpointCapabilitiesSource` contract to supply the
repository from its own pool. A blob reader is present only when the
deployment's closer has composed the scoped locator, object-store, and
encryption-key boundary; the default factory leaves that capability nil and
callers must fail closed. The bundle is copied into each immutable client set
so a future builder cannot accidentally retain a dependency from a previous
reload.

This capability bundle is an input to the future checkpoint-aware V1 runtime;
it does not implement provider dispatch, Redis reservation/reconciliation,
PostgreSQL journaling/finalization, Compact, Query, or the complete durable
runtime. It therefore does not change the accepted fail-closed status of the
production V1 seam.

## Evidence

- `golang/internal/runtime/factory.go` invokes the builder after complete
  client construction and closes clients if composition fails.
- `golang/internal/runtime/snapshot_v1_runtime.go` holds a snapshot lease for
  each one-shot method and rejects an authoritative nil source.
- `golang/internal/runtime/factory_test.go` covers builder inputs, source
  attachment, and cleanup on builder failure.
- `golang/internal/runtime/snapshot_v1_runtime_test.go` covers reload source
  selection and legacy fallback boundaries.

This ADR does not replace `activity.UnconfiguredV1Runtime` and does not mark
Task 15 complete; concrete durable ports, Compact, Query, and protected
integration evidence remain required.
