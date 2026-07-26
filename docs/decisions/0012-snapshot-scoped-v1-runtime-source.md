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

Readiness follows the same current-snapshot capability. The dependency monitor
must treat a missing or `UnconfiguredV1Runtime` source in a non-development
snapshot as unavailable: it keeps liveness up, marks readiness false, and
pauses Temporal polling. Once a replacement snapshot supplies a configured
source and its dependency probes pass, the monitor may resume polling. The
initial startup guard remains fail-closed, but the monitor must not cache that
initial v1 result across reloads.

The builder is a seam, not a production claim. It must eventually return a
runtime that implements the full Generate/Compact/Query contracts, owns the
typed phase ports from ADR 0011, materializes checkpoints, and preserves
redaction, heartbeats, idempotency, and cancellation. A missing builder still
causes production startup to fail before Temporal polling.

The snapshot client set also exposes an optional typed
`CheckpointCapabilities` bundle. It contains only the storage-neutral
`state.CheckpointRepository`, `state.CheckpointBlobReader`, and optional
`state.CheckpointHandleMaterializer` interfaces; it does not expose a
PostgreSQL pool, credentials, encryption keys, or raw object-store locators. A
builder receives it by type-asserting the exported
`runtime.CheckpointCapabilitiesSource` interface on `app.ClientSet`; it does
not need to know the concrete client-set type. The PostgreSQL closer uses the
separate `runtime.PostgresCheckpointCapabilitiesSource` contract to supply the
repository from its own pool. A blob reader is present only when the
deployment's closer has composed the scoped locator, object-store, and
encryption-key boundary; the default factory leaves that capability nil and
callers must fail closed. A complete handle materializer is an additional
optional `runtime.PostgresCheckpointMaterializerSource` capability. The
runtime accepts it only when the same closer also supplies both repository and
blob-reader capabilities, so a deployment cannot accidentally publish an
unscoped or partially configured replay path. The bundle is copied into each
immutable client set so a future builder cannot accidentally retain a
dependency from a previous reload.

The client set also exposes `runtime.V1RuntimeCapabilitiesSource`, whose
snapshot-owned `V1RuntimeCapabilities` value groups only the preparatory
contracts needed to compose the durable runtime: an `engine.SnapshotSource`,
`routing.Planner`, a private-copy `engine.AdapterRegistry`, the nested
checkpoint bundle, an optional write-only `durable.Journal`, and optional
`engine.ProviderStatusRecorder` and clock. The journal is exposed both through
the bundle and the snapshot client's `runtime.JournalSource`; both are
snapshot-owned and are closed with the PostgreSQL client set.
The bundle deliberately omits the legacy Redis admission, continuation, and
blob-result stores. Task 19 must supply PostgreSQL durable operations,
`BudgetMaterializer`/`Journal`, and the corresponding result/continuation ports
before a V1 runtime can compose them. These retained capabilities are copied
from the same snapshot build that constructs the legacy engine; the source
never derives them from that engine and never falls back to process-global
clients. The adapter registry copies its endpoint map behind an unexported
implementation, so consumers cannot mutate the legacy `engine.AdapterMap`
through aliasing or a type assertion. A reload therefore gives a builder the
preparatory capabilities of the replacement snapshot or an explicitly
incomplete value. A memory-state client set leaves `Journal` nil.

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
- `golang/internal/runtime/runtime.go` re-evaluates the current source during
  every readiness-monitor pass, so reloads cannot leave an unconfigured v1
  worker reporting ready.
- `golang/internal/runtime/factory_test.go` covers builder inputs, source
  attachment, cleanup on builder failure, snapshot-owned capability copy
  without legacy fallback, private adapter-map ownership, and journal
  ownership/closure without concrete PostgreSQL pool leakage.
- `golang/internal/runtime/snapshot_v1_runtime_test.go` covers reload source
  selection and legacy fallback boundaries.
- `golang/internal/runtime/runtime_test.go` covers readiness pause/resume when
  a reload removes and then restores the v1 source.

This ADR does not replace `activity.UnconfiguredV1Runtime` and does not mark
Task 15 complete; concrete durable ports, Compact, Query, and protected
integration evidence remain required.
