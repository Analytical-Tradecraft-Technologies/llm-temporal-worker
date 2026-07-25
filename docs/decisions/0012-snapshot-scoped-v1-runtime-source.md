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
