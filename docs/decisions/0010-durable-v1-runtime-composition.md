# ADR 0010: Durable v1 Runtime Composition

- Status: Accepted guard; durable implementation pending
- Date: 2026-07-25
- Complements: ADR 0006, ADR 0007, and ADR 0008

## Context

The Temporal boundary is three closed, one-shot Activities:
`llm.generate.v1`, `llm.compact.v1`, and `llm.query.v1`. The reusable Go engine
currently exposes the older `llm.Engine.Generate(llm.Request)` helper. That
helper performs the legacy request normalization, routing, admission,
provider dispatch, and result finalization flow. It is not a durable
implementation of the v1 records.

The v1 boundary has additional obligations that cannot be recovered from a
legacy `llm.Engine` call:

- checkpoint handles must be materialized and validated before Generate or
  Compact dispatch;
- Generate must sequence replay, bounded cache/compaction decisions, route
  selection, Redis reservation, PostgreSQL journaling, provider state, and
  durable checkpoint/cache/cost finalization;
- Compact has a distinct request/response and lifecycle, not a Generate alias;
- Query is a control-plane read with its own authorization, pagination, and
  audit ledger, and must never dispatch inference.

`ProductionEngineFactory.Build` currently returns the reusable `llm.Engine`
and its snapshot-owned clients. The process runtime therefore installs
`activity.UnconfiguredV1Runtime` until an application supplies the explicit
durable seam. This is intentional: adapting the legacy engine by type
assertion, by wrapping Generate, or by returning empty Compact/Query responses
would advertise a partially configured production worker and could bypass
durable state or charge work twice.

## Decision

Keep `activity.V1Runtime` as an explicit production composition requirement.
The runtime must not infer a v1 implementation from `llm.Engine`, and
`ProductionEngineFactory` must not provide a lossy adapter. Production and
unknown environments fail closed before Temporal polling when the seam is
absent. The development Compose profile may omit the v1 registrations because
it is a configuration/readiness fixture only.

The factory also rejects a builder result that is a typed-nil `V1Runtime`
interface. Snapshot-owned clients are drained before returning the composition
error, so an incomplete custom builder cannot register Activities that would
panic when invoked.

The future durable implementation may be supplied in either of two equivalent
ways:

1. a snapshot-scoped object implementing all three `V1Runtime` methods; or
2. a narrowly scoped composition adapter that owns the full phase order and
   receives the same immutable storage/provider clients as the reusable
   engine.

Either form must also expose checkpoint-aware Generate/Compact materialization
and the independent Query service where those capabilities are enabled. It
must return bounded, redacted errors and preserve the existing Activity
heartbeat/cancellation lifecycle. No implementation may call the legacy
`Engine.Generate` as a substitute for Compact or Query, or bypass Redis and
PostgreSQL durability before provider dispatch.

## Evidence and guard

The current boundary is visible in:

- `golang/activity/v1_runtime.go`, which validates and dispatches only an
  explicit `V1Runtime`;
- `golang/internal/runtime/runtime.go`, which installs the fail-closed
  `UnconfiguredV1Runtime` and rejects startup when it is unconfigured;
- `golang/internal/runtime/factory.go`, whose `Build` result is only
  `llm.Engine` plus snapshot clients; and
- `golang/engine/generate.go`, whose legacy flow has no Compact or Query
  contract.

`TestProductionCompositionDoesNotAdaptLegacyEngineToV1` locks this boundary:
when composition receives only the legacy engine, the v1 seam remains
unconfigured and cannot be reported as production-ready. Existing runtime and
Activity tests additionally prove startup fails before polling and that every
unconfigured v1 operation is non-dispatched and redacted.

## Consequences

- The repository does not claim Task 15 is complete while only the legacy
  engine is wired.
- A future implementation has a clear, reviewable seam and cannot silently
  regress to a lossy wrapper.
- The reusable legacy engine remains available to non-Temporal callers and
  pre-release tests.
- The remaining work is implementation and protected integration evidence for
  the durable v1 runtime; this ADR does not authorize provider dispatch.
