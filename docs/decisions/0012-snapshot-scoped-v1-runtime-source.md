# ADR 0012: Snapshot-scoped v1 runtime source

- Status: Accepted; cloud implementation available
- Date: 2026-07-25
- Complements: ADR 0010 and ADR 0011

## Context

A process-global runtime must not keep using cloud, Redis or provider clients
from a configuration snapshot that is already draining. Activity execution and
readiness must resolve capabilities from the same current snapshot.

## Decision

`ProductionFactoryOptions.V1RuntimeBuilder` runs once per immutable snapshot
after constructing its engine and clients. The result is exposed through
`V1RuntimeSource`. The stable `snapshotV1Runtime` proxy holds a snapshot lease
for each activity step, keeping clients alive until that call finishes. Reload
atomically changes the source for later calls.

An authoritative missing or unconfigured source fails closed; it never falls
back to a stale process-level runtime. Readiness reevaluates the source during
each dependency-monitor pass and pauses polling when required capabilities are
unavailable. Failed construction closes the replacement clients and retains the
previous valid snapshot.

`CheckpointCapabilitiesSource` exposes storage-neutral repository, blob-reader
and materializer contracts. `RequireMaterializer` rejects incomplete replay
capabilities. Private adapters keep concrete storage clients out of these
contracts. `V1RuntimeCapabilitiesSource` additionally exposes snapshot-owned
request/cache stores, finalization, budget leaser, planner, provider registry,
configuration/storage identities, signing keyring and estimation settings.
Those internal runtime capabilities are never caller payloads.

The provider registry is privately copied so consumers cannot mutate its map
through an alias. Cloud and Redis identities and the configuration digest bind
composition to the exact snapshot that constructed these capabilities. The
builder never derives them from the legacy engine or process-global clients.

The production cloud builder composes execution and generation planning from
this bundle. Query keeps independent authorization and reader policy. Generic
phase factories remain available for explicit embeddings; incomplete factories
cannot silently become the production workflow runtime.

## Evidence

`golang/internal/runtime/factory.go`, `provider_control.go`,
`snapshot_v1_runtime.go` and `v1_cloud_builder.go` implement the boundary. Runtime
and factory tests cover reload isolation, lease ownership, cleanup on failure,
capability identity and authoritative missing-source rejection. See
[durable runtime composition](../reference/durable-v1-runtime.md).
