# ADR 0010: Durable v1 Runtime Composition

- Status: Accepted; cloud execution composition implemented
- Date: 2026-07-25
- Complements: ADR 0006, ADR 0007 and ADR 0008

## Decision

Keep the v1 runtime an explicit composition requirement. The older
`llm.Engine.Generate` helper cannot implement durable checkpoint materialization,
independent compaction, workflow execution steps or typed control queries by
being wrapped or type-asserted. Missing or typed-nil capabilities fail closed and
drain the rejected snapshot's clients before Temporal polling begins.

The cloud implementation is `NewCloudV1RuntimeBuilder`. It requires an explicit
scope authorizer, checkpoint retention and materialization limits. It takes
request/cache/checkpoint stores, Redis budgets, provider planning and signing
keys from one immutable snapshot, validating cloud identity, Redis identity and
configuration digest before exposing any activity.

The result implements `activity.ExecutionRuntime` and
`activity.GenerationPlanningRuntime` together. The factory preserves these
interfaces instead of applying the old one-shot request wrapper. Supplying only
one is a composition error. Registered Generate and Compact activities use the
bounded methods; compatibility one-shot methods remain unavailable in this
runtime. Query delegates only to its separately configured authorized service.

The durable CLI installs the cloud builder with explicit trusted-Temporal scope
policy. It does not infer caller authority from tenant/project payloads alone.
The worker registers four workflows and eight activities; registration does not
prove that every optional Query reader is configured or that cloud/provider
integration has been qualified in a live environment.

## Direct phase composition

`NewDurableV1RuntimeBuilder`, `NewGenerateV1RuntimeBuilder` and
`NewCompactV1RuntimeBuilder` remain for explicit embeddings and contract tests.
They validate their phase callbacks but do not replace the bounded cloud runtime
required by the registered generation and compaction activities. There is no
legacy-engine fallback when cloud capabilities are unavailable.

## Evidence

The implementation resides in `golang/internal/runtime/v1_cloud_builder.go`,
`factory.go`, `snapshot_v1_runtime.go` and the activity execution boundary.
Factory and cloud-runtime tests cover capability completeness, identity binding,
client cleanup, authorization before storage, restart replay and query isolation.
`TestProductionCompositionDoesNotAdaptLegacyEngineToV1` preserves the original
fail-closed boundary.

See [durable runtime composition](../reference/durable-v1-runtime.md) for the
current settings and [workflow behavior](../reference/internal-workflows.md)
for the public and internal entry points.
