# Durable v1 runtime composition

`runtime.NewCloudV1RuntimeBuilder` composes the bounded Generate and Compact
activities from cloud storage, Redis and provider capabilities owned by one
immutable configuration snapshot. Pass the resulting builder through
`ProductionFactoryOptions.V1RuntimeBuilder`. It implements the
`activity.ExecutionRuntime` and `activity.GenerationPlanningRuntime` contracts
used by the registered activities; the legacy engine is not a fallback.

The builder requires deployment policy through `CloudV1RuntimeOptions`:

- `ResolveScope` authorizes the caller and returns the opaque scope used for
  request records, checkpoint signing and materialization. Tenant/project
  fields in an activity payload are not proof of authorization.
- `CheckpointTTL` is positive and controls checkpoint retention metadata.
- `Limits` sets the bounded checkpoint materialization limits. Negative limits
  are rejected; the materializer applies its defaults to omitted limits.

The constructor validates these options locally. When a snapshot is built, the
returned callback requires `V1RuntimeCapabilitiesSource` and verifies that the
cloud identity, Redis namespace/hash tag and configuration digest match that
snapshot. It then constructs `CloudExecutionRuntime` from the same request,
checkpoint, cache, budget, provider and signing capabilities. Missing or
inconsistent capabilities fail snapshot construction; no in-memory storage,
unscoped resolver or legacy inference runtime is invented.

The normal worker CLI does not yet select a production caller-authorization
policy or install this builder. Production startup therefore remains
fail-closed until that policy is configured and the builder is explicitly
wired. The checked-in development fixture is limited to configuration and
readiness checks. Passing library and integration tests does not establish that
the production CLI can dispatch work.

## Storage and execution boundaries

The worker has no SQL persistence backend. Generic cloud table/blob contracts
store durable requests, attempts, provider recovery context, results,
checkpoints and response-cache artifacts; the initial provider is DynamoDB/S3.
Redis owns budget reservations, claims, settlement and provider state. Budget
configuration comes from the immutable JSON settings snapshot. See
[state and storage](../architecture/state-and-storage.md).

The bounded execution runtime authorizes every step before reading request
state, including completed results and cache hits. Prepare saves the context
needed for recovery, budget acquisition attempts once, Generate/Compact submit
or resume the saved attempt, Poll makes one provider status request, and
Complete publishes the durable response. Workflow timers own budget and
provider waits. Public generation can call the separate compaction workflow;
both use the internal request and budget workflows described in
[internal workflows](internal-workflows.md).

Budget acquisition and claim use Redis directly; there is no SQL budget
journal. An unused reservation authorizes starting work within 15 minutes. Retrying
an activity recovers its existing attempt, while an explicitly requested retry
of an unknown paid submission needs a new attempt and new budget. The older
uncertain charge remains accounted for. See
[Redis budget leases](redis-budget-leases.md) and
[cloud provider execution](cloud-provider-execution.md).

## Snapshot ownership and Query

The runtime's snapshot proxy holds the configuration lease for the whole
activity step. A configuration reload cannot close that step's cloud, Redis,
provider or signing clients. The builder checks identity before exposing the
runtime, so clients from a previous snapshot cannot be attached to a new
configuration accidentally.

`llm.query.v1` has independent authorization and cursor policy. A deployment
may supply `ProductionFactoryOptions.QueryServiceBuilder`; the cloud execution
builder does not infer that policy or enable missing query readers. An
unconfigured Query capability fails closed. See
[persisted query composition](persisted-query-service.md).

## Direct phase adapters

`activity.DurableV1Runtime`, `NewDurableV1RuntimeBuilder`,
`NewGenerateV1RuntimeBuilder` and `NewCompactV1RuntimeBuilder` remain available
for direct phase composition and contract tests. They validate explicit phase
callbacks and snapshot-owned storage-neutral capabilities. Supplying only a
Generate or Compact phase is not a complete production runtime, and these
direct adapters do not replace the bounded `ExecutionRuntime` required by the
registered generation and compaction activities.

`durable.CompositionBuilder` binds storage-neutral request lifecycle ports and
the Redis budget leaser to one `StateIdentity`. Its validation does not create
clients or dispatch paid work. This remains a composition seam, not evidence
of live DynamoDB/S3, Redis restoration, caller authorization or provider
behavior; those require separate release evidence.
