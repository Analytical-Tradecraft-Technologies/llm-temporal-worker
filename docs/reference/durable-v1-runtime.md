# Durable v1 runtime composition

`activity.NewDurableV1Runtime` is the boundary adapter for a snapshot's
storage-neutral durable phase ports. It requires complete `GeneratePorts` and
`CompactPorts` values and returns an error during composition when any phase
callback is missing. Query remains optional because deployments may bind the
independent control-plane implementation through `Activities.QueryService`.

The bounded production composition currently exposes a Generate-only builder:
`runtime.NewGenerateV1RuntimeBuilder`. It requires the client set to expose
`V1RuntimeCapabilitiesSource`, and then requires a complete snapshot-owned
source, planner, adapter registry, checkpoint materializer, write-only
PostgreSQL journal, clock, and `GeneratePortsFactory`. The factory callback
receives the copied capability bundle and must construct every
`durable.GeneratePorts` callback from those same immutable adapters and stores.
No missing callback is synthesized, and the legacy `llm.Engine` is never
adapted. A successful composition returns `activity.GenerateOnlyV1Runtime`;
Compact and Query remain explicit configuration errors until their own real
ports are available. The default production factory leaves
`GeneratePortsFactory` unset, so installing this builder fails closed before
Temporal polling.

`runtime.NewCompactV1RuntimeBuilder` provides the corresponding contract-only
Compact composition. It requires the same snapshot-owned capabilities plus a
`CompactPortsFactory` and returns `activity.CompactOnlyV1Runtime`; Generate and
Query remain unavailable by design. This helper makes Compact validation and
testing independent without pretending that a partial runtime is safe to
register in production. A complete worker must compose both phase ports before
starting Temporal polling.

The runtime readiness guard treats both phase-only runtime values as
unconfigured, so accidentally installing either builder cannot mark a
production worker ready or start polling. Only a complete durable runtime (or
an explicitly supplied equivalent implementation) may cross that boundary.

The constructor performs only local validation. It does not construct clients,
read PostgreSQL or Redis, resolve provider credentials, or dispatch an
Activity. A deployment should call it from its per-snapshot runtime builder
after composing the real PostgreSQL operation/result/checkpoint/cache ports,
Redis budget materializer, provider dispatch, and reconciliation callbacks.
Those callbacks remain owned by the immutable snapshot and must be safe across
Temporal retries.

`GeneratePorts.Validate` and `CompactPorts.Validate` are also available when a
builder needs to validate ports before assembling additional wrappers. The
validation rejects both a missing callback and a typed-nil function callback;
the latter would otherwise pass through an `interface{}` check and panic only
after polling began. This is a composition guard, not evidence of live
PostgreSQL, Redis, or provider contract execution.
