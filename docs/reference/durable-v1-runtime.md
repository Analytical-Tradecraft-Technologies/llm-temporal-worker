# Durable v1 runtime composition

`activity.NewDurableV1Runtime` is the boundary adapter for a snapshot's
storage-neutral durable phase ports. It requires complete `GeneratePorts` and
`CompactPorts` values and returns an error during composition when any phase
callback is missing. Query remains optional because deployments may bind the
independent control-plane implementation through `Activities.QueryService`.

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
