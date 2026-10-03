# Exact-response cache fingerprints

The `golang/cache` package owns the v1 semantic cache identity. `cache.Input`
is canonicalized after request validation and HMACed with a deployment-scoped
secret. Its `RouteIdentity` requires provider, endpoint, resolved model and
revision, and compiler profile; account and region are included even when
empty. This keeps OpenAI, Azure/OpenRouter, account, region, revision,
quantization, and compiler changes isolated until a future ADR proves a safe
equivalence.

Operation keys, service class/fallback consent, actor and observability tags,
continuation handles, and cache age are per-call controls and do not change a
fingerprint. Tenant/project namespace, semantic request fields, immutable
conversation/provider-state digests, route identity, capability lowering,
configuration digest, cache epoch, operation domain, and variant do. Compact
is domain-separated from Generate; both operations accept independent sample indexes.

The package returns only a fixed-size HMAC digest for lookup. Callers may retain
the canonical manifest for audit, but large transcript content should be
represented by immutable digest/blob references rather than copied into the
cache row. Changing canonicalization, semantic profiles, compiler lowering, or
route identity requires a cache epoch bump.

## Opt-in policy and local fill collapse

`cache.Policy` is the provider-neutral validation boundary for the optional
exact-response cache. Omission is represented by a nil policy; an instance
may omit maximum age for unrestricted completion age, or supply a positive,
bounded maximum age. Its non-negative signed 32-bit variant is an independent
sample index (default zero). It is part of the fingerprint and is never
forwarded as provider randomness. Changing it selects a separate cache entry
for either Generate or Compact, including with unknown or zero temperature.
This requests another sample; it does not guarantee different model output.

`cache.FillCoordinator` suppresses duplicate provider work among callers in
one worker process. It returns the same normalized, operation-kind-neutral
response template to waiters and lets each waiter cancel independently. The
coordinator does not provide durability, ownership, freshness, use accounting,
or cache provenance: every call still needs the cloud response-cache
lookup, fill lease, publication, and failure path. Cloud storage remains the
cross-worker authority, including after a process crash or restart.
