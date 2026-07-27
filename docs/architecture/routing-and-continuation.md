# Routing and Continuation

> This chapter describes current routing behavior. Target phase status and
> authority are centralized in [scope](../scope.md#staged-delivery-and-document-authority).
> Phase A preserves these eligibility/pinning invariants; Phase B adds soft
> provider prompt-cache affinity, and Phase C keeps worker response-cache keys
> route-isolated.

## Routing model

Callers name a logical model, not an API hostname. Configuration expands that
logical name into ordered route entries:

```yaml
models:
  invoice-summarizer:
    routes:
      - id: openai-primary
        endpoint: openai-prod
        model: gpt-5.6-2026-06-01
        classes: [standard, priority]
      - id: bedrock-economy
        endpoint: bedrock-us-east-1
        model: anthropic.claude-example-v1
        classes: [economy, standard]
```

This route list is operator authorization to use those endpoint/model pairs.
It is distinct from `service_class_fallbacks`, which is caller authorization to
change processing class.

## Deterministic planning

For one immutable configuration snapshot, planning is pure and deterministic:

1. normalize the request and service class;
2. load and validate continuation constraints;
3. expand requested class followed by explicit fallback classes;
4. visit configured routes in order for each allowed class;
5. reject routes that fail health policy, pinning, capability, context, price,
   extension, residency, or budget-estimation prerequisites;
6. return an ordered plan containing eligible candidates and structured
   rejection diagnostics.

The ordering is class-major:

```text
requested class route 1
requested class route 2
...
first explicit fallback class route 1
first explicit fallback class route 2
...
```

This exhausts approved endpoints at the requested class before changing class.
Configuration may instead declare a narrower failover group, but it cannot
insert a class the caller omitted. Random or latency-weighted selection is
outside v1 because it weakens reproducibility; operators change order through a
new configuration snapshot.

### Provider prompt-cache affinity

An immutable checkpoint may carry an ordered `ProviderCacheAffinitySet`. Each
record contains provider/route identity, endpoint-account HMAC, region, API
family, model lineage and revision, cache epoch, observed cache-read/write
tokens, and an optional expiry. The planner validates those records and applies
them only after ordinary route eligibility has produced candidates. An exact
soft-affinity route is moved to the front of its existing requested/attempted
service-class group; it never moves across a class fallback boundary.

Affinity is a preference, not authorization. Tenant and residency policy,
endpoint enablement, credential scope, requested class, capability/context
requirements, health, pricing, and budget admission remain authoritative. An
expired or malformed observation is ignored or rejected before dispatch, and a
route absent from the immutable catalog cannot be introduced by an affinity
record. Required opaque continuation state continues to use the existing exact
provider pinning rules.

Provider prompt-cache keys are HMAC-SHA256 digests over a versioned canonical
tuple of tenant scope, parent checkpoint transcript digest, provider cache
epoch, and compatible model lineage. The output is safe provider metadata; raw
tenant IDs, prompt content, content hashes, and credentials are not emitted as
keys. Forks that share one immutable parent therefore reuse the same prefix
identity while still producing independent operations and responses. Cache
read/write token observations remain separate from worker exact-response cache
accounting.

The one-shot engine carries the immutable affinity set from a loaded
continuation into the planner and persists it on the next child. A matching
route refreshes its normalized cache-read/write observations after a successful
provider response; unrelated routes are left unchanged. The engine does not
invent a cache key or epoch for a root response because those values require a
provider-verified identity, so an observation is eligible for preference only
after that identity has already been persisted. This keeps affinity a safe
preference without making provider usage claims that the adapter did not make.

## Candidate identity

A candidate identity includes:

- route and endpoint IDs;
- API family and endpoint account/region;
- resolved immutable model identifier;
- requested, attempted, and provider service-tier value;
- capability-catalog and price-catalog versions;
- non-secret extension digest;
- continuation compatibility/pinning facts.

Admission, logs, metrics, and responses use this identity. Secrets, prompt
content, and SDK parameter objects do not.

## Fallback axes

There are two independent fallback axes:

| Axis | Authorization | May happen when |
| --- | --- | --- |
| Endpoint/model route | Ordered model-route configuration | Failure is definitely safe to retry and route policy allows the next candidate |
| Service class | Request `service_class_fallbacks` | Requested-class candidates are exhausted safely and the next class is explicitly listed |

The router never treats a provider-reported downgrade as a requested fallback.
For example, if OpenAI accepts `priority` but returns actual `default`, the
response reports requested/attempted `priority` and actual `standard`. Billing
reconciliation uses the actual tier. Operators can alert or reject future
traffic based on downgrade metrics, but the completed call is not repeated.

Provider-native hidden fallback is disabled where possible. When an endpoint
cannot disable it, its capability profile must expose that uncertainty and the
route is ineligible for strict budget/routing mode.

## Failure movement

| Outcome | Ledger action | Router action |
| --- | --- | --- |
| Compile/capability failure | No dispatch; release reservation if one exists | Try next precompiled candidate if policy permits |
| Definite rejection before provider acceptance | Record attempt outcome and exact cost, normally zero | May atomically reserve/continue to the next candidate within deadline |
| Rate/resource error explicitly marked uncharged | Record attempt outcome and update/reuse the remaining-plan reservation | May continue to the next candidate or explicit class fallback |
| Response with model output | Complete and reconcile | Stop |
| Cancellation before possible write | Record definite cancellation | Let Temporal/caller decide retry |
| Timeout/connection loss after possible write | Record ambiguous and retain reservation | Stop with non-retryable reconciliation error |
| Provider says accepted but result retrieval is supported | Record provider job/reference | Poll/retrieve through the same operation; never submit again |

The remaining caller deadline, maximum attempts, and cumulative provider time
bound every plan. Fallback does not reset the deadline.

## Operation identity and replay safety

The external idempotency key is:

```text
tenant + activity namespace + operation_key
```

The worker also stores a digest of the normalized semantic request, resolved
fallback authorization, and relevant extension data. Reuse with a different
digest is a non-retryable `operation_conflict`.

Operation states are:

```text
reserved -> dispatching -> completed
   ^                 \-> definite_failed
   |                 \-> ambiguous
   |                 \-> canceled
   +--- continue -----+  (only from a proven definite attempt outcome)
```

`Continue` atomically finalizes a definitely known attempt cost, releases the
old remaining-plan reservation, and reserves the maximum remaining candidate
against the union of its possible windows. If that reservation is denied, the
operation ends `definite_failed` with the already-incurred cost retained. Only
`reserved` may be safely reclaimed after a proven lease expiry. A
`dispatching` lease expiry becomes `ambiguous` unless provider-specific status
retrieval proves the outcome. `completed` returns the stored response reference
to every retry. Terminal operation records outlive the maximum Temporal retry
and retention horizon configured for the worker.

The system offers durable at-most-once automatic submission after a possible
write, not magical exactly-once behavior across an external API.

## Continuation handle

The public handle is opaque:

```text
ctn_v1.<key-id>.<base64url-random-id>.<base64url-mac>
```

The MAC covers version, key ID, random ID, and tenant binding. It is not a
serialized prompt or provider token. The store record contains:

- tenant/project ownership and creation/expiry;
- parent handle and immutable transcript digest;
- canonical portable transcript or blob reference;
- resolved endpoint/model/service facts for each prior turn;
- optional provider continuation IDs and opaque state;
- capability and price versions needed to interpret the record;
- last completed operation reference.

Every successful turn creates a child record. Existing records are immutable,
which permits safe concurrent branches. A child write is idempotent on completed
operation ID.

## Portable and pinned continuation

Canonical text/tool history is portable if a new candidate can compile it
without loss. Provider continuation IDs, encrypted reasoning, signatures,
redacted thinking, or provider-hosted state are pinned to:

```text
provider + endpoint account + API family + compatible model lineage
```

Planning follows these rules:

1. prefer the pinned route when opaque/provider-hosted state is required;
2. permit another route only if the continuation contains a complete canonical
   transcript and no required opaque state would be lost;
3. in strict mode, fail with `continuation_pinned` when the pinned route is not
   eligible;
4. in best-effort mode, portability may drop only state explicitly marked
   optional, with a diagnostic and a new branch.

Every persisted provider-state record carries complete provenance matching the
continuation pin. The `Required` marker is part of the continuation contract,
not a property inferred from the mere presence of provider state: it controls
whether the pinned state is a hard route constraint. Persisted optional state
must not silently turn a portable transcript into a hard route pin, nor may
unpinned opaque bytes be lowered to a provider request.

An expired provider conversation does not authorize silently replaying the
prompt. If a complete canonical transcript exists, the compiler may deliberately
reconstruct a new provider request and records that action.

## Route health

V1 health is operator-configured plus passive:

- administratively enabled/disabled;
- open after a bounded run of definite transient failures;
- half-open probes through separately admitted real calls;
- authentication and invalid-configuration failures hold the endpoint open
  until configuration changes;
- ambiguous calls do not count as safe transient failures.

Authentication and configuration state is scoped to the immutable configuration
snapshot that recorded the failure. Publishing a replacement snapshot clears
that open state for planning even before the route receives a new outcome;
health observations without a snapshot identity remain process-local diagnostic
views only.

Health state influences eligibility but never changes class, bypasses residency,
or weakens capabilities. Memory health is per process; optional Redis health is
an optimization and is not required for correctness.

## Required observability

Each plan and attempt emits only safe identifiers:

- configuration snapshot and route plan digest;
- rejection reason counts by capability;
- requested, attempted, and actual service class;
- route/endpoint/model, fallback class index, and route index;
- definite/ambiguous outcome and operation ID;
- continuation pinned/portable decision;
- durations for compile, admission, queue, provider, and finalize.
