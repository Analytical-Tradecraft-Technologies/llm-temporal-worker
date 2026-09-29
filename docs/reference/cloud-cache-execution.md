# Cloud cache preparation in the activity runners

`storage/durable.ResponseCache` connects the cloud response/fill repositories to
the existing Generate and Compact runners. Phase factories construct it with
their snapshot's `Responses`, `ResponseFills`, and `Clock` capabilities:

```go
responseCache, err := durable.NewResponseCache(
    capabilities.Responses, capabilities.ResponseFills, capabilities.Clock,
)
```

The Generate `CacheLookup` callback calls `PrepareGenerate(ctx, lease, maxAge)`;
Compact calls `PrepareCompact`. The lease must already be persisted, with its
`Attempt` equal to the Redis budget generation ID. Reuse the identical lease on
an uncertain acquisition. A new chargeable attempt uses a new generation.

Before preparing, the operation's replay/recovery phase must resolve any prior
paid work. The phase factory must derive the opaque scope from authenticated
context, authorize the complete route, and build the semantic fingerprint.
For compaction, that fingerprint covers source content and policy versions.
The Generate sample index must match the request's variant; Compact uses zero.
`nil` maximum age means unrestricted completion age at this internal boundary;
this PR does not change the existing public v1 cache-policy JSON schema.

| Preparation | Runner behavior |
| --- | --- |
| Hit | Decode the origin response and call `FinalizeCache`. No budget or provider call. `decision.Entry()` provides a copy of the origin metadata for the use receipt. |
| Owned miss | Check the eventual route and budget generation, reserve budget, atomically start the fill, claim the Redis authorization, then dispatch. |
| Wait | Return `ErrCacheWait` with the existing typed retry-after contract before routing, compaction, or admission. No activity sleeps. |
| Recovery needed or attempt finished | Return `ErrCacheRecoveryRequired`. A retry must enter operation recovery, not make another paid submission. |

The eventual `RoutePlan.CacheIdentity` must exactly equal the identity used for
lookup, including provider, endpoint, account, region, model revision, and
compiler. The operation/generation and provider/endpoint/model fields must
also match. If compaction or replanning changes that identity, the runner fails
before admission; the phase factory must prepare the appropriate key again.

Only the invocation receiving a definite successful `Start` acknowledgement
may reach Redis claim and dispatch. The grant is never an activity result or
reusable workflow token. A duplicate start, uncertain write, expired lease, or
backwards clock stops submission. A budget denial leaves the fill held so it
can be retried or expire. If start stops after reservation, the runner leaves
the unclaimed Redis reservation to its normal expiry: refunding it here could
interfere with another invocation using the same budget generation.

Once start may have committed, errors (including a failed budget claim) leave
the attempt for recovery. The cache gate does not infer whether paid work
happened or release charged budget. Recovery can resolve an unknown outcome
while retaining its charge; a subsequent attempt must acquire fresh budget.

## Completion and remaining composition

This is the preparation/dispatch gate, not full production cache activation.
Deployments still supply the phase factories. Existing custom cache ports keep
their behavior; using these helpers opts a phase into the cloud gate. Public
activity names and v1 envelopes are unchanged.

The durable finalizers still need to construct and commit consumer/origin
checkpoints, publish successful entries, record idempotent use receipts,
reconcile Redis, and complete fills in the documented order. Completed or
reconciliation-pending replay must finish that work before returning. Incomplete
responses cannot be published as successes. No automatic release is performed
on runner errors, and a started fill never becomes dispatchable just because
time has passed. See [fill ownership](cloud-cache-fills.md).

Workflow timers, provider polling/recovery composition, and removal of the
remaining SQL runtime dependencies are follow-ups. The tests exercise the real
Generate/Compact runners and cloud cache implementation with shared in-memory
KV/blob adapters and counted Redis/provider ports. They cover concurrent
independent misses and retries, restart/expiry, lost start acknowledgements,
fresh admission after a resolved unknown attempt, route fences, and safe
short-circuiting. They do not establish live AWS, Redis, or Temporal behavior.
