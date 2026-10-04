# Cloud cache preparation in the activity runners

The snapshot-bound `V1RuntimeCapabilities.NewResponseCacheLookup` helper supplies
the Generate and Compact `CacheLookup` callbacks. Construct it inside the complete
runtime builder's phase factories, after the composition has been validated:

```go
lookup, err := capabilities.NewResponseCacheLookup(generatePlanner, compactPlanner)
// Handle err before installing either callback.
generatePorts.CacheLookup = lookup.Generate
compactPorts.CacheLookup = lookup.Compact
```

The typed planners receive the original envelope and a `PreparedGenerateInput`
or `PreparedCompactInput`, then return a persisted `cache.FillLease`. Preparation
combines checkpoint history and sparse settings with the current delta before
an enabled lookup can reach its planner. See [shared request preparation](cloud-request-preparation.md)
for the effective input, exact decimal settings, and compaction prefix contract.
The [shared provider planner](cloud-provider-planning.md) supplies the selected
candidate, compiled call and complete route identity for these planners and
the subsequent admission/dispatch composition.
Planners must authorize the opaque scope and complete
route and compute the semantic fingerprint. `Attempt` must equal the Redis
budget generation ID. Reuse the identical lease on an uncertain acquisition;
a new chargeable attempt uses a new generation. The helper does not create
clients, invoke the composition factory again, or keep per-request state. It
retains its repositories and trusted clock across configuration reloads.

An omitted request cache policy returns `CacheDisabled` without calling a
planner or touching either repository. An enabled policy binds optional positive
`MaxAgeSeconds` to successful completion age (omission means unrestricted age)
and requires the sample index to match `Cache.Variant` for both Generate and
Compact, in separate domains. Invalid requests and plans fail before repository access.
An unresolved generation tool frontier also stops before planning. Compact with
no safe prefix fails before acquiring a fill; its surrounding replay/phase
composition must handle that no-work case before routing or budget admission.
Planner and storage error text is excluded from the returned provider error.
A failed lookup or uncertain acquisition never becomes a miss: a storage error
requests a retry of the same operation, using the original persisted lease;
invalid plans and corrupt returned data are non-retryable configuration and
state errors, respectively. No provider submission is authorized by lookup.

Internally, `storage/durable.ResponseCache` implements preparation and retains
the fill gate used by the runners. Custom phase factories can still construct
it directly with `durable.NewResponseCache(responses, fills, clock)` and call
`PrepareGenerate(ctx, lease, maxAge)` or `PrepareCompact`. Its
`ErrResponseCacheInvalid` and `ErrResponseCacheCorrupt` markers distinguish
invalid preparation inputs from invalid returned cache data; repository errors
remain wrapped by the runtime lookup helper at the caller boundary.

Before preparing, the operation's replay/recovery phase must resolve any prior
paid work. The phase factory must derive the opaque scope from authenticated
context, authorize the complete route, and build the semantic fingerprint.
For compaction, that fingerprint covers source content and policy versions.
Both operations' sample indexes must match their request's variant, regardless
of temperature. `nil` maximum age means unrestricted completion age.

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

After a provider result is resolved, the finalizer first commits the origin
checkpoint. For a started fill, its reconciliation port then calls
`ResponseCache.CompleteAttempt` with the **persisted** lease, completion time,
outcome, and optional successful entry. It passes an idempotent callback that
settles the original Redis budget generation. The helper publishes a successful
entry, settles Redis, then completes the fill. A provider failure, incomplete
response, or outcome-unknown attempt has no entry; Redis settlement still comes
before the fill receipt. An unknown paid outcome remains charged. A later paid
attempt needs a new budget generation and fill attempt ID.

Every retry must use identical finalization values. An uncertain publication,
Redis settlement, or fill completion leaves the operation pending and retries
this sequence without another provider dispatch. `ReconciliationPending` is
the runner's replay path for that work; `Completed` must only be returned once
all required receipts are durable. The settlement callback must itself be
idempotent because an activity can repeat after Redis settled but before the
fill receipt committed. The cloud repositories validate the committed
checkpoint, preserve newer successes, and reject an unstarted fill completion.

For a hit, `FinalizeCache` commits a distinct zero-cost consumer checkpoint,
then calls `ResponseCache.RecordUse` with `decision.Entry()` and a stable
`ResponseUse`. A lost receipt acknowledgement is retried with the identical
use. Replay must repair that receipt before reporting `Completed`; returning a
completed checkpoint early would silently skip cache accounting. A hit does
not reserve or settle Redis budget.

These helpers cover the preparation/dispatch gate and ordered finalization,
not full production cache activation.
Deployments still supply the phase factories. Existing custom cache ports keep
their behavior; using these helpers opts a phase into the cloud gate. Public
activity names and v1 envelopes are unchanged.

The cloud request repository exposes `SaveFinalizationHandoff` and
`LoadFinalizationHandoff`. The production factory now provides a snapshot-owned
`V1RuntimeCapabilities.Finalizer` (`CloudFinalizer`) backed by those methods,
the same cloud response/fill stores, and the snapshot's Redis budget authority.
Custom cloud factories must implement `CloudFinalizationStore` and
`CloudCheckpointFinalizationStore`.

A phase factory can call `CommitGenerate` or `CommitCompact` with a fully built
`state.DurableCheckpoint`, its validated response and `FinalizationEffects`.
Referenced content blobs must already exist. The helper first saves an encrypted,
immutable `CheckpointFinalization` plan in the running operation, then publishes
the checkpoint metadata, attaches the readable-checkpoint handoff and applies
the effects. Saving the plan makes the checkpoint commit and handoff persistence
recoverable across a restart. It does not mark the operation completed; the outer
runtime stores the terminal response after all effects succeed.

The repository exposes `SaveCheckpointFinalization`, `LoadCheckpointFinalization`
and `ResumeCheckpointFinalization` for that boundary. Identical retries retain the
original checkpoint metadata, timestamps, response and accounting inputs.
Competing plans conflict. The plan is replaced by its handoff after checkpoint
publication, avoiding duplicate large response payloads in the operation view.
The discovery index remains pending until the outer runtime completes the request.

For phases that already committed their checkpoint, `CompleteGenerate` and
`CompleteCompact` retain their existing handoff-and-effects behavior. These
methods cannot recover a crash between checkpoint commit and handoff save;
use the `Commit` methods to make that interval recoverable. Exactly one effects
path is allowed:

- Provider: the original fill lease, fill completion, optional successful cache
  entry, and the complete original Redis reconciliation request (including
  incarnation, event IDs, amounts and timestamps). The helper saves the encrypted
  handoff, publishes any eligible success, settles Redis, then completes the
  fill. Incomplete responses supply no entry. Valid application tool calls are
  eligible successes; the service does not execute them.
- Cache hit: the origin entry and distinct consumer use receipt. The helper
  saves the handoff and records the use without touching Redis budget.

`SaveGenerate` and `SaveCompact` persist the same typed handoffs without applying
any effects, for finalizers that need separate save/settlement callbacks.
Checkpoint scope/ID are trusted internal values. The handoff explicitly carries
the internal operation ID, which may differ from the caller's idempotency key.
The repository verifies checkpoint ownership and kind; repeated identical saves
repair uncertain acknowledgements, while changed handoffs conflict.

On a retry of a running cloud operation, the outer runtime loads and validates
its handoff or staged publication plan before invoking the inner runtime.
For a saved plan, it validates the typed response and receipts before resuming
checkpoint publication and handoff persistence. It then repeats the same
publication/settlement/receipt sequence and stores the terminal response, with
no routing, admission, claim or provider dispatch. It retains all original
accounting and completion timestamps across restarts and configuration reloads.
Malformed, incompatible or unreadable saved handoffs fail closed. In particular,
a missing referenced checkpoint cannot turn a saved handoff into a cache miss.

The bounded cloud runtime writes content blobs, constructs checkpoints and
calls this finalizer, recovering the interval between provider completion and
publication-plan persistence from saved provider execution. Production CLI
authorization still requires explicit composition. A missing handoff and plan
are not permission to dispatch again: the inner
runner's replay and single-use Redis claim remain responsible for that interval.
No automatic release is performed on runner errors, and a started fill never
becomes dispatchable just because time has passed. See
[fill ownership](cloud-cache-fills.md).

The registered workflows own timers and invoke bounded provider polling and
recovery; worker SQL dependencies have been removed. The tests exercise the real
Generate/Compact runners and cloud cache implementation with shared in-memory
KV/blob adapters and counted Redis/provider ports. They cover concurrent
independent misses and retries, restart/expiry, lost start acknowledgements,
fresh admission after a resolved unknown attempt, route fences, and safe
short-circuiting. Finalization tests additionally cover publication, budget
settlement, fill completion, cache-use receipts, uncertain writes, and restart
retries. Publication-plan tests inject failures before and after every checkpoint
metadata and operation/discovery-index write, exercise concurrent identical
retries and conflicting plans, and verify missing content remains pending.
Runtime tests cover both Generate and Compact, provider and cache-hit paths,
and reject invalid saved instructions before checkpoint publication or effects.
Lookup adapter tests additionally exercise both real runners, disabled policies,
sample and freshness binding, snapshot reloads, identical uncertain-acquisition
retries, corrupt origins, and storage failures before budget/provider work.
They do not establish live AWS, Redis, or Temporal behavior.


## Results from independent paid attempts

The bounded cloud execution path stores each paid attempt under its own child
request ID. `CloudFinalizer.LoadAttemptResult` requires an already authorized
scope and verifies the active child, immutable request manifest, saved
configuration, terminal provider result and completed budget settlement. It
returns the saved paid identity alongside a copy of the response projected to
the original public operation ID and key. Checkpoint publication uses that
public response; the underlying paid record keeps the child identity.

Set `ProviderFinalizationEffects.AttemptID` to that child ID. For this path,
`Lease.OperationID` is the public operation ID and `Lease.Attempt` is the child
ID. Redis generation remains the budget materialization generation; it is not
an attempt identifier. This differs from the legacy runner gate described
above. Finalization revalidates the saved child both before persisting a handoff
and before replaying its effects. The settlement must exactly match the child's
saved receipt, including its operation, generation and event identities.

When caching is disabled, set `Uncached`, omit the fill lease and entry, and use
a `FillNotCacheable` completion with the provider's saved completion time. The
helper performs only idempotent settlement. Free routes additionally set
`Unreserved` and have no settlement. Incomplete generated responses can finish
and settle, but cannot publish a successful cache entry. Cached paths retain
the publication, settlement and fill-completion order.

This finalization support does not activate production composition. The bounded
runtime must authorize each call, recover existing attempts, acquire any fill
lease, and publish the checkpoint before returning its completed response.

## Bounded cloud execution runtime

`V1RuntimeCapabilities.NewCloudExecutionRuntime` composes the authorized request
preparation, independent attempt records, cache, Redis budget boundary, provider
execution and checkpoint finalizer. Its required options are the scope resolver,
checkpoint signing keyring and retention period, materialization limits, and
Redis budget generation. It uses the snapshot's cloud identity, Redis identity
and budget leaser directly; it does not request legacy SQL composition ports.

The implementation satisfies `activity.ExecutionRuntime`. Preparation returns
`budget_required`, a completed cache hit, a no-work compaction, or a cache wait.
Acquisition tries Redis once and returns `acquired` or `budget_wait`. Generate
and Compact steps submit once, or resume an existing attempt. Polling performs
at most one provider read, respecting the saved next-poll time. Completion
publishes the checkpoint and reconciles cache/budget effects before returning
the typed response. Workflow timers are responsible for all waits.

Before any budget claim or provider submission, the winning durable execution
fence starts the cache fill. Losing the start acknowledgement cannot authorize
another submission. After the 15-minute recovery interval an explicit acquire
step can replace an unknown attempt with a separately charged child; the old
child remains discoverable. An unused attempt can also expire before its quote
was written, and is safely replaced without contacting a provider.

Cache fingerprints cover the complete normalized semantic request, route,
configuration, capability/compiler versions and request index. Compaction also
includes policy and prompt versions. A content digest permits large transcripts
without exceeding the smaller cache-manifest bound. Operation keys, actors,
service-class controls and lineage handles are excluded from semantic identity;
scopes and resolved provider routes remain isolated. Compaction reuse has no age
restriction; generation applies the caller's completion-age limit.

Known provider failures and invalid compaction summaries finish their cache
fills without publishing a successful entry. A truncated generation is returned
and settled without caching it as success. Independent cache consumers get new
public request IDs and signed checkpoints. Every entry point, including terminal
replay, invokes the supplied scope resolver before storage access.

Integration-style tests use the actual encrypted cloud repositories and budget
reference model with conditional in-memory cloud stores. They cover synchronous
and polling providers, both request kinds, no-work and free paths, restart,
concurrent submissions, budget/cache waits, large semantic inputs, sample
isolation, uncertain paid work, lost start acknowledgements, expired unused
attempts and authorization. These tests do not establish live dependency behavior.
Worker startup registers the Temporal workflows. Production activation still
requires explicit CLI authorization and deployment verification.
