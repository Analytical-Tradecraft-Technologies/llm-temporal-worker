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
planner or touching either repository. An enabled policy binds the positive
`MaxAgeSeconds` from the existing v1 envelope to successful completion age and
requires the Generate sample index to match `Cache.Variant`. Compact uses zero
and a separate domain. Invalid requests and plans fail before repository access.
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

Phase factories still need to write content blobs, construct checkpoints and
call this finalizer, and recover the interval between provider completion and
publication-plan persistence. This change does not activate production cloud
phase factories or remove the remaining SQL path. A missing handoff and plan
are not permission to dispatch again: the inner
runner's replay and single-use Redis claim remain responsible for that interval.
No automatic release is performed on runner errors, and a started fill never
becomes dispatchable just because time has passed. See
[fill ownership](cloud-cache-fills.md).

Workflow timers, provider polling/recovery composition, and removal of the
remaining SQL runtime dependencies are follow-ups. The tests exercise the real
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
