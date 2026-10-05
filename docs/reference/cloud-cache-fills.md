# Cloud cache-fill coordination

`cache.FillRepository` coordinates an exact response-cache fill across workers
using the generic cloud KV/blob contracts. The default AWS stores are DynamoDB
and S3. `cloudstate.Repository.ResponseFills()` shares the request repository's
namespace, encryption key and stores. Snapshots expose it through
`V1RuntimeCapabilities.ResponseFills`; a configured cloud factory without this
capability fails construction and drains its clients.

`cache.Prepare` composes response lookup with fill acquisition. It returns
immediately with an origin response template, ownership, a wait decision, or a
recovery decision. It rechecks the cache after acquisition to cover publication
between the first miss and acquisition. An unstarted owner releases its fill
when that second read finds a success. Storage errors cannot silently become
paid cache misses.

This is an internal coordination building block. The
[response cache execution adapter](cloud-cache-execution.md) connects its
decisions to the Generate/Compact runners' dispatch gates. Deployment phase
factories, workflow timers and consuming cache-replay checkpoints still need
composition. The public v1 API and cache validation are unchanged.
Production phase/finalizer composition
remains tracked by
[#815](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/815)
under [#812](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/812).

## Identity and transitions

A lease contains the full `ResponseKey`, operation ID, unique attempt ID,
acquisition time and expiry. The key isolates authenticated scope,
Generate/Compact, resolved provider route, semantic fingerprint and request
index. Persist the proposed lease and retain identical values across uncertain
acquisition retries. New attempts require fresh attempt IDs and acquisition
times. Times come from a trusted worker clock. IDs are not authorization.

| Current state | Acquisition result or allowed transition |
| --- | --- |
| Absent | Conditionally create `held`; one owner wins. |
| Held, unexpired | Same attempt retains ownership; another attempt waits. |
| Held, expired | A newer attempt may replace it conditionally, fencing the old owner. |
| Started | Recovery required; expiry never permits takeover. |
| Released | Only an unstarted owner may release; another unexpired attempt can acquire. |
| Finished | The resolved attempt is immutable; another unexpired attempt can acquire. |

An attempt can replace a released or finished record when its own lease had
not expired at that record's terminal time. This includes a waiter created
before the owner ended without publishing: it owns the next fill on its next
acquisition, not after its attempt is renewed. A lease that had already
expired by then conflicts, and the conflict returns the current record. That
fences an owner whose held lease was taken over, because its successor can
only have acquired, and so ended, after the old lease expired.

The pre-dispatch lease is bounded at 15 minutes. It is distinct from the Redis
budget reservation and does not extend that reservation's deadline. There is
no lease-renewal loop. Started work stays pinned until explicitly resolved.

`Start` conditionally changes `held` to `started`. **Only the invocation that
receives `true` may proceed to provider dispatch**, with Redis authorization
and the operation's durable dispatch checks. A repeated call returns `false`,
including after restart. An unknown write result also requires recovery. Never
reconstruct permission to dispatch from a stored `started` record. Definite
conditional conflicts receive bounded local retries; uncertain writes surface
immediately.
Call `Start` inside the activity invocation that submits. Its boolean must not
be returned by a separate activity and replayed as a reusable dispatch token.

This fences concurrent starts but cannot provide exactly-once provider effects.
A process can die between the start marker and submission, or submission can
succeed without a response. Recovery must inspect durable operation/provider
context; absence of a provider identifier does not prove submission failed.

## Workflow and finalizer responsibilities

1. Replay the logical operation and recover existing paid attempts first.
   `Prepare` is for a new, undispatched attempt. A cache hit must never abandon
   started work or its budget reservation.
2. Materialize and authorize the request; resolve its route and fingerprint.
3. Call `Prepare`. For a hit, create a distinct zero-cost response/checkpoint
   and use receipt. Never return the origin response unchanged. For a wait,
   return to a workflow timer instead of sleeping inside an activity.
4. For ownership, obtain budget and persist attempt recovery context before
   `Start`. Only one successful start invocation may submit. If ownership was
   lost, reconcile the unused reservation through its original Redis receipt.
5. `recovery_needed` identifies the existing owner. Observers must wait for
   live work or arrange operation recovery. Lease expiry alone does not justify
   declaring that owner's outcome unknown.
6. After resolving provider state and reconciling the original budget, call
   `Complete` with a stable outcome and timestamp. For `published`, commit the
   origin checkpoint and publish the cache success first. Completion verifies
   the key, origin operation, timestamps and visible success pointer, allowing
   that entry to have been superseded by a newer success.
7. Other terminal outcomes are `not_cacheable`, `failed`, and `outcome_unknown`.
   They do not invalidate an existing cache success. Unknown paid work stays
   charged; retrying it needs a **new attempt and fresh Redis budget**.

`Complete` records the finalizer's decision; it cannot independently verify
provider resolution or Redis settlement. Those remain caller obligations.
There is no public cancellation API or automatic recovery sweep.

## Persistence and verification

Fill states live in bounded encrypted immutable blobs. The table contains only
a versioned pointer under an HMAC-derived `cache/fill` key. Each update writes
its blob before conditionally replacing the pointer. Prompt, route, scope,
operation and attempt identifiers are not plaintext table metadata.

Released/finished attempts receive an immutable `cache/fill-receipt` pointer
to their terminal blob. Acquisition preserves this receipt before replacing a
terminal head, so a finalizer can reconcile a lost acknowledgement after a new
attempt starts. Changed terminal outcomes conflict. Receipt publication can
also fail or have an unknown outcome and must be retried.

There is no independent TTL or garbage collection. Removing heads or receipts
while operations can retry would break their fences. Losing conditional writes
can leave orphan encrypted blobs; cleanup and managed secrets are separate
follow-ups. Missing referenced blobs and corruption fail closed.

Offline tests use shared linearizable KV/blob doubles across independent
repository instances. They cover 100 simultaneous misses and 100 same-attempt
retries yielding one synthetic dispatch grant, expiry/start races, stale owners,
restart recovery, lost acknowledgements at blob/head/receipt writes, takeover
during finalization recovery, bounded contention, publication races, key
isolation, corruption and encryption. They also verify runtime capability
binding. They do not establish live AWS/Temporal behavior or production phase
activation.
