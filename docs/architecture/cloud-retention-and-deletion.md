# Cloud request retention and safe blob deletion

Status: design for [#818](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/818),
tracked by [#812](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/812).
Nothing in this document is implemented yet. The initial production deployment
retains everything. This design does not block that deployment. Each phase
below is a separate, opt-in exit with its own evidence.

Unknown-cost reconciliation is designed separately in
[unknown-cost reconciliation](unknown-cost-reconciliation.md) (#819). That
document defines the reconciliation obligations this one must protect.

## Goals and non-goals

Goals:

- reclaim storage for terminal requests, attempts, superseded event payloads,
  expired checkpoints, unreachable cache artifacts and orphaned blobs, but only
  once every retry, recovery, cache, lineage and accounting obligation for them
  has ended;
- work in bounded pages and keep making progress past protected records;
- delete blobs only after a reference fence, with idempotent claims, a durable
  completion record, and recovery from interrupted or ambiguous deletes;
- prove that a late writer cannot attach a reference after the fence, without
  cross-resource SQL transactions, using only the single-key linearizable
  conditional operations of the generic KV contract;
- run under maintenance credentials that the request-serving worker never
  holds.

Non-goals:

- table TTLs, S3 lifecycle rules or any other deletion that does not recheck
  references. No such rule may target worker data, now or later;
- a cancellation API, SQL migration, SQL fallback or budget journal;
- Redis key retention. Redis operation records and settlement deduplication
  entries currently have no TTL (see
  [state and storage](state-and-storage.md#retention-and-recovery)). Their
  cleanup belongs to the Redis budget work in #814 and is constrained by #819.

## What exists today

All claims in this section describe current code.

### Storage contracts

The generic contracts come from the `cloud-storage` module
(`providercontracts/kv`, `providercontracts/blob`, `eventsourcing`):

- `kv.KeyValueStore` offers `Get`, `QueryPartition`, `Create` (only if
  absent), `Replace` (only at an expected version) and `Delete` (only at an
  expected version; a missing key returns `ErrConflict`). Single-key
  operations are linearizable. `QueryPartition` reads one partition in sort-key
  order. Its pages are strongly consistent but are not an atomic snapshot:
  "concurrent inserts behind the cursor may be missed". An uncertain mutation
  reports `ErrOutcomeUnknown`. There is no multi-item transaction and no
  cross-partition scan.
- `blob.BlobStore` offers `Create` (only if absent), `Open` and `Delete`.
  `Delete` is unconditional by key. A missing blob is a successful no-op. A
  retried delete "can delete a newly created blob if the key was reused". An
  unconfirmed delete reports `ErrOutcomeUnknown`. There is no listing.
- `eventsourcing.EventStreamStore.TryAppend` creates the row for revision
  `expected+1`. When `expected != 0` it first requires the row for `expected`
  to exist. Revision 1 has no predecessor check. The store has no delete
  operation.

### Records and keys written by `cloudstate`

All keys live under the repository namespace
([`repository.go`](../../golang/storage/cloudstate/repository.go)). Every
payload is an AES-GCM blob whose key is
`<ns>/payload/<HMAC(stream, plaintext)>` (`Repository.blobKey` and
`writeBlob` in [`crypto.go`](../../golang/storage/cloudstate/crypto.go)).
Blob keys are therefore content-addressed per stream. Writing identical bytes
to the same stream yields the same key, and `writeBlob` treats
`ErrAlreadyExists` as success after re-reading and comparing the stored
object. **Two independent publications can share one blob key.** That is the
central hazard for deletion.

| Durable item | Key / partition | Mutability | Blob references |
| --- | --- | --- | --- |
| Request/attempt event stream | `<ns>/request/<id>`, one row per revision (`Repository.stream`, `publish`) | append-only; completed/failed are immutable (`validTransition` in [`model.go`](../../golang/storage/cloudstate/model.go)) | every revision's `recordPointer.Blob`, re-read by `readRevision` when a retry reconciles an identical `TryUpdate` |
| Pending discovery index | `<ns>/pending/<0..7>`, sort key = request ID ([`pending.go`](../../golang/storage/cloudstate/pending.go)) | `Replace` by version; terminal rows are kept and filtered by `ListPending` | none |
| Operation identity | request ID derived from `HMAC(scope, kind, operation key)` (`Repository.operationID` in [`operation.go`](../../golang/storage/cloudstate/operation.go)) | `BeginOperation` returns the existing record on replay | via the request stream |
| Request attempts | separate request records linked from the root's `request_attempt` progress ([`request_attempt.go`](../../golang/storage/cloudstate/request_attempt.go)) | as request streams; unknown paid attempts keep their own pending row | via their streams |
| Request parent snapshots | stream `request-parent/<scope tag>` ([`request_preparation.go`](../../golang/storage/cloudstate/request_preparation.go)) | immutable | `request_preparation.parent_snapshot_ref` in every later revision of the root and of each attempt child; **deduplicated across requests of one scope**. A `blob` edge from each referencing request revision, like the record blobs. Older preparations embed the snapshot inline instead |
| Provider execution, budget plan, finalization hand-off | progress fields inside the attempt stream ([`provider_execution.go`](../../golang/storage/cloudstate/provider_execution.go), [`budget_plan.go`](../../golang/storage/cloudstate/budget_plan.go), [`finalization_handoff.go`](../../golang/storage/cloudstate/finalization_handoff.go)) | `validExecutionTransition` | via the stream |
| Checkpoint rows | `<ns>/checkpoint/{id,handle,operation}/<tag>` ([`checkpoint.go`](../../golang/storage/cloudstate/checkpoint.go)) | `Create` only | `checkpointPointer.Blob` (stream `checkpoint/<id tag>`) |
| Checkpoint content blobs | stream `checkpoint-blob/<HMAC(scope, media type)>` ([`checkpoint_blob.go`](../../golang/storage/cloudstate/checkpoint_blob.go)) | immutable | delta, response, settings patch, snapshot and provider-state references inside `state.DurableCheckpoint`; **deduplicated across checkpoints of one scope** |
| Cache success head | `<ns>/cache/success/<digest>` ([`response_cache.go`](../../golang/storage/cloudstate/response_cache.go)) | `Replace` to a newer entry | points at an entry blob |
| Cache entries | `<ns>/cache/entry/<digest>` | `Create` only | entry blob; `ResponseEntry.OriginCheckpointID` names a checkpoint |
| Cache uses | `<ns>/cache/use/<digest(scope, operation)>` | `Create` only | use blob naming `EntryID` and `CheckpointID` |
| Cache fills and receipts | `<ns>/cache/fill/...`, `<ns>/cache/fill-receipt/...` ([`cache_fill.go`](../../golang/storage/cloudstate/cache_fill.go), [`cache_fill_receipt.go`](../../golang/storage/cloudstate/cache_fill_receipt.go)) | fill head `Replace`; receipt `Create` | each fill revision writes a new blob; a replaced head leaves its old blob unreferenced unless a receipt seals it |
| Budget initialization receipt | `<ns>/budget-initialization` ([`budget_initialization.go`](../../golang/storage/cloudstate/budget_initialization.go)) | permanent, no delete | none |

No `cloudstate` or `internal/runtime` code calls `blob.BlobStore.Delete` or
`kv.KeyValueStore.Delete`. The only `Delete` in `cloudstate` is the pass-through
in `regionalTable.Delete`. `regionalBlobs.Delete` returns `ErrUnsupported`
because a partial regional delete cannot uphold the generic contract, and
`regionalBlobs.Open` falls back to a replica when the primary reports a missing
object ([`regional.go`](../../golang/storage/cloudstate/regional.go)).

Readers fail a committed reference whose object is gone as `ErrCorrupt`
(`Repository.readReferencedBlob`), never as "not found". A wrong deletion is
therefore visible corruption, not a silent cache miss
([blob garbage collection](../reference/blob-garbage-collection.md)).

### Checkpoint lineage and expiry

`state.DurableCheckpoint` carries `ParentID`, `CompactedThroughID`,
`OriginCacheEntryID`, its blob references and `ExpiresAt`
([`checkpoint_port.go`](../../golang/state/checkpoint_port.go)). Expired rows
"remain valid immutable history". `checkpointStore.publish` calls
`validateReferences`, which reads the parent, `CompactedThroughID` and every
referenced blob, and *then* writes the new rows. That check-then-publish
sequence is the race a deletion fence has to close.

After #1206, the durable materializer checks expiry only on the requested
checkpoint. Ancestors past their own deadline stay readable through live
descendants, and reading stops at the newest snapshot row
([checkpoint materializer](checkpoint-materializer.md#stopping-at-the-newest-snapshot)).
Retention cannot delete a checkpoint just because its own `expires_at` has
passed. There is no child index today: from a checkpoint, nothing lists its
descendants.

### Existing maintenance scaffolding

[`golang/maintenance`](../../golang/maintenance/retention.go) holds
storage-neutral contracts with in-memory adapters only. No production code
imports it.

- `RetentionRecord` / `RetentionRecord.Eligible` express the right predicates:
  `Active`, `HasRetainedDescendant`, `HasActiveFill`, `HasActiveUse` and
  `HasExternalBlobReference` each veto eligibility.
  `HasCandidateBlobReference` (a blob owned by the candidate) does not.
- `RetentionPolicy` bounds a pass (`Limit` 1 to 10000) with a cutoff per
  `ResourceKind`.
- `OutboxStore` / `Dispatcher` ([`outbox.go`](../../golang/maintenance/outbox.go))
  implement leased claims with a random `LeaseToken`, `MaxOutboxLease` of 24h,
  and `EventDeleteBlob` events whose payload is only `{"blob_id": ...}`.
  `Dispatcher.RunOnce` completes an event when the delete handler succeeds or
  returns `ErrObjectNotFound`, and otherwise schedules a retry.

The comments still assume a SQL row lock ("FOR UPDATE SKIP LOCKED", "share one
transaction"). This design keeps the predicates and the lease-token idea. It
replaces the transactional assumptions with the KV-only protocol below.

## Design overview

```
                 +-------------------------------+
 request path    |  reference edges + catalog    |  written BEFORE a reference
 (worker role)   |  (Create only, small rows)    |  is published (new)
                 +---------------+---------------+
                                 |
 maintenance     +---------------v---------------+     +---------------------+
 role, phase A:  | retention sweeper             | --> | retire / release    |
 KV cleanup      | bounded pages over catalogs   |     | conditional writes  |
                 +---------------+---------------+     +---------------------+
                                 | blob candidates
 maintenance     +---------------v---------------+     +---------------------+
 role, phase B:  | blob deletion ledger          | --> | BlobStore.Delete in |
 blob deleter    | fence -> verify -> delete ->  |     | every replica       |
                 | durable completion            |     +---------------------+
                 +-------------------------------+
```

The design has three parts:

1. **Reference edges and a retention catalog** (written by the request path).
   Every publication that makes a durable item reference a shared object
   first creates an *edge row* in the referent's edge partition. Every newly
   published retainable item also gets a *catalog row* in a time-bucketed
   partition, so a sweep can enumerate candidates without scans.
2. **Generic-KV conditional cleanup** (maintenance role). This retires
   terminal requests, expired checkpoints and unreachable cache artifacts with
   conditional writes, and releases their edges.
3. **Blob deletion** (a separately operated maintenance role). A per-blob
   ledger item fences new references, rechecks edges, deletes in every
   replica, and records completion durably.

Every new item uses the existing encodings: JSON in a single `kv.Bytes`
field, keyed digests from `Repository.digest`, and encrypted blobs for
anything that is not an opaque identifier.

## Eligibility rules

A record is eligible only when all of the following hold. The sweeper checks
them at the moment of the conditional write that retires it (a recheck), never
from an earlier page.

1. Its retention clock has passed under an explicit, configured policy. There
   is no default horizon. A kind with no configured horizon is never touched,
   which matches `RetentionPolicy` today.
2. It is in a terminal state that admits no further legitimate writer (see
   the table below).
3. None of the protections in the next section applies.
4. It was published after the **reference-index epoch** (see
   [Rollout gate](#rollout-gate-the-reference-index-epoch)). Records without
   a catalog row are never candidates.

### Protected records

None of the following may be retired or deleted, whatever their age:

| Protection | How it is detected |
| --- | --- |
| Non-terminal requests and attempts: `pending`, `running`, `provider_pending`, `outcome_unknown` | `Record.Status` is not `completed`/`failed` (`Status.terminal`) |
| Unknown or ambiguous paid attempts | `ProviderExecution.Stage == ExecutionUnknown`, or a settlement containing `finalize_unknown` events (`executionSettlement`), until #819 records an applied resolution or the operator-approved unknown-cost horizon passes. The attempt's pending row stays. |
| Unsettled paid work | `ProviderExecution.Settlement != nil && !Settled` |
| Pending provider jobs | `ProviderExecution.Stage == ExecutionPending`, or `ProviderOperationID != ""` without a terminal stage |
| Claims inside the recovery window | `Stage` is `claiming`/`submitting` (`RecoverAfter` = `StartedAt + durable.BudgetStartLease`) |
| Unfinished finalization | a `FinalizationHandoff` or `CheckpointFinalization` without its completion, or a root whose `CompleteOperation` has not committed |
| Roots with a live attempt chain | any attempt (root `request_attempt` and `PreviousID` chain) that is itself protected |
| **Checkpoint ancestors read by live descendants** | any unreleased child edge in the checkpoint's lineage-edge partition (below). This covers ancestors older than a snapshot too: phase 1 keeps the full ancestor chain of every live checkpoint, not only the rows up to its nearest snapshot. |
| Checkpoints named by `CompactedThroughID` of a retained checkpoint | an edge of kind `compacted_through` |
| Checkpoints and blobs named by provider state (`CheckpointProviderState.StateBlob`) of a retained checkpoint | blob edges from that checkpoint |
| Origin checkpoints of retained cache entries | an edge of kind `cache_origin` from the entry to `OriginCheckpointID` |
| Active cache fills | fill head in `held`/`started`, or a terminal fill whose receipt (`fill-receipt`) has not been sealed |
| Active cache uses | a use edge from an operation whose request is not terminal |
| The current cache success head | a live `cache_head` edge. `responseCache.Publish` creates this edge and passes the entry's fence check (veto) before it creates or replaces the head. Superseded entries may become eligible once that edge is released. |
| Referenced blobs | any unreleased blob edge, and any ledger item not in `deleted` state for a different generation (see blob protocol) |
| Unknown-cost receipts and their attempts | a #819 resolution receipt without its applied marker; and every receipt for the configured audit horizon |
| Budget initialization receipt | never eligible |

The snapshot cut is an optional later optimization: deleting ancestors older
than the newest snapshot on every descendant path. It must first prove that
no reader (materializer, `validateReferences`, compaction, cache replay,
lineage-digest verification) touches those rows. Until that proof exists and
is tested, it stays out of scope.

### Terminal states per kind

| Kind | Retire when |
| --- | --- |
| Root request | status `completed` or `failed`; every attempt in its chain eligible; operation-key late-retry horizon configured |
| Attempt request | status `completed`/`failed`; execution `Settled`; if it carried `finalize_unknown`, see #819 |
| Checkpoint | `ExpiresAt` older than the checkpoint cutoff **and** zero unreleased child, `compacted_through` and `cache_origin` edges |
| Cache entry | zero unreleased `cache_head` and `cache_use` edges; older than the cache cutoff |
| Cache use | consuming operation terminal; older than the cache cutoff |
| Fill revision blob | head is terminal and sealed, and the blob is not the sealed receipt's blob |
| Orphan blob | catalog row older than the orphan grace and no edges (blob protocol) |

## Reference edges and catalogs (request path)

### Edge rows

An edge records "referrer R uses referent T". It lives in the referent's own
partition:

```
partition: <ns>/edge/<referent kind>/<referent tag>
sort key:  <referrer kind>/<referrer tag>
fields:    edge = {"v":1,"kind":...,"created_at":...,"state":"live"|"released"}
```

Edges are written with `Create` before the referrer's publication point.
An existing identical edge counts as success, which keeps them idempotent
under retries. Edge kinds:

- `blob`: referrer is a request revision, checkpoint, cache entry, cache use,
  fill receipt or unknown-cost receipt; referent is a blob key;
- `parent`, `compacted_through`: referrer is a child checkpoint; referent is a
  checkpoint;
- `cache_origin`: referrer is a cache entry; referent is its origin
  checkpoint;
- `cache_use`: referrer is a consuming operation; referent is a cache entry;
- `cache_head`: referrer is a `cache/success` head row; referent is the cache
  entry that the head points at, or is about to point at.

The sweeper releases an edge (`Replace` to `released`, then `Delete` at that
version) only after its referrer has been retired. A crashed writer that
created an edge and never published leaves a live edge. That leak is safe: it
protects the referent. The sweeper may release such an edge only after it
proves the publication can never happen. For example, the referrer's stream
has advanced past the revision the edge names with a different blob, or the
referrer has itself been retired. Age alone never releases an edge.

### Catalog rows

```
partition: <ns>/retention/<kind>/<UTC day>/<shard 0..15>
sort key:  <item tag>
```

A catalog row is created before the item's first write. Blobs get one before
`writeBlob` calls `BlobStore.Create`, so interrupted publications leave a
discoverable orphan.

Blob keys are content-addressed, so the first write after the epoch can
produce bytes identical to a blob written before it. `BlobStore.Create` then
reports `ErrAlreadyExists`, and the existing object may still be referenced by
data from before the epoch that has no edge rows. A blob catalog row therefore
starts as `unconfirmed`. The writer replaces it with `created` (conditional
`Replace`) only after its own `BlobStore.Create` returned success. Only a
`created` row whose creation time is after the reference-index epoch makes a
blob a deletion candidate. Rows for deduplicated blobs (`ErrAlreadyExists`)
and rows whose `Create` outcome was unknown stay `unconfirmed`. They are
ineligible, even when a later retry or another writer observes the object,
because nothing proves that this key had no earlier, unindexed referrers. A
crash between a successful `Create` and the confirmation also leaves the row
`unconfirmed`. That case leaks storage but loses no data. Such blobs are
retained until a separately approved backfill indexes all earlier references
to them. Requests reuse the existing pending index, because its
eight partitions already enumerate every request, with terminal rows kept.
The day bucket bounds each sweep to partitions whose items are old enough.
The shard bounds partition size.

### Cost

Each reference publication adds one `Create` per edge and one `Get` on the
referent's fence (below). Each new blob adds one catalog `Create`. Phase 0
measures this on the real DynamoDB adapter before anything is enabled. The
measurements are WCU/RCU per Generate and per Compact, hot-partition risk on
the edge partitions of popular shared blobs, and the S3 request mix.

## Generic-KV conditional cleanup

### Requests: logical retirement first

Physical deletion of event rows cannot be made safe with today's
`TryAppend`. Revision 1 is created without a predecessor check, and a later
revision only checks that its predecessor row exists. Deleting rows in any
order therefore lets a delayed writer recreate a row. So retention retires
requests **logically**:

1. Add a new record status `retired`, reachable only from `completed` or
   `failed`, in `validTransition` and `buildState`. Retirement is an ordinary
   `TryUpdate` at `ExpectedRevision = head`. It linearizes with every other
   writer through the single event row for `head+1`: a concurrent writer at
   the same expected revision gets `ErrConflict`, and terminal records
   already reject every non-identical update.
2. The retired revision points at a tiny tombstone payload: request ID, binding
   digest, retired-at, prior terminal status and the policy version. It holds
   no request content.
3. Reads change as follows. `Read`, `ReadForRecovery` and `BeginOperation`
   return a new definitive `ErrRetired` for a retired head and never load
   earlier payloads. `TryUpdate` on a retired stream returns `ErrRetired`
   instead of reconciling through `readRevision`. `Create` (via
   `ensureIndex`) on an ID whose index row is retired returns `ErrRetired`.
4. The pending index row is replaced (by version) with status `retired`.
   `ListPending` keeps filtering it.
5. Earlier revision payload blobs then become blob-deletion candidates, each
   released through its `blob` edge.

`ErrRetired` must map to a non-retryable, content-free activity error. It must
never map to "not found": a caller treating it as absent would start new paid
work for an operation key whose result was already delivered.

### Late-retry tombstones

The retired head row, the revision-1 row and the retired pending row together
form the **late-retry tombstone**. They are small rows that only hold pointers
and digests. They guarantee three things:

- a Temporal activity retry or a zombie worker that read the record before
  retirement cannot append, because the next revision row exists;
- `BeginOperation` with a reused operation key cannot create a fresh record,
  because the derived ID's index row and stream exist and are retired;
- a stale `Create` cannot recreate revision 1, because it already exists.

**Retention of tombstones:** indefinite by default. The tombstone may be
dropped only under a separately configured `operation_key_reuse_horizon`. The
operator must declare that callers never reuse an operation key after that
horizon. Dropping a tombstone re-enables a fresh, separately budgeted
execution for a reused key, so it is an explicit policy and never a side
effect of request retention. Dropping tombstones physically also needs a
guarded create in the KV contract: a conditional create that also checks a
fence item, which DynamoDB supports with `TransactWriteItems` and a
`ConditionCheck`. The generic contract has no such operation today. Until it
does, tombstones are kept. Event rows between revision 2 and the head are
pointer-only (≤ 4 KiB each, bounded by `maxRevisions`) and stay with the
tombstone in phase 1.

### Checkpoints: post-order release

Checkpoints are retired leaves first:

1. Page the checkpoint catalog for day buckets older than the cutoff.
2. For each checkpoint C: read its rows. Skip C unless `ExpiresAt` is before
   the cutoff. Query C's edge partition and skip C if any edge is `live`.
3. Write C's fence: `Create <ns>/fence/checkpoint/<tag>` with state `fenced`
   (see the protocol below; checkpoints use the same fence as blobs).
4. Re-query C's edge partition. If any edge is `live`, `Replace` the fence to
   `released` and skip C.
5. `Replace` the fence `fenced -> retiring` at the version returned by step 3.
   A writer's veto changes that version, so this step fails if any writer
   vetoed.
6. Delete C's `id`, `handle` and `operation` rows (`Delete` at the versions
   read in step 2), then release C's own outgoing edges: `parent`,
   `compacted_through`, and `blob` edges in the referents' partitions.
7. `Replace` the fence to `retired`. The fence stays as C's tombstone, so a
   late `checkpointStore.publish` for C's ID fails instead of recreating it.

Releasing C's `parent` edge may make the parent eligible in a later page or
pass. Progress is bounded per page and per pass. A protected record is
skipped and counted, and never blocks the rest of the page.

### Cache artifacts

- **Head publication takes part in the entry fence.** Before
  `responseCache.Publish` creates or replaces the `cache/success` head so it
  points at entry E, it does two things. First, it creates a `cache_head`
  edge in E's edge partition (W1). Second, it reads E's fence and vetoes a
  `fenced` state (W2). Only then does it write the head (W3). A delayed
  `Publish` retry for an aged entry is therefore covered by the same
  linearizability argument as blobs. Either the sweeper sees the edge, or
  the writer vetoes the fence, or the writer sees `retiring`/`retired` and
  aborts without moving the head.
- The `cache_head` edge for E is released only after a later committed head
  replacement points at a different entry. The writer that replaced the head
  releases it, or the sweeper does after re-reading the head and confirming
  this. `Publish` never moves the head to an entry with an older
  `CompletedAt` (or an equal `CompletedAt` with a lower ID), so a released
  head edge cannot become necessary again unless a new `Publish` first
  recreates it.
- A superseded cache entry with no live `cache_head` or `cache_use` edges,
  older than the cache cutoff, is fenced and retired like a checkpoint. Its
  `cache_origin` and `blob` edges are then released.
- Cache use rows are retired when the consuming operation's request is
  retired. Retiring a use releases its `cache_use` edge.
- Fill heads in `held`/`started` are never touched. For a terminal fill head,
  the old revision blobs written by `cacheFills.write` before it are released
  once the head's receipt is sealed. The sealed receipt's blob is retained
  with the receipt.

`maintenance.RetentionRecord` maps one-to-one onto these checks. `Active`,
`HasRetainedDescendant`, `HasActiveFill`, `HasActiveUse` and
`HasExternalBlobReference` are computed from the live edges and states read
at recheck time. The KV adapter implements `maintenance.RetentionStore.Prune`
as one bounded page with no cross-item transaction.

## Blob deletion protocol

### Ledger item

One item per blob key:

```
key:    <ns>/fence/blob/<blob tag>    (sort key "v1")
fields: fence = {
  "v":1, "state": "fenced"|"released"|"deleting"|"deleted"|"vetoed",
  "generation": n, "claim": <lease token digest>, "claim_expires": t,
  "replicas": {"<region/bucket>": "pending"|"absent"|"unknown"}, "updated_at": t }
```

The ledger is the single linearization point for one blob key. Only the
maintenance role creates it. Request-path writers only `Get` it and, when it
is `fenced`, `Replace` it to `vetoed`.

### Writer side (request path, new)

Before publishing any reference to blob key K, a writer does the following.
That covers a new blob, a deduplicated `ErrAlreadyExists` blob, and a copied
reference.

```
W1  Create edge(K, referrer)                    -- or confirm an identical edge
W2  Get fence(K)
      absent | released | vetoed  -> proceed
      fenced                      -> Replace fence(K) fenced->vetoed at its version
                                     success -> proceed
                                     conflict -> repeat W2
      deleting | deleted          -> abort: mark the edge released, write the
                                     content under generation-suffixed key
                                     K' = K + "." + (generation+1), go to W1 for K'
W3  publish the reference (the existing conditional KV write)
```

A writer that holds plaintext (every `writeBlob` caller) can always take the
abort path. A writer that only copies a reference never holds plaintext. A
child checkpoint naming a parent is an example. For such a writer the abort
path returns a retryable `state_unavailable`, and the source record is
re-read. If the source was fenced, it is also being retired, so the copy is
refused. Generation suffixes extend `validBlobKey` with an optional
`.<decimal>` suffix. `readBlob` already binds the AAD to the full key.

### Deleter side (maintenance role)

```
D1  Create fence(K) {state: fenced, generation: g}
      ErrAlreadyExists -> resume from the stored state (recovery, below)
D2  QueryPartition(edge(K)) to the end; if any edge is live:
      Replace fence(K) fenced->released; stop
D3  Replace fence(K) fenced->deleting at the version from D1
      ErrConflict -> a writer vetoed; Replace ->released; stop
D4  for each configured replica bucket: BlobStore.Delete(K)
      record per-replica result with Replace (absent | unknown)
D5  when every replica is absent (re-verified with Open after the
      replication-settle delay): Replace fence(K) deleting->deleted
```

### Why a late reference cannot attach after the fence

Every operation on `fence(K)` is linearizable. A writer always runs W1 (edge
create) before W2 (fence read), and W3 (publish) only after W2 sees a
non-deleting state. The deleter always runs D1 before D2 (edge scan) and D2
before D3. Take any writer whose W3 publishes a reference to K, and look at
the position of its W2 relative to D1 and D3 in the linearization of
`fence(K)`:

1. **W2 before D1.** Then W1 happened before W2, which happened before D1,
   which happened before D2 started. The edge row existed before the scan
   began. The partition query is strongly consistent and only misses inserts
   that happen during the traversal, so D2 sees the live edge. The deleter
   releases the fence and never reaches D3.
2. **W2 between D1 and D3.** W2 reads `fenced` and the writer replaces it with
   `vetoed`. Both the veto and D3 are conditional `Replace` calls on the same
   version, so exactly one wins. If the veto wins, D3 fails and the blob is
   not deleted. If D3 wins, the veto fails, the writer re-reads, sees
   `deleting`, and aborts before W3.
3. **W2 after D3.** The writer reads `deleting` or `deleted` and aborts
   before W3.

A later fencing round on an item left `released` or `vetoed` starts with a
conditional `Replace` back to `fenced` instead of the D1 `Create`. The
argument is unchanged: "D1" is whichever write set `fenced` for the round.

So no reference to generation g of K is published once D3 has committed. A
reference published before D1 has an edge row, and D2 sees it (case 1). The
argument needs nothing beyond single-key linearizability and the
strong-consistency rule for partition queries. It uses no clocks, no leases
and no cross-item transactions. Clocks only bound how long a claim is held
(below), never safety.

The same argument holds for checkpoints and cache entries. Their fences are
`fence/checkpoint/<tag>` and `fence/cache-entry/<tag>`, their edges are the
lineage/use edges, and "publish" is the child's or use's conditional `Create`.

### Generation suffixes prevent key reuse

`BlobStore.Delete` is unconditional, so a delete retried after a new object
reused the key would destroy live data. Once `fence(K)` reaches `deleting`,
writers never write K again (W2's abort path), and the deleter never deletes a
key in any state other than `deleting`. A retried D4 can therefore only
delete objects of generation g that nobody references. Writers that recreate
K bytes through a stale code path (the abort path not deployed) are excluded
by the rollout gate.

### Idempotent claims

Several deleter replicas may run. A deleter claims a ledger item by
`Replace`-ing `claim` (the SHA-256 of a fresh `maintenance.LeaseToken`) and
`claim_expires` (at most `maintenance.MaxOutboxLease`, typically minutes) at
the observed version. Every later step is a `Replace` at the version that
deleter last wrote. A deleter whose lease expired and whose item was
reclaimed gets `ErrConflict` on its next step and stops. Claims bound
liveness only. Safety comes from the conditional state transitions above, so
two deleters racing on the same item cannot both commit contradictory
transitions.

`maintenance.OutboxStore` can carry the work queue (`EventDeleteBlob` with
`{"blob_id": <tag>}`), but the ledger, not the outbox row, is the authority on
state. Its in-memory `Dispatcher.RunOnce` behavior (`ErrObjectNotFound` counts
as completion) matches the contract below.

### Durable completion and recovery

- **Missing objects succeed.** `BlobStore.Delete` already treats a missing
  blob as success. D4 records `absent` for that replica.
- **Ambiguous acknowledgement.** `ErrOutcomeUnknown` from D4 records `unknown`
  for that replica and leaves the item in `deleting`. A retry repeats D4. This
  is safe because the key cannot be live in generation g (above).
- **Interrupted deletion.** A crash at any step leaves the ledger in `fenced`
  or `deleting`. The sweeper pages `fence/blob` items through the catalog
  (`<ns>/retention/fence/<day>/<shard>`, written alongside D1) and resumes:
  `fenced` restarts at D2, and `deleting` restarts at D4. `deleted` is
  terminal.
- **Regional replicas.** `regionalBlobs.Open` serves from a replica when the
  primary reports a missing object, and S3 replication is asynchronous. D4
  therefore deletes in every configured `payload_replicas` bucket, not only
  the routed one. D5 waits a configured replication-settle delay, then
  verifies with `Open` in each bucket that the object is gone. A late
  replicated copy found then sends the item back through D4. The
  maintenance-side blob deleter must use the per-bucket stores directly.
  `regionalBlobs.Delete` stays `ErrUnsupported` for the request path.
- **Completion is durable** only once every replica is `absent` and the item
  is `deleted`. The `deleted` item is kept as the generation record that
  prevents key reuse. Like request tombstones, it is never dropped in phase 1.
- **Restored references.** A backup or point-in-time restore of the table can
  bring back edges and references to blobs whose ledger says `deleted`.
  Readers then report `ErrCorrupt`, which is correct and visible. The restore
  runbook (phase 5) must replay `deleted` ledger items against the restored
  table and fail readiness until each such reference is resolved. Blob
  versioning or backups (deployment-owned, see the `BlobStore.Delete`
  contract) are the only recovery source for the payload.

## Rollout gate: the reference-index epoch

Edges protect only references written by code that writes edges. A worker
binary from before this design publishes references with no edge. Retention
is therefore enabled only after these steps:

1. Every request-path binary that can run writes edges and catalog rows, and
   honors fences and `ErrRetired`. The release evidence (#821) shows that no
   older image can be scheduled.
2. The operator records a **reference-index epoch** T as a permanent KV item.
   It is written once with `Create`, like the budget initialization receipt.
3. A candidate is eligible only if its catalog row and every edge partition it
   depends on were created after T plus a configurable margin. Anything
   older, including all data from the initial retain-all deployment, is
   retained until a separately designed and approved backfill indexes it.

## Metrics and cost measurement

All metrics use the existing `llmtw_` prefix. They carry no tenant, project,
key, digest or content labels. Labels are limited to `kind`, `outcome` and
`reason` from a fixed vocabulary.

- `llmtw_retention_examined_total{kind}`, `..._retired_total{kind}`,
  `..._skipped_total{kind,reason}`. `reason` is one of `protected_active`,
  `live_edge`, `vetoed`, `fence_conflict`, `before_epoch`, `unknown_cost`,
  `fill_active` or `use_active`.
- `llmtw_retention_pass_duration_seconds{kind}` and
  `llmtw_retention_page_items{kind}`.
- `llmtw_blob_delete_total{outcome}`, where `outcome` is one of `deleted`,
  `already_absent`, `ambiguous`, `vetoed`, `released` or `error`.
- `llmtw_blob_delete_backlog{state}`: ledger items in `fenced` or `deleting`,
  computed per page and exported as a gauge.
- `llmtw_blob_delete_oldest_age_seconds{state}`.
- `llmtw_retention_edge_writes_total{kind}` on the request path, to measure
  write amplification.
- `llmtw_retention_tombstones{kind}`: a sampled gauge of the tombstone set
  size.

Phase 0 measures, on real DynamoDB and S3:

- RCU/WCU per request with edges enabled;
- the throttling rate on edge partitions of heavily deduplicated checkpoint
  blobs;
- sweep RCU per thousand candidates;
- S3 `DELETE`/`HEAD` and replication costs per thousand blobs;
- the storage growth of tombstones and `deleted` ledger items.

These numbers decide page sizes and shard counts before any phase is enabled.

## IAM separation

- **Request-path worker role:** keeps today's grants and adds nothing that
  deletes. The edge/catalog `Create` and fence `Get`/`Replace` calls use the
  existing item-level write grants on the same table. The role keeps no
  `s3:DeleteObject` and no `dynamodb:DeleteItem`, even though the generic
  `regionalTable.Delete` pass-through exists.
- **Retention sweeper role** (phase 2 and later): DynamoDB `GetItem`,
  `Query`, `PutItem` (conditional) and `DeleteItem` (conditional), restricted
  by leading-key conditions to the worker namespace. No S3 delete.
- **Blob deleter role** (phase 3 and later): DynamoDB access to `fence/blob`,
  `edge/blob` and catalog items only, plus `s3:DeleteObject` and
  `s3:GetObject` on the payload prefix of each replica bucket. No access to
  Redis and no provider credentials.
- Both maintenance roles are provisioned only when their feature flag is
  enabled (#820). They run as separate Kubernetes service accounts from the
  `llm-temporal-worker` identity, never in the request-serving process. Each
  is a bounded CLI subcommand, invoked by a scheduler, that runs one pass and
  exits. Neither needs the encryption secret for reading content: edges,
  fences and catalogs hold only keyed digests. The sweeper does need the
  repository secret to compute keys, and to decrypt request pointers when
  checking terminal state. That secret is mounted read-only.

## Phased implementation plan

| Phase | Exit | Default |
| --- | --- | --- |
| 0 | Cost measurement harness against real DynamoDB/S3; this document | n/a |
| 1 | Request path writes edges and catalogs, honors fences and `ErrRetired`, generation-suffixed keys readable. No deletion code. Reference-index epoch recorded after fleet evidence. | edges on, cleanup off |
| 2 | KV adapter for `maintenance.RetentionStore`; logical request retirement; checkpoint and cache retirement with fences; tombstones; sweeper CLI and IAM role | off |
| 3 | Blob ledger, deleter CLI and IAM role, per-replica deletion and verification, recovery of `fenced`/`deleting` items | off |
| 4 | Orphan blob collection from blob catalogs (interrupted publications and superseded fill blobs) | off |
| 5 | Restore runbook replaying `deleted` ledger items; optional snapshot cut; tombstone dropping after a guarded-create KV contract exists | off |

Phase 1 is the only one that changes the request path. Phases 2 to 5 are
independent jobs that stay disabled in the initial retain-all deployment.

## Test matrix

Every row runs against the generic conformance fixtures
([`golang/storage/conformance`](../../golang/storage/conformance)) and the
in-memory KV/blob stores. Rows marked **real** also run in the opt-in AWS
gate against DynamoDB and S3 (including a two-region replica configuration),
as the AWS gate in #821 already does.

| # | Scenario | Expectation | Real |
| --- | --- | --- | --- |
| 1 | Page of 1000 candidates, 999 protected | 1 retired, 999 skipped with reasons; cursor advances; no retries of the same page forever | yes |
| 2 | Every protection in the table above | never retired; reason counted | yes |
| 3 | Writer W2 strictly before deleter D1 | D2 sees the edge; blob kept | yes |
| 4 | Writer veto racing D3 (injected interleaving at each step) | exactly one of veto/D3 succeeds; never a published reference to a deleted blob | yes |
| 5 | Writer after D3 with plaintext | aborts, writes generation g+1, publishes K'; reads succeed | yes |
| 6 | Writer after D3 copying a reference | retryable error; no publication | no |
| 7 | Crash after each of D1..D5 | resumable; final state `deleted` or `released`; idempotent | yes |
| 8 | `ErrOutcomeUnknown` on D4 in one replica | stays `deleting`; retry completes; no other key touched | yes |
| 9 | Object already missing | `already_absent`, completes | yes |
| 10 | Late replicated copy after D4 | detected in D5, deleted again | yes |
| 11 | Two deleters, expired lease | stale deleter's next step conflicts; no double transition | no |
| 12 | Zombie `TryUpdate`/`Create`/`BeginOperation` after retirement | `ErrRetired`; no new revision; no new pending row | yes |
| 13 | Reused operation key after retirement | `ErrRetired`, never a fresh paid request | yes |
| 14 | Checkpoint chain with one live leaf, expired ancestors (#1206) | every ancestor retained; leaf materializes | yes |
| 15 | Child published concurrently with parent fence | veto or refusal; never a child of a retired parent | yes |
| 16 | Cache head replaced during entry retirement | old entry retired only when superseded and unused; head entry never | no |
| 17 | Active fill held/started; terminal unsealed fill | not touched | no |
| 18 | Unknown-cost attempt without applied #819 receipt | retained | no |
| 19 | Restored table with references to `deleted` blobs | readers `ErrCorrupt`; restore check fails readiness | yes |
| 20 | Pre-epoch data | never a candidate | no |
| 21 | Request-path role attempts `DeleteObject`/`DeleteItem` | denied by IAM | yes |

## Open questions for the owner

- The default `operation_key_reuse_horizon`: indefinite tombstones (the
  proposed default) or a contractual caller limit.
- Whether the cloud-storage module will gain a guarded create that checks a
  fence item, so event rows and tombstones can eventually be removed
  physically.
- Whether the snapshot-cut optimization is worth its proof cost, given the
  measured checkpoint storage.
