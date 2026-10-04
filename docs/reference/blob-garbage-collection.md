# Blob garbage-collection contract

> Historical SQL design: the SQL implementation has been removed. Use the
> [cloud request reference](cloud-request-repository.md) for the current runtime.
> Cloud cleanup and unknown-cost reconciliation remain separately deferred;
> the procedures below must not be used against the cloud stores.

Blob metadata is retained in PostgreSQL while the encrypted locator is used by
the external object store. Cleanup is therefore a fenced, two-phase state
machine rather than a direct `DELETE`:

1. A bounded maintenance pass marks expired, unreferenced `retained` rows as
   `eligible` using `FOR UPDATE SKIP LOCKED`.
2. A deleter claims an eligible row (or an explicit outbox target) and changes
   it to `deleting`. The claim query repeats every reference check while the
   row is locked.
3. The deleter performs the object-store operation outside the SQL
   transaction, then calls finalize to change `deleting` to `deleted`.

The schema also installs writer-side guards on every direct blob reference
and every cache-mediated reference. A writer that races a committed
`deleting` claim blocks on the blob row fence and then fails closed; it cannot
add a new foreign-key reference during the external-delete window. The
`eligible` state remains referenceable until the deleting claim repeats its
locked recheck.

The reference recheck protects request and result blobs held by non-terminal
operations, terminal operations inside their retention window, unexpired
conversation checkpoints, unexpired or non-expiring provider state, ready
response-cache entries, active cache uses through an operation, and active
cache fills. A tombstoned cache row is logically dead and does not by itself
keep the object alive. This is important because cache tombstoning and blob
deletion are intentionally separate outbox work.

`ClaimBlobDeletion` accepts an explicit blob ID for a durable delete intent,
which allows non-expiring cache metadata to be handled safely. An empty ID
list claims previously marked `eligible` rows. Both forms are bounded and
skip rows locked by another maintenance worker.

`FinalizeBlobDeletion` is idempotent: an already deleted or missing metadata
row succeeds. Consequently an object-store `not found` response can be
acknowledged without retrying forever. A row in `retained` or `eligible` cannot
be finalized without first acquiring the `deleting` fence. Finalization then
rechecks active references while holding that fence and fails closed if a stale
or privileged deletion state still has a live reference.

This slice is the PostgreSQL metadata contract only. It does not wire a
`BlobResultStore` or runtime engine to an object-store client; the maintenance
outbox/deleter integration must call claim, perform the physical delete, and
then finalize. A worker crash after claiming leaves a row in `deleting` and
there is intentionally no automatic lease expiry in this contract. Recovery
must be an explicit, audited operator/reconciliation action rather than a
blind retry that could race a still-running deleter.

The implementation lives in `golang/storage/postgres/blob_gc.go`; the
integration test is skipped unless `LLMTW_POSTGRES_ADDR` is configured and
covers active operation, checkpoint, provider-state, and ready-cache fences,
late writer rejection after a committed claim, the lifecycle transition,
idempotent completion, and missing-object success.
