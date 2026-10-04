# Maintenance boundary

The current worker has no durable retention, unknown-cost reconciliation or
blob-deletion CLI. The `maintenance` Go package retains storage-neutral policy,
fencing and outbox contracts with in-memory test adapters. They are not wired
to production cloud cleanup.

Cloud request/attempt records and pending indexes preserve recovery context.
Checkpoints and results are immutable encrypted artifacts. A failed publication
can leave orphan blobs or reservations; their presence is not permission to
delete them. Redis retains paid authorization and settlement identities so old
activity retries cannot grant or refund money twice.

A future retention implementation must recheck every live reference atomically
at its publication boundary, use bounded pages, fence competing writers and
make an uncertain deletion acknowledgement safe to retry. Blob deletion must
also preserve active cache fills and checkpoint ancestry. See
[blob garbage collection](blob-garbage-collection.md).

Unknown-cost reconciliation is similarly separate: a missing or ambiguous
provider charge must remain unknown until authoritative evidence resolves it.
It cannot be replaced with zero to make a maintenance pass finish.

Current work is tracked in [retention and blob deletion #818](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/818)
and [unknown-cost reconciliation #819](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/819).
The only budget initialization command is described in
[the CLI reference](cli.md#budget-initialize); initialization is not recovery or
retention.
