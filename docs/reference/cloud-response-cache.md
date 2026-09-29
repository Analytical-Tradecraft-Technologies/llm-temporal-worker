# Cloud response-cache persistence

`cache.ResponseRepository` is the storage-neutral persistence contract for exact
response reuse. `cloudstate.Repository.Responses()` implements it over the same
configured table, bucket and independent secret used for requests/checkpoints.
The AWS implementation therefore uses DynamoDB and S3 through `cloud-storage`.
No resource provisioning or SQL data import is performed.

When `state.requests` is configured, the snapshot factory supplies
`V1RuntimeCapabilities.Responses` alongside its cloud checkpoint bundle. Custom
cloud factories must expose a non-nil `CloudResponseCacheSource`; otherwise the
snapshot is rejected and its clients drained before Activity construction.
This supplies persistence to phase/finalizer builders. It does not enable
automatic caching, implement fill ownership, or replace the complete production
Generate/Compact finalizers. Existing public v1 cache-policy validation is
unchanged; the new internal lookup contract supports an omitted maximum age.

## Cache identity and lookup

`ResponseKey` contains the authenticated opaque checkpoint scope, Generate or
Compact operation domain, resolved route, semantic fingerprint and request
index. Route identity includes provider, endpoint, account, region, model,
model revision and compiler profile. Every dimension participates in the keyed
storage identity. Request index defaults to zero and only separates samples.

The phase builder must fingerprint all execution-relevant content and effective
settings, including materialized context and policy/compiler versions. It must
exclude operation IDs, freshness controls and completion timestamps. Compaction
fingerprints include source content and compaction policy/prompt versions.
`cache.Compute` remains available for its supported normalized manifest; the
repository receives an already computed fingerprint and cannot independently
prove that the caller included every semantic input.

`Lookup` returns the newest published successful completion, or nil for a miss.
The caller supplies `Now`; nil `MaxAge` means no age restriction. A supplied age
must be positive. The boundary is inclusive, measured from `CompletedAt`;
lookups and cache-use receipts never refresh it. Future-dated completions are
not returned. Storage errors, missing referenced objects and corruption are
errors, never cache misses that could silently cause another paid request.

## Publishing a successful origin

1. Commit the origin checkpoint and its encrypted blobs.
2. Call `Publish` with a stable entry ID, resolved key, operation/checkpoint
   identities, original successful-completion timestamp and validated v1 JSON.
3. On an uncertain result, retry the identical entry. Do not change its ID or
   timestamp, and do not redispatch paid work to repair cache publication.

Publication requires a committed checkpoint in the same scope with the expected
operation and kind. Generate output must match that checkpoint's response-blob
digest; checkpoint depth/parent presence must also agree. Authentication of
public handles and selection of the authorized scope/route remain responsibilities
of the finalizer. A cache hit cannot be republished as a new origin success.
Generate accepts completed and application-tool-call responses; truncated,
filtered, refused, failed and pending outcomes cannot replace a success.
Compact requires its validated completed-response contract.

The full entry, including response, origin provenance, route and timestamps,
is encrypted in an immutable blob. There is no inline SQL response limit: the
complete serialized entry is bounded at 8 MiB (and response/codec validation
applies its own limits). The table contains only versioned pointers and keyed
identities under `<namespace>/cache/`:

- `entry`: immutable scope/entry-ID binding.
- `success`: current successful entry for one exact key.
- `use`: immutable consuming-operation receipt.

The entry blob and ID binding precede the conditional success-pointer update.
Completion time determines the winner; lexicographic entry ID breaks exact
timestamp ties. A delayed older success or retry cannot move the pointer back.
There is no failure publication that could invalidate an older eligible success.
Only definite conditional conflicts receive bounded local retries; unknown
write outcomes remain visible to the caller.

## Finalizing a cache replay

An entry's stored response belongs to its origin operation. **Do not return it
unchanged to another caller.** The phase/finalizer must construct a distinct
operation and checkpoint, bind `OriginCacheEntryID`, and produce the consuming
caller's response with zero new inference cost and explicit cache provenance.
For Generate, the new checkpoint has kind `cache_replay`. Compact retains kind
`compaction` and carries its origin cache entry.

After committing that checkpoint, call `RecordUse`. It verifies the entry and
checkpoint, consuming-operation identity, cache provenance, completion ordering,
kind, and agreement with the origin response-blob digest. The receipt is unique
per scope/operation. Identical retries are no-ops; changing an existing receipt
conflicts. `ReadUse` recovers that binding after a restart. Receipts allow later
accounting without incrementing counters repeatedly on Temporal retries; no
aggregate use-count or spend query is implemented here.

There is no portable transaction across checkpoint and cache publication. The
finalizer must retry an interrupted publication/receipt before declaring the
operation complete. These methods neither acquire/refund Redis budget nor
authorize a provider call. In particular, concurrent cache misses still require
separate durable fill coordination and paid-attempt authorization.

## Retention and verification

There is no automatic TTL, deletion or garbage collection. Losing publications
can leave encrypted orphan objects, and superseded entries remain addressable
for receipts. Do not independently expire these rows, origin checkpoints or
referenced objects. Cleanup, cache phase composition, zero-cost response
construction and remaining SQL removal are subsequent migration work tracked
under [#815](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/815)
and [#812](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/812).
Managed secrets/key rotation are tracked in
[#861](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/861).

Offline tests cover concurrent identical publication and receipt recording,
monotonic latest-success selection, all publication/receipt failure boundaries,
restart recovery, freshness and isolation dimensions, compaction, large
responses, application tool calls, provenance checks and ciphertext corruption.
These tests do not establish live AWS permissions or one paid submission for
concurrent misses; no provider call is made by this repository.
