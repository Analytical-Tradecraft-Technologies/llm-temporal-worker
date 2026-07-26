# State and Storage

> This chapter describes current storage behavior. Target phase status and
> authority are centralized in [scope](../scope.md#staged-delivery-and-document-authority).
> The exact PostgreSQL system-of-record/Redis materialization boundary, schema,
> constraints, indexes, and transaction protocols are in
> [PostgreSQL state, cache, accounting, and control plane](postgresql-state-cache-and-control-plane.md).
> Temporal's own PostgreSQL schema is never modified.

## Storage responsibilities

V1 persists three kinds of state behind separate domain ports:

| State | Mutable | Contains model content | Atomicity requirement |
| --- | --- | --- | --- |
| Operation ledger and budget buckets | state-machine updates | no; completed result is a reference | one transaction across operation and all matching windows |
| Continuation records | immutable after creation | canonical transcript and opaque provider state, size-bounded in the record | create-if-absent by child ID |
| Result/blob objects | immutable | possibly | digest-verified put/get |

Redis implements operation, budget, and continuation state. A blob-store port
handles payloads that exceed safe Redis or Temporal inline limits; filesystem is
development-only and object storage is the production example.

The production S3 store uses a create-if-absent write keyed by tenant and
SHA-256 digest. When S3 reports that the key already exists, the worker only
treats the write as idempotent after `HeadObject` proves the stored byte length,
content type, digest metadata, and digest-length metadata match the requested
reference. A present but unverified or replaced object is reported as a
conflict, so a caller cannot persist a BlobRef for content that has not been
proven to match. Reads apply the same integrity boundary: the returned object
must carry the reference's media type (and its bytes must still match the
digest and length), otherwise the read fails closed as a digest mismatch.

This is the current pre-release layout, not the accepted final division of
responsibility. In the target design PostgreSQL is the durable system of record,
while Redis is the required production optimization that materializes the
complete active-window budget working set plus request, token, and concurrency
throttles. Redis makes each reservation/reconciliation atomic and coordinates
replicas; it does not become the durable financial record. Normal budget
admission reads Redis only. Every accepted Redis reservation must be journaled
to PostgreSQL before paid dispatch; a failed write releases Redis best-effort
and never dispatches. Both dependencies fail closed for new paid work. Exact
PostgreSQL decimals are conservatively represented as safe-integer nano-USD in
Redis as specified by the target schema chapter.

The `storage/redis` implementation uses the official go-redis v9 client and
one embedded, versioned Redis Function library for each admission mutation.
Every key touched by a mutation is supplied explicitly and carries the
configured `{admission}` hash tag. The preferred Function mode is provisioned
outside the worker and verified by library/function version/digest before
polling begins. An explicitly configured Lua compatibility mode may use a
preloaded script, but neither mode lets the runtime load, replace, or rewrite
shared Redis code. A transport error is never retried blindly; the caller reads
the operation or continuation index to resolve whether the write committed.
Offline command/function harnesses exercise the same store ports without
requiring a Redis daemon, including lost replies after admission terminal and
continuation mutations and immutable child creation. `storage/conformance` then
runs the same public
admission and continuation contract against memory and the pinned live Redis
fixture in both Function mode and the explicitly configured preloaded-Lua
compatibility mode. `make redis-integration` creates one uniquely named
loopback-only container, discovers its ephemeral port, enables the configured
AOF/RDB persistence profile, explicitly provisions the immutable Function and
compatibility script only inside that test dependency, and removes it on
completion. It tests a post-mutation timeout resolved by a read, restart
persistence, fail-closed Function/Lua identity mismatch, and the single
configured hash slot. Failure logs pass through the repository redactor; the
trusted master workflow runs this live gate while pull-request CI remains
offline.

Operational request, token, and concurrency limits use the separate
`ThrottleStore` and its `llmtw_throttle_v1/throttle_v1` Function. A throttle
lease is an opaque, idempotent reservation: every requested limit is checked
before any counter increments, and a transport error is resolved by `Lookup`
before retrying. Throttle counters and leases are namespaced by the same
configured prefix/hash tag and expire at the largest requested window. They
are coordination state, not monetary facts, and are never used as a
PostgreSQL accounting fallback. See [Redis operational throttles](../reference/redis-throttles.md)
for the public API and deployment contract.

Budget hashes receive a Redis TTL only when the operation has an explicit
expiry; the TTL is the operation retention plus the longest matching window.
This keeps every bucket needed by an in-flight operation visible while
allowing expired windows to be collected. Operations without an expiry are
retained until the configured Redis retention/GC policy removes them.

## Operation record

```go
type Operation struct {
	ID                    string
	ScopeKey              string
	ExternalKeyHash       [32]byte
	RequestDigest         [32]byte
	State                 OperationState
	ReservedMicroUSD      pricing.MicroUSD
	IncurredMicroUSD      pricing.MicroUSD
	FinalMicroUSD         pricing.MicroUSD
	Reservations          []WindowReservation
	MatchedWindowsDigest  [32]byte
	ConfigVersion         string
	PriceVersion          string
	Attempt               AttemptFacts
	ResultRef             *state.BlobRef
	LeaseUntil            time.Time
	CreatedAt             time.Time
	UpdatedAt             time.Time
	ExpiresAt             time.Time
}
```

`AttemptFacts` holds route IDs, provider request IDs, tier values, and dispatch
observation; it contains no prompt or output text. Legal state transitions are
validated in both Go and the Redis Function. A terminal state cannot return to a
nonterminal state.

External operation keys are HMACed for Redis key construction. The original key
is not logged or stored unless an operator explicitly chooses a safe identifier.

## Continuation record

```go
type Continuation struct {
	ID                 string
	Tenant             string
	ParentID           string
	Transcript         []llm.Item
	TranscriptDigest   [32]byte
	TranscriptComplete bool
	ProviderState      []OpaqueStateRef
	Pinning            Pinning
	LastOperationID    string
	CapabilityVersion  string
	PriceVersion       string
	CreatedAt          time.Time
	ExpiresAt          time.Time
	Depth              int
}
```

This is the current in-process domain record. `ID` is the signed, opaque handle
issued for `Tenant`; it is not a prompt, provider token, or tenant hash. The
Redis and memory stores copy records on write and read so callers cannot mutate
an existing continuation by retaining a slice or provider-state byte buffer.

`CanonicalTranscript` canonicalizes each item and hashes a versioned container
with schema `continuation/v1`. Nil items, malformed semantic items, missing
identity, negative depth, non-future expiry, digest mismatches, and incomplete
provider-state entries are rejected. Each provider-state entry must identify
its provider, endpoint, family, media type, and non-empty opaque bytes; its
`Required` flag controls whether routing may drop it in best-effort mode.

`CreateRoot` issues a handle at depth zero. `PutChild` requires the child to
use the same tenant, the parent handle as `ParentID`, and exactly the parent
depth plus one. A non-empty operation key makes the child write idempotent
within the tenant and parent-handle scope; the same operation returns the
existing child handle, while a conflicting child is rejected. The same key
may be reused independently on another branch. Reads verify the handle
signature, tenant binding, digest,
and expiry before returning the cloned record. Redis additionally bounds the
encoded record by its configured continuation byte limit and expires it using
the record's `ExpiresAt` value.

Deletion is retention-driven. Deleting a parent does not mutate children.
Redis derives one TTL from each continuation's `ExpiresAt` and applies it to
the immutable record, opaque-handle index, and child operation-idempotency
index. Redis expires keys independently, so a read that finds the record gone
rechecks the handle index: if it also disappeared, that is a normal
expired/not-found result; if it remains, the persisted state is dangling and
fails closed as unavailable. Redis transport failures and malformed persisted
values also fail closed; callers do not treat them as an expired continuation.
The memory store exposes an explicit sweep for the same policy. Immutable result
blobs are retained until no live operation or other record references them.

## Redis key layout

```text
<key-prefix>:{admission}:op:<scope-hmac>:<operation-hmac>
<key-prefix>:{admission}:budget:<policy-version>:<window>:<bucket>
<key-prefix>:{admission}:function-version

<key-prefix>:{admission}:continuation:<tenant-hmac>:<continuation-id>
<key-prefix>:{admission}:continuation:index:<handle-hmac>
<key-prefix>:{admission}:continuation-operation:<operation-hmac>
<key-prefix>:result:<tenant-hmac>:<digest>
```

**state.redis.key_prefix** configures **key-prefix** and defaults to **llmtw**.
**LLMTW_REDIS_KEY_PREFIX**, when present, overrides the YAML value before
validation. The effective value must match
**[A-Za-z0-9][A-Za-z0-9._-]{0,63}**. It is a process-lifetime setting and is
passed to every admission, continuation, result, readiness, cleanup, and test
key constructor; no production call site may supply a literal fallback.
**print-effective-config** emits the resolved non-secret prefix.

The prefix is a data namespace and Redis ACL selector, not a tenant security
boundary. A shared Redis deployment should constrain the worker role to
**~<key-prefix>:*** and the required commands. The Redis Function library name
is server-global and remains separately configured; every Function invocation
receives only keys in the configured prefix. The hash tag remains independently
configurable because all keys in one atomic admission mutation must share one
Redis Cluster slot.

Changing the prefix points the worker at a different empty namespace. This is a
pre-release configuration change: do not add Redis key copying, backfill,
dual-read, dual-write, legacy-prefix fallback, or a namespace rename command.
If a prefix must change after the first release, design and review that data
migration separately at that time.

All keys touched by admission share the literal configured `{admission}` hash
tag so a Redis Cluster executes one atomic Function. This creates an intentional
single-slot throughput ceiling in v1. Continuation/result keys need not share
that slot because they are referenced only after their immutable write
completes.

Redis values use explicit schema versions and integer strings. Function
arguments are validated for count, length, numeric range, and allowed transition.
The Function uses Redis `TIME`. Dynamic key names, source code, or caller
expressions are never interpolated into Lua.

## Redis operation protocol

`Begin`, `MarkDispatching`, `Continue`, `Complete`, and `Fail` are
versioned Function entry points. Deployment loads code by digest and verifies
the server's digest before readiness.

- `Begin` performs idempotency lookup, every budget check/increment, and
  operation creation atomically.
- `MarkDispatching` compares state, operation token, and lease before recording
  the selected attempt.
- `Continue` records a definite attempt outcome, reconciles the old reservation
  vector, and atomically admits/reserves the remaining plan or ends denied.
- `Complete` stores a prewritten immutable result reference, reconciles the
  original buckets, and transitions terminally.
- `Fail` records definite/ambiguous/canceled classification and applies the
  matching reservation rule.

The result/blob is written before `Complete`. An orphan is safe and garbage
collectable; a completed ledger entry never points to an unwritten object.

Network loss after a Function call is resolved by reading the operation record
with the same operation token. The client never assumes the mutation failed.

## Memory store

The memory implementation uses the exact public store ports, one mutex for
atomic admission, an injected clock, immutable copied byte slices, and bounded
maps with expiry. It is not a simplified fake: the common conformance suite
runs unchanged against memory and Redis.

Memory mode is available only as the explicitly development-only
`state.kind: memory` composition. The runtime factory constructs the in-process
admission, continuation, and content-addressed blob stores without dialing
Redis, PostgreSQL, or an external blob service. It remains rejected when:

- production mode is enabled;
- durable continuation is required across restart;
- configuration reload would orphan live operations;
- an external blob-store kind is selected.

The process-local blob map enforces the configured per-blob byte limit and
expiry cleanup; a lifecycle owner should call its `Sweep` method when idle.
Restart loses all memory state, so this mode cannot provide crash recovery,
multi-replica admission, backups, or release evidence.

## Leases and recovery

A reservation has a short pre-dispatch lease. Its owner renews while compiling
or waiting for a connection. If it expires while still `reserved` and there is
proof that `BeforePossibleWrite` never ran, another Activity attempt may claim
it without another charge.

Once `dispatching` is recorded, lease expiry never authorizes resubmission. A
reconciler may:

1. query a provider by a stored idempotency/job/reference ID when supported;
2. complete from a recovered result;
3. otherwise transition to `ambiguous` and retain the reservation.

The reconciler is a separate process mode or scheduled workflow and never needs
prompt content.

## Retention

Configuration compilation enforces:

```text
operation terminal retention
  >= maximum Activity retry horizon
   + maximum queue delay
   + clock/skew safety margin

continuation retention
  >= advertised continuation lifetime

blob retention
  >= maximum live referring record lifetime + GC safety margin
```

Ambiguous records use the longest operation retention. Tombstones retain the
operation key/request digest after result deletion so late retries cannot submit
again.

## Redis production profile

- TLS and ACL credentials supplied by secret reference;
- `noeviction` and memory alerts before exhaustion;
- AOF and RDB enabled, with persistence status in readiness/alerts;
- backups plus restore drills;
- replica/failover mode tested with Function availability;
- command allow-list limited to required operations;
- bounded client pools, timeouts, circuit metrics, and no client retries around
  ambiguous writes without read-after-error;
- keyspace notifications not required for correctness.

If Redis is unavailable or persistence is unhealthy under configured policy,
admission and continuation fail closed. Existing provider calls still attempt a
bounded ambiguity/finalization write before returning.
