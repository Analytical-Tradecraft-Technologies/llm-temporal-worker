# PostgreSQL State, Cache, Accounting, and Control Plane

## Status and database boundary

This document is the normative home for the PostgreSQL/Redis responsibility
split, budget-read rules, workload envelope, and physical schema. It is
design-only until the applicable delivery phase is implemented. The unreleased
Generate v1 contract changes in place; no compatibility-only v2 is created.
The staged delivery and document-authority rules are defined in
[scope](../scope.md#staged-delivery-and-document-authority); summaries elsewhere
link here instead of restating these invariants.

The worker receives its own credentials, schema version, backup policy, and
connection pool. The configured database, schema, and table prefix select its
physical namespace. A dedicated database and schema are the production
recommendation, but a shared PostgreSQL server or database is supported when
the resulting schema-qualified relations are distinct. A table prefix can also
avoid collisions in an explicitly shared schema, though it is not a security
boundary. The worker must never create, query, or modify a relation owned by
Temporal.

PostgreSQL is the system of record for durable and queryable facts. Redis
remains a required production optimization: it is a provably-current
materialization of the active budget horizon, the cross-replica decision
serializer, and the low-latency throttle/coordination layer. Redis never becomes
the durable financial authority, and PostgreSQL is never queried to calculate a
normal live admission decision. The responsibility split is:

| Responsibility | Live decision/read path | Durable record |
| --- | --- | --- |
| Operation idempotency and ambiguous-dispatch ledger | PostgreSQL | PostgreSQL operations and attempts |
| Active sliding-window monetary budget | Redis atomic Function over a conservative materialization | PostgreSQL append-only exact budget journal and current operation facts |
| Request/token/concurrency throttles | Redis atomic Function | Redis persistence; no financial record implied |
| Immutable continuation state | PostgreSQL | checkpoints and blob references |
| Completed-result replay | PostgreSQL | operation result blob |
| Exact-response cache | PostgreSQL | entries, uses, and fill leases |
| Provider continuation/poll IDs | PostgreSQL | encrypted operation/attempt fields |
| Provider health and credit observations | PostgreSQL | event log and current projection |
| Provider model listing | PostgreSQL | inventory snapshots and normalized model rows |
| USD price catalogs | in-memory immutable snapshot | PostgreSQL USD catalog |
| Current budget-status query | Redis only | response cites Redis generation and stream high-water mark |
| Historical spend query | PostgreSQL operation/cost rows | no Redis budget read |

### Redis and PostgreSQL safety boundary

The same operation is represented in both systems, but no row and Redis key
form a jointly committed fact. Normal admission performs zero PostgreSQL budget
reads. The state machines make every cross-store failure conservative:

1. Resolve a completed/terminal PostgreSQL operation replay. This is an
   operation-ledger read, not a budget-state read; a replay creates no new Redis
   reservation.
2. For new work, call one Redis Function with the scoped operation identity,
   matching policy/window set, conservative nano-USD worst-case amounts, request/token bounds,
   and expected budget generation. It validates complete coverage, compares
   every limit, creates an idempotent reservation, updates all buckets, and
   appends one event to the shared budget Stream atomically.
3. Append/insert the corresponding operation and budget-journal facts in
   PostgreSQL without reading active budget state. A PostgreSQL write failure
   means no provider dispatch; release the Redis reservation best-effort and
   rely on its TTL if cleanup cannot be confirmed.
4. Dispatch only after the Redis reservation and PostgreSQL journal write both
   succeed. Persist provider acceptance/poll IDs and exact-or-unknown cost in
   PostgreSQL as defined below.
5. Commit the terminal PostgreSQL operation and budget-journal event before
   idempotently reconciling Redis. A Redis failure leaves the larger reservation
   charged and fails new admission closed until reconciliation or recovery; it
   never creates budget capacity.

### Go composition contract

The storage-neutral Go contract in
[`golang/storage/durable`](../../golang/storage/durable/contract.go) makes this
ordering executable for runtime adapters. A `StateIdentity` binds the
PostgreSQL namespace, Redis key prefix/hash tag, and immutable configuration
digest; a worker must reject a composition whose identity is incomplete or
does not match its snapshot. `Composition` exposes PostgreSQL operation,
continuation, result, and write-only journal ports alongside the Redis-only
`BudgetMaterializer` port. The journal deliberately has append methods only,
so a normal admission implementation cannot accidentally query PostgreSQL
budget projections.

The `Lifecycle` helper accepts only the sequence
`operation_replay -> redis_accepted -> postgres_journaled -> dispatched ->
postgres_finalized -> redis_reconciled`. An accepted Redis result must carry
at least one identity-matched journal event; a journal error therefore blocks
dispatch. After dispatch, PostgreSQL finalization remains authoritative and a
Redis reconciliation error is retried independently rather than rolling back
the result. Runtime wiring is the remaining integration step; this contract
is the seam that prevents a future adapter from reintroducing the superseded
Redis durable-operation composition.

### Complete Redis budget working set

All data required to decide every active budget window lives in the configured
Redis namespace and one Redis Cluster hash slot. It includes:

- `<redis-prefix>:{<budget-hash-tag>}:budget:active-generation`, pointing to an
  immutable random generation ID;
- `<redis-prefix>:{<budget-hash-tag>}:budget:g:<generation>:manifest`, containing schema/config/price versions,
  `rebuild_complete`, coverage bounds, policy/window/bucket counts, the budget
  Stream high-water mark, the nano-USD rounding version, and a digest of the
  expected member catalog;
- `<redis-prefix>:{<budget-hash-tag>}:budget:g:<generation>:window:<window-hmac>`
  for each policy/window, containing its limit, complete zero-or-value bucket
  fields for the entire active horizon, retained reservations, and version;
- `<redis-prefix>:{<budget-hash-tag>}:budget:g:<generation>:operation:<operation-hmac>`
  as the reservation index for idempotent acquire/reconcile/release;
- `<redis-prefix>:{<budget-hash-tag>}:budget:events`, the one Redis Stream to
  which the Function appends every acquire,
  denial-state change, reconciliation, release, policy refresh, horizon advance,
  and generation switch; and
- `<redis-prefix>:{<budget-hash-tag>}:budget:workers`, containing leases,
  persistent process-session roster entries, and each
  worker's last consumed Stream ID.

These are the exact key families; implementation may add only a documented
version suffix inside them. Every key comes from the one validated
`state.redis.key_prefix` and `budget_hash_tag`, so two deployments sharing Redis
remain isolated while all keys touched by one Function stay in one Cluster
slot. The Stream is the particular cross-worker communication key; Pub/Sub is
not used because its notifications are lossy.

Every expected bucket is materialized, including zero-valued buckets, so a
missing field is corruption rather than an ambiguous zero. The Function checks
the active generation, manifest completion, time coverage, policy/window
versions, expected member presence, and reservation identity before every
admission. Redis uses `noeviction`; persistence errors, a missing sentinel,
partial key loss, wrong generation, or incomplete coverage make readiness and
admission fail closed.

Every worker may tail `budget:events` independently with `XREAD` from the cursor
in its lease; a shared consumer group is not used because the feed is broadcast
coordination, not work distribution. This is an optimization, not a correctness
dependency. It lets workers invalidate expensive route/budget planning and
negative-result hints immediately, wakes waiters after a generation switch, and
avoids independent polling during large batch drains. Those hints can reject
early but can never authorize a request; every authorization still comes from
the atomic Redis Function, which also returns the current generation. A worker
with Stream consumption disabled, or one that falls behind retention, discards
its hints and reloads the current manifest/policy state from Redis only. Stream
trimming uses the minimum cursor of non-expired worker leases plus a safety
margin. Losing or disabling the Stream can increase Redis calls and latency but
cannot create budget capacity or require a PostgreSQL budget read.

Each Go process generates one random session ID and retains it in memory across
Redis reconnects. The roster entry is persisted with the budget generation,
while the liveness lease expires normally. After a persistent Redis restart,
still-running processes present their existing rostered session IDs and renew
without a PostgreSQL read even if the outage outlasted the lease TTL. A newly
started process has a new session ID. This distinguishes a persistent Redis
outage from a full Go-fleet restart without treating hostnames or pod names as
stable identity. Stale roster entries are bounded by generation replacement and
maintenance only after no active lease/cursor references them.

### Conservative nano-USD materialization in Redis

PostgreSQL and public Go/JSON/OCaml contracts retain exact **NUMERIC(38,18)**
USD values. Redis is only the admission materialization, so it uses non-negative
integer nano-USD values. Go converts without floating point: every positive
journal increase is rounded up to `ceil(usd * 10^9)`, every configured limit is
rounded down to `floor(usd * 10^9)`, and each operation record stores the exact
integer applied to every Redis window. Finalize/release subtracts that stored
identity-keyed integer rather than recomputing it, then adds the rounded-up
actual charge when required. Rebuild applies the same versioned conversion to
the exact journal, producing the identical materialization.

Redis Functions use only checked integers no greater than `2^53 - 1`; a
monetary window limit above **9007199.254740991 USD** is rejected at config
compile time until a different representation receives its own ADR. The
materialization can over-throttle by less than one nano-dollar per positive
event plus the discarded fractional remainder of the limit, but it can never
under-throttle relative to the exact PostgreSQL record. `budget_status` exposes
decimal USD fields derived from these integers and marks them
**nano_usd_conservative**; exact historical spend continues to come from
PostgreSQL. Property tests cover rounding boundaries, safe-integer bounds,
identity-keyed subtraction, rebuild determinism, and non-negative sums. No
hand-written arbitrary-precision Lua arithmetic is introduced.

### The only PostgreSQL budget-read conditions

A worker joining an already-live fleet registers/renews its Redis lease, reads
the Redis manifest, and tails the Stream. It performs no PostgreSQL budget
read. Routine admission, completion, budget-status queries, policy refresh,
stream-gap recovery, health checks, maintenance, and single-worker restarts
also perform no PostgreSQL budget read.

PostgreSQL budget reads are allowed only after Redis adoption fails and one of
these two conditions holds:

1. Redis worker leases prove there are zero live Go workers and no reconnecting
   process presents a rostered session ID, so the fleet is performing a cold
   full-service bootstrap; or
2. Redis has a verified new process/dataset incarnation after a restart or
   failover and the persisted generation is missing/incomplete, proving that
   the replacement did not retain the required data.

Before either rebuild path, a cold worker validates the existing manifest,
member catalog, generation/incarnation binding, high-water mark, and all active
horizon sentinels. An intact same-incarnation generation is adopted in place,
even when the live-lease set is empty; a fleet that scaled to zero does not
rebuild merely because it restarted. This adoption reads Redis only.

When adoption is impossible and one allowed condition is proven, one worker
acquires the namespaced bootstrap fence; all others wait and keep admission
closed. The coordinator reads only the active horizon plus required
open reservations and ordered journal tail, constructs a new generation under
new keys, verifies every count/digest/bucket, and atomically switches
`budget:active-generation` while appending the generation-switch Stream event.
For the final catch-up it takes a PostgreSQL advisory fence also taken in shared
mode by budget-journal writers, captures the final journal sequence, applies
through it, flips Redis, then releases the fence. A writer racing the flip may
reapply an event, so every event is idempotent by journal ID.

Each running worker keeps the last verified Redis server run ID and dataset
incarnation in process memory; the manifest records the incarnation that owns
the generation. A changed server run ID alone does not authorize PostgreSQL:
an intact, verified generation resumes entirely from Redis. A missing member or
digest mismatch under the same incarnation is unexplained corruption and fails
closed without a PostgreSQL read. Recovery then requires operators to restore
Redis or deliberately replace the dataset under the documented fenced
procedure. A new Go process that has no prior Redis observation can rebuild only
through the zero-live-worker full-fleet condition.

### Availability trade and operator recovery

New paid work deliberately fails closed when Temporal, PostgreSQL, Redis, the
encrypted blob store, or KMS cannot satisfy its part of the protocol. The
same-incarnation corruption rule can therefore turn a bounded risk of budget
overspend into a full paid-work outage requiring an operator decision. This is
intentional: the worker cannot prove which Redis members disappeared, and
silently rebuilding while another writer might still rely on that generation
could create budget capacity. Read-only health endpoints may remain available;
Generate, Compact, cache fills, and provider-refresh queries do not bypass the
failed dependency.

The design-time
[Redis budget generation recovery runbook](../runbooks/redis-budget-generation-recovery.md)
distinguishes
an intact generation to adopt, persistent restart, verified new empty
incarnation, same-incarnation partial loss, and an operator-authorized dataset
replacement. It requires evidence capture, freezes paid dispatch, identifies
the active journal high-water mark, uses the bootstrap fence, verifies the new
generation before the atomic flip, and defines rollback/escalation. Readiness
must name the failed proof and link the runbook; it must not suggest `FLUSHDB`
or an unfenced manual key edit. Implementation must replace its placeholders
with tested deployment-specific commands before production paid work is enabled.

There is no periodic PostgreSQL-to-Redis reconciliation, no per-worker startup
query, and no PostgreSQL fallback on a Redis miss. Historical spend queries use
operation/cost rows, not budget working-set tables. The design assumes normal
ongoing throughput stays vastly below 100 new logical LLM requests per second
across the entire deployment. That figure is solely a sizing and test envelope:
the worker does not enforce it, expose it as configuration, or reject work
because of it. Occasional large batches remain queued in Temporal and drain
under the configured worker/provider concurrency. The design intentionally
retains one atomic hash slot and is not sharded for hypothetical sustained
traffic. If measured ongoing demand approaches or exceeds the envelope,
perform a new architecture review, including whether Temporal is still the
appropriate execution model.

Because no production deployment exists, implementation initializes an empty
PostgreSQL namespace and changes the pre-release storage composition directly.
Keeping Redis throttles is not a compatibility dual-write: Redis and PostgreSQL
own different facts. Do not build Redis-to-PostgreSQL copying, backfill, legacy
namespace fallback, or an in-place relation rename for this change. If a
durable namespace or schema must change after the first release, it receives a
separate migration design then.

## Production schema rules

- Use PostgreSQL 17 or a later explicitly qualified version.
- Default to the **llm_worker** database and schema, an empty table prefix, and
  least-privileged owner/runtime/maintenance roles. Apply the configured
  physical namespace rules below when operators select another layout.
- All timestamps are **timestamptz** and use database time. Sessions set
  **TIME ZONE 'UTC'**; UTC is an interpretation guarantee, not a different
  PostgreSQL storage type.
- Application IDs are UUIDv7 generated in Go from an injected source. Public
  handles remain random, MAC-authenticated opaque strings.
- Sensitive lookup values use keyed HMAC columns. Reversible provider IDs and
  object locators use envelope-encrypted bytea plus a key ID. Secrets and raw
  prompt text do not enter indexed columns.
- Each Generate/Compact operation stores a bounded, content-free request
  manifest as JSONB plus exactly one envelope-encrypted inline payload or
  encrypted blob reference. Cache entries likewise store only a canonical
  manifest of digests and references as JSONB. Raw prompt/tool/output text is
  never stored as ordinary JSONB, and no manifest receives a broad GIN index.
- SHA-256 and HMAC columns are exactly 32 bytes. No raw tenant, prompt, API key,
  provider poll token, or provider error body is stored.
- Every monetary column is **NUMERIC(38,18)**, non-negative, and denominated in
  USD. JSON encodes it as a decimal string. No **real**, **double precision**,
  PostgreSQL **money**, or Go floating point may enter accounting arithmetic.
- Foreign-key columns used for joins or deletion are explicitly indexed;
  PostgreSQL does not create those indexes automatically.
- Low-cardinality flags are handled with partial indexes, not stand-alone
  Boolean indexes. JSONB is not given a broad GIN index until an observed query
  requires a specific expression index.
- External blobs are immutable and content-addressed. Database rows store
  bounded metadata and encrypted locators.
- Install the entire initial schema transactionally and create its indexes
  normally because there is no existing data. Post-release online schema
  changes are out of scope for this change.

PostgreSQL supports substantially more decimal precision than this design uses.
**NUMERIC(38,18)** gives 20 whole-dollar digits and 18 fractional digits while
keeping one uniform type across price, budget, reservation, and actual-cost
arithmetic.

This deliberately replaces the current integer micro-USD authority. It can
represent costs below one millionth of a dollar while also representing whole
dollars, ten-dollar charges, and aggregate budgets up to 20 whole-dollar
digits. An unknown price or actual cost is **NULL**, never numeric zero. Zero
means the worker knows the charge is exactly zero. Every nullable money value is
paired with a closed status and, when unknown, a safe reason code. Estimates,
reservations, and conservative budget charges stay in separate non-null fields;
none is copied into **actual_cost_usd** merely to avoid a null.

## Configurable physical namespace

The state configuration selects three independent identifiers:

~~~yaml
postgres:
  database: llm_worker
  schema: llm_worker
  table_prefix: ""
~~~

**LLMTW_POSTGRES_DATABASE**, **LLMTW_POSTGRES_SCHEMA**, and
**LLMTW_POSTGRES_TABLE_PREFIX** may override those non-secret YAML fields at
process start. The effective values are validated before opening a connection,
included in the configuration digest, and exposed by
**print-effective-config**. Startup verifies **current_database()** equals the
configured database and never relies on a mutable **search_path**.

Database and schema names match **[a-z][a-z0-9_]{0,62}**. Table prefix is empty
or matches **[a-z][a-z0-9_]{0,22}_**. The prefix is prepended to every
worker-owned schema object: tables, indexes, sequences, schema-version table,
constraints with explicit names, and any future view or function. Prefixing
indexes as well as tables is mandatory because index names are schema-global.
The 24-byte prefix maximum plus the longest name in this design is at most
PostgreSQL's 63-byte identifier limit; a schema-contract test enumerates every
rendered identifier and rejects truncation or collision.

DDL and queries represent identifiers as a validated structured value:

~~~go
type Namespace struct {
	Database    string
	Schema      string
	TablePrefix string
}

func (n Namespace) Relation(logicalName string) pgx.Identifier {
	return pgx.Identifier{n.Schema, n.TablePrefix + logicalName}
}
~~~

The implementation must use **pgx.Identifier.Sanitize** or an equivalent
driver identifier builder for schema objects and bind all data as parameters.
Activity input, request JSON, tenant values, and query filters can never select
an identifier. Prepared SQL is generated once per validated process-lifetime
namespace; raw strings are not interpolated into ad hoc statements.

The DDL below uses **llm_worker.logical_name** as readable logical notation. At
installation and runtime it means:

~~~text
quote_identifier(configured_schema)
  .quote_identifier(configured_table_prefix ^ logical_name)
~~~

The same substitution applies to explicitly named indexes, sequences, and
constraints. The readable DDL below omits repetitive constraint names. The
installer must emit every constraint explicitly as
`<table-prefix>c_<kind>_<table_abbrev>_<invariant_slug>`, where `kind` is `pk`,
`uq`, `fk`, or `ck`. The schema-contract fixture owns the reviewed logical
table abbreviations and semantic invariant slugs, enumerates the complete
`(table, invariant, rendered name)` catalog, and rejects any name over 63 bytes
or any collision. Names must make a production violation understandable without
a fixture lookup, for example `c_ck_ops_completed_result` and
`c_fk_cache_origin_operation`. Hash-only suffixes and PostgreSQL-generated or
truncated names are not accepted.

**CREATE SCHEMA** and schema-level **REVOKE** are executed only
when the configured schema is dedicated and owned by the worker schema-owner
role. In shared-schema mode the operator provisions schema privileges, while
the installer creates only prefixed worker objects. Runtime and maintenance
roles receive object-level grants; table-prefix isolation alone is never
described as authorization.

Namespace settings are immutable for a process. A reload that changes one is
rejected with a restart-required error. Pointing a new pre-release deployment
at a different database/schema/prefix creates another clean namespace; it does
not move or discover state from the previous selection.

## Final schema

The DDL below defines the intended initial shape. The schema installer may split
it into ordered files, but must not weaken a constraint or silently add a
different index.

### Scope, configuration, and blobs

~~~sql
CREATE SCHEMA llm_worker;
REVOKE ALL ON SCHEMA llm_worker FROM PUBLIC;

CREATE TABLE llm_worker.scopes (
    scope_id uuid PRIMARY KEY,
    tenant_hmac bytea NOT NULL CHECK (octet_length(tenant_hmac) = 32),
    project_hmac bytea NOT NULL CHECK (octet_length(project_hmac) = 32),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    deleted_at timestamptz,
    UNIQUE (tenant_hmac, project_hmac)
);

CREATE TABLE llm_worker.configuration_snapshots (
    config_digest bytea PRIMARY KEY CHECK (octet_length(config_digest) = 32),
    config_version text NOT NULL,
    source_digest bytea NOT NULL CHECK (octet_length(source_digest) = 32),
    sanitized_config jsonb NOT NULL,
    loaded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    retired_at timestamptz,
    UNIQUE (config_version, source_digest)
);

CREATE TABLE llm_worker.blobs (
    blob_id uuid PRIMARY KEY,
    scope_id uuid NOT NULL
        REFERENCES llm_worker.scopes(scope_id) ON DELETE RESTRICT,
    store_id text NOT NULL,
    locator_ciphertext bytea NOT NULL,
    locator_key_id text NOT NULL,
    sha256 bytea NOT NULL CHECK (octet_length(sha256) = 32),
    byte_length bigint NOT NULL CHECK (byte_length >= 0),
    media_type text NOT NULL,
    encryption_context_digest bytea NOT NULL
        CHECK (octet_length(encryption_context_digest) = 32),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    expires_at timestamptz,
    deletion_state text NOT NULL DEFAULT 'retained'
        CHECK (deletion_state IN ('retained', 'eligible', 'deleting', 'deleted')),
    UNIQUE (scope_id, store_id, sha256, byte_length, media_type)
);

CREATE INDEX blobs_scope_created_idx
    ON llm_worker.blobs (scope_id, created_at DESC, blob_id);

CREATE INDEX blobs_expiry_idx
    ON llm_worker.blobs (expires_at, blob_id)
    WHERE expires_at IS NOT NULL AND deletion_state = 'retained';
~~~

**sanitized_config** is bounded, secret-free evidence of the compiled routing
snapshot. The source file remains operator configuration; the database copy
makes an operation auditable after reload. A schema validator, byte cap, and
key deny-list run before insertion.

### Operations and attempts

~~~sql
CREATE TABLE llm_worker.operations (
    operation_id uuid PRIMARY KEY,
    scope_id uuid NOT NULL
        REFERENCES llm_worker.scopes(scope_id) ON DELETE RESTRICT,
    operation_kind text NOT NULL
        CHECK (operation_kind IN ('generate', 'compact')),
    api_version text NOT NULL,
    operation_key_hmac bytea NOT NULL
        CHECK (octet_length(operation_key_hmac) = 32),
    request_fingerprint_hmac bytea NOT NULL
        CHECK (octet_length(request_fingerprint_hmac) = 32),
    request_schema_version integer NOT NULL
        CHECK (request_schema_version > 0),
    request_manifest_jsonb jsonb NOT NULL
        CHECK (jsonb_typeof(request_manifest_jsonb) = 'object'),
    request_inline_ciphertext bytea,
    request_blob_id uuid
        REFERENCES llm_worker.blobs(blob_id) ON DELETE RESTRICT,
    request_key_id text,
    config_digest bytea NOT NULL
        REFERENCES llm_worker.configuration_snapshots(config_digest)
        ON DELETE RESTRICT,
    state text NOT NULL CHECK (
        state IN (
            'reserved',
            'dispatching',
            'provider_pending',
            'completed',
            'definite_failed',
            'ambiguous',
            'canceled'
        )
    ),
    parent_operation_id uuid
        REFERENCES llm_worker.operations(operation_id) ON DELETE RESTRICT,
    parent_checkpoint_id uuid,
    result_inline_ciphertext bytea,
    result_blob_id uuid
        REFERENCES llm_worker.blobs(blob_id) ON DELETE RESTRICT,
    result_key_id text,
    result_digest bytea CHECK (
        result_digest IS NULL OR octet_length(result_digest) = 32
    ),
    cache_entry_id uuid,
    route_id text,
    endpoint_id text,
    provider text,
    endpoint_family text,
    resolved_model text,
    route_model_revision text,
    requested_service_class text CHECK (
        requested_service_class IS NULL OR
        requested_service_class IN ('economy', 'standard', 'priority')
    ),
    attempted_service_class text CHECK (
        attempted_service_class IS NULL OR
        attempted_service_class IN ('economy', 'standard', 'priority')
    ),
    actual_service_class text CHECK (
        actual_service_class IS NULL OR
        actual_service_class IN ('economy', 'standard', 'priority')
    ),
    provider_idempotency_key_hmac bytea CHECK (
        provider_idempotency_key_hmac IS NULL OR
        octet_length(provider_idempotency_key_hmac) = 32
    ),
    provider_idempotency_key_ciphertext bytea,
    provider_operation_id_hmac bytea CHECK (
        provider_operation_id_hmac IS NULL OR
        octet_length(provider_operation_id_hmac) = 32
    ),
    provider_operation_id_ciphertext bytea,
    provider_reference_key_id text,
    provider_submit_started_at timestamptz,
    provider_accepted_at timestamptz,
    provider_pending_at timestamptz,
    poll_after timestamptz,
    last_polled_at timestamptz,
    poll_count integer NOT NULL DEFAULT 0 CHECK (poll_count >= 0),
    lease_owner uuid,
    lease_expires_at timestamptz,
    reserved_cost_usd numeric(38,18) NOT NULL DEFAULT 0
        CHECK (reserved_cost_usd >= 0),
    incurred_cost_usd numeric(38,18) NOT NULL DEFAULT 0
        CHECK (incurred_cost_usd >= 0),
    actual_cost_usd numeric(38,18)
        CHECK (actual_cost_usd IS NULL OR actual_cost_usd >= 0),
    cost_status text NOT NULL DEFAULT 'pending' CHECK (
        cost_status IN ('pending', 'exact', 'unknown')
    ),
    cost_method text CHECK (
        cost_method IS NULL OR
        cost_method IN (
            'provider_reported',
            'catalog_usage',
            'reconstructed_usage',
            'worker_cache_zero'
        )
    ),
    cost_unknown_reason_code text CHECK (
        cost_unknown_reason_code IS NULL OR
        cost_unknown_reason_code ~ '^[a-z0-9_]{1,64}$'
    ),
    cost_catalog_version text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    retention_expires_at timestamptz,
    UNIQUE (scope_id, operation_kind, operation_key_hmac),
    CHECK (
        state <> 'provider_pending' OR (
            provider_operation_id_ciphertext IS NOT NULL AND
            provider_operation_id_hmac IS NOT NULL AND
            provider_reference_key_id IS NOT NULL AND
            provider_pending_at IS NOT NULL
        )
    ),
    CHECK (
        state <> 'completed' OR (
            completed_at IS NOT NULL AND
            ((result_inline_ciphertext IS NOT NULL)::integer +
             (result_blob_id IS NOT NULL)::integer = 1) AND
            result_digest IS NOT NULL AND
            cost_status IN ('exact', 'unknown')
        )
    ),
    CHECK (
        (request_inline_ciphertext IS NOT NULL)::integer +
        (request_blob_id IS NOT NULL)::integer = 1
    ),
    CHECK (
        request_inline_ciphertext IS NULL = (request_key_id IS NULL)
    ),
    CHECK (
        result_inline_ciphertext IS NULL = (result_key_id IS NULL)
    ),
    CHECK (
        (cost_status = 'pending' AND
            actual_cost_usd IS NULL AND
            cost_method IS NULL AND
            cost_unknown_reason_code IS NULL) OR
        (cost_status = 'exact' AND
            actual_cost_usd IS NOT NULL AND
            cost_method IS NOT NULL AND
            cost_unknown_reason_code IS NULL) OR
        (cost_status = 'unknown' AND
            actual_cost_usd IS NULL AND
            cost_method IS NULL AND
            cost_unknown_reason_code IS NOT NULL)
    ),
    CHECK (
        provider_operation_id_ciphertext IS NULL =
        (provider_operation_id_hmac IS NULL)
    ),
    CHECK (
        provider_idempotency_key_ciphertext IS NULL =
        (provider_idempotency_key_hmac IS NULL)
    ),
    CHECK (
        provider_idempotency_key_ciphertext IS NULL OR
        provider_reference_key_id IS NOT NULL
    ),
    CHECK (
        provider_operation_id_ciphertext IS NULL OR
        endpoint_id IS NOT NULL
    ),
    CHECK (
        cost_method <> 'worker_cache_zero' OR
        actual_cost_usd = 0
    ),
    CHECK (
        cost_method NOT IN ('catalog_usage', 'reconstructed_usage') OR
        cost_catalog_version IS NOT NULL
    ),
    CHECK (completed_at IS NULL OR completed_at >= created_at),
    CHECK (
        (state IN ('completed', 'definite_failed', 'canceled') AND
            retention_expires_at IS NOT NULL) OR
        (state IN ('reserved', 'dispatching', 'provider_pending', 'ambiguous') AND
            retention_expires_at IS NULL)
    )
);

CREATE TABLE llm_worker.operation_attempts (
    attempt_id uuid PRIMARY KEY,
    operation_id uuid NOT NULL
        REFERENCES llm_worker.operations(operation_id) ON DELETE RESTRICT,
    attempt_number integer NOT NULL CHECK (attempt_number > 0),
    route_index integer NOT NULL CHECK (route_index >= 0),
    fallback_index integer NOT NULL CHECK (fallback_index >= 0),
    route_id text NOT NULL,
    endpoint_id text NOT NULL,
    provider text NOT NULL,
    endpoint_family text NOT NULL,
    resolved_model text NOT NULL,
    route_model_revision text NOT NULL,
    state text NOT NULL CHECK (
        state IN (
            'planned',
            'pre_write_failed',
            'submitted',
            'provider_pending',
            'completed',
            'definite_failed',
            'ambiguous',
            'canceled'
        )
    ),
    provider_request_id_hmac bytea CHECK (
        provider_request_id_hmac IS NULL OR
        octet_length(provider_request_id_hmac) = 32
    ),
    provider_request_id_ciphertext bytea,
    provider_reference_key_id text,
    safe_error_code text,
    dispatch_disposition text CHECK (
        dispatch_disposition IS NULL OR
        dispatch_disposition IN (
            'not_dispatched',
            'rejected',
            'accepted',
            'ambiguous'
        )
    ),
    reserved_cost_usd numeric(38,18) NOT NULL DEFAULT 0
        CHECK (reserved_cost_usd >= 0),
    actual_cost_usd numeric(38,18)
        CHECK (actual_cost_usd IS NULL OR actual_cost_usd >= 0),
    cost_status text NOT NULL DEFAULT 'pending' CHECK (
        cost_status IN ('pending', 'exact', 'unknown')
    ),
    cost_method text CHECK (
        cost_method IS NULL OR
        cost_method IN (
            'provider_reported',
            'catalog_usage',
            'reconstructed_usage',
            'definite_uncharged_zero'
        )
    ),
    cost_catalog_version text,
    cost_unknown_reason_code text CHECK (
        cost_unknown_reason_code IS NULL OR
        cost_unknown_reason_code ~ '^[a-z0-9_]{1,64}$'
    ),
    started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    possible_write_at timestamptz,
    finished_at timestamptz,
    safe_diagnostics jsonb NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE (operation_id, attempt_number),
    CHECK (
        provider_request_id_ciphertext IS NULL =
        (provider_request_id_hmac IS NULL)
    ),
    CHECK (
        provider_request_id_ciphertext IS NULL OR
        provider_reference_key_id IS NOT NULL
    ),
    CHECK (
        (cost_status = 'pending' AND
            actual_cost_usd IS NULL AND
            cost_method IS NULL AND
            cost_unknown_reason_code IS NULL) OR
        (cost_status = 'exact' AND
            actual_cost_usd IS NOT NULL AND
            cost_method IS NOT NULL AND
            cost_unknown_reason_code IS NULL) OR
        (cost_status = 'unknown' AND
            actual_cost_usd IS NULL AND
            cost_method IS NULL AND
            cost_unknown_reason_code IS NOT NULL)
    ),
    CHECK (finished_at IS NULL OR cost_status IN ('exact', 'unknown')),
    CHECK (
        cost_method <> 'definite_uncharged_zero' OR actual_cost_usd = 0
    ),
    CHECK (
        cost_method NOT IN ('catalog_usage', 'reconstructed_usage') OR
        cost_catalog_version IS NOT NULL
    ),
    CHECK (finished_at IS NULL OR finished_at >= started_at)
);

CREATE UNIQUE INDEX operations_provider_operation_uidx
    ON llm_worker.operations (endpoint_id, provider_operation_id_hmac)
    WHERE provider_operation_id_hmac IS NOT NULL;

CREATE INDEX operations_parent_operation_idx
    ON llm_worker.operations (parent_operation_id)
    WHERE parent_operation_id IS NOT NULL;

CREATE INDEX operations_parent_checkpoint_idx
    ON llm_worker.operations (parent_checkpoint_id)
    WHERE parent_checkpoint_id IS NOT NULL;

CREATE INDEX operations_cache_entry_idx
    ON llm_worker.operations (cache_entry_id)
    WHERE cache_entry_id IS NOT NULL;

CREATE INDEX operations_config_idx
    ON llm_worker.operations (config_digest, operation_id);

CREATE INDEX operations_result_blob_idx
    ON llm_worker.operations (result_blob_id)
    WHERE result_blob_id IS NOT NULL;

CREATE INDEX operations_request_blob_idx
    ON llm_worker.operations (request_blob_id)
    WHERE request_blob_id IS NOT NULL;

CREATE INDEX operations_lease_recovery_idx
    ON llm_worker.operations (lease_expires_at, operation_id)
    WHERE state IN ('reserved', 'dispatching');

CREATE INDEX operations_poll_due_idx
    ON llm_worker.operations (poll_after, operation_id)
    INCLUDE (endpoint_id, provider)
    WHERE state = 'provider_pending';

CREATE INDEX operations_scope_spend_idx
    ON llm_worker.operations (scope_id, completed_at DESC, operation_id)
    INCLUDE (
        operation_kind,
        actual_cost_usd,
        cost_status,
        cost_method,
        endpoint_id,
        resolved_model,
        route_model_revision
    )
    WHERE state = 'completed';

CREATE INDEX operations_unknown_cost_idx
    ON llm_worker.operations (scope_id, completed_at DESC, operation_id)
    INCLUDE (operation_kind, endpoint_id, resolved_model, cost_unknown_reason_code)
    WHERE state = 'completed' AND cost_status = 'unknown';

CREATE INDEX operations_terminal_expiry_idx
    ON llm_worker.operations (retention_expires_at, operation_id)
    WHERE state IN ('completed', 'definite_failed', 'canceled');

CREATE INDEX operations_exact_terminal_expiry_idx
    ON llm_worker.operations (retention_expires_at, operation_id)
    WHERE state IN ('completed', 'definite_failed', 'canceled')
      AND cost_status = 'exact';

CREATE INDEX operations_completed_brin_idx
    ON llm_worker.operations USING brin (completed_at)
    WITH (pages_per_range = 64)
    WHERE completed_at IS NOT NULL;

CREATE INDEX operation_attempts_provider_request_idx
    ON llm_worker.operation_attempts (endpoint_id, provider_request_id_hmac)
    WHERE provider_request_id_hmac IS NOT NULL;

CREATE INDEX operation_attempts_route_time_idx
    ON llm_worker.operation_attempts
        (endpoint_id, resolved_model, started_at DESC, attempt_id)
    INCLUDE (state, safe_error_code, actual_cost_usd, cost_status);
~~~

The completed-operation check is intentionally explicit: a successful Generate
or Compact settles to either **exact** or **unknown** cost. Exact requires
a decimal USD amount and method. Unknown requires a safe reason and a NULL
amount/method; it is never rewritten to zero or to the reservation. A
provider-reported price is used when trustworthy; otherwise exact catalog
arithmetic over final usage is used. An ambiguous external dispatch outcome
remains operation state **ambiguous**, rather than masquerading as a completed
operation with a zero cost.

**updated_at** changes only in the same transaction as a state transition; no
generic trigger creates extra writes. State transitions use a compare-and-set
predicate on current state and lease owner. **operation_attempts** preserves
each route/fallback cost and classified outcome instead of overwriting the
operation with only its last attempt.

`retention_expires_at` is not an execution deadline. It is NULL for every live
state and for **ambiguous**, so crossing a caller timeout never makes a pending
provider job replayable or reclaimable. Lease/poll recovery continues until a
classified transition. Terminal completion/failure/cancellation sets the
retention timestamp. Ambiguous work retains its budget bound and row without an
automatic expiry until an operator or trusted billing reconciliation resolves
it; that resolution then sets the normal terminal retention timestamp.

The request and result manifests contain schema versions, settings/content
digests, and encrypted-object references only. Payloads below a strict operator
cap may use the inline ciphertext columns; larger payloads use **blobs**. Both
forms use per-scope envelope encryption and authenticated context binding the
scope, operation, payload kind, and digest. The database never contains raw
prompt/output bytes in JSONB or plaintext bytea, while small calls avoid a
mandatory object-store round trip.

### Immutable checkpoints and provider state

~~~sql
CREATE TABLE llm_worker.conversation_checkpoints (
    checkpoint_id uuid PRIMARY KEY,
    scope_id uuid NOT NULL
        REFERENCES llm_worker.scopes(scope_id) ON DELETE RESTRICT,
    public_id_hmac bytea NOT NULL UNIQUE
        CHECK (octet_length(public_id_hmac) = 32),
    handle_key_id text NOT NULL,
    parent_checkpoint_id uuid
        REFERENCES llm_worker.conversation_checkpoints(checkpoint_id)
        ON DELETE RESTRICT,
    checkpoint_kind text NOT NULL CHECK (
        checkpoint_kind IN ('generation', 'compaction', 'cache_replay')
    ),
    depth integer NOT NULL CHECK (depth >= 0),
    origin_operation_id uuid NOT NULL UNIQUE
        REFERENCES llm_worker.operations(operation_id) ON DELETE RESTRICT,
    origin_cache_entry_id uuid,
    delta_blob_id uuid NOT NULL
        REFERENCES llm_worker.blobs(blob_id) ON DELETE RESTRICT,
    response_blob_id uuid NOT NULL
        REFERENCES llm_worker.blobs(blob_id) ON DELETE RESTRICT,
    settings_patch_blob_id uuid NOT NULL
        REFERENCES llm_worker.blobs(blob_id) ON DELETE RESTRICT,
    materialized_snapshot_blob_id uuid
        REFERENCES llm_worker.blobs(blob_id) ON DELETE RESTRICT,
    canonical_lineage_digest bytea NOT NULL
        CHECK (octet_length(canonical_lineage_digest) = 32),
    materialized_settings_digest bytea NOT NULL
        CHECK (octet_length(materialized_settings_digest) = 32),
    tool_frontier_digest bytea NOT NULL
        CHECK (octet_length(tool_frontier_digest) = 32),
    schema_version integer NOT NULL CHECK (schema_version > 0),
    compiler_epoch text NOT NULL,
    compaction_policy_version text,
    compaction_prompt_version text,
    compacted_through_checkpoint_id uuid
        REFERENCES llm_worker.conversation_checkpoints(checkpoint_id)
        ON DELETE RESTRICT,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    expires_at timestamptz NOT NULL,
    CHECK (
        (parent_checkpoint_id IS NULL AND depth = 0) OR
        (parent_checkpoint_id IS NOT NULL AND depth > 0)
    ),
    CHECK (parent_checkpoint_id IS DISTINCT FROM checkpoint_id)
);

CREATE TABLE llm_worker.checkpoint_provider_state (
    checkpoint_id uuid NOT NULL
        REFERENCES llm_worker.conversation_checkpoints(checkpoint_id)
        ON DELETE RESTRICT,
    ordinal integer NOT NULL CHECK (ordinal >= 0),
    provider text NOT NULL,
    endpoint_id text NOT NULL,
    endpoint_account_hmac bytea NOT NULL
        CHECK (octet_length(endpoint_account_hmac) = 32),
    region text NOT NULL,
    endpoint_family text NOT NULL,
    model_lineage text NOT NULL,
    state_kind text NOT NULL,
    state_blob_id uuid NOT NULL
        REFERENCES llm_worker.blobs(blob_id) ON DELETE RESTRICT,
    state_digest bytea NOT NULL CHECK (octet_length(state_digest) = 32),
    required boolean NOT NULL,
    immutable_fork_safe boolean NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    expires_at timestamptz,
    PRIMARY KEY (checkpoint_id, ordinal)
);

CREATE TABLE llm_worker.checkpoint_provider_affinities (
    checkpoint_id uuid NOT NULL
        REFERENCES llm_worker.conversation_checkpoints(checkpoint_id)
        ON DELETE RESTRICT,
    affinity_rank smallint NOT NULL CHECK (affinity_rank >= 0),
    provider text NOT NULL,
    route_id text NOT NULL,
    endpoint_id text NOT NULL,
    endpoint_account_hmac bytea NOT NULL
        CHECK (octet_length(endpoint_account_hmac) = 32),
    region text NOT NULL,
    endpoint_family text NOT NULL,
    model_lineage text NOT NULL,
    route_model_revision text NOT NULL,
    provider_cache_key_hmac bytea CHECK (
        provider_cache_key_hmac IS NULL OR
        octet_length(provider_cache_key_hmac) = 32
    ),
    cache_epoch text NOT NULL,
    hard_pinned boolean NOT NULL,
    observed_cache_read_tokens bigint NOT NULL DEFAULT 0
        CHECK (observed_cache_read_tokens >= 0),
    observed_cache_write_tokens bigint NOT NULL DEFAULT 0
        CHECK (observed_cache_write_tokens >= 0),
    last_success_at timestamptz NOT NULL,
    expires_at timestamptz,
    PRIMARY KEY (checkpoint_id, affinity_rank)
);

CREATE INDEX checkpoints_parent_idx
    ON llm_worker.conversation_checkpoints
        (parent_checkpoint_id, created_at, checkpoint_id)
    WHERE parent_checkpoint_id IS NOT NULL;

CREATE INDEX checkpoints_scope_created_idx
    ON llm_worker.conversation_checkpoints
        (scope_id, created_at DESC, checkpoint_id);

CREATE INDEX checkpoints_retention_idx
    ON llm_worker.conversation_checkpoints (expires_at, checkpoint_id);

CREATE INDEX checkpoints_compacted_through_idx
    ON llm_worker.conversation_checkpoints (compacted_through_checkpoint_id)
    WHERE compacted_through_checkpoint_id IS NOT NULL;

CREATE INDEX checkpoints_delta_blob_idx
    ON llm_worker.conversation_checkpoints (delta_blob_id);

CREATE INDEX checkpoints_response_blob_idx
    ON llm_worker.conversation_checkpoints (response_blob_id);

CREATE INDEX checkpoints_settings_blob_idx
    ON llm_worker.conversation_checkpoints (settings_patch_blob_id);

CREATE INDEX checkpoints_snapshot_blob_idx
    ON llm_worker.conversation_checkpoints (materialized_snapshot_blob_id)
    WHERE materialized_snapshot_blob_id IS NOT NULL;

CREATE INDEX checkpoint_provider_state_blob_idx
    ON llm_worker.checkpoint_provider_state (state_blob_id);

CREATE INDEX checkpoint_provider_state_expiry_idx
    ON llm_worker.checkpoint_provider_state (expires_at, checkpoint_id, ordinal)
    WHERE expires_at IS NOT NULL;

CREATE INDEX checkpoint_provider_state_route_idx
    ON llm_worker.checkpoint_provider_state
        (endpoint_id, endpoint_family, model_lineage, checkpoint_id);

CREATE INDEX checkpoint_affinity_route_idx
    ON llm_worker.checkpoint_provider_affinities
        (checkpoint_id, hard_pinned DESC, affinity_rank)
    INCLUDE (route_id, endpoint_id, route_model_revision, expires_at);

ALTER TABLE llm_worker.operations
    ADD CONSTRAINT operations_parent_checkpoint_fk
    FOREIGN KEY (parent_checkpoint_id)
    REFERENCES llm_worker.conversation_checkpoints(checkpoint_id)
    ON DELETE RESTRICT
    DEFERRABLE INITIALLY DEFERRED;
~~~

Checkpoint INSERT runs after the operation and all referenced blobs exist, then
operation completion and result publication occur in the same transaction.
The deferred parent foreign key permits that ordered finalization without
disabling integrity.

The installer reconciles object privileges for the pre-provisioned roles on
every install (including an idempotent install against an existing contract).
The runtime role receives `SELECT, INSERT` on
`conversation_checkpoints`, `checkpoint_provider_state`, and
`checkpoint_provider_affinities`; it receives neither `UPDATE` nor `DELETE` on
those tables. The same append-only rule applies to provider status events and
inventory snapshots/models. Blob rows are immutable apart from a runtime
`UPDATE (expires_at)` used to extend retention. Operations, cache fills/uses,
and the mutable provider-route and budget projections receive only the writes
their state machines require. The runtime role never receives `DELETE` on any
worker table. Query-execution rows are immutable after publication except for
the one-column `response_digest` finalization performed in the same
transaction. Retention and other destructive work uses the separate
`llmtw_maintenance` role, while the schema-owner role remains responsible for
DDL. This database permission makes immutability stronger than a repository
convention and prevents a broad `GRANT ... ON ALL TABLES` regression.

### Route-isolated cache identity

The first cache phase does not share entries across providers or routes. Public
provider documentation does not reliably expose weights revision, quantization,
chat template, or hidden safety-transform identity, so a database certification
catalog would create ceremony without evidence. The cache key includes a keyed
route identity derived from configuration digest, provider, endpoint/account,
region, resolved model/revision, and compiler profile. An OpenAI, Azure OpenAI,
OpenRouter, or third-party route therefore remains isolated even when their
display model names match.

Cross-provider reuse remains a deliberate future optimization because a caller
may know that two endpoints host the same artifact. It is not implemented until
at least one concrete pair has independently verifiable artifact and lowering
evidence. A superseding ADR must define that evidence, negative quantization and
hidden-transform tests, invalidation, schema/index changes, and a new cache
epoch. Existing entries are never silently re-keyed. This deferral preserves the
requested extension point without encouraging operators to certify on guesswork.

### Exact-response cache

~~~sql
CREATE TABLE llm_worker.response_cache_entries (
    cache_entry_id uuid PRIMARY KEY,
    scope_id uuid NOT NULL
        REFERENCES llm_worker.scopes(scope_id) ON DELETE RESTRICT,
    fingerprint_version integer NOT NULL CHECK (fingerprint_version > 0),
    semantic_fingerprint_hmac bytea NOT NULL
        CHECK (octet_length(semantic_fingerprint_hmac) = 32),
    canonical_request_digest bytea NOT NULL
        CHECK (octet_length(canonical_request_digest) = 32),
    canonical_request_jsonb jsonb NOT NULL
        CHECK (jsonb_typeof(canonical_request_jsonb) = 'object'),
    variant integer NOT NULL CHECK (variant >= 0),
    cache_route_identity_hmac bytea NOT NULL
        CHECK (octet_length(cache_route_identity_hmac) = 32),
    semantic_profile_version text NOT NULL,
    cache_epoch text NOT NULL,
    response_inline_ciphertext bytea,
    response_key_id text,
    response_blob_id uuid
        REFERENCES llm_worker.blobs(blob_id) ON DELETE RESTRICT,
    response_digest bytea NOT NULL CHECK (octet_length(response_digest) = 32),
    origin_operation_id uuid NOT NULL UNIQUE
        REFERENCES llm_worker.operations(operation_id) ON DELETE RESTRICT,
    origin_checkpoint_id uuid NOT NULL
        REFERENCES llm_worker.conversation_checkpoints(checkpoint_id)
        ON DELETE RESTRICT,
    origin_provider text NOT NULL,
    origin_endpoint_id text NOT NULL,
    origin_resolved_model text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz NOT NULL,
    last_used_at timestamptz NOT NULL,
    use_count integer NOT NULL DEFAULT 1
        CHECK (use_count BETWEEN 1 AND 2147483647),
    state text NOT NULL DEFAULT 'ready'
        CHECK (state IN ('ready', 'tombstoned')),
    CHECK (
        (response_inline_ciphertext IS NOT NULL)::integer +
        (response_blob_id IS NOT NULL)::integer = 1
    ),
    CHECK (
        response_inline_ciphertext IS NULL = (response_key_id IS NULL)
    ),
    CHECK (completed_at >= created_at),
    CHECK (last_used_at >= completed_at)
);

CREATE UNIQUE INDEX response_cache_reusable_key_uidx
    ON llm_worker.response_cache_entries (
        scope_id,
        fingerprint_version,
        semantic_fingerprint_hmac,
        variant,
        cache_route_identity_hmac
    )
    WHERE state = 'ready';

CREATE TABLE llm_worker.response_cache_uses (
    cache_entry_id uuid NOT NULL
        REFERENCES llm_worker.response_cache_entries(cache_entry_id)
        ON DELETE RESTRICT,
    operation_id uuid NOT NULL UNIQUE
        REFERENCES llm_worker.operations(operation_id) ON DELETE RESTRICT,
    first_used_at timestamptz NOT NULL,
    PRIMARY KEY (cache_entry_id, operation_id)
);

CREATE TABLE llm_worker.response_cache_fills (
    scope_id uuid NOT NULL
        REFERENCES llm_worker.scopes(scope_id) ON DELETE RESTRICT,
    fingerprint_version integer NOT NULL CHECK (fingerprint_version > 0),
    semantic_fingerprint_hmac bytea NOT NULL
        CHECK (octet_length(semantic_fingerprint_hmac) = 32),
    variant integer NOT NULL CHECK (variant >= 0),
    cache_route_identity_hmac bytea NOT NULL
        CHECK (octet_length(cache_route_identity_hmac) = 32),
    owner_operation_id uuid NOT NULL
        REFERENCES llm_worker.operations(operation_id) ON DELETE RESTRICT,
    state text NOT NULL CHECK (state IN ('filling', 'completed', 'failed')),
    lease_expires_at timestamptz NOT NULL,
    cache_entry_id uuid
        REFERENCES llm_worker.response_cache_entries(cache_entry_id)
        ON DELETE RESTRICT,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (
        scope_id,
        fingerprint_version,
        semantic_fingerprint_hmac,
        variant,
        cache_route_identity_hmac
    ),
    CHECK (state <> 'completed' OR cache_entry_id IS NOT NULL)
);

CREATE INDEX response_cache_gc_idx
    ON llm_worker.response_cache_entries (last_used_at, cache_entry_id)
    WHERE state = 'ready';

CREATE INDEX response_cache_route_invalidation_idx
    ON llm_worker.response_cache_entries
        (cache_route_identity_hmac, completed_at DESC, cache_entry_id)
    WHERE state = 'ready';

CREATE INDEX response_cache_origin_checkpoint_idx
    ON llm_worker.response_cache_entries (origin_checkpoint_id);

CREATE INDEX response_cache_response_blob_idx
    ON llm_worker.response_cache_entries (response_blob_id)
    WHERE response_blob_id IS NOT NULL;

CREATE INDEX response_cache_fill_owner_idx
    ON llm_worker.response_cache_fills (owner_operation_id);

CREATE INDEX response_cache_fill_entry_idx
    ON llm_worker.response_cache_fills (cache_entry_id)
    WHERE cache_entry_id IS NOT NULL;

CREATE INDEX response_cache_fill_lease_idx
    ON llm_worker.response_cache_fills (lease_expires_at, owner_operation_id)
    WHERE state = 'filling';

ALTER TABLE llm_worker.operations
    ADD CONSTRAINT operations_cache_entry_fk
    FOREIGN KEY (cache_entry_id)
    REFERENCES llm_worker.response_cache_entries(cache_entry_id)
    ON DELETE RESTRICT
    DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE llm_worker.conversation_checkpoints
    ADD CONSTRAINT checkpoints_origin_cache_entry_fk
    FOREIGN KEY (origin_cache_entry_id)
    REFERENCES llm_worker.response_cache_entries(cache_entry_id)
    ON DELETE RESTRICT
    DEFERRABLE INITIALLY DEFERRED;

CREATE INDEX checkpoints_origin_cache_entry_idx
    ON llm_worker.conversation_checkpoints (origin_cache_entry_id)
    WHERE origin_cache_entry_id IS NOT NULL;
~~~

PostgreSQL's integer type is signed int32, matching the wire variant and
**use_count** requirement. The application validates temperature/variant
semantics because temperature is part of materialized encrypted content, not a
cache table column.

The semantic fingerprint includes an explicit Generate-versus-Compact domain
separator. It excludes provider/endpoint/account/region, requested
service class/fallbacks, operation identity, maximum cache age, price
versions, health, deadlines, budgets, tracing, timestamps, and actor-only tags.
It includes conversation content, every output-affecting setting/extension,
route-cache identity, semantic/compiler/cache epochs, required opaque
state identity, and compaction versions/artifact identity. The database key adds
variant as its own int32 column, and Compact permits only variant zero. A
supposedly neutral provider extension may be excluded only when its catalog
schema marks it non-semantic and a fixture proves identical lowering.

Cache lookup uses the B-tree unique key containing
**semantic_fingerprint_hmac**, which is HMAC-SHA-256 over a versioned canonical
encoding. It never searches **canonical_request_jsonb**. After a key match, the
worker verifies **canonical_request_digest** and the canonical manifest before
reuse so a canonicalizer/version defect fails closed rather than serving the
wrong response.

The operation **request_manifest_jsonb** is a content-free normalized manifest
for its Generate or Compact Activity. It includes scope digests, parent-handle
digest, payload references, sparse-patch digest, and cache policy; the actual
delta/patch is envelope-encrypted inline or stored as an encrypted blob. The cache
**canonical_request_jsonb** is a materialized Generate-or-Compact cache-key
manifest: operation-kind domain, semantic settings and ordered content
references/digests, route-cache identity, compiler/compaction epochs, and
variant. It does not duplicate ancestor blob
bytes or the full transcript. The JSONB rows support audit, safe debugging, and
hash verification; all equality and freshness lookups use fixed-size columns.
Application canonical bytes, not PostgreSQL's JSONB text serialization, are
hashed.

An inline cached response is envelope-encrypted and authenticated to its scope,
cache entry, response digest, and payload kind; `response_key_id` identifies
the wrapping key. Larger responses reference an encrypted blob. Cache rows
never retain a plaintext model output merely because it is small.

On a hit, this statement pattern increments usage exactly once per logical
operation and saturates instead of overflowing:

~~~sql
WITH inserted AS (
    INSERT INTO llm_worker.response_cache_uses (
        cache_entry_id, operation_id, first_used_at
    )
    VALUES ($1, $2, clock_timestamp())
    ON CONFLICT (operation_id) DO NOTHING
    RETURNING 1
)
UPDATE llm_worker.response_cache_entries
SET last_used_at = clock_timestamp(),
    use_count = CASE
        WHEN use_count < 2147483647 THEN use_count + 1
        ELSE 2147483647
    END
WHERE cache_entry_id = $1
  AND EXISTS (SELECT 1 FROM inserted);
~~~

The origin operation inserts both the entry with **use_count = 1** and its use
row in the same transaction. A Temporal retry cannot increment again because
**operation_id** is unique in uses. **last_used_at** changes only on a newly
inserted logical use, not on polling or replay of the same operation.

Cleanup first changes a ready entry to **tombstoned** and commits the outbox
intent. The partial unique index then permits a new ready entry for the same
route-isolated fingerprint without rewriting the old origin/use provenance.
Fill publication still serializes on **response_cache_fills** and tombstones any
reusable predecessor before inserting the replacement in one transaction;
physical blob deletion remains asynchronous. Fill acquisition locks the
existing route-isolated key row and, only when it is terminal and no reusable
ready entry exists, replaces its owner,
lease, and state in place. The fill row is the reusable per-key mutex; a prior
`completed` row therefore cannot block a later refill.

### Budgets and reservations

Every compiled budget policy is scoped, versioned, and normalized into one or
more windows. A configuration that conceptually matches several scopes is
materialized to explicit policy rows; request-time admission does not execute
an unindexed JSON query over all policies.

~~~sql
CREATE TABLE llm_worker.budget_policies (
    policy_id uuid PRIMARY KEY,
    scope_id uuid NOT NULL
        REFERENCES llm_worker.scopes(scope_id) ON DELETE RESTRICT,
    policy_key text NOT NULL,
    config_digest bytea NOT NULL
        REFERENCES llm_worker.configuration_snapshots(config_digest)
        ON DELETE RESTRICT,
    selector_digest bytea NOT NULL CHECK (octet_length(selector_digest) = 32),
    sanitized_selector jsonb NOT NULL
        CHECK (jsonb_typeof(sanitized_selector) = 'object'),
    priority integer NOT NULL,
    enabled boolean NOT NULL,
    effective_from timestamptz NOT NULL,
    effective_until timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (scope_id, policy_key, config_digest),
    CHECK (
        effective_until IS NULL OR effective_until > effective_from
    )
);

CREATE TABLE llm_worker.budget_windows (
    window_id uuid PRIMARY KEY,
    policy_id uuid NOT NULL
        REFERENCES llm_worker.budget_policies(policy_id) ON DELETE RESTRICT,
    window_key text NOT NULL,
    duration_seconds bigint NOT NULL CHECK (duration_seconds > 0),
    bucket_seconds bigint NOT NULL CHECK (bucket_seconds > 0),
    limit_usd numeric(38,18) NOT NULL CHECK (limit_usd > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (policy_id, window_key),
    CHECK (bucket_seconds <= duration_seconds),
    CHECK (duration_seconds % bucket_seconds = 0)
);

CREATE TABLE llm_worker.budget_redis_generations (
    generation_id uuid PRIMARY KEY,
    generation_slot smallint NOT NULL DEFAULT 1
        CHECK (generation_slot = 1),
    reason text NOT NULL CHECK (
        reason IN ('initial_cold_start', 'full_service_restart', 'redis_data_loss')
    ),
    state text NOT NULL CHECK (
        state IN ('building', 'active', 'superseded', 'failed')
    ),
    source_journal_id bigint NOT NULL CHECK (source_journal_id >= 0),
    coverage_start timestamptz NOT NULL,
    coverage_end timestamptz NOT NULL,
    manifest_digest bytea NOT NULL
        CHECK (octet_length(manifest_digest) = 32),
    started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    safe_failure_code text,
    CHECK (coverage_end > coverage_start),
    CHECK (
        (state IN ('building') AND completed_at IS NULL AND
            safe_failure_code IS NULL) OR
        (state IN ('active', 'superseded') AND completed_at IS NOT NULL AND
            safe_failure_code IS NULL) OR
        (state = 'failed' AND completed_at IS NOT NULL AND
            safe_failure_code IS NOT NULL)
    )
);

CREATE TABLE llm_worker.budget_journal_events (
    journal_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id uuid NOT NULL UNIQUE,
    redis_generation_id uuid NOT NULL
        REFERENCES llm_worker.budget_redis_generations(generation_id)
        ON DELETE RESTRICT,
    operation_id uuid NOT NULL
        REFERENCES llm_worker.operations(operation_id) ON DELETE RESTRICT,
    window_id uuid NOT NULL
        REFERENCES llm_worker.budget_windows(window_id) ON DELETE RESTRICT,
    bucket_start timestamptz NOT NULL,
    reservation_revision integer NOT NULL
        CHECK (reservation_revision >= 0),
    event_kind text NOT NULL CHECK (
        event_kind IN (
            'reserve', 'finalize_exact', 'finalize_unknown',
            'retain_ambiguous', 'resolve_unknown_exact', 'release'
        )
    ),
    reserved_increase_usd numeric(38,18) NOT NULL DEFAULT 0
        CHECK (reserved_increase_usd >= 0),
    reserved_decrease_usd numeric(38,18) NOT NULL DEFAULT 0
        CHECK (reserved_decrease_usd >= 0),
    accounted_increase_usd numeric(38,18) NOT NULL DEFAULT 0
        CHECK (accounted_increase_usd >= 0),
    accounted_decrease_usd numeric(38,18) NOT NULL DEFAULT 0
        CHECK (accounted_decrease_usd >= 0),
    actual_cost_usd numeric(38,18)
        CHECK (actual_cost_usd IS NULL OR actual_cost_usd >= 0),
    actual_cost_status text NOT NULL CHECK (
        actual_cost_status IN ('pending', 'exact', 'unknown')
    ),
    actual_cost_unknown_reason_code text CHECK (
        actual_cost_unknown_reason_code IS NULL OR
        actual_cost_unknown_reason_code ~ '^[a-z0-9_]{1,64}$'
    ),
    occurred_at timestamptz NOT NULL,
    persisted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (operation_id, window_id, reservation_revision),
    CHECK (
        reserved_increase_usd > 0 OR reserved_decrease_usd > 0 OR
        accounted_increase_usd > 0 OR accounted_decrease_usd > 0 OR
        event_kind = 'retain_ambiguous'
    ),
    CHECK (
        (actual_cost_status = 'pending' AND
            actual_cost_usd IS NULL AND
            actual_cost_unknown_reason_code IS NULL) OR
        (actual_cost_status = 'exact' AND
            actual_cost_usd IS NOT NULL AND
            actual_cost_unknown_reason_code IS NULL) OR
        (actual_cost_status = 'unknown' AND
            actual_cost_usd IS NULL AND
            actual_cost_unknown_reason_code IS NOT NULL)
    ),
    CHECK (
        (event_kind = 'reserve' AND
            reserved_increase_usd > 0 AND reserved_decrease_usd = 0 AND
            accounted_increase_usd = 0 AND accounted_decrease_usd = 0 AND
            actual_cost_status = 'pending') OR
        (event_kind = 'finalize_exact' AND
            reserved_increase_usd = 0 AND reserved_decrease_usd > 0 AND
            accounted_increase_usd = actual_cost_usd AND
            accounted_decrease_usd = 0 AND
            actual_cost_status = 'exact') OR
        (event_kind = 'finalize_unknown' AND
            reserved_increase_usd = 0 AND reserved_decrease_usd > 0 AND
            accounted_increase_usd = reserved_decrease_usd AND
            accounted_decrease_usd = 0 AND
            actual_cost_status = 'unknown') OR
        (event_kind = 'retain_ambiguous' AND
            reserved_increase_usd = 0 AND reserved_decrease_usd = 0 AND
            accounted_increase_usd = 0 AND accounted_decrease_usd = 0 AND
            actual_cost_status = 'unknown') OR
        (event_kind = 'resolve_unknown_exact' AND
            reserved_increase_usd = 0 AND
            ((reserved_decrease_usd > 0 AND
                accounted_decrease_usd = 0) OR
             (reserved_decrease_usd = 0 AND
                accounted_decrease_usd > 0)) AND
            accounted_increase_usd = actual_cost_usd AND
            actual_cost_status = 'exact') OR
        (event_kind = 'release' AND
            reserved_increase_usd = 0 AND reserved_decrease_usd > 0 AND
            accounted_increase_usd = 0 AND accounted_decrease_usd = 0 AND
            actual_cost_status = 'exact' AND actual_cost_usd = 0)
    )
);

CREATE TABLE llm_worker.budget_buckets (
    window_id uuid NOT NULL
        REFERENCES llm_worker.budget_windows(window_id) ON DELETE RESTRICT,
    bucket_start timestamptz NOT NULL,
    reserved_cost_usd numeric(38,18) NOT NULL DEFAULT 0
        CHECK (reserved_cost_usd >= 0),
    accounted_cost_usd numeric(38,18) NOT NULL DEFAULT 0
        CHECK (accounted_cost_usd >= 0),
    last_journal_id bigint NOT NULL
        REFERENCES llm_worker.budget_journal_events(journal_id)
        ON DELETE RESTRICT,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (window_id, bucket_start)
);

CREATE TABLE llm_worker.operation_budget_reservations (
    operation_id uuid NOT NULL
        REFERENCES llm_worker.operations(operation_id) ON DELETE RESTRICT,
    window_id uuid NOT NULL
        REFERENCES llm_worker.budget_windows(window_id) ON DELETE RESTRICT,
    bucket_start timestamptz NOT NULL,
    state text NOT NULL CHECK (
        state IN ('reserved', 'finalized', 'retained_ambiguous', 'released')
    ),
    reserved_cost_usd numeric(38,18) NOT NULL
        CHECK (reserved_cost_usd >= 0),
    actual_cost_usd numeric(38,18)
        CHECK (actual_cost_usd IS NULL OR actual_cost_usd >= 0),
    actual_cost_status text NOT NULL DEFAULT 'pending' CHECK (
        actual_cost_status IN ('pending', 'exact', 'unknown')
    ),
    actual_cost_unknown_reason_code text CHECK (
        actual_cost_unknown_reason_code IS NULL OR
        actual_cost_unknown_reason_code ~ '^[a-z0-9_]{1,64}$'
    ),
    budget_charge_usd numeric(38,18) NOT NULL DEFAULT 0
        CHECK (budget_charge_usd >= 0),
    budget_charge_basis text NOT NULL DEFAULT 'reserved' CHECK (
        budget_charge_basis IN (
            'reserved', 'exact_actual', 'retained_bound', 'released'
        )
    ),
    reservation_revision integer NOT NULL
        CHECK (reservation_revision >= 0),
    last_journal_id bigint NOT NULL
        REFERENCES llm_worker.budget_journal_events(journal_id)
        ON DELETE RESTRICT,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    finalized_at timestamptz,
    PRIMARY KEY (operation_id, window_id),
    FOREIGN KEY (window_id, bucket_start)
        REFERENCES llm_worker.budget_buckets(window_id, bucket_start)
        ON DELETE RESTRICT,
    CHECK (
        state = 'reserved' OR finalized_at IS NOT NULL
    ),
    CHECK (
        (actual_cost_status = 'pending' AND
            actual_cost_usd IS NULL AND
            actual_cost_unknown_reason_code IS NULL) OR
        (actual_cost_status = 'exact' AND
            actual_cost_usd IS NOT NULL AND
            actual_cost_unknown_reason_code IS NULL) OR
        (actual_cost_status = 'unknown' AND
            actual_cost_usd IS NULL AND
            actual_cost_unknown_reason_code IS NOT NULL)
    ),
    CHECK (
        (budget_charge_basis = 'reserved' AND state = 'reserved' AND
            budget_charge_usd = 0 AND actual_cost_status = 'pending') OR
        (budget_charge_basis = 'exact_actual' AND state = 'finalized' AND
            actual_cost_status = 'exact' AND
            budget_charge_usd = actual_cost_usd) OR
        (budget_charge_basis = 'retained_bound' AND
            state IN ('finalized', 'retained_ambiguous') AND
            actual_cost_status = 'unknown' AND
            budget_charge_usd = reserved_cost_usd) OR
        (budget_charge_basis = 'released' AND state = 'released' AND
            budget_charge_usd = 0 AND
            actual_cost_status = 'exact' AND actual_cost_usd = 0)
    )
);

CREATE INDEX budget_policies_scope_active_idx
    ON llm_worker.budget_policies
        (scope_id, enabled, priority, effective_from, policy_id)
    INCLUDE (effective_until, selector_digest);

CREATE INDEX budget_policies_config_idx
    ON llm_worker.budget_policies (config_digest, policy_id);

CREATE INDEX budget_windows_policy_idx
    ON llm_worker.budget_windows (policy_id, window_id)
    INCLUDE (duration_seconds, bucket_seconds, limit_usd);

CREATE INDEX budget_buckets_retention_idx
    ON llm_worker.budget_buckets (bucket_start, window_id);

CREATE INDEX budget_journal_window_rebuild_idx
    ON llm_worker.budget_journal_events
        (window_id, bucket_start, journal_id)
    INCLUDE (
        operation_id,
        event_kind,
        reserved_increase_usd,
        reserved_decrease_usd,
        accounted_increase_usd,
        accounted_decrease_usd,
        actual_cost_usd,
        actual_cost_status
    );

CREATE INDEX budget_journal_operation_idx
    ON llm_worker.budget_journal_events (operation_id, journal_id);

CREATE INDEX budget_journal_generation_idx
    ON llm_worker.budget_journal_events
        (redis_generation_id, journal_id);

CREATE UNIQUE INDEX budget_redis_generation_active_idx
    ON llm_worker.budget_redis_generations (generation_slot)
    WHERE state = 'active';

CREATE INDEX budget_redis_generation_history_idx
    ON llm_worker.budget_redis_generations (started_at DESC, generation_id)
    INCLUDE (state, reason, source_journal_id, completed_at);

CREATE INDEX budget_journal_time_brin_idx
    ON llm_worker.budget_journal_events USING brin (persisted_at)
    WITH (pages_per_range = 64);

CREATE INDEX budget_buckets_last_journal_idx
    ON llm_worker.budget_buckets
        (last_journal_id, window_id, bucket_start);

CREATE INDEX operation_budget_window_idx
    ON llm_worker.operation_budget_reservations
        (window_id, bucket_start, state, operation_id)
    INCLUDE (
        reserved_cost_usd,
        budget_charge_usd,
        budget_charge_basis,
        actual_cost_usd,
        actual_cost_status
    );

CREATE INDEX operation_budget_open_idx
    ON llm_worker.operation_budget_reservations
        (window_id, bucket_start, operation_id)
    INCLUDE (reserved_cost_usd, reservation_revision, last_journal_id)
    WHERE state IN ('reserved', 'retained_ambiguous');

CREATE INDEX operation_budget_last_journal_idx
    ON llm_worker.operation_budget_reservations
        (last_journal_id, operation_id, window_id);
~~~

Redis, not SQL, resolves and locks the live active-window state. After Redis
accepts, PostgreSQL uses a write-only budget transaction: insert the idempotent
journal event; only when that insert returns a new row, upsert the bucket
projection by the event deltas and insert/update the operation reservation to
the same `journal_id`. `INSERT ... ON CONFLICT DO NOTHING RETURNING` in a CTE
guards every projection update, so a Temporal retry cannot apply a delta twice.
The Go hot path issues no SELECT over policies, windows, buckets, reservations,
or journal rows.

Finalization follows the same write-only protocol. When actual cost is exact,
the journal moves the reserved amount to **budget_charge_usd** with basis
**exact_actual**. When the real price is unknown, it records NULL actual cost
and retains the full reservation with basis **retained_bound**. That bound
protects the Redis budget but is never reported as actual spend. Definite
pre-write failures release reservations. Ambiguous dispatch retains the bound.
Only the fenced full-service/Redis-loss bootstrap described above reads these
tables to reconstruct the active horizon.

If trustworthy billing data later resolves an unknown cost, append a new
**resolve_unknown_exact** journal revision; never rewrite the earlier unknown
event. A previously finalized retained bound moves from accounted bound to the
exact amount, while a still-ambiguous reservation moves from reserved bound to
the exact accounted amount. The projection update uses the event's separate
decrease/increase columns, rejects subtraction underflow, updates the operation
cost and reservation state in the same PostgreSQL transaction, and only then
idempotently reconciles Redis. Until that succeeds, the larger retained bound
continues to protect admission.

### USD prices and deferred FX boundary

The current generic catalog **currency** field and external runtime currency
input are removed. All price columns are USD-specific by name and type.
Downstream Go/JSON/OCaml contracts expose USD decimal properties and never
report **currency = USD**.

All currently supported provider price sources must declare USD, so this phase
has no FX adapter, rate-refresh policy, snapshot table, configuration, or
maintenance job. The catalog loader rejects a source that declares another
currency before it can enter a runtime snapshot; it never accepts a
caller-supplied rate or silently treats a foreign amount as USD. An omitted
component in an otherwise valid USD source remains unknown and cannot support
catalog-derived actual cost or monetary admission unless the explicit unpriced
policy allows it. When a concrete non-USD provider is added, a separate ADR
must design the Go-owned rate source, staleness/failure policy, exact conversion,
audit evidence, and schema against that real provider contract. The Go worker
will remain responsible for conversion and will persist/report only normalized
USD; foreign amounts and a downstream currency discriminator remain forbidden.

Catalog component amounts are nullable because a provider's real price can be
unknown or only partly verified. **price_status=exact** means every normalized
component required by the pinned **usage_formula_version** is known; an exact
zero is stored as zero. **partial** preserves known components and leaves
unknown components NULL. **unknown** leaves all components NULL. The closed
component codes and safe reason explain which evidence is missing. A NULL is
never interpreted as free, as “not applicable,” or as inheriting another rate;
the usage-formula compiler must normalize included/non-applicable billing rules
before an entry can be exact.

The Go `storage/postgres.PricingCatalogRepository` is the durable publication
boundary for these tables. It revalidates the compiled USD digest, writes the
catalog header and all entries in one synchronous transaction, and retires a
previous active snapshot only as part of publishing the replacement. Replaying
the same version with the same source and compiled digests is idempotent; a
digest mismatch is rejected. Runtime roles retain read-only access, while the
maintenance role owns publication. This keeps an interrupted reload from
leaving a catalog header without its complete entry set.

Because the current schema does not materialize optional source prose or a
separate entry-version column, the repository rejects entries that would lose
those digest inputs. Future-dated publication records a retirement timestamp
while leaving the predecessor active for `LoadActive` until the replacement is
effective.

Only an **exact** catalog entry can produce a catalog-derived actual cost or a
monetary-budget reservation. A partial/unknown entry may still support a route
allowed by the explicit unpriced policy. If that provider later returns a
trustworthy authoritative billed amount, the operation can record an exact
provider-reported cost; otherwise it completes with NULL actual cost and an
unknown reason while retaining its conservative budget bound where one exists.

~~~sql
CREATE TABLE llm_worker.price_catalogs (
    price_catalog_id uuid PRIMARY KEY,
    catalog_version text NOT NULL UNIQUE,
    source_digest bytea NOT NULL CHECK (octet_length(source_digest) = 32),
    compiled_digest bytea NOT NULL UNIQUE
        CHECK (octet_length(compiled_digest) = 32),
    loaded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    effective_from timestamptz NOT NULL,
    retired_at timestamptz,
    status text NOT NULL CHECK (status IN ('active', 'retired', 'rejected'))
);

CREATE TABLE llm_worker.price_entries (
    price_entry_id uuid PRIMARY KEY,
    price_catalog_id uuid NOT NULL
        REFERENCES llm_worker.price_catalogs(price_catalog_id)
        ON DELETE RESTRICT,
    provider text NOT NULL,
    endpoint_family text NOT NULL,
    endpoint_id text NOT NULL,
    region text NOT NULL,
    resolved_model text NOT NULL,
    provider_tier text NOT NULL,
    usage_formula_version text NOT NULL,
    price_status text NOT NULL CHECK (
        price_status IN ('exact', 'partial', 'unknown')
    ),
    input_per_million_usd numeric(38,18)
        CHECK (input_per_million_usd IS NULL OR input_per_million_usd >= 0),
    output_per_million_usd numeric(38,18)
        CHECK (output_per_million_usd IS NULL OR output_per_million_usd >= 0),
    cache_read_per_million_usd numeric(38,18)
        CHECK (
            cache_read_per_million_usd IS NULL OR
            cache_read_per_million_usd >= 0
        ),
    cache_write_per_million_usd numeric(38,18)
        CHECK (
            cache_write_per_million_usd IS NULL OR
            cache_write_per_million_usd >= 0
        ),
    reasoning_per_million_usd numeric(38,18)
        CHECK (
            reasoning_per_million_usd IS NULL OR
            reasoning_per_million_usd >= 0
        ),
    per_request_usd numeric(38,18)
        CHECK (per_request_usd IS NULL OR per_request_usd >= 0),
    unknown_component_codes text[] NOT NULL DEFAULT '{}'::text[] CHECK (
        array_position(unknown_component_codes, NULL) IS NULL AND
        unknown_component_codes <@ ARRAY[
            'input_tokens',
            'output_tokens',
            'cache_read_tokens',
            'cache_write_tokens',
            'reasoning_tokens',
            'request'
        ]::text[]
    ),
    price_unknown_reason_code text CHECK (
        price_unknown_reason_code IS NULL OR
        price_unknown_reason_code ~ '^[a-z0-9_]{1,64}$'
    ),
    source_price_digest bytea NOT NULL CHECK (octet_length(source_price_digest) = 32),
    effective_from timestamptz NOT NULL,
    effective_until timestamptz,
    UNIQUE (
        price_catalog_id,
        provider,
        endpoint_family,
        endpoint_id,
        region,
        resolved_model,
        provider_tier,
        effective_from
    ),
    CHECK (
        effective_until IS NULL OR effective_until > effective_from
    ),
    CHECK (
        (price_status = 'exact' AND
            input_per_million_usd IS NOT NULL AND
            output_per_million_usd IS NOT NULL AND
            cache_read_per_million_usd IS NOT NULL AND
            cache_write_per_million_usd IS NOT NULL AND
            reasoning_per_million_usd IS NOT NULL AND
            per_request_usd IS NOT NULL AND
            cardinality(unknown_component_codes) = 0 AND
            price_unknown_reason_code IS NULL) OR
        (price_status = 'partial' AND
            num_nonnulls(
                input_per_million_usd,
                output_per_million_usd,
                cache_read_per_million_usd,
                cache_write_per_million_usd,
                reasoning_per_million_usd,
                per_request_usd
            ) BETWEEN 1 AND 5 AND
            cardinality(unknown_component_codes) > 0 AND
            price_unknown_reason_code IS NOT NULL) OR
        (price_status = 'unknown' AND
            num_nonnulls(
                input_per_million_usd,
                output_per_million_usd,
                cache_read_per_million_usd,
                cache_write_per_million_usd,
                reasoning_per_million_usd,
                per_request_usd
            ) = 0 AND
            cardinality(unknown_component_codes) > 0 AND
            price_unknown_reason_code IS NOT NULL)
    )
);

CREATE INDEX price_entries_resolution_idx
    ON llm_worker.price_entries (
        endpoint_id,
        region,
        resolved_model,
        provider_tier,
        price_status,
        effective_from DESC,
        price_entry_id
    )
    INCLUDE (
        effective_until,
        usage_formula_version,
        input_per_million_usd,
        output_per_million_usd,
        cache_read_per_million_usd,
        cache_write_per_million_usd,
        reasoning_per_million_usd,
        per_request_usd,
        price_catalog_id
    );

CREATE INDEX price_entries_unresolved_idx
    ON llm_worker.price_entries
        (price_status, effective_from DESC, price_entry_id)
    INCLUDE (
        endpoint_id,
        resolved_model,
        provider_tier,
        unknown_component_codes,
        price_unknown_reason_code
    )
    WHERE price_status IN ('partial', 'unknown');

~~~

The public schema names amounts **reserved_cost_usd** and
**actual_cost_usd**. A known amount is a validated arbitrary-precision
**Usd_decimal.t** in OCaml. Nullable catalog/actual prices decode to a closed
unknown-cost constructor, never to zero and never to **float** or a
currency-tagged record. The current v1 **currency** response field and
configuration **pricing.currency** are removed during the breaking contract
replacement in Phase A; one-shot OCaml helpers are rebuilt on the v1
root-request path.

### Provider status, credit state, and inventory

Events are append-only classified observations. The current table is a
transactionally maintained projection used by routing and cheap queries. Raw
provider error bodies and account secrets never enter either table.

~~~sql
CREATE TABLE llm_worker.provider_status_events (
    event_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    config_digest bytea NOT NULL
        REFERENCES llm_worker.configuration_snapshots(config_digest)
        ON DELETE RESTRICT,
    route_id text NOT NULL,
    endpoint_id text NOT NULL,
    endpoint_account_hmac bytea NOT NULL
        CHECK (octet_length(endpoint_account_hmac) = 32),
    provider text NOT NULL,
    endpoint_family text NOT NULL,
    observed_at timestamptz NOT NULL,
    source text NOT NULL CHECK (
        source IN (
            'inference',
            'management_api',
            'startup_probe',
            'operator'
        )
    ),
    availability text NOT NULL CHECK (
        availability IN ('available', 'degraded', 'unavailable', 'unknown')
    ),
    credit_state text NOT NULL CHECK (
        credit_state IN ('ok', 'low', 'exhausted', 'unknown')
    ),
    billing_state text NOT NULL CHECK (
        billing_state IN ('ok', 'issue', 'unknown')
    ),
    safe_error_code text,
    provider_code text,
    evidence_digest bytea NOT NULL CHECK (octet_length(evidence_digest) = 32),
    config_epoch text NOT NULL,
    expires_at timestamptz NOT NULL
);

CREATE TABLE llm_worker.provider_route_status (
    config_digest bytea NOT NULL
        REFERENCES llm_worker.configuration_snapshots(config_digest)
        ON DELETE RESTRICT,
    route_id text NOT NULL,
    endpoint_id text NOT NULL,
    endpoint_account_hmac bytea NOT NULL
        CHECK (octet_length(endpoint_account_hmac) = 32),
    provider text NOT NULL,
    endpoint_family text NOT NULL,
    availability text NOT NULL CHECK (
        availability IN ('available', 'degraded', 'unavailable', 'unknown')
    ),
    credit_state text NOT NULL CHECK (
        credit_state IN ('ok', 'low', 'exhausted', 'unknown')
    ),
    billing_state text NOT NULL CHECK (
        billing_state IN ('ok', 'issue', 'unknown')
    ),
    circuit_state text NOT NULL CHECK (
        circuit_state IN ('closed', 'open', 'half_open')
    ),
    consecutive_definite_failures integer NOT NULL DEFAULT 0
        CHECK (consecutive_definite_failures >= 0),
    last_event_id bigint NOT NULL
        REFERENCES llm_worker.provider_status_events(event_id)
        ON DELETE RESTRICT,
    last_success_at timestamptz,
    last_failure_at timestamptz,
    credit_confirmed_at timestamptz,
    observed_at timestamptz NOT NULL,
    stale_after timestamptz NOT NULL,
    projection_version bigint NOT NULL CHECK (projection_version > 0),
    PRIMARY KEY (config_digest, route_id)
);

CREATE TABLE llm_worker.provider_inventory_snapshots (
    inventory_snapshot_id uuid PRIMARY KEY,
    config_digest bytea NOT NULL
        REFERENCES llm_worker.configuration_snapshots(config_digest)
        ON DELETE RESTRICT,
    provider text NOT NULL,
    endpoint_id text NOT NULL,
    endpoint_account_hmac bytea NOT NULL
        CHECK (octet_length(endpoint_account_hmac) = 32),
    endpoint_family text NOT NULL,
    region text NOT NULL,
    source text NOT NULL CHECK (
        source IN ('provider_api', 'configured_only', 'unsupported')
    ),
    observed_at timestamptz NOT NULL,
    complete boolean NOT NULL,
    next_cursor_ciphertext bytea,
    next_cursor_key_id text,
    inventory_digest bytea NOT NULL CHECK (octet_length(inventory_digest) = 32),
    expires_at timestamptz NOT NULL,
    UNIQUE (
        config_digest,
        endpoint_id,
        endpoint_account_hmac,
        observed_at
    ),
    CHECK (
        next_cursor_ciphertext IS NULL =
        (next_cursor_key_id IS NULL)
    )
);

CREATE TABLE llm_worker.provider_inventory_models (
    inventory_snapshot_id uuid NOT NULL
        REFERENCES llm_worker.provider_inventory_snapshots(inventory_snapshot_id)
        ON DELETE RESTRICT,
    provider_model_id text NOT NULL,
    display_name text,
    owned_by text,
    created_at_provider timestamptz,
    lifecycle_state text NOT NULL CHECK (
        lifecycle_state IN ('available', 'deprecated', 'unavailable', 'unknown')
    ),
    capability_digest bytea NOT NULL CHECK (octet_length(capability_digest) = 32),
    safe_metadata jsonb NOT NULL DEFAULT '{}'::jsonb
        CHECK (jsonb_typeof(safe_metadata) = 'object'),
    PRIMARY KEY (inventory_snapshot_id, provider_model_id)
);

CREATE INDEX provider_status_route_time_idx
    ON llm_worker.provider_status_events
        (config_digest, route_id, observed_at DESC, event_id DESC)
    INCLUDE (
        availability,
        credit_state,
        billing_state,
        safe_error_code,
        source
    );

CREATE INDEX provider_status_credit_incident_idx
    ON llm_worker.provider_status_events
        (observed_at DESC, endpoint_id, event_id)
    WHERE credit_state IN ('low', 'exhausted')
       OR billing_state = 'issue';

CREATE INDEX provider_status_event_brin_idx
    ON llm_worker.provider_status_events USING brin (observed_at)
    WITH (pages_per_range = 64);

CREATE INDEX provider_status_expiry_idx
    ON llm_worker.provider_status_events (expires_at, event_id);

CREATE INDEX provider_route_current_problem_idx
    ON llm_worker.provider_route_status
        (availability, credit_state, billing_state, observed_at DESC, route_id)
    WHERE availability <> 'available'
       OR credit_state <> 'ok'
       OR billing_state <> 'ok';

CREATE INDEX provider_route_last_event_idx
    ON llm_worker.provider_route_status (last_event_id);

CREATE INDEX provider_inventory_latest_idx
    ON llm_worker.provider_inventory_snapshots
        (config_digest, endpoint_id, observed_at DESC, inventory_snapshot_id)
    INCLUDE (source, complete, expires_at);

CREATE INDEX provider_inventory_latest_account_idx
    ON llm_worker.provider_inventory_snapshots
        (config_digest, provider, endpoint_id, endpoint_account_hmac,
         observed_at DESC, inventory_snapshot_id)
    INCLUDE (source, complete, expires_at, inventory_digest);

CREATE INDEX provider_inventory_expiry_idx
    ON llm_worker.provider_inventory_snapshots
        (expires_at, inventory_snapshot_id);

CREATE INDEX provider_inventory_model_lookup_idx
    ON llm_worker.provider_inventory_models
        (provider_model_id, inventory_snapshot_id)
    INCLUDE (lifecycle_state, display_name, capability_digest);
~~~

A generic HTTP 429 is never classified as exhausted credit. **exhausted** or
**billing issue** requires a provider-specific code/field documented in the
adapter contract or an operator event. Transient success may clear degraded
health, but a confirmed billing/credit incident remains until a provider
management response, credential/config epoch change, or authorized operator
event clears it.

Model inventory is informational. Configured logical-model routes remain the
authorization source. A discovered model is not routable until a reviewed
configuration snapshot and capability/equivalence entry authorizes it. Provider
listing support is capability data; unsupported providers return a typed
**unsupported** source rather than a fabricated empty list.

Initial adapters reverify official provider list contracts during
implementation:

- [OpenAI Models API](https://platform.openai.com/docs/api-reference/models/object?lang=curl)
- [Anthropic Models API](https://platform.claude.com/docs/en/api/models/list)
- [Amazon Bedrock model discovery](https://docs.aws.amazon.com/bedrock/latest/userguide/models-get-info.html)

### Maintenance outbox

Blob deletion and other external cleanup cannot be atomic with PostgreSQL. A
small outbox makes the intent durable without performing object-store work
inside the committing transaction.

~~~sql
CREATE TABLE llm_worker.maintenance_outbox (
    outbox_id uuid PRIMARY KEY,
    event_kind text NOT NULL CHECK (
        event_kind IN (
            'delete_blob',
            'delete_provider_state',
            'refresh_inventory'
        )
    ),
    aggregate_type text NOT NULL,
    aggregate_id uuid NOT NULL,
    dedupe_key bytea NOT NULL CHECK (octet_length(dedupe_key) = 32),
    safe_payload jsonb NOT NULL,
    state text NOT NULL DEFAULT 'pending'
        CHECK (state IN ('pending', 'processing', 'completed', 'failed')),
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    available_at timestamptz NOT NULL,
    lease_expires_at timestamptz,
    lease_token uuid,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    UNIQUE (event_kind, dedupe_key),
    CHECK (state <> 'processing' OR (lease_expires_at IS NOT NULL AND lease_token IS NOT NULL)),
    CHECK (state = 'processing' OR lease_expires_at IS NULL)
);

CREATE INDEX maintenance_outbox_ready_idx
    ON llm_worker.maintenance_outbox (available_at, outbox_id)
    WHERE state IN ('pending', 'failed');

CREATE INDEX maintenance_outbox_lease_idx
    ON llm_worker.maintenance_outbox (lease_expires_at, outbox_id)
    WHERE state = 'processing';
~~~

Workers claim outbox rows in short transactions with **FOR UPDATE SKIP LOCKED**.
Each claim writes a fresh opaque `lease_token`; completion and retry must match
that token while the lease is still live. A reclaim therefore fences a stale
worker, while retaining the terminal token makes a duplicate completion or
failure request idempotent. The payload contains encrypted locators or safe
identifiers only. A missing external object is a successful delete.

The reusable Go contract and dispatcher live in
[`golang/maintenance`](../../golang/maintenance); the PostgreSQL adapter is
kept in `storage/postgres/maintenance.go` and runs only with the dedicated
`llmtw_maintenance` role. Cache candidates are rechecked while locked before
they are tombstoned and before a blob-delete event is inserted. Other durable
tables retain their independent horizons until their foreign-key and audit
dependencies have a bounded, transaction-safe delete path.

### Update-heavy table storage

~~~sql
ALTER TABLE llm_worker.operations SET (fillfactor = 80);
ALTER TABLE llm_worker.budget_buckets SET (fillfactor = 80);
ALTER TABLE llm_worker.response_cache_entries SET (fillfactor = 80);
ALTER TABLE llm_worker.response_cache_fills SET (fillfactor = 80);
ALTER TABLE llm_worker.provider_route_status SET (fillfactor = 80);
ALTER TABLE llm_worker.maintenance_outbox SET (fillfactor = 80);
~~~

These tables receive in-place state or timestamp updates; spare page space
increases HOT-update probability. Immutable event/checkpoint/price tables keep
the default fillfactor. Production installation sets table-specific autovacuum
thresholds after load testing rather than embedding arbitrary scale assumptions
in the initial DDL.

## Query Activity contract

The Activity name is **llm.query.v1**. Request and response are tagged unions,
not an untyped envelope. Queries do not use the paid LLM operation state machine
or external result blobs. Each query is scoped/authorized, bounded, and recorded
in a dedicated inline audit ledger with **actual_cost_usd**. A stored-state query normally records
**0.000000000000000000**, **cost_status=exact**, and
**control_query_zero**. If a future management API is billable but the worker
cannot establish its real charge, the query still completes with
**actual_cost_usd=NULL**, **cost_status=unknown**, and a safe reason; it must not
report the estimate or reservation as actual.

~~~sql
CREATE TABLE llm_worker.query_executions (
    query_execution_id uuid PRIMARY KEY,
    scope_id uuid NOT NULL
        REFERENCES llm_worker.scopes(scope_id) ON DELETE RESTRICT,
    api_version text NOT NULL,
    operation_key_hmac bytea NOT NULL
        CHECK (octet_length(operation_key_hmac) = 32),
    request_fingerprint_hmac bytea NOT NULL
        CHECK (octet_length(request_fingerprint_hmac) = 32),
    query_kind text NOT NULL CHECK (
        query_kind IN (
            'provider_status', 'model_inventory', 'credit_status',
            'budget_status', 'spend_summary'
        )
    ),
    request_jsonb jsonb NOT NULL
        CHECK (jsonb_typeof(request_jsonb) = 'object'),
    response_jsonb jsonb NOT NULL
        CHECK (jsonb_typeof(response_jsonb) = 'object'),
    response_digest bytea NOT NULL CHECK (octet_length(response_digest) = 32),
    source text NOT NULL,
    actual_cost_usd numeric(38,18)
        CHECK (actual_cost_usd IS NULL OR actual_cost_usd >= 0),
    cost_status text NOT NULL CHECK (cost_status IN ('exact', 'unknown')),
    cost_method text CHECK (
        cost_method IS NULL OR
        cost_method IN (
            'control_query_zero', 'provider_reported', 'catalog_usage'
        )
    ),
    cost_unknown_reason_code text CHECK (
        cost_unknown_reason_code IS NULL OR
        cost_unknown_reason_code ~ '^[a-z0-9_]{1,64}$'
    ),
    started_at timestamptz NOT NULL,
    completed_at timestamptz NOT NULL,
    retention_expires_at timestamptz NOT NULL,
    UNIQUE (scope_id, operation_key_hmac),
    CHECK (completed_at >= started_at),
    CHECK (
        (cost_status = 'exact' AND actual_cost_usd IS NOT NULL AND
            cost_method IS NOT NULL AND cost_unknown_reason_code IS NULL) OR
        (cost_status = 'unknown' AND actual_cost_usd IS NULL AND
            cost_method IS NULL AND cost_unknown_reason_code IS NOT NULL)
    ),
    CHECK (cost_method <> 'control_query_zero' OR actual_cost_usd = 0)
);

CREATE INDEX query_executions_scope_time_idx
    ON llm_worker.query_executions
        (scope_id, completed_at DESC, query_execution_id)
    INCLUDE (query_kind, actual_cost_usd, cost_status, source);

CREATE INDEX query_executions_retention_idx
    ON llm_worker.query_executions
        (retention_expires_at, query_execution_id);

CREATE INDEX query_executions_unknown_cost_idx
    ON llm_worker.query_executions
        (scope_id, completed_at DESC, query_execution_id)
    INCLUDE (query_kind, cost_unknown_reason_code)
    WHERE cost_status = 'unknown';
~~~

The request and response JSON are bounded control data; they cannot contain
prompt, tool, model-output, credential, or raw provider-error content. An
operation-key replay returns the recorded response after matching the request
fingerprint. `budget_status` reads its live values from Redis and performs no
PostgreSQL budget SELECT, but it commits this small PostgreSQL INSERT before the
Activity completes because the product requirement is that every completed
query records its exact-or-unknown charge. PostgreSQL unavailability therefore
makes the Activity retry; it does not move the budget calculation to SQL or put
the blob store on the query path.

`query_execution_id` is deliberately distinct from the paid inference
`operation_id`. Historical spend aggregation unions completed Generate/Compact
costs with this ledger and may expose `Query` as an aggregation kind without
pretending Query rows use the inference operation state machine.

Common request fields are:

~~~json
{
  "api_version": "llm.temporal/query/v1",
  "operation_key": "daily-provider-status-2026-07-18",
  "context": {
    "tenant": "acme",
    "project": "claims",
    "actor": "workflow:daily-smoke"
  },
  "kind": "provider_status",
  "query": {
    "provider": "openai",
    "include_healthy": false,
    "refresh_if_older_than_seconds": 300,
    "page_size": 100,
    "cursor": null
  }
}
~~~

The response repeats **kind** and uses exactly one associated result:

~~~json
{
  "api_version": "llm.temporal/query/v1",
  "operation_key": "daily-provider-status-2026-07-18",
  "query_execution_id": "qry_01J...",
  "kind": "provider_status",
  "observed_at": "2026-07-18T03:14:15.926535Z",
  "source": "persisted_and_refreshed",
  "freshness": "current",
  "complete": true,
  "next_cursor": null,
  "result": {
    "routes": [
      {
        "route_id": "openai-primary",
        "provider": "openai",
        "endpoint": "https://api.openai.com",
        "availability": "available",
        "credit_state": "ok",
        "billing_state": "ok",
        "observed_at": "2026-07-18T03:14:15.100000Z",
        "stale_after": "2026-07-18T03:19:15.100000Z"
      }
    ]
  },
  "cost_status": "exact",
  "actual_cost_usd": "0.000000000000000000",
  "cost_method": "control_query_zero"
}
~~~

For an unknown charge, **cost_status** is **unknown**,
**actual_cost_usd** is JSON null, **cost_method** is omitted, and
**cost_unknown_reason_code** is present. The decoder rejects every other
combination.

Supported tag/result pairs are:

| Kind | Query-specific filters | Result shape |
| --- | --- | --- |
| **provider_status** | provider, endpoint, availability, include healthy, freshness | page of typed route-status rows |
| **model_inventory** | provider, endpoint, model prefix, lifecycle, freshness | page of typed model rows plus inventory support/completeness |
| **credit_status** | provider, endpoint, include ok, freshness | page of typed credit/billing rows and confirmed-at evidence |
| **budget_status** | policy key, current instant within Redis coverage, include windows | Redis-only matching policies/windows with conservative nano-USD-derived limit, reserved, accounted charge, available USD, generation ID, and Stream high-water mark |
| **spend_summary** | half-open UTC interval, group-by dimensions, operation kinds | typed buckets with known actual-cost USD, exact/unknown operation counts, and completeness |

The common response has **observed_at**, **source**, **freshness**, **complete**,
and an opaque signed cursor. Page size has an operator cap, ordering is stable,
and cursor contents bind query tag, scope, filters, sort key, snapshot horizon,
expiry, and MAC. Unknown tags, mismatched result tags, unknown fields, or
expired/tampered cursors are typed non-retryable errors.

The Go wire boundary applies the same closed-world rule before any service
dispatch: provider availability and inventory lifecycle filters (including
the persisted `available`, `deprecated`, `unavailable`, and `unknown` states),
spend dimensions and operation kinds, response source/freshness tags, and query
cost methods are allow-listed. Query intervals and response observation times
must be valid RFC3339 timestamps; malformed timestamps, duplicate dimensions,
and unknown enum values are rejected rather than passed to storage or a
provider management API.

**refresh_if_older_than_seconds** is optional for provider/model/credit queries.
Omission reads durable state only. When present, the worker may call a documented provider management/list
API and then store the observation before answering. It never invokes an
inference endpoint, bypasses provider rate limits, or returns raw provider JSON.
One refresh owner collapses concurrent requests per endpoint.

`budget_status` is the deliberate exception to the persisted-query default: it
reads the complete current Redis working set and never reads PostgreSQL budget
tables. Its requested instant must lie inside the manifest's active coverage;
an older/historical instant returns a typed `budget_history_not_available`
error instead of falling back to PostgreSQL. The response binds the generation,
manifest digest, and Stream high-water mark so callers can compare pages from
one snapshot. `spend_summary` reads completed operation/cost rows in PostgreSQL;
it does not read budget policy/window/bucket/reservation/journal rows.

Spend aggregation combines completed **operations** with **query_executions**
and sums only rows with **cost_status=exact**. It returns
**known_actual_cost_usd** plus exact and unknown counts by kind; any unknown row
makes the bucket's cost completeness **partial**. SQL must not
**COALESCE(actual_cost_usd, 0)**. Cache hits remain exact-zero operations and
free queries remain exact-zero query executions rather than absent records.
Half-open intervals and database UTC time prevent boundary double counting.

## Transaction protocols

### Begin and operation replay

1. Derive tenant/project HMACs and upsert scope.
2. Insert operation using the unique scoped key.
3. On conflict, lock the existing row and compare kind, API version,
   request-fingerprint HMAC, schema version, content-free request manifest, and
   encrypted payload digest/reference.
4. Return stored result for **completed**; resume **provider_pending** by poll
   ID; resume only a provably pre-write expired lease; return typed conflict or
   ambiguous state for every unsafe case.
5. Resolve parent and materialize outside a long transaction, then revalidate
   its immutable digests when the short admission transaction begins.

### Redis budget acquisition and PostgreSQL journal write

1. After operation replay/cache checks and route pricing, invoke the versioned
   Redis Function once for all matching monetary windows and operational
   throttles. The Function is the only active-window read and decision
   serializer over the validated Redis materialization.
2. On acceptance, insert the operation/reservation journal events and update
   PostgreSQL bucket/reservation projections through the insert-guarded CTE.
   Do not select budget state before or after the write.
3. If the PostgreSQL write fails, do not dispatch; invoke idempotent Redis
   release. If release cannot be confirmed, keep readiness false or let the
   conservative TTL expire according to the failure classification.
4. On retry, operation replay determines whether this is new, terminal, or
   pending before Redis is touched. Redis operation reservation identity and
   journal event identity make both sides idempotent.
5. Terminal completion commits operation/cost/journal/projection changes in
   PostgreSQL, then applies the corresponding Redis reconciliation event.

### Exact cache lookup and fill

1. Plan authorized route-cache identities from the configuration snapshot.
   Provider health and budget do not block reuse of an otherwise authorized
   cached result.
2. Build one fingerprint per eligible route identity in deterministic route
   order. Providers/endpoints/accounts/regions and model revisions remain
   isolated in this phase.
3. Lock a ready entry and compare **completed_at** with one database timestamp
   minus caller **max_age**.
4. On a hit, insert use, saturating-update last use/count, create the
   cache-replay child/result, complete the operation at zero USD, and commit.
5. On a miss, insert or claim the fill row. Do not hold a PostgreSQL transaction
   while calling a provider.
6. Waiters observe the fill with bounded database notification/polling and
   never dispatch while a valid owner exists. An expired owner is recoverable
   only after resolving its operation state; **dispatching** is not assumed
   safe.
7. The owner finalization transaction writes output/checkpoint, cache entry,
   origin use, exact-or-unknown cost state, releases the fill, and completes the
   operation.

The initial cache never serves an entry across route identities. Response
provenance reports **served_by = worker_cache** and the origin model/route; it
never pretends a current provider was invoked. Cross-provider reuse requires a
future superseding ADR and cache epoch.

### Resumable provider submission

1. Lock operation and set route, attempt, **dispatching**, deterministic
   provider idempotency key, and possible-write boundary before network I/O.
2. Submit once through an adapter whose retry count is zero.
3. If the provider returns a pollable ID, immediately envelope-encrypt it and
   transition to **provider_pending** before the first poll or heartbeat.
4. Poll attempts lock/verify the row, release the transaction, poll the same ID,
   then persist status and next poll time.
5. A restarted Activity begins from the operation row and polls the stored ID.
6. The acceptance-before-ID persistence gap is recoverable only through a
   provider idempotency/lookup contract. Otherwise record **ambiguous** and do
   not resubmit.

Heartbeats contain operation ID and phase only. The database is authoritative
for provider IDs, poll count, status, and cost.

### Completion and cache-use accounting

Operation, attempt, budget journal/projection, encrypted inline/blob result,
checkpoint, cache use/fill, provider status event/projection, and
exact-or-unknown USD cost status finalize in one serializable or explicitly
locked PostgreSQL transaction. Redis budget reconciliation follows that commit
idempotently. External
blob bytes are written first under an uncommitted locator; the transaction
publishes their rows. Failed publication schedules idempotent orphan cleanup.

Transactions retry serialization/deadlock failures with a small bounded policy
before possible provider write. They never retry a provider submission. Lock
order is documented and tested:

~~~text
scope -> operation -> cache fill/entry -> budget journal/projection rows
      -> checkpoint parent -> status projection
~~~

Redis mutations are never performed while a PostgreSQL transaction is open.
The cross-store order and conservative compensation protocol above replace any
claim of a shared lock order.

## Index rationale and query plans

| Index family | Production query served | Why this order |
| --- | --- | --- |
| Scoped operation unique | Activity Begin/replay | equality on scope, kind, HMAC |
| Operation lease/poll partial | crash recovery and due polling | excludes terminal rows and orders by due time |
| Scope spend covering | spend query | scope and time range; included cost/dimensions reduce heap reads |
| Unknown-cost partial | reconciliation queue | scoped completed unknown rows only |
| Operation BRIN | global retention/audit scans | append-correlated time at low index cost |
| Parent operation/checkpoint/cache | FK enforcement, lineage, deletion | PostgreSQL does not index FK children |
| Attempt route/time | provider incident queries | endpoint/model then recent observations |
| Checkpoint parent | fork traversal and retention | equality parent then stable child order |
| Checkpoint scope/time | tenant audit and deletion | scoped recent range |
| Cache unique | direct exact lookup | all equality key components |
| Cache last-use partial | future unused-entry cleanup | ready entries in oldest-use order |
| Cache fill lease partial | abandoned-fill recovery | only active fills ordered by expiry |
| Budget journal window rebuild | exceptional cold/lost-Redis active-horizon replay | window and bucket range, then monotonic journal ID with covered deltas |
| Budget journal operation | idempotency/audit linkage | operation equality then journal order |
| Budget journal generation | generation audit and FK enforcement | generation equality then monotonic journal order |
| Budget journal BRIN | retention and exceptional time-range recovery | append-correlated persisted time at low index cost |
| Budget bucket primary | exceptional cold/lost-Redis snapshot load | window equality plus bucket time range |
| Open reservation partial | exceptional rebuild of in-flight/ambiguous bounds | active states only, ordered by window and bucket |
| Budget last-journal FK indexes | event retention and referential checks | journal equality locates bucket/reservation children without scans |
| Redis generation active/history | one active generation and rebuild audit | partial singleton uniqueness plus recent runs |
| Price resolution covering | route/model/tier at time | equality identity then newest effective price |
| Unresolved-price partial | catalog review/refresh | only partial or unknown entries by status/time |
| Status route/time | latest/history query | configuration/route equality then newest event |
| Status problem partial | outage/credit dashboards | indexes only exceptional current rows |
| Inventory latest/model | latest snapshot and model lookup | endpoint/time and model equality paths |
| Outbox partial | maintenance claim/recovery | only runnable or leased rows |

Implementation captures **EXPLAIN (ANALYZE, BUFFERS)** for cache lookup, Begin,
the exceptional budget rebuild snapshot/tail, spend summary, latest status,
model inventory, and 180-day cache cleanup at representative cardinalities.
There is deliberately no PostgreSQL active-budget-admission query to plan. The
reconciliation suite also proves the unknown-cost and unresolved-price partial
indexes. Tests reject
sequential scans on large seeded tables for those bounded queries. Index count
is also measured: an index that does not serve a named query is removed.

## Retention, vacuum, and growth

- Operations/checkpoints/results outlive the maximum Workflow retry and
  retention horizons plus an operator safety margin.
- Cache cleanup defaults may later choose 180 days since **last_used_at**. The
  protocol does not hard-code that duration.
- Cleanup selects a bounded batch through **response_cache_gc_idx**, locks rows
  with **SKIP LOCKED**, rechecks last use/no fill/no retained descendant, marks
  tombstoned, and schedules blob deletion.
- Status events and inventory snapshots have independent bounded retention;
  current projections remain.
- PostgreSQL budget journal/bucket projections are deleted only after every
  matching window, operation reconciliation, and permitted cold-rebuild horizon
  has expired. Maintenance uses bounded indexed DELETE statements and never
  loads active budget state into a running worker.
- The maintenance role runs frequent small deletes. It never performs an
  unbounded delete or vacuum from an Activity.
- Monitor dead tuples, HOT update ratio, autovacuum lag, transaction age, index
  bloat, WAL volume, pool saturation, lock wait, cache-table growth, and blob
  orphan count.

Initial schema is unpartitioned because global idempotency/cache uniqueness is
more valuable than premature time partitioning. BRIN indexes and batched
retention support the initial scale. Before the largest table reaches a tested
threshold such as 100 million rows, capacity review may introduce partitioning
through a shadow table and a separate global key registry. It must prove global
uniqueness and query plans before cutover; ad hoc partitions are not an
emergency maintenance step.

## PostgreSQL operating requirements

- Require TLS certificate verification, separate schema-owner/runtime/
  maintenance roles, password or workload-identity rotation, and
  schema-qualified SQL.
- Set connection, statement, lock, idle-in-transaction, and transaction
  timeouts. Size each replica's pool against the database connection budget.
- Use prepared statements or typed generated queries; never concatenate
  identifiers or filters from Activity input.
- Use synchronous commit for operation/provider-ID/checkpoint/cache/budget
  transitions. A deployment may not weaken durability for these tables.
- Enable encrypted storage, encrypted verified backups, WAL archiving, point-in-
  time recovery, cross-failure-domain copies, and scheduled restore drills.
- Readiness checks a transaction, initial schema version, configured physical
  namespace, required indexes, server time, and role capability. It does not
  create or alter schema objects.
- Startup rejects missing/invalid constraints, an unexpected schema version,
  non-UTC session behavior, or any computed relation that overlaps a
  Temporal-owned relation. A safely isolated schema in the same database is
  valid.
- Metrics/traces contain safe IDs and durations, never SQL parameters with
  tenant/content/provider IDs.

## Staged schema initialization sequence

This sequence implements the phases in [scope](../scope.md#staged-delivery-and-document-authority)
without treating every optional subsystem as one release gate:

1. Freeze the accepted DDL in schema-installer and schema-contract fixtures.
2. Provision the configured worker database/schema/prefix and roles, then apply
   the clean initial schema in an ephemeral environment.
3. Implement exact decimal USD and remove public/config currency plus integer
   microUSD assumptions from the authoritative store.
4. Phase A adds PostgreSQL repositories for configuration, encrypted payloads,
   operations/attempts, continuations, USD prices, results, checkpoints, forks,
   and resumable polling.
5. Phase B adds compaction, the write-only exact budget journal/projections,
   conservative nano-USD Redis generation/Stream conformance, prompt-cache
   affinity, and the operator recovery runbook.
6. Phase C adds the route-isolated exact-response cache only after its named
   staging/incident-reproduction caller and measurements are recorded.
7. Phase D adds provider status/inventory, dedicated query audit, and outbox
   repositories. Cross-provider cache equivalence and FX remain future ADRs.
8. Run repository conformance tests from newly constructed fixtures; do not
   import or copy Redis data.
9. Switch each phase only after its focused gates pass so PostgreSQL owns the
   newly introduced durable replay/journal/
   accounting/conversation/cache/control state and Redis owns active budgets
   and throttles. Readiness requires both dependencies for new paid dispatches.
10. Remove only the superseded Redis durable-operation, continuation, and result
   representations. Replace the old microUSD budget Function with the conservative nano-USD
   generation/Stream Function. Keep Redis client, budget/throttle Functions,
   configured prefix, deployment, health checks, and focused persistence tests.
11. Run cross-store crash-boundary tests at every Redis-reserve, PostgreSQL-
   journal-write, dispatch, finalize, and Redis-reconcile boundary, plus concurrent
   budget/cache/fork, query-plan, retention, and restore tests with at least two
   worker replicas.
12. Record clean-namespace initialization evidence. Before the first release,
    rollback means rebuilding the disposable test environment from the earlier
    application revision, not preserving or translating stored data.

Because there is no released dataset, implementation initializes clean
namespaces and does not add compatibility/data-movement work for these phases.
The sequence still isolates schema installation, repository composition, and
Redis responsibility-split commits so a cheaper implementation agent can test
and review each boundary. Redis remains in production after the split. After a
release, any incompatible schema or namespace change gets a separate design
based on the deployed version and real data at that time.

## Database acceptance gates

- Exact decimal arithmetic round-trips values with 18 fractional digits through
  PostgreSQL, Go, JSON, and OCaml without float conversion.
- Operation/cache JSONB contains only bounded content-free manifests; raw
  payloads use authenticated envelope-encrypted inline bytes or encrypted blobs.
  Indexed lookup uses HMAC-SHA-256 and never a JSONB scan.
- Every completed Generate/Compact operation, cache hit, and query execution has a valid
  exact-or-unknown cost state. Exact has non-null **actual_cost_usd** and a
  method; unknown has NULL amount/method and a safe reason. Cache hits and local
  queries are exact zero.
- Price-catalog component NULLs remain unknown through Go, SQL, JSON, and OCaml;
  they are never coerced to zero. Tests cover exact $0, sub-micro-dollar values,
  $1, $10, the maximum **NUMERIC(38,18)** value, and overflow rejection.
- The database exposes no generic downstream currency column.
- All foreign keys have a named supporting index or a documented parent-side
  access path.
- Concurrent operation-key reuse produces one operation or a digest conflict.
- Phase C concurrent identical cache misses produce one provider submission.
- A Temporal retry increments cache **use_count** at most once; max-int
  saturation does not overflow, and a tombstone cannot block a replacement
  ready entry with the same fingerprint.
- An old entry used yesterday survives a 180-day unused-entry sweep.
- A worker restart polls a persisted provider operation ID without submission.
- Redis budget admission never exceeds any exact window under concurrent
  Functions; conservative nano-USD rounding, identity-keyed subtraction, and
  rebuild conversion are deterministic and never under-throttle.
- The steady-state budget hot path performs zero PostgreSQL budget SELECTs;
  statement instrumentation proves only insert/update journal/projection writes.
- Joining/restarting one worker and recovering a Redis Stream gap read budget
  state only from Redis. A zero-live-worker cold start adopts an intact
  same-incarnation generation before considering a PostgreSQL rebuild. The
  exact exceptional read proof follows the normative section above.
- An allowed cold/new-incarnation bootstrap reconstructs one verified generation from the PostgreSQL
  active horizon/journal tail and fences concurrent journal writers at the
  final high-water mark; unexplained same-incarnation partial Redis loss neither
  admits work nor reads PostgreSQL.
- Bounded sizing and queued-batch tests implement the normative workload
  envelope above without turning it into a runtime limit.
- Credit exhaustion is not inferred from an unclassified 429.
- Phase C cache entries never cross route identities; a future equivalence ADR
  has no acceptance gate in these phases.
- Checkpoint tables reject runtime UPDATE and retention cannot delete a needed
  ancestor/blob.
- Named hot queries use the intended indexes at representative volume.
- Backup restore plus blob integrity verification reconstructs completed replay
  and pending polling state.
- Query executions use bounded inline JSON and never require an external result
  blob; a budget query performs no PostgreSQL budget SELECT even though its
  required audit INSERT must commit before Activity completion.
- Same-incarnation corruption exercises the documented operator runbook and
  never recommends an unfenced manual key mutation.
