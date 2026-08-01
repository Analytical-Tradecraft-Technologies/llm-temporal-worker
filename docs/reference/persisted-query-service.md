# Persisted control-plane query composition

The runtime now exposes an explicit `runtime.NewPersistedQueryService`
composition for the PostgreSQL-backed provider-status, model-inventory,
credit-status, and spend-summary query families, plus an explicitly supplied
Redis budget-status reader. It binds every page to the immutable
configuration snapshot digest and uses the storage pages only after the
control layer has authenticated the tenant scope and signed cursor.
The typed response boundary additionally rejects duplicate or out-of-order
page keys before signing a continuation or committing query audit evidence.
This protects the same keyset invariant for deployment-owned handlers as for
the built-in PostgreSQL readers.

Optional string filters (`provider`, `endpoint`, `model_prefix`, and
`policy_key`) are omitted when unset; JSON `null` is not an alternate spelling
for omission. The v1 decoder rejects `null` before authorization or storage
access so a malformed request cannot silently become a broader unfiltered
query.

The low-level `NewPersistedQueryService` constructor requires the three
security/observability seams below; a fourth budget-status seam is required
only when that query kind is enabled:

- `control.AuthorizeFunc` for tenant/project/actor authorization;
- a keyed `control.CursorCodec` for scope/filter/horizon-bound cursors; and
- a `control.AuditFunc` that records the completed query before the Activity
  returns.

`PersistedQueryOptions.BudgetStatus` is the fourth, deliberately explicit
composition seam. Its `runtime.BudgetStatusReader` receives the typed budget
filter and requested instant and must read the active Redis generation only.
The reader is responsible for validating the active pointer and manifest,
rejecting instants outside manifest coverage, checking every expected window
member, and binding the result to the generation, manifest digest, and Stream
high-water mark. It must never fall back to PostgreSQL budget tables. The
runtime also rejects a reader result whose `active_at` does not exactly match
the requested instant.

`control.CursorCodec` also validates the typed request itself whenever a token
is signed or decoded. Direct storage adapters therefore cannot mint a cursor
for an unsafe tenant/project/actor scope, an invalid page size, or a filter
whose kind does not match the query kind. The Activity still performs the
same validation at its wire boundary; the duplicate check is intentional so
the reusable cursor seam remains fail-closed when called independently.

Every query response also has an explicit completion contract. Provider-status,
model-inventory, and credit-status pages are complete only when `next_cursor` is
absent; an incomplete page must carry its signed continuation cursor. Budget and
spend responses are bounded snapshots, so they must always be complete and never
include a cursor. The v1 wire codec enforces these combinations before a custom
query service can return a response or the Activity can record audit evidence.

The production factory accepts these choices through
`ProductionFactoryOptions.QueryServiceBuilder`. Use
`runtime.NewPersistedQueryServiceBuilder` for the production persisted-query
contract. It requires deployment-owned authorization and cursor key material,
then binds `PostgresQueryRepositories.QueryAudit.RecordAudit` from the same
immutable client set as the read repositories. It fails snapshot construction
when that PostgreSQL audit repository is absent, rather than accepting a
separate callback at this production composition boundary. A PostgreSQL
closer may expose the query capabilities through
`PostgresQueryRepositoriesSource`; missing read repositories remain a
permanent unsupported-capability response rather than an empty result.
Budget status remains unavailable through this helper: a process-lifetime
builder must not capture a Redis reader across reloads. An embedding may use
the low-level constructor only when it can supply a reader owned by that exact
snapshot; the default production factory does not yet expose that capability.
For the same reload-safety reason, spend summary obtains its scope resolver
from `PostgresQueryRepositories.ScopeResolver`, not from process-lifetime
builder options.

The PostgreSQL composition is persisted-only. Refresh requests are rejected
until an explicit management refresh adapter is supplied. Budget status
remains fail-closed until a concrete Redis generation/window reader is
composed through `BudgetStatus`. The storage package currently exposes the
generation pointer/manifest port, but not yet a versioned window-hash field
reader; deployments must not infer that layout or treat a manifest-only read
as a complete budget answer. The
PostgreSQL `SpendSummaryRepository` provides the storage read seam for spend:
it unions completed `operations` with completed `query_executions`, joins each
operation to its highest-numbered durable attempt for provider and model
grouping, uses the operation scope/time index plus a bounded lateral attempt
lookup, and aggregates exact NUMERIC(38,18) amounts without treating unknown
costs as zero. Its interval is half-open (`start_time <= completed_at <
end_time`) and groups are ordered by their typed dimensions with NULLs first.

## Versioned budget-status reader contract

The existing durable Redis v1 hashes are not a `BudgetStatus` source. Their
aggregate fields do not distinguish reserved from accounted amounts, their
keys are not bound to the generation named by the active pointer, and the v1
manifest does not provide a complete member-to-window mapping with a limit for
each member. The operation records also have no bounded operation index that a
materializer can use to apply expiry, release, or reconciliation without an
unbounded scan. Reading those hashes as if they were a current snapshot could
therefore mix generations, omit a window, or report an amount that cannot be
explained by an idempotent operation transition. A v1 or legacy key is
unsupported for `budget_status`, even when it happens to contain plausible
numbers.

Before a production `BudgetStatusReader` or materializer is enabled, the
deployment must publish a versioned v2 generation with the following minimum
contract:

1. **Generation-scoped window records.** Every window member is stored under
   the immutable generation selected by the active pointer. The member key is
   derived from the manifest's canonical member identifier and the configured
   Redis namespace/hash tag; it is never a process-local or non-generation-
   scoped hash. A window record carries the schema version and generation
   identity needed to reject a key from another generation.
2. **Separate accounting fields.** Each member has distinct non-negative
   integer fields for `limit_nano_usd`, `reserved_nano_usd`, and
   `accounted_nano_usd` (plus the bounded bucket fields required by the
   admission policy). The reader computes available capacity from these fields
   and validates the safe-integer bound, non-negative values, and the policy
   invariant before constructing decimal USD output. It never reconstructs one
   field from an aggregate total or treats an absent field as zero.
3. **Atomic, idempotent transitions.** The versioned Redis Function/Lua
   implementation must perform reserve, reconcile, release, and expiry updates
   atomically for the complete operation member set. Each operation records its
   generation, member deltas, state, expiry, and transition fingerprint in a
   bounded generation-scoped operation index. Retries with the same fingerprint
   are no-ops; a conflicting operation, generation, or fingerprint fails
   closed. Expired index entries and their reservation deltas are removed by the
   same atomic path, not by a reader-side scan.
4. **Complete manifest catalog.** The immutable v2 manifest must enumerate the
   exact member identifiers, policy/window identity, coverage interval, and
   per-member limit (or a digest of a separately immutable limit catalog whose
   contents are validated with the manifest). The catalog count and digest are
   checked before adoption. A manifest that cannot derive every expected
   member key and limit is incomplete, even if all observed hashes are valid.
5. **Bounded, coherent read.** A reader first validates the active pointer and
   immutable manifest, then rejects an instant outside the manifest coverage.
   One server-side Redis Function/Lua invocation must perform the bounded
   expiry drain, read every catalog member's fixed field set (using bounded
   `HMGET` operations), and capture the Stream high-water mark. A version-
   fenced read/retry is an equivalent alternative only when it proves that
   every member and the high-water mark came from one generation; independent
   client-side `HMGET` commands are not sufficient. The invocation must not
   use `HSCAN`, `HGETALL`, or an unbounded operation lookup on the query path.
   Missing members/fields, duplicate catalog entries, wrong schema or
   generation, malformed integers, digest/provenance mismatches, and
   `reserved + accounted > limit` all fail closed. The response cites the
   generation, manifest digest, and captured Stream high-water mark and is
   never completed from PostgreSQL budget rows.

   Expiry must be current before those values are returned. The same
   invocation performs a bounded atomic expiry drain, or a freshness-verified
   sweeper performs that drain immediately before the read. If the sweeper is
   behind its freshness bound, cannot prove the drain completed, or encounters
   an ambiguous expiry record, the reader fails closed rather than reporting a
   stale reservation.

   The query is current-only. A requested `active_at` older than the current
   snapshot instant (or any historical instant that is merely inside the
   coverage interval) returns the typed `budget_history_not_available` error;
   coverage membership is not a time-travel guarantee. The current snapshot
   instant, generation, manifest digest, and Stream high-water mark must all
   be captured and validated by the same coherent read.

The Go storage adapter is `redis.NewRedisBudgetStatusReader`. Its v2 window
keys are derived from the generation and canonical `policy_id`/`window_id`
member key, and each member hash must carry `budget-window/v2`, the generation,
incarnation, manifest digest, member key, and the three nano-USD accounting
fields. `BudgetManifestMember.limit_nano_usd` is required for this reader;
older manifests that omit it remain valid only for generation adoption and are
not a `budget_status` source. The adapter invokes the preloaded
`budget_status_v2` Redis Function (or its explicitly configured Lua SHA) once
for the full catalog. The Function drains at most the configured expiry bound,
performs fixed-field `HMGET` for every member, and captures `XINFO STREAM`'s
high-water mark. The adapter has the method shape of
`internal/runtime.BudgetStatusReader`, so deployments can pass it directly as
the snapshot-owned `PersistedQueryOptions.BudgetStatus` seam.

Migration is an explicit, fenced operation. A v1/legacy active pointer keeps
`budget_status` unavailable; there is no in-place reinterpretation, automatic
dual-read, or PostgreSQL fallback. The deployment must build a complete v2
generation from the durable journal under the documented Redis replacement or
cold-bootstrap fence, validate every member and operation index entry, and
atomically switch the active pointer to v2. During a mixed rollout the reader
accepts only a complete v2 generation, while v1 keys remain admission-owned
until their bounded expiry/retention window has elapsed. Old generations are
garbage-collected only after no lease, reservation, cursor, or audit reference
can reach them. Any missing or ambiguous migration evidence leaves paid work
and `budget_status` fail-closed.

Spend composition also requires `PersistedQueryOptions.ResolveScope`. This
explicit resolver maps the already-authorized tenant/project pair to the
opaque PostgreSQL scope UUID using the deployment's existing keyed scope
repository. The runtime rejects the query if the resolver is absent, fails, or
returns the nil UUID; it never invents HMAC keys or creates a scope as a query
side effect. An unconfigured deployment therefore fails closed rather than
returning an incomplete answer. No provider call or streaming path is used by
these Temporal query Activities.

Example deployment wiring:

```go
queryBuilder, _ := runtime.NewPersistedQueryServiceBuilder(runtime.PersistedQueryBuilderOptions{
    Authorize: authorizeTenant,
    Cursor:    &control.CursorCodec{Key: cursorKey, TTL: 15 * time.Minute},
})
factory, _ := runtime.NewProductionEngineFactory(runtime.ProductionFactoryOptions{
    QueryServiceBuilder: queryBuilder,
})
```

The builder receives the same immutable snapshot used to construct the worker;
it must not resolve credentials or mutate that snapshot. The deployment's
`PostgresQueryRepositoriesSource` must provide `QueryAudit`; the default
PostgreSQL closer deliberately does not invent the HMAC keyrings and scope
repository required to construct that ledger. A deployment that enables spend
summary must also expose a same-snapshot `ScopeResolver` in that bundle.
