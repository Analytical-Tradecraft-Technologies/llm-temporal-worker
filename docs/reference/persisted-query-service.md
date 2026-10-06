# Persisted control-plane query composition

The runtime now exposes an explicit `runtime.NewPersistedQueryService`
composition for Redis-backed provider-status, model-inventory, and credit-status,
an optional spend-summary reader, and an explicitly supplied
Redis budget-status reader. See [provider control](provider-control.md) for
atomic updates, last-known inventory, and query-view retention. It binds every page to the immutable
configuration snapshot digest and uses the storage pages only after the
control layer has authenticated the tenant scope and signed cursor.
The typed response boundary additionally rejects duplicate or out-of-order
page keys before signing a continuation or emitting query audit metadata.
This protects the same keyset invariant for deployment-owned handlers as for
the built-in storage readers.

Optional string filters (`provider`, `endpoint`, `model_prefix`, and
`policy_key`) are omitted when unset; JSON `null` is not an alternate spelling
for omission. The v1 decoder rejects `null` before authorization or storage
access so a malformed request cannot silently become a broader unfiltered
query.

## Production CLI composition

The production CLI installs this service for every durable snapshot. It
authorizes a query exactly when the caller's tenant/project pair is listed in
`authorization.allowed_scopes`, the same trusted-Temporal policy that gates
Generate and Compact, and it checks before any storage read. Query cursors are
signed with an HMAC key derived from the primary `continuation.handle_keys`
secret under the domain `llmtw:query-cursor:v1` and the configuration snapshot
digest. Rotating that key or reloading a changed configuration therefore
invalidates outstanding cursors, which are short-lived, so a page never resumes
under another snapshot; no extra secret is needed. Provider status, model inventory and credit status read the snapshot's
Redis provider state.

Budget status reads the active Redis budget generation through the built-in
`redis.NewRedisBudgetStatusReader` when `state.redis.admission_mode` is
`function`. The worker never loads Redis code, so before every budget read the
CLI runs `FUNCTION LIST LIBRARYNAME llmtw_budget_status_v3 WITHCODE` and
requires that exact library code (the digest of
`redis.BudgetStatusFunctionLibrarySource()`) with its `budget_status_v3`
Function, and then requires a published active budget generation. The worker
does not publish that generation itself (see the reader contract below). When either
is missing, or the library code differs, `budget_status` returns the typed
unsupported-query error (`unsupported_capability`, not retryable). An operator
can load the library or publish a generation without a configuration reload.
`FUNCTION LIST` is keyless, so with Redis Cluster the CLI runs it on every
master (`ForEachMaster`; every shard for a Ring client). Every master must
hold the exact library before the read is attempted. This check does not
depend on which master owns the budget key's slot, so it stays valid after a
resharding moves that slot. A cluster with no reachable master fails as
`state_unavailable`.
A Redis failure during these checks or during the read is a retryable
`state_unavailable` error. Neither path invents a value. In `lua` admission mode
nothing provisions the budget script by SHA, so `budget_status` stays
unsupported. The query honours `policy_key` and `include_windows=false`. The
latter returns the generation provenance with an empty `windows` array. It is
a single bounded snapshot, so `page_size` and `cursor` are rejected as unknown
fields, and the response is always complete with no `next_cursor`.

Spend summary stays a typed unsupported-query error until a durable cloud spend
reader exists. Refresh requests (`refresh_if_older_than_seconds` > 0) are
served as described in [Provider management refresh](#provider-management-refresh).

Every authorization decision is logged as an ordinary structured log entry,
`control query access decision`. Each entry has `outcome` (`allowed` at info,
`denied` at warn), `query_kind`, `tenant_hash` and `project_hash`. The actor,
query body, cursor and the authorizer's error are never logged. Because
authorization precedes any cursor decode or storage read, a denied query is
logged even though it never completes. Completed queries are still logged
separately as `control query completed`. Both are best-effort logs, not a
durable audit store.

The low-level `NewPersistedQueryService` constructor requires
`control.AuthorizeFunc` for tenant/project/actor authorization and a keyed
`control.CursorCodec` for scope/filter/horizon-bound cursors. Its optional
`control.AuditFunc` observes completed queries on a best-effort basis. When
omitted, it logs metadata with the supplied logger or the snapshot's configured
log format and level on stderr. Audit encoding and logging failures never
fail a successful read. A budget-status reader is required only when that
query kind is enabled.

`PersistedQueryOptions.BudgetStatus` is the explicit
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
then logs completed queries through the optional supplied logger or normal
snapshot-configured logs. The repository bundle no longer contains a SQL audit
repository. A snapshot client set exposes read
capabilities through `QueryRepositoriesSource`; missing read
repositories remain a permanent unsupported-capability response rather than
an empty result.

Budget status is composed through the separate
`ProductionFactoryOptions.BudgetStatusReaderFactory` seam. The factory invokes
it once for each immutable snapshot and passes the exact Redis client,
generation port, key space, Function mode, and clock owned by that snapshot.
`runtime.NewRedisBudgetStatusReaderFactory()` adapts the built-in
`redis.NewRedisBudgetStatusReader`; deployments that need a custom invoker may
wrap that constructor, but must preserve the same generation/provenance and
bounded-read contract. A nil factory, nil reader, or unavailable Redis
generation leaves `budget_status` unsupported and never falls back to
PostgreSQL. For the same reload-safety reason, spend summary obtains its
scope resolver from `QueryRepositories.ScopeResolver`, not from
process-lifetime builder options.

Refresh requests need `QueryRepositories.Refresh`. The production factory
supplies it for durable snapshots; without it, refresh requests are rejected as
unsupported. Budget status
remains fail-closed until the deployment explicitly composes the built-in
versioned Redis generation/window reader through `BudgetStatus`. The storage
package provides `redis.NewRedisBudgetStatusReader`. A library caller of
`NewProductionEngineFactory` must supply
`ProductionFactoryOptions.BudgetStatusReaderFactory`; the production CLI
supplies its Function-checking factory itself (see above). The production factory
then supplies the snapshot-owned Redis client, generation port, key space, and
approved Function version needed for the bounded read. A nil factory or
incomplete Redis capability leaves `budget_status` unsupported; after a reader
is configured, an unavailable generation or Function produces a propagated,
fail-closed read error. Deployments must not infer the layout or treat a
manifest-only read as a complete budget answer. The
SQL spend-summary implementation has been removed. Spend summary remains a
typed unsupported capability until a deployment supplies an authenticated
aggregation reader. No empty success or invented zero cost substitutes for the
missing reader.

## Provider management refresh

A `provider_status`, `model_inventory` or `credit_status` query with
`refresh_if_older_than_seconds` > 0 asks the worker to fetch fresh provider
state before reading Redis. The production factory composes a
`runtime.ProviderRefresher` for each durable snapshot from its routing catalog
and provider adapters. Refresh runs inside the authorized handler, so a query
denied by `Authorize` never reaches a provider.

Only model inventory has a provider fetcher today: the `provider.ModelLister`
extension, implemented by the direct OpenAI Chat and Responses adapters (see
[provider control](provider-control.md#provider-model-list-capability)).
No adapter exposes a provider status or credit/balance management API, so:

- `provider_status` and `credit_status` refresh requests return the typed
  unsupported-query error (`unsupported_capability`, not retryable).
- A `model_inventory` refresh returns the same error, before any fetch, when no
  configured endpoint matches the `provider`/`endpoint` filter, or when any
  matching endpoint has no model-list fetcher. An endpoint is also unsupported
  when its routes disagree on provider, family, region or account identity,
  because the listing would otherwise be written under a guessed identity.
  Narrow the filter to a supported endpoint to refresh it.

For each matching endpoint whose last provider listing is older than
`refresh_if_older_than_seconds`, the worker lists the provider's models and
writes the listing with `PersistInventorySnapshot`. The worker applies these
bounds:

- **Minimum interval.** A listing younger than one minute is never refetched,
  whatever the request asks. An authorized caller therefore cannot turn queries
  into a high-rate provider management API client.
- **Collapsed concurrency.** Concurrent refreshes of the same endpoint identity
  share one provider fetch. The shared fetch is detached from the first
  caller's cancellation, so one canceled caller does not fail the others; each
  caller can still stop waiting when its own context ends.
- **Timeout.** Each endpoint's complete listing is bounded by 10 seconds.
  Endpoints in scope are refreshed in parallel.
- **Per-query bound.** At most four endpoints are fetched per query. The bound
  counts only endpoints whose persisted listing is too old, so endpoints that
  are already fresh never use it up. Further stale endpoints are not fetched.
- **Recheck before fetch.** The owner of an endpoint's shared fetch re-reads
  the persisted listing first and skips the provider call when another
  refresh has just written a fresh one.
- **Validity.** A refreshed listing is reported current for one hour.

Refresh happens only on the first page. A continuation page reads the view
pinned by its signed cursor and never calls the provider again.

The response reports the outcome explicitly:

- When at least one endpoint was refreshed, the source is
  `persisted_and_refreshed`. Otherwise it is `persisted`.
- When a fetch fails, times out or returns an invalid listing, or an endpoint
  was skipped by the per-query bound, the existing Redis listing is left
  unchanged and returned with freshness `stale`. The worker never fabricates
  or clears a listing on failure.
- A Redis failure while reading the existing listing is the retryable
  `state_unavailable` error, as for an ordinary read.

Known limitation: the Redis store caches a query view for one second of
horizon. A non-refresh read in the same second just before a refresh can make
that refresh's first page omit the new listing; the next query sees it.

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
   The active-generation pointer is included in that same invocation and is
   fenced against the generation, incarnation, and manifest digest supplied by
   the reader. A pointer switch between the client-side manifest load and the
   Function call therefore fails closed instead of returning a stale generation.
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
`budget_status_v3` Redis Function (or its explicitly configured Lua SHA) once
for the full catalog. Version 3 is deliberate: the active-generation pointer
is now an input key and is fenced inside the Function, so this key layout must
not replace the v2 Function in place. During a rolling upgrade, preload both
the v2 and v3 libraries and keep the v2 reader on old workers until they have
drained; the distinct library/function identities let both versions coexist.
The Function drains at most the configured expiry bound, performs fixed-field
`HMGET` for every member, and captures `XINFO STREAM`'s high-water mark. If
expired reservations remain after the bounded drain, it returns
`state_unavailable` instead of a snapshot that still counts them; the entries
it did drain stay drained, so a retry continues where it stopped. The
adapter has the method shape of
`internal/runtime.BudgetStatusReader`, so deployments can pass it directly as
the snapshot-owned `PersistedQueryOptions.BudgetStatus` seam.

The generation reader is an optional composition contract. A legacy or missing
active pointer keeps `budget_status` unavailable; the reader never reinterprets
admission keys as a complete status generation. The worker does not provide a
production rebuild/migration coordinator for these generation contracts. The
normal budget authority initializer supplies a receipt and marker, not this
status projection. Wiring a verified reader to the current accounting authority
remains separate work; see [MVP status](mvp-v1-status.md).

Spend composition also requires `PersistedQueryOptions.ResolveScope`. This
explicit resolver maps the already-authorized tenant/project pair to the
opaque scope UUID using the deployment's existing keyed scope
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
    QueryServiceBuilder:          queryBuilder,
    BudgetStatusReaderFactory: runtime.NewRedisBudgetStatusReaderFactory(),
})
```

The builder receives the same immutable snapshot used to construct the worker;
it must not resolve credentials or mutate that snapshot. The budget reader
factory receives that snapshot's Redis capabilities independently and must not
retain them after the reader is drained. A deployment that enables spend summary must expose a same-snapshot
`ScopeResolver` in the repository bundle. No `QueryAudit` repository is needed.

Query composition exposes spend through `control.SpendSummaryReader` and
`control.SpendSummaryListOptions`, independent of a concrete storage adapter. A
provider receives an already-authorized opaque scope ID, a half-open time range,
and grouping/operation filters. Cloud mode leaves spend unsupported until a
reader is explicitly supplied; nil and typed-nil readers fail closed. Query
authorization and cursor handling remain independent of the chosen storage
implementation; query audit logs remain best-effort.
