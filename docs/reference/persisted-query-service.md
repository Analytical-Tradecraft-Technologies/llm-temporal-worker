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

Deployments must supply the three security/observability seams below; a
fourth budget-status seam is required only when that query kind is enabled:

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
`ProductionFactoryOptions.QueryServiceBuilder`. It does not invent keys,
authorization, or an audit repository. A PostgreSQL closer may expose the
read repositories through `PostgresQueryRepositoriesSource`; missing
repositories remain a permanent unsupported-capability response rather than
an empty result.

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
factory, _ := runtime.NewProductionEngineFactory(runtime.ProductionFactoryOptions{
    QueryServiceBuilder: func(ctx context.Context, snapshot *config.Snapshot, repos runtime.PostgresQueryRepositories) (activity.QueryService, error) {
        return runtime.NewPersistedQueryService(snapshot, repos, runtime.PersistedQueryOptions{
            Authorize: authorizeTenant,
            Cursor:    &control.CursorCodec{Key: cursorKey, TTL: 15 * time.Minute},
            Audit:     auditQuery,
            BudgetStatus: redisBudgetReader,
            ResolveScope: func(ctx context.Context, scope control.QueryScope) (uuid.UUID, error) {
                return authenticatedScopeIDs.Lookup(ctx, string(scope.Tenant), string(scope.Project))
            },
        })
    },
})
```

The builder receives the same immutable snapshot used to construct the worker;
it must not resolve credentials or mutate that snapshot.
