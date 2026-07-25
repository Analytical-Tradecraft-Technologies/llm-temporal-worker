# Persisted control-plane query composition

The runtime now exposes an explicit `runtime.NewPersistedQueryService`
composition for the PostgreSQL-backed provider-status, model-inventory,
credit-status, and spend-summary query families. It binds every page to the immutable
configuration snapshot digest and uses the storage pages only after the
control layer has authenticated the tenant scope and signed cursor.

Deployments must supply all three security/observability seams:

- `control.AuthorizeFunc` for tenant/project/actor authorization;
- a keyed `control.CursorCodec` for scope/filter/horizon-bound cursors; and
- a `control.AuditFunc` that records the completed query before the Activity
  returns.

`control.CursorCodec` also validates the typed request itself whenever a token
is signed or decoded. Direct storage adapters therefore cannot mint a cursor
for an unsafe tenant/project/actor scope, an invalid page size, or a filter
whose kind does not match the query kind. The Activity still performs the
same validation at its wire boundary; the duplicate check is intentional so
the reusable cursor seam remains fail-closed when called independently.

The production factory accepts these choices through
`ProductionFactoryOptions.QueryServiceBuilder`. It does not invent keys,
authorization, or an audit repository. A PostgreSQL closer may expose the
read repositories through `PostgresQueryRepositoriesSource`; missing
repositories remain a permanent unsupported-capability response rather than
an empty result.

The composition is persisted-only. Refresh requests are rejected until an
explicit management refresh adapter is supplied. Budget status remains
fail-closed until its Redis budget-generation repository is composed. The
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
            ResolveScope: func(ctx context.Context, scope control.QueryScope) (uuid.UUID, error) {
                return authenticatedScopeIDs.Lookup(ctx, string(scope.Tenant), string(scope.Project))
            },
        })
    },
})
```

The builder receives the same immutable snapshot used to construct the worker;
it must not resolve credentials or mutate that snapshot.
