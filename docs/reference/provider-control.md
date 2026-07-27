# Provider control observations

The provider-control domain (`golang/control`) is the boundary between an
adapter observation and the durable PostgreSQL projections described in the
[control-plane design](../architecture/postgresql-state-cache-and-control-plane.md#provider-status-credit-state-and-inventory).
It is intentionally independent of provider SDKs and database clients. The
PostgreSQL hand-off is implemented by `storage/postgres.ProviderStatusRepository`
and `storage/postgres.InventoryRepository`; provider adapters and query
Activities remain separate integration layers.

This package remains the normalization and projection-invariant slice of Task
13. The domain package itself does not open a database, call a provider
management API, or register a query Activity. The PostgreSQL repositories
described below are the separate durable boundary; keeping that boundary
explicit prevents an in-memory `RouteStatus` or `InventorySnapshot` from being
mistaken for persisted production state.

## Runtime composition

Production snapshots bind provider outcomes to a snapshot-scoped recorder. The
recorder receives the immutable configuration digest/epoch and route/endpoint
identity, constructs a bounded `control.StatusEvent`, and persists it through
the `ProviderStatusRepository` from the same PostgreSQL client set. It never
stores prompts, outputs, credentials, raw provider bodies, or unbounded
provider text. Recorder failures are reported as control-plane telemetry and
do not alter the provider result; provider-control persistence is not a
process-readiness gate. Memory-mode snapshots do not construct a PostgreSQL
recorder.

## Status and credit

Adapters convert inference, startup, management, and operator observations to
`control.StatusObservation` and call `NewStatusEvent`. The constructor rejects
missing identity/evidence digests, unknown enum values, invalid observation
intervals, and unbounded or unsafe provider text. Only the resulting
`StatusEvent` is suitable for an append-only event ledger; raw response bodies
and credentials are never part of the event.

Credit classification is conservative. A generic HTTP 429 is a rate/capacity
signal and does not establish exhausted credit. Exhausted/billing incidents
require one of the reviewed provider codes (`insufficient_quota`,
`billing_hard_limit`, `payment_required`, `quota_low`, `billing_warning`,
`quota_restored`, or `billing_restored`) or an authorized operator event.
Arbitrary provider text, even when printable and bounded, is not incident
evidence.
`RouteStatus.Apply` keeps a confirmed incident sticky across ordinary
inference and startup observations. A provider management response, matching
configuration epoch, or operator event may explicitly clear it. Changing the
configuration epoch starts a fresh projection; it does not inherit the old
incident. The projection is also bound to its immutable configuration digest:
an event from another snapshot is rejected even when it advertises a newer
epoch.

For bounded reads that need to rebuild this domain projection, see the
[status replay reference](status-replay.md). Replay follows persisted
`EventID` order, applies an inclusive observation horizon, and reports whether
the storage read was complete; it is not a public cursor or a replacement for
the PostgreSQL projection.

## Model inventory

`InventorySnapshot` validates bounded, sorted model rows, explicit lifecycle
values, safe metadata, and current/stale/unsupported provenance. Provider APIs
may paginate through `NextCursor`; an unsupported listing is represented by an
explicit `unsupported` source rather than an empty successful list.

Inventory is informational. `ConfiguredModel` is a predicate only: discovered
models never mutate the configured routing catalog or become routable without a
reviewed configuration snapshot and capability entry.

`RefreshCoordinator` collapses concurrent refreshes per endpoint. A failed
refresh preserves the last valid snapshot for stale reads while returning the
refresh error, allowing query callers to report stale provenance instead of
silently presenting a fabricated current result.

### Provider model-list capability

Provider adapters may opt into the neutral `provider.ModelLister` extension.
`ListModels` accepts a bounded `ModelListQuery` and returns a validated,
sorted `ModelListPage` with explicit completion and an opaque continuation
cursor. The page model contains only bounded IDs, lifecycle values, capability
digests, and safe string metadata; raw provider bodies and credentials never
cross the adapter boundary. An adapter that does not implement `ModelLister`
is explicitly unsupported for management refresh. The registry does not probe
providers or infer an inventory from inference calls, and discovered models
never become configured routes automatically.

This is an integration seam, not a claim that every provider supports model
listing. A deployment must supply a provider-specific management implementation
and persist its page through `InventoryRepository`; until then the existing
`configured_only` or `unsupported` inventory sources remain the honest result.

## Persistence hand-off

`ProviderStatusRepository.PersistStatusEvent` consumes a validated
`StatusEvent` in one transaction. It appends the event (with its deterministic
digest as an idempotency key), takes a transaction-scoped advisory lock for the
configuration/route identity (including when the projection row does not yet
exist), locks the route projection, applies the domain sticky-incident rules,
and commits the current projection. Stale observations
remain in the append-only ledger but do not replace the current projection.
`GetRouteStatus` reads only the projection and never returns raw provider
response data.

`InventoryRepository.PersistSnapshot` validates the deterministic inventory
digest, writes one immutable snapshot and all normalized model rows in one
transaction, and treats a repeated observation/digest as an idempotent replay.
Continuation cursors are envelope-encrypted with authenticated context bound
to the configuration, endpoint account, snapshot identity, and inventory
digest; plaintext cursors are never stored. `GetSnapshot` revalidates the
loaded rows and `OpenCursor` authenticates the cursor before returning it.

The repositories do not register provider management adapters or a query
Activity. Runtime composition supplies the inference/status recorder
separately; query integration and provider management/list adapters remain
explicit follow-up work. Discovered models remain informational rather than
routable, and configured-only/unsupported inventory is valid until a provider
management adapter exists.

### Persisted status pages

`ProviderStatusRepository.ListRouteStatuses` is the bounded query-side
repository method for a provider-status query. It reads the current
`provider_route_status` projection
with one set-based SQL statement, applies provider/endpoint/availability
filters, excludes healthy routes unless requested, and returns deterministic
`route_id` keyset pages. The returned `NextRouteID` is an unsigned storage
position, not a public cursor: the query service must bind its signed cursor
to the authorized scope, tag, filter, and snapshot horizon before reusing it.

The page contains only normalized projection fields. It never scans the
append-only event ledger, invokes a provider API, or exposes raw provider
responses and credentials. Staleness is represented by the persisted
`stale_after` timestamp so the query layer can report current versus stale
provenance without changing the stored projection.

### Persisted model inventory pages

`InventoryRepository.ListInventoryModels` is the bounded query-side repository
method for a model-inventory query. It selects the newest immutable snapshot for each
matching provider/endpoint, then reads normalized model rows in deterministic
`provider, endpoint, provider_model_id` order. Provider, endpoint, model-prefix,
and lifecycle filters are applied in PostgreSQL; the page limit is bounded to
1..1000 and the read never invokes inference or a provider management API.

The first page discovers a `SnapshotHorizon`, the maximum observation time of
the selected latest snapshots. A continuation must pass that horizon and the
returned keyset position (`provider`, `endpoint`, snapshot identity, and
`provider_model_id`). Reads run in one repeatable-read transaction, so a newly
persisted snapshot cannot replace the snapshot set halfway through a page
sequence. Latest selection also partitions on the endpoint-account HMAC
internally; the HMAC is never returned, while the immutable snapshot identity
keeps account epochs distinct for cursor binding. The returned
`InventorySnapshotInfo` carries snapshot identity, source, completeness,
observation/expiry timestamps, and inventory digest; its `ProvenanceAt` helper
reports current, stale, or explicitly unsupported state.

This storage page intentionally retains the capability digest and bounded safe
metadata from the normalized `control.Model`. The `llm.ModelInventoryPage`
wire contract requires a list of capability names, which is not derivable from
a digest. The authorization, signed public cursor, and an explicit capability
encoding/mapping decision remain follow-up work in the control/query layer;
storage does not fabricate capability names or make discovered models
routable.

## Typed query boundary

`control.QueryRequest` and `control.QueryResponse` provide a typed view of the
closed `llm.QueryRequestV1`/`llm.QueryResponseV1` envelopes. Provider, endpoint,
operation, model, policy, and monetary values use named types rather than
unbounded `string` fields. Each query kind has its own filter and result rows;
the only untyped data is inside the `llm` package's JSON boundary. Use
`EncodeQueryRequest`/`DecodeQueryRequest` and
`EncodeQueryResponse`/`DecodeQueryResponse` at that boundary. Encoding and
decoding re-run the wire validator, so unknown fields, malformed timestamps,
invalid enums, and unsafe decimal values cannot enter the typed model.

`QueryBillingState` intentionally uses the wire value `blocked`; it is not an
alias for the domain `BillingState`, whose incident value is `issue`. Adapters
must make that mapping explicit. `control.QueryService` admits all five closed
query kinds after authorization and wire validation. Only provider-status,
model-inventory, and credit-status carry keyset cursors; budget-status and
spend-summary are complete bounded snapshots without a public cursor. This
package does not add storage reads, provider refreshes, budget aggregation, or
Activity registration; those remain composition work behind
`control.QueryService`. For the audit requirement, `QueryService.Audit` offers
a storage-neutral callback after response and cursor validation. It receives
canonical redacted envelopes and exact-or-unknown cost metadata and must commit
the record before returning; failures are surfaced as retryable finalize state
errors. The runtime now carries an optional, snapshot-scoped
`PostgresQueryRepositories` snapshot/query-side bundle from the PostgreSQL
closer into its `productionClientSet`. A custom closer may provide inventory,
query-audit, and scope-resolution capabilities when their key material and
schema are provisioned; the default closer exposes provider status and spend
summary. Persisted-query composition implements spend summary from PostgreSQL;
budget status remains deployment-provided and fail-closed. A companion
`QueryService` is forwarded only when the same closer supplies one, so an
unconfigured query family stays fail-closed rather than falling back to an
in-memory answer. The low-level persisted-query constructor accepts a
snapshot-scoped Redis budget-status reader, but the production builder leaves
that family unsupported until runtime composition owns such a reader.

`CursorCodec` signs a bounded opaque position with HMAC-SHA256. Its claims bind
the query kind, full tenant/project/actor scope (including tags), canonical
filter digest, and an explicit snapshot horizon. Tokens are base64url encoded,
expire by default after 15 minutes, reject future-issued or oversized values,
and are never accepted for a different scope, filter, or key. The signed
horizon is validated by `control.QueryService` before a typed handler runs and
is returned in `BoundCursorClaims` for the storage adapter to enforce against
its repeatable-read snapshot before using the opaque position. The service
also validates any outgoing cursor against the same typed request and a fresh
clock sample, so a handler cannot return an unsigned or horizon-free
continuation. The raw `Handler` interface remains source-compatible while
adapters migrate; new storage adapters should implement `TypedHandler`.
