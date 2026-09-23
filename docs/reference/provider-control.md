# Provider control observations

The provider-control domain (`golang/control`) defines storage-neutral status,
credit, and inventory models. Production runtime snapshots use
`storage/redis.ProviderStateStore` for these records and their query readers.
The previous `storage/postgres.ProviderStatusRepository` and
`storage/postgres.InventoryRepository` remain legacy adapters during the
staged SQL removal; production no longer selects them for provider state.
No existing SQL data is migrated.

## Runtime composition

Production snapshots bind inference outcomes to the same Redis client and key
namespace used by the worker. The recorder constructs a validated
`control.StatusEvent`, then atomically updates the route projection. It stores
safe codes and digests, never prompts, outputs, credentials, or raw provider
bodies. Recorder failures remain control-plane telemetry and do not replace a
provider result. Memory-mode snapshots do not create external stores.

`QueryRepositories.ProviderStatus` and `.Inventory` expose storage-neutral
read interfaces. `V1RuntimeCapabilities.ProviderInventory` exposes
`control.InventoryStore` to deployment-owned refresh scheduling. Authorized
query service composition still requires explicit authorization and cursor
keys. This change does not add automatic model refreshes or provider API calls.

## Status and credit

Adapters convert inference, startup, management, and operator observations to
`control.StatusObservation` and call `NewStatusEvent`. The constructor rejects
missing identity/evidence digests, unknown enum values, invalid observation
intervals, and unbounded or unsafe provider text. Only the resulting
`StatusEvent` is suitable for persistence; raw response bodies
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
the current Redis projection. Redis does not retain a status-event history.

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

The production OpenAI Chat and Responses adapters implement this extension for
the direct OpenAI API. They fetch the documented `/models` management endpoint,
normalize and sort the bounded result, and expose an opaque continuation cursor
bound to a digest of the fetched snapshot; if the provider changes the listing
during pagination, the worker fails closed instead of skipping or duplicating
models. Compatible OpenAI endpoints (including Azure) remain explicitly
unsupported until they have a provider-specific management contract; a
deployment must not infer inventory from inference responses. A deployment
still owns refresh scheduling and must persist each page through
`ProviderStateStore.PersistInventorySnapshot`; when no supported lister is configured, the existing
`configured_only` or `unsupported` inventory sources remain the honest result.

## Redis persistence and retention

`PersistStatusEvent` uses optimistic compare-and-swap to apply `RouteStatus.Apply`
atomically. Concurrent workers recompute the transition if another worker wins;
older or duplicate observations cannot replace a newer projection. Separate
credit and billing evidence survives ordinary successful inference. Explicit
management/operator clearance and configuration epoch changes retain the
existing domain rules. Failure counters are updated once per applied event.
There is no SQL journal or dual write on this path.

Current status and inventory use one bounded hash per configuration and record
kind. Keys and field identities are HMAC-derived, scoped to the configured
prefix, secret, and Redis Cluster hash tag. Each record is limited to 4 MiB;
each hash is limited to 4,096 records and 32 MiB of record payloads. Capacity,
corruption, and Redis failures return errors rather than partial state.

Latest records deliberately have no key TTL. `StaleAfter` and `ExpiresAt`
determine freshness. Expiry must not clear sticky credit/billing incidents,
allow older observations to win, or discard a last-known listing after a
failed refresh. Configuration digests isolate reloads; cleanup of retired
configuration hashes is a later maintenance task. Persistence uses the same
HA/AOF/no-eviction Redis deployment as the worker's other shared state.

`PersistInventorySnapshot` validates the model digest and atomically replaces
only an older listing for the same provider, endpoint, and account identity.
Duplicate writes are no-ops; different observations at the same timestamp are
rejected. `GetInventorySnapshot` returns the last known listing, including its
freshness and completeness. Provider pagination cursors use AES-GCM with a
random nonce and authenticated configuration/endpoint/account/snapshot context;
the encryption key is domain-separated from the Redis namespace key secret.
The cursor is decrypted only when reading a validated inventory record.

## Query pages

`ListRouteStatuses`, `ListCreditStatuses`, and `ListInventoryModels` implement
the neutral `control` page contracts with the existing filters and limits
(default 100, maximum 1,000). Credit queries select the newest route projection
per provider/endpoint with route ID as the tie breaker, then apply the healthy
filter. Inventory remains informational and cannot change routing.

The first read captures the bounded configuration hash with one atomic
`HGETALL`. A temporary view pins that data for subsequent pages, including
safe incident evidence and encrypted inventory cursors. Views are shared at
the existing public cursor's one-second horizon resolution; queries in the
same second reuse that view. Direct current-record reads do not use views.
A newer listing or status observation cannot remove or replace rows midway
through pagination. Filters and the keyset position are applied to the pinned
view; public scope/filter authentication stays in `control.CursorCodec`.

Views expire 15 minutes after creation, independently of observation freshness.
An expired continuation returns `control.ErrProviderViewExpired`; callers must
restart pagination. It never silently reconstructs a continuation from newer
state, even when a deployment gives its signed cursor a longer lifetime.
These views are temporary query snapshots, not a historical event ledger.

The inventory page retains model capability digests and safe metadata.
Capability names are not derivable from a digest; storage does not fabricate
capabilities or make discovered models routable.

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
`control.QueryService`. For auditing, `QueryService.Audit` provides a best-effort callback after response
and cursor validation; runtime composition logs audit metadata normally.
`QueryRepositories` carries Redis provider readers, the optional Redis budget
reader, and the remaining SQL spend reader. `PostgresQueryRepositories` is a
compatibility alias for deployment builders. Missing optional capabilities
continue to fail closed.

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
