# Persisted credit-status query

`storage/redis.ProviderStateStore.ListCreditStatuses` implements the neutral
`control.ProviderStatusReader` for `credit_status` queries. It reads the current
configuration-bound route projections and their retained credit/billing
evidence. It does not call a provider inference or management endpoint. The
previous PostgreSQL read side and schema have been removed.

The result contains one row per provider/endpoint identity, ordered by provider
and endpoint. If several routes share an endpoint, the newest `ObservedAt`
projection wins, with route ID breaking ties. By default only non-OK credit or
billing states are returned; `IncludeOK` also includes healthy endpoints.
Provider and endpoint filters use bounded identifiers. Page size defaults to
100 and is capped at 1,000.

Billing evidence takes precedence when present; otherwise the mapper uses the
retained credit evidence. Responses contain normalized state, confirmation and
freshness timestamps, and bounded safe evidence codes. The query wire mapper
exposes the billing-domain value `issue` as `blocked`. Raw provider responses,
credentials and event digests are never query results. Ordinary successful
inference does not clear sticky billing or credit incidents.

## Pagination and authorization

Redis persists a bounded read view for the requested snapshot horizon. Later
pages use that same saved view, so concurrent status updates cannot change a
page already being traversed. An `AfterEndpointKey` requires a nonzero
`SnapshotHorizon`. A missing or expired continuation view fails closed instead
of reconstructing pages from the latest mutable projection.

The repository's `NextEndpointKey` is an internal unsigned position. An
authorized query service must bind it to the caller, normalized request,
configuration snapshot and horizon using the cursor codec before returning a
public cursor. Refresh and authorization are explicit query-service
responsibilities. Production query activation still requires those bindings;
the Redis reader alone does not enable a public control API.

See [provider-control persistence and query pages](provider-control.md#query-pages)
for retention and failure behavior. Redis retains the latest projections and
bounded query views, not an append-only provider audit ledger.
