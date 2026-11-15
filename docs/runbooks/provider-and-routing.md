# Providers and routing

Runbooks for provider failures, shared route health, service-class downgrades,
missing routes and catalog price expiry. Provider endpoints are deliberately
excluded from readiness, because one route can be down while another serves
the request, so none of these alerts changes `/health/ready`. Background:
[routing and continuation](../architecture/routing-and-continuation.md#route-health)
and [provider control](../reference/provider-control.md). Start from
[on-call escalation and severity](on-call.md) for the rules that apply to
every incident.

## Priority requests downgraded or actual tier unknown

- **Signal:** `llmtw_service_class_actual_total{requested,actual,endpoint}`
  with `actual` below `requested` (for example `requested="priority"`,
  `actual="standard"`), or `actual="unknown"` when the provider did not report
  the tier that served the request.
- **Readiness reason:** none.
- **Default severity:** SEV3; SEV2 when a contract promises priority
  processing to a caller and most of its traffic is downgraded.
- **Safe action:**
  1. Group by `endpoint`. A downgrade on one endpoint usually means the
     provider served a lower tier (capacity or account entitlement); check the
     provider's status and the account's tier with the provider account owner.
  2. Check `service_class_policies` in the running configuration: a policy
     that leaves out `priority` serves those requests at `standard` by design.
  3. For `actual="unknown"`, confirm whether the provider ever reports the
     served tier for that endpoint family; if it never does, the series is
     expected and the alert threshold should exclude it.
  4. Requests are priced at the class the provider reported serving, when the
     plan captured that class's price, so a downgrade does not overcharge.
- **Forbidden:** remapping an endpoint's `service_classes` so a lower provider
  tier is labelled `priority`, or editing service-class policies for one
  tenant without the owner's approval. Neither changes what the provider
  serves; both hide the downgrade.

## No eligible routes

- **Signal:** `llmtw_activity_failure_total{origin="caller"}` for planning
  failures, or `{origin="worker"}` when every candidate failed at the `price`
  phase. Logs name the cause: the `requested model is not in the route catalog`
  warning (`model`, `cause=model_not_in_catalog`), and one
  `route rejected during provider planning` warning per rejected route with
  `route_id`, `cause` (such as `route_quote_unavailable`,
  `route_capability_unavailable:<feature>`,
  `route_service_class_unavailable`, `route_context_limit`,
  `route_tenant_not_allowed`, `route_region_not_allowed` or
  `continuation_pinned`), `error_code` and `phase`. The request fails with
  Temporal type `llm_invalid_argument`, code `no_route` (or
  `unsupported_capability` when a route declined a feature the request needs),
  and the first cause in the safe `reason` detail.
- **Readiness reason:** none.
- **Default severity:** SEV2 when a model in production use has no route;
  SEV3 for one caller asking for an unsupported capability.
- **Safe action:**
  1. Read the rejection causes. `model_not_in_catalog` usually follows a
     model-sync change or a removed model; `route_quote_unavailable` or a
     `price`-phase failure points at the price catalog (see
     [catalog price expiry](#catalog-price-approaching-or-past-expiry));
     capability, context and continuation causes are caller-side and need no
     operator change.
  2. Fix the route, model or price in the configuration or catalog files,
     check them with `llm-temporal-worker validate-config`, then reload and
     confirm `llmtw_config_reload_total{outcome="success"}`.
  3. If every candidate is held by route health instead, the request fails
     with `provider_unavailable` and retries; follow
     [route held](#route-held).
- **Forbidden:** adding an unreviewed price, or a route to an endpoint without
  a reviewed `outbound_hosts` policy, just to make planning succeed. Both can
  send paid work to the wrong place or at the wrong price.

## Provider failures

- **Signal:** `llmtw_provider_attempt_total{outcome="failure"}` and
  `llmtw_activity_failure_total{origin="provider"}` rise. Each failed paid
  attempt logs a `provider attempt failed` warning with `route_id`,
  `endpoint`, `model`, `outcome`, `error_code` (`authentication`,
  `permission_denied`, `provider_rate_limited`, `provider_unavailable`,
  `provider_invalid_response`, `deadline_exceeded`, `ambiguous_dispatch` and
  so on), `phase`, `dispatch`, `retry`, and, when present, the upstream HTTP
  `status`, a closed `cause` and the provider's `finish_reason`. Metrics do
  not separate authentication failures from rate limits or 5xx responses; the
  log's `error_code` does.
- **Readiness reason:** none.
- **Default severity:** SEV2 for a provider-wide failure; SEV3 for a single
  model or a short burst that route health absorbs.
- **Safe action:**
  1. Group the warnings by `endpoint` and `error_code` to see which provider
     and which failure class.
  2. `authentication`: the credential was rejected. Follow
     [provider credential rotation](configuration-and-credentials.md#provider-credential-rotation).
  3. `provider_rate_limited`: check the provider quota with the provider
     account owner. Lowering `temporal.worker.max_concurrent_activities`
     requires a rollout; prefer letting route health hold the route.
  4. `provider_unavailable` or `deadline_exceeded`: check the provider's
     status page and the egress path. Route health holds the route after a
     bounded run of failures and planning moves to other routes.
  5. A rising `outcome=outcome_unknown` share means attempts may have reached
     the provider: follow [stuck ambiguous attempt](stuck-ambiguous-attempt.md).
- **Forbidden:** replaying failed requests by hand, widening an endpoint's
  `outbound_hosts`, or routing provider traffic through a broad proxy to get
  around an egress failure. Never paste provider error bodies into the
  incident; they can contain request content.

## Route held

This covers the "Route held by an open circuit, exhausted credit or a billing
issue" alert.

- **Signal:** `llmtw_route_held_total{endpoint,reason}`, one per route
  candidate that planning skipped because shared route health holds it, with
  `reason` `circuit_open`, `credit_exhausted` or `billing_issue`. It counts
  even when another route served the request. On the durable path an
  out-of-credit provider response (HTTP 402) opens the circuit, so it appears
  as `circuit_open`; `credit_exhausted` and `billing_issue` come from sticky
  credit incidents, which only the legacy engine records.
- **Readiness reason:** none.
- **Default severity:** SEV3 while other routes serve; SEV2 when the held
  route is the only one for a model in use.
- **Safe action:**
  1. Find why the route was held from the `provider attempt failed` warnings
     for that `endpoint` before the hold.
  2. Credit or billing: the provider account owner tops up or fixes billing.
     An insufficient-credit hold lasts 15 minutes or until the configuration
     digest changes, and a top-up made in place recovers after one probe
     request.
  3. Authentication or invalid configuration: fix the credential or
     configuration. The hold lasts 24 hours from the rejection unless a
     configuration with a different digest is published. Replacing a secret
     value, restarting, or reloading an unchanged file keeps the same digest
     and does not release it; see
     [provider credential rotation](configuration-and-credentials.md#provider-credential-rotation)
     for the trade-offs of releasing it early.
  4. Transient failures: wait for the half-open probe. The
     `route circuit closed` info log and
     `llmtw_route_health_transition_total{to="circuit_closed"}` confirm
     recovery.
- **Forbidden:** deleting or editing route health records in Redis to force a
  route back into service. Shared route health is keyed by route, endpoint,
  model, tier and configuration digest; hand edits can reopen a route that is
  still failing for every worker.

## Route circuit opened

- **Signal:** `llmtw_route_health_transition_total{endpoint,to="circuit_open"}`
  and the `route circuit opened; planning holds the route` warning with
  `endpoint`, `route_id`, `model` and `status=circuit_open`. It fires on the
  worker whose observation changed the shared record, so it fires even for a
  route with no current traffic, and the sum across workers counts each flip
  once.
- **Readiness reason:** none.
- **Default severity:** SEV4 for a circuit that closes again within its hold;
  SEV3 if it reopens repeatedly; SEV2 if it holds the only route for a model.
- **Safe action:** as for [route held](#route-held). Correlate with
  `llmtw_route_held_total` to see whether traffic is being turned away, and
  with the provider's status for the `endpoint`.
- **Forbidden:** as for [route held](#route-held).

## Route unavailable or degraded

- **Signal:** `llmtw_route_health_transition_total{endpoint,to="unavailable"}`
  or `{to="degraded"}`, counted when a shared route health record changes into
  that availability state. `to="circuit_closed"` counts recoveries. Every
  configured endpoint is exported at zero before its first flip. Memory-state
  workers keep no shared route health and never emit these.
- **Readiness reason:** none.
- **Default severity:** SEV3; SEV2 when the route is the only one for a model.
- **Safe action:**
  1. Check the provider's status for the `endpoint` and the
     `provider attempt failed` warnings around the transition.
  2. Confirm other routes are serving: `llmtw_provider_attempt_total{outcome="success"}`
     for the model's other endpoints.
  3. If the provider is down for a long time, a reviewed configuration change
     can disable the route; reload and confirm success.
- **Forbidden:** as for [route held](#route-held).

## Catalog price approaching or past expiry

This covers the "Priced route approaching or past its catalog
`effective_until`" alert.

- **Signal:** `llmtw_catalog_price_expiry_timestamp_seconds - time()` below the
  catalog renewal lead time, or negative once a route's price has lapsed. The
  gauge is the earliest end of the current priced window over the served
  snapshot's routes, evaluated on every scrape; `+Inf` means no price expires.
  It names no route: find the entry with the earliest `effective_until` in
  the files listed under `pricing.catalogs`.
- **Readiness reason:** none.
- **Default severity:** SEV3 while approaching; SEV2 once negative, because a
  route without a current price cannot be quoted: planning rejects it with
  `route_quote_unavailable`, and a request with no other route fails with
  `no_route` at the `price` phase.
- **Safe action:**
  1. Find the expiring entry and obtain the provider's reviewed price for the
     next period, with its provenance.
  2. Add a following entry whose `effective_from` equals the old
     `effective_until`, so the windows merge without a gap.
  3. Update the catalog file and its `pricing.catalogs[].sha256`, run
     `llm-temporal-worker validate-config`, then reload. A wrong digest fails
     the reload with `cause=catalog` and the old snapshot keeps serving.
  4. Confirm the gauge moved to the new window's end.
- **Forbidden:** extending `effective_until` on an existing entry without a
  reviewed price, turning off `pricing.require_price_when_budgeted`, or
  pointing the catalog at an unreviewed price source. Each lets paid work run
  at an unverified price.
