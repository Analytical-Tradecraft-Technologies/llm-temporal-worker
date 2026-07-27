# Pricing and Budgets

> Implementation status and phase authority are centralized in
> [scope](../scope.md#staged-delivery-and-document-authority). This chapter
> labels current behavior; the target storage split, budget-read rules, money
> materialization, and workload envelope are normative only in the
> [PostgreSQL/control-plane design](postgresql-state-cache-and-control-plane.md).

## Goals

Budget enforcement is shared, conservative, and independent of any provider
SDK. A request is admitted only when its worst eligible spend fits every
matching budget window. Completion replaces the reservation with measured cost;
an ambiguous call keeps its full reservation.

V1 budgets govern monetary spend in exact fixed-scale USD (`NUMERIC(38,18)`).
Provider rate limits and token/request quotas are separate controls and do not
pretend to be financial budgets.

## Exact money representation

Configuration and provider decimal prices enter as decimal strings. They are
parsed exactly into integer numerator/scale form; binary floating point is
forbidden in pricing and admission.

```go
type USD struct { /* checked integer at 10^-18 scale */ }

type UnitPrices struct {
	InputPerMillion       DecimalUSD
	OutputPerMillion      DecimalUSD
	CacheReadPerMillion   DecimalUSD
	CacheWritePerMillion  DecimalUSD
	ReasoningPerMillion   DecimalUSD
	PerRequest            DecimalUSD // absolute USD, charged once per request
}
```

Token components are multiplied with integer usage and divided by 1,000,000;
the fixed per-request component is added once as an absolute USD amount. Each
component is rounded up only at the fixed 18-digit USD boundary, then summed
with checked exact arithmetic. Values smaller than one microUSD remain
representable. A nil USD pointer means unknown; a non-nil zero value means
known free.

Redis Lua numbers are exact only within their integer-safe range. The current
combined AdmissionStore retains a legacy microUSD compatibility path, whose
safe bound is checked independently. The Phase B Redis budget materialization
uses the versioned nanoUSD contract below; it rejects any source USD value
above the `2^53 - 1` nanoUSD limit and checks every sum before a Redis write.

The exact Go pricing and budget contracts now use the same **NUMERIC(38,18)**
shape as the durable ledger, providing 18 fractional digits and 20 whole-dollar
digits. Redis compatibility materialization remains an explicit boundary and
does not define the public money representation.
When an exact provider-reported amount crosses that boundary, positive
fractional nanoUSD is rounded up (ceiling) so integer admission accounting
cannot undercharge; configured limits are rounded down. Exact zero remains
zero. Provider JSON number exponents are normalized into the same exact USD
representation before this materialization. The reusable conversion contract
and its safe-integer bound are documented in
[Conservative nano-USD materialization](../reference/nano-usd-materialization.md).

## Price catalog

A price entry is immutable and keyed by:

```text
provider + endpoint family + billing region/account class
+ resolved model + provider tier + effective interval
```

It contains all applicable USD unit prices, provenance, effective timestamps,
and a content digest/version. USD is the only supported denomination: field
names and the strict `pricing.CompileUSD` boundary establish the denomination;
there is no generic source `currency` field or caller-supplied FX rate. Logical
aliases are resolved before pricing.

The compiled boundary also rejects duplicate pricing identities (including two
entries with the same effective start but different end times or prices). This
keeps resolver selection deterministic and prevents an ambiguous catalog from
silently choosing whichever duplicate happened to be listed first.

Catalog precedence is explicit:

1. endpoint-specific operator override;
2. verified built-in catalog entry;
3. no price.

There is no guessed price. A candidate with any matching monetary budget is
ineligible without a current price. `pricing.require_price_when_budgeted: true`
is the only configuration that permits an unpriced candidate, and only when it
matches no monetary budget. `false` is strict: every candidate needs a current
price. `budgets.require_match: true` excludes unmatched candidates before this
decision, so that combination never dispatches an unpriced route.

An allowed unpriced result reports `cost_status=unknown`; nullable exact USD
fields remain NULL, which means “not priced”, not a zero-dollar claim. Method
and catalog version are empty, no monetary reservation is created, and a
provider-reported amount is not promoted to an auditable cost without a current
catalog quote. Metrics make the condition visible. Integer microUSD exists only
at the explicit Redis admission/materialization boundary.

Unknown catalog components and actual costs are NULL with an explicit
status/reason, while exact zero means known free. Known spend totals exclude
NULLs and separately report unknown operation counts.

The current YAML loader carries omitted components as an `unknown` marker on
the compiled pricing entry. Costing and reservation estimation fail closed if
the request needs that component; omission is never decoded as a free USD
price. The marker is the in-memory boundary for the nullable/partial catalog
state described above and will map directly to the durable NULL/status fields.

## Estimation

The estimate is an upper bound for one candidate:

```text
estimated input tokens at a conservative tokenizer ratio
+ maximum configured output tokens
+ maximum billable reasoning tokens where separate
+ cache-write assumption when cache behavior is uncertain
+ fixed per-request charge
```

Endpoint/model-specific tokenizers may tighten the estimate only after
conformance tests. The fallback estimator uses UTF-8 byte length plus structural
overhead and a configurable safety factor. It must never use the model's average
completion length. The current catalog has no media-unit price, so media content
does not create a separate estimate component until that pricing contract is
added explicitly.

The request reservation is the maximum single-attempt estimate across every
candidate the router is authorized to attempt, including all explicit
service-class fallbacks. That amount is reserved against the union of policies/
windows that could match any candidate; this may over-reserve an endpoint-
specific budget but cannot undercount a later route. Only one candidate is
billable at a time.

At the engine admission boundary, the exact USD maximum is carried in
`BeginRequest.ReservationUSD` (and `ContinueRequest.RemainingUSD`) alongside
the bounded `MicroUSD` compatibility amount. Exact-capable durable stores use
the USD value for operation and budget journal rows; legacy Redis admission
continues enforcing the independently validated microUSD projection. This
prevents sub-micro-dollar estimates from being silently recorded as zero while
preserving the conservative integer materialization required by Redis.

After a definite uncharged failure the remaining-plan reservation is reused or
reduced. After a definite charged failure, `Continue` atomically converts the
attempt's matching share to incurred cost, refunds other old-window shares, and
reserves the maximum remaining candidate against the union of its windows.
Denial stops fallback after retaining the known incurred cost. No next provider
dispatch occurs before `Continue` succeeds.

## Reconciliation

Final cost uses this precedence:

1. authoritative provider-reported billed cost;
2. provider-reported usage priced by the pinned catalog entry;
3. locally reconstructed usage priced by the pinned catalog;
4. full reservation when the outcome or usage is ambiguous.

The public `llm.Cost` response includes nullable `reserved_cost_usd` and
`actual_cost_usd` values, method, and catalog version. It has no generic
currency discriminator. The estimate is used for admission but is not
serialized as a separate response cost field. When an adapter receives an
authoritative provider cost, the raw value remains in the safe provider or
usage raw-facts maps; it is not copied into `llm.Cost` without an exact catalog
quote.

If measured cost exceeds the conservative reservation, completion still records
the full cost because it was already incurred. It atomically adds the excess,
marks a `reservation_underestimated` policy violation, and alerts. A budget may
then be over its limit; subsequent admissions fail. Silently clipping cost is
forbidden.

## Budget policies

```yaml
budgets:
  require_match: true
  policies:
    - id: acme-production
      match:
        tenant: acme
        project: assistant
        actor_prefix: service-
        environment: production
        logical_model: reasoning
        endpoint: openai-prod
        service_class: priority
      windows:
        - duration: 1h
          limit_usd: "25.000000000000000000"
          bucket: 1m
        - duration: 24h
          limit_usd: "250.000000000000000000"
          bucket: 5m
        - duration: 30d
          limit_usd: "3000.000000000000000000"
          bucket: 1h
```

Matchers cover tenant, project, actor prefix, environment, logical model,
endpoint, and service class. A policy must declare at least one restriction;
an exact `*` is a wildcard rather than a restriction, so wildcard-only policies
are rejected. All matching policies and all windows within them apply; policies
are not first-match-wins. Missing context cannot match a restricted policy. With
`budgets.require_match: true`, every authorized
candidate must match at least one policy before it can be priced, admitted, or
dispatched. This filtering is candidate-specific, so an explicit fallback that
matches remains eligible even if the requested service class does not. If no
authorized candidate matches, the request terminates as `no_route` without an
admission operation or provider request.

The compiled Go `budget.Policy` boundary repeats this validation for policies
loaded from an immutable snapshot, so direct callers cannot bypass it. Empty
matchers and matcher fields containing only `*` are rejected; an
`actor_prefix: "*"` is treated as a wildcard as well.

Limits must be positive. Bucket size must divide into bounded operational
resolution and produce no more than the configured maximum buckets per window.
Policy IDs and window definitions are immutable across a catalog version;
changes create a new version with explicit carry-forward behavior.

## Conservative sliding windows

For time `t`, duration `W`, and bucket size `D`:

```text
first = floor((t - W) / D)
last  = floor(t / D)
active sum = every bucket index from first through last, inclusive
```

The full first bucket is counted even though only part intersects the exact
window. This can deny slightly early but cannot undercount spend. The current
reservation is added to the current bucket only after checking:

```text
active sum + requested reservation <= limit
```

The operation record stores the original bucket for each union-window
reservation. Reconciliation reduces those original buckets by unused shares and
adds actual/excess cost only to windows that matched the attempt, so a refund
cannot create a negative current bucket or move spend across time. Expired
buckets may be deleted after the longest window plus the maximum operation-
finalization delay.

Redis server time is authoritative for shared admission. The memory backend uses
an injected clock. Clock rollback in memory fails closed until time catches up;
Redis `TIME` avoids worker clock disagreement.

## Atomic admission

`AdmissionStore.Begin` performs one atomic operation:

1. locate the operation record by scoped operation key;
2. return a completed response reference, ambiguity, or conflict if it exists;
3. load the union of policy/window totals that could match the authorized plan;
4. deny with the earliest conservative retry time if any limit would be
   exceeded;
5. increment all union-window current buckets by the conservative plan amount;
6. create the `reserved` operation with request digest, amount, price version,
   and lease.

Memory uses one lock around the conformance transaction. Redis uses a
versioned server-side Function, with a Lua-script fallback for compatible Redis
versions. All ledger and budget keys use one configured hash tag in v1 so Redis
Cluster can run the transaction atomically. That intentional single-slot
constraint is documented and monitored.

Begin is idempotent: the same digest returns the existing state without charging
again; another digest returns `operation_conflict`.

`AdmissionStore.Continue` is also atomic. It verifies the dispatch token and
definite outcome, reconciles the prior attempt only in the windows that matched
that attempt, releases unused union-window shares, checks the remaining plan's
union windows, and either creates the next `reserved` attempt or terminally
records denial. Complete performs the same matching reconciliation for the
successful final attempt and releases reservations held only for unused routes.

## Target throttle and monetary-budget split

Target Phase B does not retire Redis. It splits the current combined
`AdmissionStore` responsibility into two explicitly sequenced ports:

- `BudgetStore` in Redis holds every bucket/reservation needed for the complete
  active horizon and atomically enforces a conservative monetary materialization alongside
  request, token, and concurrency throttles.
- `BudgetJournal` in PostgreSQL is the system of record and records each accepted reservation,
  reconciliation, release, and exact/unknown actual cost before the associated
  provider side effect. It is the rebuild/audit source, not a steady-state
  budget-read path.

Redis uses checked safe-integer nano-USD derived without floating point: charges
round up, limits round down, and transitions subtract the exact
identity-keyed integer previously applied. PostgreSQL, Go, JSON, and OCaml keep
exact `NUMERIC(38,18)` semantics. The Redis Stream provides optional
cross-worker invalidation/wake-up coordination; it never authorizes a request.

For new work the engine reserves Redis first, appends the PostgreSQL journal,
then dispatches. PostgreSQL write failure triggers a best-effort Redis release; an
unconfirmed release remains charged until its TTL, which is conservative.
Completion commits exact/unknown cost in PostgreSQL before idempotently
reconciling the Redis budget. Retry first resolves the durable PostgreSQL
operation so a completed replay is never charged again. Both stores fail closed
for new paid dispatches. This is not a shared cross-store transaction and no
correctness claim depends on both writes committing atomically.

## Denial and retry

A denial reports:

- every policy/window that denied;
- current conservative usage, requested reservation, and limit;
- the earliest time each window could admit if no new spend occurs;
- a safe aggregate `retry_after`;
- configuration and price versions.

The Activity turns denial into a retryable Temporal application error only when
the caller's schedule-to-close horizon can reach `retry_after` and retry policy
permits it. Otherwise it is terminal for that Activity. Denial never contacts a
provider.

## Durability modes

| Mode | Use | Guarantee |
| --- | --- | --- |
| Memory | unit tests and explicitly single-process development | process-local only; restart loses state |
| Redis plus PostgreSQL | production and multi-replica development | PostgreSQL is the durable system of record; Redis is the live conservative decision/coordination materialization |

Production Redis remains required and uses authentication/TLS, `noeviction`, AOF plus RDB, monitored
persistence errors, and backups. `appendfsync always` provides the strongest
reservation durability at higher latency; `everysec` may lose about a second of
writes during a crash and must be an explicit risk acceptance. Redis or
PostgreSQL failure causes new paid admission to fail closed.

## Conformance properties

The current memory/Redis stores retain their existing conformance. The staged
split adds conservative nano-USD Redis budget-state and PostgreSQL journal
contracts plus end-to-end tests proving:

- no accepted schedule exceeds a limit under concurrent Begin calls;
- overlapping policies and windows are all charged atomically;
- request replay never charges twice;
- request digest conflicts never inherit another result;
- completion/refund is idempotent and cannot make a bucket negative;
- ambiguity retains the reservation;
- exact-boundary buckets overcount rather than undercount;
- time rollback, overflow, expired leases, and persistence errors fail closed;
- the earliest retry calculation is conservative.
