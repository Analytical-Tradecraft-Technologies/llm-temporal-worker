# Pricing and Budgets

Redis owns monetary authorization and settlement. Cloud request records preserve
attempts and exact-or-unknown cost facts. The production workflow uses the
[Redis budget lease contract](../reference/redis-budget-leases.md); the older
`Begin`/`Continue` admission API described below remains a direct-engine
compatibility boundary, not the cloud workflow path.

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
and a content digest/version.

The reservation is quoted at the attempted class's provider tier. The quote
also captures, at the same instant, the entries of the route's other service
classes, so when the provider reports serving a response at another class (for
example a priority request served at standard), usage is priced with that
class's entry. A served-class entry that cannot price the usage (a partial
entry marking a used component unknown) falls back to the quoted entry, so a
paid response is never discarded. A provider-reported exact cost is still used
as reported. USD is the only supported denomination: field
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
price. During ordered route planning, an entry with an unknown required
component or a compatibility-boundary overflow is an unusable quote for that
candidate: the engine records the safe no-price condition and continues to the
next authorized fallback. It only fails the plan when no candidate remains.
An active partial entry is never dispatched as an unknown-cost operation; the
explicit unpriced policy applies only when the resolver has no active entry at
all and the candidate has no matching monetary budget. Tokenizer, request, and
estimator configuration errors remain hard planning failures. The marker is the
in-memory boundary for the nullable/partial catalog state described above and
will map directly to the durable NULL/status fields.

## Estimation

The estimate is an upper bound for one candidate:

```text
estimated input tokens at a conservative tokenizer ratio
+ maximum configured output tokens
+ maximum billable reasoning tokens where separate
+ cache-write assumption when cache behavior is uncertain
+ fixed per-request charge
```

The context-window check (`ValidateContext`, and the same check inside the
reservation) counts the estimated input plus the output cap only. Reasoning
tokens are generated inside that cap on every supported family: Anthropic
`thinking.budget_tokens` must be less than `max_tokens`, and OpenAI
`max_output_tokens` / `max_completion_tokens` bound visible output and
reasoning together. A requested reasoning `token_budget` is therefore not
added to the window a second time. The reservation still prices the reasoning
component separately at the catalog's reasoning rate, on top of the full
output cap, so it stays an upper bound.

The Go `budget.Estimator` accepts an optional candidate-aware exact tokenizer
hook. A configured hook must be deterministic, return a non-negative count,
and account for the provider's request structure (including tools and schema);
the candidate argument allows provider-family/model-specific tokenization. A
hook error or negative result fails closed. When no hook is configured, the
fallback estimator uses UTF-8 byte length plus structural overhead and a
configurable safety factor. It must never use the model's average completion
length.

Providers bill media on its decoded content (pixels or pages), not on the
bytes the worker serializes, and a URL contributes only its string to the
UTF-8 estimate. The fallback estimator therefore leaves inline image and
document bytes (base64 in the serialized request) out of the UTF-8 estimate
and adds a conservative per-part input-token allowance for every image or
document part (URL, inline bytes, or blob reference) in instructions,
messages, and tool results:

| Part | Allowance | Basis |
|---|---|---|
| Image | 6,000 tokens | Above the largest documented per-image counts of supported providers (about 1,600 standard, about 2,500 patch-based, about 4,800 high-resolution). |
| Inline text document (`text/*` media type) | Decoded byte length | The provider tokenizes the content as text and a token covers at least one byte. A quarter of the bytes is counted in the UTF-8 estimate like any other text; the allowance reserves the rest. |
| Inline PDF with a provable page count | Pages x 9,000 tokens, capped at 5,400,000 | When the PDF has no object streams (`/ObjStm`), every page dictionary appears in the file, so counting `/Type /Page` bounds the pages. An object stream, a `/Type` name written with `#` escapes, or no visible page gives no bound. |
| Any other document | 5,400,000 tokens | 600 pages (the largest supported PDF page limit, Anthropic on 1M-token-context models) x 9,000 tokens per page (up to 3,000 extracted-text tokens plus the rendered page image, charged at the 6,000-token image allowance). |

Inline text is bounded by its size and an inline PDF by its visible pages. A
URL or blob reference has no content to measure at admission, and a PDF whose
page dictionaries may be compressed into object streams has no provable page
count (every page is billed as an image), so those keep the unknown-size
assumption.

The allowance is added to the serialized-size estimate, so text-only requests
are unchanged. When the candidate declares a context window, the allowance is
capped at the room left after the text estimate and the reserved output and
reasoning: the provider must reject input beyond its window, so the allowance
alone never excludes a candidate for context size. The unknown-size document
allowance deliberately exceeds every supported context window, so on any
candidate that declares one, such a document reserves the whole remaining
input room (the most the
provider can bill), including on 1M-token-context routes; only a candidate
without a declared context window reserves the full 5,400,000-token constant.
A capability profile that accepts documents must declare its context window
(`context_tokens` / `max_context_tokens`); the catalog is rejected otherwise.
A reservation larger than a matched budget window's limit can never be
admitted, so planning skips that candidate, and when every candidate is
skipped for that reason it fails with a non-retryable `budget_denied` instead
of waiting for capacity. Context-fit checks and
compaction planning (`ValidateContext`, `CountInputTokens`) use the same UTF-8
estimate without the allowance, so inline image and PDF bytes do not count
against the context window and inline text documents count at the ordinary
text baseline. A configured exact tokenizer is responsible for media and
replaces the allowance. The catalog still has no media-unit price; media is
priced as input tokens and settlement records actual usage.

The request reservation is the maximum single-attempt estimate across every
candidate the router is authorized to attempt, including all explicit
service-class fallbacks. That amount is reserved against the union of policies/
windows that could match any candidate; this may over-reserve an endpoint-
specific budget but cannot undercount a later route. Only one candidate is
billable at a time.

At the engine admission boundary, the exact USD maximum is carried in
`BeginRequest.ReservationUSD` (and `ContinueRequest.RemainingUSD`) alongside
the bounded `MicroUSD` compatibility amount. Durable request records retain
the exact USD value; legacy Redis admission
continues enforcing the independently validated microUSD projection. This
prevents sub-micro-dollar estimates from being silently recorded as zero while
preserving the conservative integer materialization required by Redis.
The memory and Redis admission implementations fail closed when an exact
projection is present but diverges: every exact amount must match the scalar
USD reservation and its MicroUSD amount is rounded up at the boundary, while a
MicroUSD limit may only be more conservative than its exact USD limit. This
keeps compatibility writes from under-reserving or over-admitting while
retaining exact values in the domain and durable request facts.
If any component or the checked sum cannot be represented in Redis-safe
microUSD, estimation fails closed rather than dropping that component from the
compatibility reservation.

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
the full cost because it was already incurred and atomically adds the excess. A
budget may then be over its limit; subsequent admissions fail. Silently clipping
cost is forbidden. No dedicated `reservation_underestimated` violation record or
alert is emitted yet; an overrun is visible as accounted spend above the window
limit in budget status.

A completed response whose usage is entirely absent (all token counts zero) and
that carries no provider-reported cost is settled as cost unknown, retaining its
reservation, rather than as an exact near-zero catalog cost.

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

Each window is accounted under `<policy id>/<window id>`. The window id is the
optional `id` field, or `<duration>-<bucket>` when it is omitted (`24h-5m`). It
is never the window's list position, so removing, inserting or reordering
windows cannot point one window at another window's spend. Two windows of one
policy with the same geometry need explicit ids. A limit change keeps the
identity and its spend; a reload that changes duration or bucket behind an
unchanged identity is rejected. See
[budget window identity](../reference/configuration.md#budget-window-identity).

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
Both admission transitions validate that every per-window amount equals the
request-wide scalar amount and that each policy/window/bucket identity appears
only once. Invalid envelopes fail before any bucket mutation; this keeps the
legacy scalar compatibility field and the durable reservation vector
conservative and unambiguous.

## Production throttle and monetary-budget split

`BudgetLeaser` in Redis owns reserve, single-use claim and settlement. Operational
request, token and concurrency throttles use a separate `ThrottleStore`. Cloud
request/attempt records preserve provider recovery and result facts; they are
not a budget reconstruction journal.

Redis uses checked safe-integer nano-USD derived without floating point: charges
round up, limits round down, and transitions subtract the identity-keyed integer
previously applied. Go, JSON and OCaml keep exact decimal USD semantics. The
optional Redis Stream publishes hints atomically with budget mutations; it never
authorizes dispatch and is not sufficient to reconstruct lost accounting.

Before a paid submission, acquisition reserves every matching window and claim
consumes that authorization once within 15 minutes. Insufficient capacity returns
wait with a retry hint, including requests larger than the current limit. The
budget workflow uses a Temporal timer, then tries again. A waiting activity does
not occupy a worker slot.

Completion persists its cloud result and retries Redis settlement idempotently.
An uncertain paid submission retains its original charge. Retrying paid work
requires a separate attempt and reservation; elapsed time cannot refund a
claimed authorization. Only unused reservations expire by the start deadline.
Cloud writes and Redis mutations are separate operations with explicit recovery
states, not a distributed transaction.

## Durability modes

| Mode | Use | Guarantee |
| --- | --- | --- |
| Memory | Tests and explicit single-process development | Process-local only; restart loses state |
| Redis-only legacy adapters | Direct-engine development and conformance fixtures | Not the production cloud workflow runtime |
| Cloud KV/blob plus Redis | Durable workflow composition | Cloud request/artifact persistence and Redis accounting authority |

Production Redis requires the configured persistence, `noeviction`, monitoring
and authority-marker checks. Dataset loss cannot be repaired from cloud records
or Stream hints automatically. A permanent initialization receipt prevents an
absent authority marker from being treated as a fresh installation. Partial loss
and rollback still require separate recovery validation. See the
[recovery boundary](../runbooks/redis-budget-generation-recovery.md).

## Conformance properties

The budget and cloud execution tests cover:

- atomic reservation across overlapping policies and windows;
- acquisition replay without another charge and rejection of payload conflicts;
- single-use claims and a server-enforced start deadline;
- idempotent settlement, including a lost reply after mutation;
- retained uncertain paid work and fresh budget for a replacement attempt;
- fail-closed overflow, malformed state and missing authority;
- durable result recovery without another provider submission; and
- conservative rounding and bounded expiry work.

Live Redis tests exercise the actual Functions and preloaded Lua compatibility
mode. Offline model tests alone do not prove persistence or restore behavior.

### Inclusive OpenAI output usage

The `openai_chat` and `openai_responses` adapters preserve the provider's
inclusive output-token count, which already contains reasoning tokens. Their
catalog entries must therefore use an explicit zero `reasoning_per_million`;
`output_per_million` pays for the entire output, including reasoning. The
reasoning counter remains available for reporting. Nonzero additive reasoning
prices are rejected during catalog compilation, reservation estimation, and
usage settlement to prevent charging the same tokens twice. Supporting separate
reasoning rates for these families requires disjoint usage normalization first.
