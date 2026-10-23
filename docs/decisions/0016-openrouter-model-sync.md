# ADR 0016: OpenRouter model sync

- Status: Accepted; implemented behind the optional `model_sync` configuration
- Date: 2026-10-06
- Amends: the "never refresh silently from an untrusted endpoint" rule in
  [Pricing and budgets](../architecture/pricing-and-budgets.md#price-catalog)
  for deployments that opt in

## Context

Every routable model used to need four hand-written pieces: a `models` route,
an endpoint, a capability profile naming that exact model and a price entry.
OpenRouter publishes every model it serves, every upstream endpoint for each
model and each endpoint's per-token prices through a public API, including the
tier prices of OpenAI (default, flex, priority) and the long-prompt price
overrides of OpenAI and Anthropic. OpenAI's and Anthropic's own model APIs
publish no prices.

## Decision

`model_sync` makes every OpenRouter model routable under its OpenRouter ID
(for example `openai/gpt-5.4`), priced from OpenRouter's published endpoint
prices.

- **One fetcher, shared through state storage.** Each worker schedules a
  refresh after a uniformly random 55-65 minutes (configurable). The worker
  that wins a Redis lease (`SET NX PX`) fetches `/models` and each model's
  `/endpoints`, unless another worker published within 90% of the minimum
  interval. The result is one canonical, size-bounded, schema-versioned
  document in one Redis hash, replaced only by a strictly later fetch. Every
  worker polls its digest each minute and installs a new document. Memory state
  uses an in-process store. Readiness does not wait for the first fetch. Until
  a document is installed, a failed refresh is retried after 30 seconds,
  doubling to 5 minutes, so a fresh deployment whose boot-time fetch fails
  gains its catalog within minutes rather than an hour. Only OpenRouter API data is stored; rules and
  endpoint configuration are applied by each worker on install.
- **Text models only.** A model whose output is not text-only (image, audio
  and music generators) is never routable, on OpenRouter or directly; models
  that merely accept media input stay. OpenRouter alias (`~`), variant (`:`)
  and variable-priced router IDs are skipped too.
- **A failed fetch never shrinks the catalog.** An endpoint lookup answered
  with 429 or 5xx is retried, at most three attempts within the fetch timeout.
  A fetch with more than 10% of endpoint lookups still failing is rejected and
  the previous document stays published. In an accepted fetch, a model whose
  lookup failed keeps its entry from the previous document. A model is removed
  only when `/models` no longer lists it or its `/endpoints` lookup succeeds
  with no usable endpoint. The document has no TTL.
- **One namespace, checked-in direct prices.** Direct OpenAI, Anthropic and
  Exa endpoints serve the same OpenRouter IDs. Their models, provider model
  IDs, limits and per-tier prices are hard-coded in a checked-in rules file
  (`golang/internal/modelsync/rules/default.yaml`) that is compiled into the
  binary and therefore ships in every image; direct routes never depend on a
  fetched price. `go run ./tools/modelsyncdefaults` regenerates the OpenAI and
  Anthropic sections from the first-party upstream endpoints OpenRouter
  publishes (its `openai`, `openai/flex`, `openai/fast` and `anthropic`
  endpoints, which carry those providers' list prices) for review. Exa, which
  OpenRouter does not list, is maintained by hand: `exa/answer` (the Answer
  API on Chat Completions, $0.005 per request) and `exa/agent` (the Agent API
  on Responses, at most $0.10 per request because the adapter always sends a
  fixed effort no higher than medium). Operators layer
  SHA-256-pinned rule files over the built-in rules at runtime to change a
  price or limit, add or exclude a model, or exclude a model everywhere.
- **Direct first.** A synced model routes to configured direct endpoints in
  order, then to OpenRouter. A configured `models` entry of the same name always
  wins, and configured price identities win over synced ones.
- **Only configured credentials are used.** An endpoint marked `optional` is
  disabled when its environment credential is unset, and model sync never
  routes to it. Only model sync may reference an optional endpoint.
- **OpenRouter selects the upstream among the allowed endpoints.** The sync
  endpoint sends `provider.require_parameters: true` and names the endpoints
  the upstream policy allows in `provider.only` (see the
  [configuration reference](../reference/configuration.md#openrouter-upstream-policy));
  callers cannot steer the upstream. Its price is the componentwise maximum
  over every allowed upstream endpoint and long-prompt override, so a
  reservation never undercounts. OpenRouter's
  reported cost (`usage.cost`) settles the call. That is only the amount
  charged to the OpenRouter account, so a BYOK response (`usage.is_byok:
  true`), whose inference the upstream provider bills to the operator's own
  key, settles at `usage.cost` plus `cost_details.upstream_inference_cost`.
  A BYOK response missing either amount settles as cost unknown, so the
  reservation is charged. Non-BYOK responses also report
  `upstream_inference_cost`, already included in `usage.cost`, so only
  `is_byok` selects the sum.
- **Direct routes stay exact.** A direct route uses its tier's checked-in
  price. A tier with a long-prompt price (OpenAI's 2x input and 1.5x output
  over 272,000 prompt tokens, Claude Haiku 5.5's 5x over 100,000) carries the
  threshold and its prices in the price entry, and the route keeps the
  provider's real context window. The threshold is exclusive, as both
  providers bill prompts over it. Settlement uses the long-prompt prices when
  the reported prompt (input, cache read and cache write) exceeds the
  threshold. Reservation uses them when the text estimate plus the uncapped
  media allowance can exceed it, as media and documents are not bounded by a
  price threshold. A price threshold beyond the first, and the long-context
  window of Anthropic models before Claude 4.6 (which needs a beta header the
  adapters never send), still cap the window.
  Anthropic cache writes use the five-minute price, the only cache the
  adapters request.
- **In-flight requests are unaffected.** An installed document produces a new
  immutable engine snapshot that keeps the configuration digest and epoch.
  Synced routes are unpinned (empty price version), so each quote binds the
  synced entry current at planning time and the durable plan persists it. The
  capability version is the endpoint profile's, so a refresh never changes a
  candidate's identity. Model features OpenRouter's `supported_parameters`
  exclude (tools, structured output, reasoning) are narrowed to unsupported in
  routing without changing that version.

## Consequences

- OpenRouter becomes the price source of OpenRouter routes for deployments
  that opt in. Direct prices change only with a reviewed rules change or an
  operator override; negotiated discounts belong in an override file.
- Media, web search and audio prices OpenRouter publishes have no catalog
  component and are not reserved; such calls rely on reported cost.
- Recovery of a request whose synced route disappeared in a refresh fails as
  for a removed configured route.
