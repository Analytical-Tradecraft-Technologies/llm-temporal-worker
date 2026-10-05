# Catalog loader contract

`golang/internal/catalog` is the only package that turns operator-supplied capability
and price YAML into runtime snapshots. It is deliberately separate from the
main configuration decoder so a reload can authenticate and compile every
referenced file before publishing one new snapshot.

## File integrity and resource limits

Each `config.CatalogRef` supplies an absolute `file` path and a lowercase or
uppercase SHA-256 digest. The loader:

1. rejects relative paths and malformed digests;
2. checks the file size before reading and reads at most 4 MiB (a lower bound
   may be selected with `Load*WithOptions`);
3. computes SHA-256 over the exact bytes on disk and compares it in constant
   time; and
4. decodes only after the digest matches.

An empty document, a second YAML document, duplicate mapping key, unknown
field, malformed scalar, or duplicate catalog identity is a hard error. A
failed load never returns a partial catalog.

## Capability documents

The production shape is a versioned `entries` list. Per-feature `max_bytes`
is not supported: the strict decoder rejects it, including on reference or
service-class claims, instead of accepting an unenforced limit. This is separate
from the catalog file-size bound above.

The entry shape is:

```yaml
version: llmtw-capabilities/v1
entries:
  - id: openai-prod
    family: openai_responses
    model: {exact: gpt-example}
    verified_at: 2026-07-13T00:00:00Z
    features:
      input.text: {level: native}
      tools.auto: {level: native}
      output.json_schema: {level: emulated, transform: json-schema-tool}
    limits:
      context_tokens: 400000
      output_tokens: 32768
```

The local fixture's `profiles` map is also supported. Its `input`, `output`,
and `service_classes` lists are validated against the same closed vocabulary.
The loader retains declared service classes on the compiled capability profile.
For the versioned `entries` shape, `service.economy`, `service.standard`, and
`service.priority` claims are retained in the same field; an `unsupported`
claim is not considered available. An omitted service-class declaration is
distinguished from an explicitly empty declaration for compatibility with
older catalogs. `reference` is
validated as a known catalog claim but is not emitted because the provider
port has no external-reference feature. An `emulated` claim must name a
transform. Family aliases used by config (`azure_openai_responses` and
`bedrock_anthropic_messages`; `bedrock_converse` maps to the dedicated Bedrock
Converse provider family) are normalized to their provider family.

The model's `limits.output_tokens` (or `profiles.*.max_output_tokens`) is retained
as a route ceiling. Routing excludes a model when the effective request output
cap exceeds that ceiling; it can select another authorized route with a larger
allowance. It never silently lowers the caller's cap. A positive ceiling also
requires an explicit positive request cap; budgeted runtime preparation supplies
the configured default when the caller omits it. Zero in the catalog means no
declared model ceiling. Per-feature byte limits are unsupported and rejected.

Model `limits.context_tokens` (or `profiles.*.max_context_tokens`) is retained
through routing and checked before new-request compilation or budget acquisition.
Admission reserves the estimated input count, effective output cap, and the larger
of the requested and configured reasoning budgets. A candidate that does not fit
is skipped in favor of another authorized route; if none fits, admission fails
without dispatch. Free and unpriced routes are checked too. Compaction preflight
uses the same accounting to trigger compaction for an existing conversation.
Previously submitted provider work can still be polled after a catalog change.

Context checks use the configured deterministic tokenizer when supplied. Otherwise,
they use the existing approximate UTF-8 input estimate used by budget reservation;
this is not an exact provider token count or a guarantee of provider acceptance.
Zero means no declared model limit. Separately bounded request sizes still apply.

During runtime snapshot compilation, Bedrock Messages and Bedrock Converse
routes are checked against an explicitly declared model service-class set. A
route that advertises an undeclared class is rejected before any provider
adapter can dispatch it. This keeps model-specific tier availability (for
example, a Claude or Nova model that does not support Priority) fail-closed at
the shared catalog boundary rather than relying on an adapter to reject it
later.

An endpoint reused by multiple model routes must also carry one identical
capability declaration at the compiled version. A state, transform, or reason
that differs between routes is rejected during runtime composition instead of
letting map iteration choose an arbitrary adapter profile at worker startup.

## Price documents

Price documents contain a version, immutable `id`, and USD-denominated entries.
USD is the only supported denomination in this release and is encoded by the
field names and `pricing.CompileUSD`; a generic `currency` field, caller-owned
rate, or foreign-currency amount is not accepted. Strict YAML decoding rejects
an attempted `currency` field instead of silently treating it as USD. Because
USD is implicit, `currency: USD` is rejected just like any other currency
value; omit the field. The canonical entry identity is provider, endpoint ID,
endpoint family, region, model, provider tier, and effective start time.
Prices are quoted decimal strings and are compiled by `pricing.CompileUSD`;
floating-point values are not accepted. The local fixture's `endpoint` and
`service_class` aliases are
accepted, but `service_class` is still exactly one of `economy`, `standard`, or
`priority`. There is no `provider_default` class.

Compilation normalizes insignificant leading and trailing zeroes in each
decimal price before JSON canonicalization. Equivalent values such as `1.23`
and `001.2300` therefore produce the same catalog digest; values outside the
`NUMERIC(38,18)` range or with more than 18 fractional digits are rejected.

```yaml
version: llmtw-prices/v1
id: catalog-2026-07-13
entries:
  - provider: openai
    endpoint_id: openai-production
    endpoint_family: openai_responses
    region: global
    model: gpt-example
    provider_tier: standard
    input_per_million: "1.250000"
    output_per_million: "10.000000"
    cache_read_per_million: "0.125000"
    cache_write_per_million: "0"
    reasoning_per_million: "0"
    per_request: "0"
    source: operator-verified
```

`Load` resolves every capability and pricing reference in `config.Config` and
fails if an endpoint names a missing profile/catalog, if its family does not
match the profile, or if its price catalog has no entry for that endpoint and
family. It also rejects duplicate profile IDs and duplicate price-catalog IDs
across files. Model-specific price selection remains a routing concern.

All decimal price properties are known by contract to be USD. A non-USD source
is rejected at the strict catalog boundary; there is no FX adapter, rate
schema, or caller-supplied conversion. Neither configuration nor downstream
Go/JSON/OCaml records carry a currency discriminator. This is a pre-release
replacement, so old integer-microUSD and generic-currency response fields are
rejected rather than dual-read or converted. A future concrete non-USD provider
requires a superseding ADR defining worker-owned rate retrieval, exact
conversion, staleness, failure, and audit behavior; that provider will still
persist and report only USD after the ADR is implemented.

An omitted component is not a zero-dollar quote. The loader retains the
component as `unknown` on the compiled pricing entry (the zero value remains
reserved for an explicitly quoted free component). `pricing.CostFromUsage` and
the budget estimator fail closed when a request needs an unknown component, so
partial catalogs cannot silently undercharge.

Every reservation estimate charges `cache_write_per_million` (the whole input
may be written to cache) and one `per_request` unit, and reasoning-enabled
requests also charge `reasoning_per_million`. An entry that omits
`cache_write_per_million` or `per_request` therefore loads but can never be
selected: every request on that route is skipped as an unusable quote and fails
with `no_route` when no other candidate remains. Write an explicit `"0"` for a
component the provider does not bill.

Final usage reconciliation also computes the bounded Redis `MicroUSD`
compatibility projection. If any component or the checked aggregate exceeds
that safe integer range, `pricing.CostFromUsage` returns an error instead of
dropping the component or returning an understated compatibility amount; the
exact USD result is not treated as sufficient to bypass this boundary.

Each entry also retains its optional `source`/`provenance` audit linkage. When
both are supplied they must agree, and the linkage contributes to the compiled
catalog digest. Effective intervals allow a new price source to replace an
older one without rewriting history; resolution at the boundary selects the
new interval and preserves the source identity in the quote. The replacement
entry may carry a different `version`: the route then binds its price version
at quote time, so the boundary takes effect without a reload. A missing source
file, digest mismatch, or invalid replacement rejects the complete candidate
catalog. Reload publication is atomic, so the last verified snapshot remains
active during a source outage rather than exposing a partial or guessed price.
The `pricing.PriceResolver` applies the same boundary to in-process reloads:
`ReloadValidated` checks the compiled digest before publishing, and a failed
replacement leaves the previous snapshot in place. The compatibility `Reload`
method is also fail-closed when its caller does not need the returned error.

## Catalog persistence

The SQL catalog repository has been removed. The runtime loads and verifies the
configured catalog files into an immutable snapshot using the contracts above.
No worker database, SQL catalog tables or catalog migration is required.

The exact `pricing.USD` representation remains independent of storage: explicit
zero is distinct from an omitted price, and missing prices never authorize free
paid work. A future catalog store must preserve the compiled/source digest and
snapshot checks rather than silently reinterpret unknown prices.
