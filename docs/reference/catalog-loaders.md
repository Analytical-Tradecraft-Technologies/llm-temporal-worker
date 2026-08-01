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

The production shape is a versioned `entries` list:

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
The loader converts claims to `provider.CapabilitySet`; `reference` is
validated as a known catalog claim but is not emitted because the provider
port has no external-reference feature. An `emulated` claim must name a
transform. Family aliases used by config (`azure_openai_responses` and
`bedrock_anthropic_messages`; `bedrock_converse` maps to the dedicated Bedrock
Converse provider family) are normalized to their provider family.

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

Final usage reconciliation also computes the bounded Redis `MicroUSD`
compatibility projection. If any component or the checked aggregate exceeds
that safe integer range, `pricing.CostFromUsage` returns an error instead of
dropping the component or returning an understated compatibility amount; the
exact USD result is not treated as sufficient to bypass this boundary.

Each entry also retains its optional `source`/`provenance` audit linkage. When
both are supplied they must agree, and the linkage contributes to the compiled
catalog digest. Effective intervals allow a new price source to replace an
older one without rewriting history; resolution at the boundary selects the
new interval and preserves the source identity in the quote. A missing source
file, digest mismatch, or invalid replacement rejects the complete candidate
catalog. Reload publication is atomic, so the last verified snapshot remains
active during a source outage rather than exposing a partial or guessed price.
The `pricing.PriceResolver` applies the same boundary to in-process reloads:
`ReloadValidated` checks the compiled digest before publishing, and a failed
replacement leaves the previous snapshot in place. The compatibility `Reload`
method is also fail-closed when its caller does not need the returned error.

## PostgreSQL catalog snapshots

`storage/postgres.PricingCatalogRepository` is the maintenance/control-plane
writer and runtime snapshot reader for the `price_catalogs` and
`price_entries` tables. `Store` validates the compiled digest again, requires
both source and compiled SHA-256 digests, and inserts the catalog and every
entry in one synchronous transaction. Repeating the same version and digests
is idempotent; reusing a version or compiled digest for different content is a
hard error. A successful newer snapshot retires older active snapshots in the
same transaction, so readers never observe a half-published catalog.

The existing projection is intentionally strict about digest round-tripping:
persisted entries must have a non-zero `effective_from`, no prose `provenance`,
and an entry `version` equal to the catalog version. Catalogs carrying
unrepresentable metadata are rejected instead of silently changing their
compiled digest. A future-dated snapshot schedules predecessor retirement and
`LoadActive` continues returning the predecessor until the replacement's
effective time.

The PostgreSQL projection is intentionally USD-only. Decimal values are bound
as exact text to `NUMERIC(38,18)`; an explicitly quoted zero is stored as zero,
while an omitted component is stored as `NULL` and listed in
`unknown_component_codes`. `price_status` is therefore `exact`, `partial`, or
`unknown` and the runtime cannot reinterpret `NULL` as free. The source
document's prose provenance is represented by the source digest; the compiled
digest remains authoritative when a snapshot is loaded.
