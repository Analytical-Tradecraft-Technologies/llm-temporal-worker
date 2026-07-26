# Configuration Reference

## Format and loading

The worker reads one strict YAML document. Unknown fields, duplicate keys,
unknown enum values, unresolved references, and integer overflow are errors.
The command-line binary selects the document with the `--config` flag; the
default path is `/etc/llmtw/config.yaml`. It does not use a process-wide
environment-variable override for the configuration path or command mode. See
the [command-line reference](cli.md) for the validation and startup commands.

Secret values are referenced with typed `env`, `file`, or workload-identity
configuration. Those references are resolved by the production worker during
runtime construction, while `validate-config` and
`print-effective-config` only parse and canonicalize the non-secret document.
The effective non-secret configuration is canonicalized and hashed as
`config_version`.

## Complete shape

This example shows the v1 fields. Names and model identifiers are illustrative;
they are not claims that a particular deployment currently supports a feature.

```yaml
version: llm-temporal-worker/v1
environment: production

server:
  health_address: 0.0.0.0:8080
  metrics_address: 0.0.0.0:9090
  shutdown_timeout: 45s
  finalization_timeout: 10s
  readiness_probe_interval: 5s
  readiness_probe_timeout: 2s
  inline_payload_bytes: 524288

temporal:
  target: temporal.example.internal:7233
  namespace: production
  task_queue: llm-inference
  identity_prefix: llmtw
  tls:
    enabled: true
    server_name: temporal.example.internal
    ca_file: /var/run/ca/temporal.pem
  worker:
    max_concurrent_activities: 64
    max_concurrent_activity_task_polls: 8
    graceful_stop_timeout: 30s
    heartbeat_keepalive_interval: 1s

state:
  kind: durable
  operation_terminal_retention: 45d
  ambiguous_retention: 90d
  continuation_retention: 30d
  reservation_lease: 2m
  redis:
    addresses: [redis.example.internal:6379]
    key_prefix: llmtw
    username:
      kind: env
      name: REDIS_USERNAME
    password:
      kind: file
      path: /var/run/secrets/redis-password
    tls:
      enabled: true
      server_name: redis.example.internal
      ca_file: /var/run/ca/redis.pem
    budget_hash_tag: budget
    budget_mode: function
    function_library: llmtw_budget_v1
    budget_version: budget_v1
    budget_digest: c09e24d73750bebee4aad8cd9b1f05abaa22001528cef0ff6842f2241bb8c20b
    worker_lease_ttl: 30s
    coordination_stream_enabled: true
    stream_trim_safety: 10m
    max_connections: 96
    dial_timeout: 2s
    operation_timeout: 3s
    required_persistence: aof_and_rdb
  postgres:
    addresses: [postgres.example.internal:5432]
    database: llm_worker
    schema: llm_worker
    table_prefix: ""
    username:
      kind: env
      name: LLMTW_POSTGRES_USERNAME
    password:
      kind: file
      path: /var/run/secrets/llmtw-postgres-password
    tls:
      enabled: true
      server_name: postgres.example.internal
      ca_file: /var/run/ca/postgres.pem
    max_connections: 64
    dial_timeout: 2s
    statement_timeout: 30s
    lock_timeout: 2s

blob_store:
  kind: s3
  inline_bytes: 262144
  s3:
    bucket: acme-llmtw-production
    region: ap-southeast-2
    prefix: v1
    auth:
      kind: aws_default_chain

limits:
  request_bytes: 1048576
  items: 512
  parts_per_item: 64
  tools: 128
  schema_bytes: 262144
  json_depth: 64
  continuation_depth: 256
  route_attempts: 6
  provider_timeout: 120s
  max_output_tokens: 32768
  max_budget_buckets_per_window: 2048
  token_estimate_safety_ratio: "1.35"

endpoints:
  openai-prod:
    family: openai_responses
    base_url: https://api.openai.com/v1
    outbound_hosts: [api.openai.com]
    auth:
      kind: bearer_env
      name: OPENAI_API_KEY
    account_region: global
    timeout: 115s
    service_classes:
      economy:
        provider_value: flex
      standard:
        provider_value: default
      priority:
        provider_value: priority
    capability_profile: openai-responses-prod-v3
    price_catalog: catalog-2026-07-13
    provider_storage:
      permitted: false

  azure-openai-au:
    family: azure_openai_responses
    base_url: https://example.openai.azure.com/openai/v1
    outbound_hosts: [example.openai.azure.com]
    auth:
      kind: azure_default_credential
    account_region: australiaeast
    timeout: 115s
    service_classes:
      standard:
        provider_value: default
      priority:
        provider_value: priority
    capability_profile: azure-responses-au-v2
    price_catalog: catalog-2026-07-13
    extensions:
      azure:
        # Azure API versions are deployment-specific and must be declared.
        api_version: 2024-10-21

  azure-chat-au:
    # Azure Chat is a separate family: never configure this as generic
    # openai_chat or infer it from the Azure host name.
    family: azure_openai_chat
    base_url: https://example.openai.azure.com
    outbound_hosts: [example.openai.azure.com]
    auth:
      # Azure Chat currently accepts an API key only. Token/default-credential
      # modes fail closed until a dedicated token client is implemented.
      kind: header_env
      name: AZURE_OPENAI_API_KEY
    account_region: australiaeast
    timeout: 115s
    service_classes:
      standard:
        provider_value: default
      priority:
        provider_value: priority
    capability_profile: azure-chat-au-v1
    price_catalog: catalog-2026-07-13
    extensions:
      azure:
        # Both values are deployment-specific and required before auth lookup.
        api_version: 2025-01-01
        deployment: gpt-example-chat-deployment

  openrouter-pinned:
    family: openai_chat
    base_url: https://openrouter.ai/api/v1
    outbound_hosts: [openrouter.ai]
    auth:
      kind: bearer_env
      name: OPENROUTER_API_KEY
    account_region: global
    timeout: 115s
    service_classes:
      standard:
        provider_value: standard
    capability_profile: openrouter-chat-pinned-v2
    price_catalog: catalog-2026-07-13
    extensions:
      openrouter:
        provider_order: [ProviderA]
        allow_fallbacks: false
        require_parameters: true

  exa-answer:
    family: openai_chat
    base_url: https://api.exa.ai
    outbound_hosts: [api.exa.ai]
    auth:
      kind: bearer_env
      name: EXA_API_KEY
    account_region: global
    timeout: 115s
    service_classes:
      standard:
        provider_value: standard
    capability_profile: exa-chat-v1
    price_catalog: catalog-2026-07-13
    extensions:
      # The marker selects Exa's profile and wire contract explicitly.
      exa: {}

  anthropic-direct:
    family: anthropic_messages
    base_url: https://api.anthropic.com
    outbound_hosts: [api.anthropic.com]
    auth:
      kind: header_env
      name: ANTHROPIC_API_KEY
    account_region: global
    timeout: 115s
    service_classes:
      standard:
        provider_value: standard_only
      priority:
        provider_value: auto
        requires_capability: priority_capacity
    capability_profile: anthropic-messages-v4
    price_catalog: catalog-2026-07-13

  anthropic-aws-us-east-1:
    # This is Anthropic's AWS gateway, not Amazon Bedrock. Keep both endpoint
    # families and their pinned continuations separate.
    family: anthropic_aws_messages
    base_url: https://aws-external-anthropic.us-east-1.api.aws
    outbound_hosts: [aws-external-anthropic.us-east-1.api.aws]
    region: us-east-1
    aws_workspace_id: ws-example-123
    auth:
      kind: aws_default_chain
    timeout: 115s
    service_classes:
      standard:
        provider_value: standard_only
      priority:
        provider_value: auto
        requires_capability: priority_capacity
    capability_profile: anthropic-aws-us-east-1-v1
    price_catalog: catalog-2026-07-13
    provider_storage:
      permitted: false

  bedrock-us-east-1:
    family: bedrock_anthropic_messages
    outbound_hosts: [bedrock-runtime.us-east-1.amazonaws.com]
    region: us-east-1
    auth:
      kind: aws_default_chain
    timeout: 115s
    service_classes:
      economy:
        provider_value: flex
      standard:
        provider_value: default
      priority:
        provider_value: priority
    capability_profile: bedrock-anthropic-v2
    price_catalog: catalog-2026-07-13

models:
  invoice-summarizer:
    allowed_tenants: [acme]
    data_regions: [global, australiaeast]
    routes:
      - id: openai-primary
        endpoint: openai-prod
        model: gpt-example-2026-07-01
        classes: [economy, standard, priority]
      - id: azure-secondary
        endpoint: azure-openai-au
        model: gpt-example-deployment
        classes: [standard, priority]
      - id: anthropic-secondary
        endpoint: anthropic-direct
        model: claude-example-2026-06-01
        classes: [standard, priority]
      - id: anthropic-aws-secondary
        endpoint: anthropic-aws-us-east-1
        model: claude-example-2026-06-01
        classes: [standard, priority]
      - id: bedrock-economy
        endpoint: bedrock-us-east-1
        model: anthropic.claude-example-v1
        classes: [economy, standard, priority]

capabilities:
  catalogs:
    - file: /etc/llmtw/capabilities.yaml
      sha256: 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
  unknown_in_strict_mode: reject

pricing:
  catalogs:
    - file: /etc/llmtw/prices.yaml
      sha256: abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789
  require_price_when_budgeted: true

budgets:
  require_match: true
  policies:
    - id: acme-production
      match:
        tenant: acme
        environment: production
      windows:
        - duration: 1h
          bucket: 1m
          limit_usd: "25.000000000000000000"
        - duration: 24h
          bucket: 5m
          limit_usd: "250.000000000000000000"
        - duration: 30d
          bucket: 1h
          limit_usd: "3000.000000000000000000"

continuation:
  handle_keys:
    - id: key-2026-07
      primary: true
      secret:
        kind: file
        path: /var/run/secrets/continuation-hmac
  retain_canonical_transcript: true
  allow_provider_hosted_state: false

telemetry:
  logs:
    format: json
    level: info
  metrics:
    enabled: true
  tracing:
    enabled: true
    otlp_endpoint: otel-collector.observability:4317
    sample_ratio: "0.05"
  content_logging: disabled
```

`temporal.worker.heartbeat_keepalive_interval` controls the fixed, redacted
heartbeat emitted while a one-shot provider call is in flight. It defaults to
`1s` when omitted and is not derived from a provider SDK timeout. Every
workflow Activity policy using this worker must use the same cadence and a
`heartbeat_timeout` of at least three times that cadence.

When `telemetry.tracing.enabled` is true, `otlp_endpoint` names the OTLP/gRPC
collector and `sample_ratio` is a decimal from `0` through `1`. Runtime uses
the secure OTLP transport default; deploy a collector endpoint with TLS. The
tracer exports only bounded lifecycle metadata and hashes tenant identifiers;
it never exports request content, provider payloads, continuation handles, or
resolved credentials.

## State namespace selection

**state.kind** is **durable** or the legacy development-only **redis** fixture.
Durable is the production default
and requires both **state.redis** and **state.postgres**: PostgreSQL is the
system of record, while Redis provides the active budget/throttle materialization
and cross-worker coordination optimization.

In durable mode the runtime constructs and probes both stores before admitting
work. The PostgreSQL dependency probe checks the current database, UTC session
timezone, and the installed schema contract for the configured namespace; the
Redis probe performs its normal connectivity, clock, policy, and configured
Function or Lua script identity checks. Production additionally reads the
configured Redis keyspace's active budget-generation pointer and canonical,
complete manifest. A missing or invalid pointer/manifest keeps readiness closed; the
probe is read-only and never publishes or rebuilds a generation. The validated
`state.redis.key_prefix` is applied to every worker-owned key constructor and
the active-generation read therefore also proves the configured namespace is
being addressed. A failure or timeout keeps readiness closed, and both clients
are closed during a failed build, configuration replacement, or worker
shutdown. Schema installation remains a deployment concern (`postgres.Install`)
and is never performed by a worker during readiness probing.

`state.kind: redis` is retained only as a development/test fixture for the
legacy Redis-only composition and is rejected in production. `state.kind:
memory` is an explicitly non-durable, single-process development composition.
It is rejected in production and must use `blob_store.kind: memory`:

~~~yaml
environment: development
state:
  kind: memory
  operation_terminal_retention: 45d
  ambiguous_retention: 90d
  continuation_retention: 30d
  reservation_lease: 2m
blob_store:
  kind: memory
  inline_bytes: 262144
~~~

Memory mode is constructed by the production factory without dialing Redis or
PostgreSQL and without creating an external blob client. It uses the shared
in-process admission and continuation implementations plus a bounded,
content-addressed process-local blob map. Provider adapters are still built
normally, so provider credentials and egress remain subject to the same
validation and authorization rules as durable mode.

Operations, checkpoints, budget/throttle state, and blobs are process local;
restart loses everything and provider-pending jobs cannot be recovered after
process loss. The mode must not be used for durable continuation/recovery
guarantees, multi-replica admission, backups, or production readiness. Redis
and PostgreSQL addresses and credentials are ignored by the memory factory and
should be omitted. `blob_store.kind: memory` is rejected unless
`state.kind: memory` is also selected, and external blob kinds are rejected
with memory state.

**state.redis.key_prefix** has a
default of **llmtw**. The optional **LLMTW_REDIS_KEY_PREFIX** environment
variable overrides only that field. The same validated prefix is injected into
every worker-owned Redis key constructor; the runtime factory must not hardcode
**llmtw**. It is distinct from **budget_hash_tag**, which controls Redis
Cluster co-location rather than the outer data namespace.

When Redis is shared, grant the worker role the configured key pattern
**~<key-prefix>:*** (for example **~llmtw:***), plus the reviewed command set.
The prefix is namespace isolation only; it is not a tenant boundary or an
authentication mechanism, and the Redis Function library name remains global.
The effective prefix is immutable for the lifetime of a worker process: a
configuration reload that changes it is rejected before new clients are built.
Deploy a new worker process when moving to a different Redis namespace.

The required PostgreSQL subsection has this shape:

~~~yaml
state:
  postgres:
    addresses: [postgres.example.internal:5432]
    database: llm_worker
    schema: llm_worker
    table_prefix: ""
    username:
      kind: env
      name: LLMTW_POSTGRES_USERNAME
    password:
      kind: file
      path: /var/run/secrets/llmtw-postgres-password
    tls:
      enabled: true
      server_name: postgres.example.internal
      ca_file: /var/run/ca/postgres.pem
    min_connections: 8
    max_connections: 64
    dial_timeout: 2s
    statement_timeout: 30s
    lock_timeout: 2s
    idle_transaction_timeout: 30s
~~~

The database, schema, and relation prefix are independent:

| Field | Default | Environment override | Meaning |
| --- | --- | --- | --- |
| **database** | **llm_worker** | **LLMTW_POSTGRES_DATABASE** | PostgreSQL database selected when opening the pool |
| **schema** | **llm_worker** | **LLMTW_POSTGRES_SCHEMA** | Schema containing worker-owned objects |
| **table_prefix** | empty | **LLMTW_POSTGRES_TABLE_PREFIX** | Prefix applied to every worker-owned schema object |

`min_connections` and `max_connections` bound each worker replica's pgx pool;
`min_connections` may be zero so startup does not wait for a warm pool. The
worker still requires the PostgreSQL transaction/schema contract probe before
it polls new work. `idle_transaction_timeout` is applied to every PostgreSQL
session independently of `statement_timeout` and `lock_timeout`; it bounds
sessions accidentally left inside a transaction and is never used as a
readiness shortcut. All three timeout values must be positive and the minimum
pool size cannot exceed the maximum.

Environment overrides are read once before strict validation, appear in
**print-effective-config**, and are included in **config_version**. They are
not secret and are not hot-reloaded. Database and schema match
**[a-z][a-z0-9_]{0,62}**. Table prefix is empty or matches
**[a-z][a-z0-9_]{0,22}_**; the trailing underscore makes physical names
unambiguous and the 24-byte maximum leaves room for the longest specified index
name under PostgreSQL's 63-byte identifier limit. Schema-contract tests reject
any generated name that would be truncated. Constraints use the architecture's
readable deterministic
`<prefix>c_<kind>_<table_abbrev>_<invariant_slug>` names rather than hashes or
PostgreSQL-generated names.

The recommended production layout is a dedicated database and schema with an
empty table prefix. Sharing a PostgreSQL server is fully supported. Sharing a
database is supported with a dedicated schema. A non-empty table prefix also
permits an explicitly shared schema, but it is collision avoidance rather than
a privilege boundary; dedicated roles/schema remain preferable. The worker
must never resolve a physical relation to one owned by Temporal.

These namespace choices only select where a clean initial schema is created.
This unreleased change must not implement or document copying, backfill,
dual-read, dual-write, legacy namespace fallback, or renaming between namespace
choices. A post-release namespace change requires a separate migration design.

The schema foundation lives in
[`golang/storage/postgres`](../../golang/storage/postgres/namespace.go). The
checked-in migration template is rendered only after namespace validation and
uses schema-qualified `pgx.Identifier` values; startup verification is
read-only, while `Install` is reserved for an explicit provisioning step.
Run `make postgres-integration` from `golang/` to exercise the namespace and
contract gates against the pinned PostgreSQL service image.

## Pricing and budget matching

`pricing.require_price_when_budgeted` controls the explicit unpriced policy.
When it is `true`, a route without a current catalog quote is eligible only if
it matches no monetary budget policy. A matching policy always requires a
current quote. When it is `false`, all candidates require a current quote.

`budgets.require_match: true` is independent: it removes a candidate with no
matching budget policy before quote selection. Consequently, setting both
fields to `true` requires every dispatched candidate to match a budget and to
have a current price. An intentionally allowed unpriced result has
`cost_status: unknown`; its zero cost fields are unknown accounting facts, not
a free-use assertion. This describes the current pre-release implementation
only. The accepted Phase A PostgreSQL contract replaces the zero sentinel with
nullable price/actual-cost fields plus an explicit unknown reason; exact zero
then means confirmed free.

## Provider egress policy

Every endpoint requires a non-empty `outbound_hosts` list. Entries are DNS
hostnames, not URLs or IP literals: they are normalized to lowercase ASCII
without a trailing dot, and duplicates are rejected. The hostname in a
non-Bedrock `base_url` must be present in that list. Bedrock endpoints with no
`base_url` must instead name the exact regional runtime hostname that the SDK
will use, such as `bedrock-runtime.us-east-1.amazonaws.com`.

`anthropic_aws_messages` is Anthropic's AWS gateway, not Amazon Bedrock. It
requires an explicit HTTPS `base_url`, `region`, `aws_workspace_id`, and
`auth.kind: aws_default_chain`. The production constructor rejects API keys,
static AWS credentials, named AWS profiles, auth-skipping mode, and
`ANTHROPIC_AWS_API_KEY`; that preserves the configured default AWS credential
chain instead of silently changing authentication. Its catalog family and
pinned continuation endpoint remain distinct from Bedrock.

At runtime the provider client permits HTTPS requests only to those configured
hostnames and the configured HTTPS port for the endpoint. A base URL hostname
is permitted only on its explicit base URL port (or 443 when no port is given);
additional outbound hostnames, if used by an SDK, are limited to 443. It
resolves the host for every new connection, rejects a DNS answer containing
loopback, private, link-local, multicast, unspecified, carrier-grade NAT,
benchmarking, or cloud metadata addresses, then dials the validated address
directly and checks the connected peer address again. A request-time URL cannot
broaden the endpoint policy merely by naming a different host.

Provider clients do not use environment proxy settings and never follow
redirects automatically. No v1 endpoint is documented as redirecting. Adding
one requires an explicit reviewed policy that validates every redirect hop.
The endpoint timeout bounds the full response read; connection and TLS
handshake phases are independently bounded to at most 10 seconds. Transport
failures are emitted as a safe endpoint-scoped classification, never as a URL,
credential, authorization header, request body, continuation value, or raw
provider response.

## Readiness and Redis budget policy

`server.readiness_probe_interval` and
`server.readiness_probe_timeout` are required positive durations; the timeout
cannot exceed the interval. The worker checks its required state dependencies
at initial construction, before a reload is published, and periodically while
running. A failed check makes `/health/ready` return `503` and stops Temporal
polling, while `/health/live` remains available for the process supervisor.
Polling resumes only after every required dependency passes again. During a
transient worker drain the monitor keeps checking dependencies, but it never
starts a replacement poller until the previous poller has fully stopped.

Readiness checks Redis with `PING`, `TIME`, the configured persistence and
`noeviction` policy, and configured budget code identity. It does not yet
inspect the worker keyspace or verify the active-generation manifest,
coverage, or enabled Stream's structural health; the storage validator and
recovery runbook cover those records until an authoritative read-only readiness
contract is added. It also checks a bounded PostgreSQL read-only transaction,
the configured physical namespace/schema contract, every relation and both
explicit and constraint-backed indexes declared by that contract, and UTC
session state. Runtime role grants remain
deployment-owned and are exercised by the worker's normal least-privilege
operations. It checks
the configured S3 bucket with bucket metadata only; it never reads or writes a
tenant object. Provider endpoints are intentionally excluded because one route
can be unavailable while another eligible route remains.

`state.redis.budget_mode: function` is the preferred Redis 7+ path. Before
starting a worker, deployment automation must provision the exact versioned
Function library and set `function_library`, `budget_version`, and
`budget_digest` to its immutable identity. The running worker only verifies
and calls that Function; it never loads, replaces, or rewrites shared Redis
code. `budget_mode: lua` is an explicit compatibility fallback: its
`budget_digest` must be the SHA-256 of the preloaded Lua source, and
readiness requires Redis `SCRIPT EXISTS` for that source. The worker never
falls back from a missing Lua script to `EVAL` or `SCRIPT LOAD`.

`required_persistence` selects the deployment policy: `aof_and_rdb` requires
both AOF and a non-empty RDB save policy, while `aof` and `rdb` require only
their named mechanism. Any mismatch fails readiness closed.

`coordination_stream_enabled` defaults to `true` in durable deployments and
keeps cross-replica invalidation/generation-switch wake-ups fast. It does not
change authority: a Stream gap or an explicitly disabled tailer discards local
hints and reloads the manifest/policy state directly from Redis. Readiness
does not yet validate the Stream key/type/retention policy; an authoritative
read-only readiness contract must add that check. A recoverable cursor gap does
not invalidate an otherwise complete budget generation once the storage and
recovery validators have proved it.

The active-generation manifest is a bounded `budget-manifest/v1` value. Its
immutable generation and Redis-incarnation IDs, configuration/price versions,
policy/window hashes, journal and Stream high-water marks, coverage bounds,
rounding version, member-count catalog digest, and complete policy/window
member set are validated before adoption. Every member must cover the same
positive horizon, have matching provenance, and account for its complete
bucket count; duplicate or missing members, non-concrete Stream IDs, digest
mismatches, and oversized values fail closed. The current Go validator is
storage-neutral and does not itself publish a Redis pointer or run an atomic
budget Function; deployment wiring must still perform those operations under
the recovery procedure below.

Workers keep leases and broadcast cursors in the configured Redis budget
namespace. Joining an existing live lease set, restarting one worker, handling
a Stream gap, checking readiness, and serving `budget_status` read budget state
only from Redis. The service may read PostgreSQL budget tables only when the
Redis lease set proves a zero-live-worker cold bootstrap or the Redis generation
is missing/incomplete under a verified new Redis process/dataset incarnation
after persistence loss. Same-incarnation partial corruption fails closed and
does not read PostgreSQL. There is no config switch
that enables a routine PostgreSQL fallback.

## Service-class rules

The request enum remains exactly `economy`, `standard`, and `priority`.
Configuration may omit unsupported entries for an endpoint; it cannot define a
fourth public class. A mapping's `provider_value` is adapter-profile validated
and cannot be supplied by a request.

A request without `service_class` becomes `standard`. There is no configurable
provider default. `service_class_fallbacks` is request data, not a worker-wide
default, because only the caller can authorize a cost/latency class change.

## Capability catalog shape

Each entry binds claims to an exact profile/model matcher:

```yaml
version: llmtw-capabilities/v1
entries:
  - id: openai-responses-prod-v3
    family: openai_responses
    model:
      exact: gpt-example-2026-07-01
    verified_at: 2026-07-13T00:00:00Z
    features:
      input.text: {level: native}
      input.image: {level: native, max_bytes: 20971520}
      tools.auto: {level: native}
      tools.required: {level: native}
      tools.parallel: {level: native}
      output.json_schema: {level: native, dialect: draft-2020-12-subset}
      continuation.response_id: {level: native, pinned: true}
      stream.typed_usage: {level: native}
      service.economy: {level: native}
      service.standard: {level: native}
      service.priority: {level: native}
    limits:
      context_tokens: 400000
      output_tokens: 32768
```

Every `emulated` capability names a transform ID that has unit/golden tests.
Catalog compilation rejects duplicate or overlapping matchers with different
claims.

## Budget policy matching

`budgets.require_match: true` is an admission-policy switch, not a provider
default. Before pricing, admission, or a provider request, the worker evaluates
every authorized route candidate, including explicit service-class fallbacks.
Candidates that match no budget policy are excluded. If none remains, the
request terminates as `no_route`; it creates no admission operation and sends
no provider request. Set `require_match: false` only when an unmatched route is
intentionally allowed to proceed without a monetary budget reservation.

Each policy `match` must name at least one restriction. The supported keys are
`tenant`, `project`, `actor_prefix`, `environment`, `logical_model`, `endpoint`,
and `service_class`. All populated keys must match the request and candidate;
`service_class` is limited to the public `economy`, `standard`, and `priority`
enum. `*` is an exact-field wildcard, not a restriction, so a policy with only
wildcards is rejected. A missing request or candidate fact cannot satisfy an
exact or prefix restriction.

## Price catalog shape

```yaml
version: llmtw-prices/v1
id: catalog-2026-07-13
entries:
  - endpoint_family: openai_responses
    model: gpt-example-2026-07-01
    provider_tier: default
    effective_from: 2026-07-13T00:00:00Z
    input_per_million: "1.250000"
    output_per_million: "10.000000"
    cache_read_per_million: "0.125000"
    source: operator-verified
```

Prices in examples are illustrative. Production catalogs require provenance and
review; they never refresh silently from an untrusted endpoint.
Every decimal property is defined as USD by its field name and catalog
contract; no generic currency discriminator is accepted or reported.

## Validation

`validate-config` performs the local checks in `config.Load` without starting a
worker:

- schema version, unknown/duplicate fields, documented defaults, duration and
  URL syntax, normalized provider host policy, and base URL membership;
- secret references are structurally valid, without reading their values;
- Temporal, state, blob, endpoint, and provider timeout bounds are valid;
- routes reference declared endpoints and only use the three public service
  classes mapped by those endpoints;
- budget windows, continuation keys, Redis numeric bounds, and retention
  inequalities are safe;
- telemetry settings obey their environment and content-logging rules.

It does not read catalog files or compare their digests, resolve environment or
file secret contents, construct provider/Temporal/Redis/S3 clients, inspect
Redis hash-slot or Function state, verify SDK retry options, or validate
Kubernetes deployment settings. The production `worker` command performs the
reference and client-construction checks during runtime composition; catalog
and deployment verification are separate gates.

Reload performs the same checks and publishes only a complete valid snapshot.
