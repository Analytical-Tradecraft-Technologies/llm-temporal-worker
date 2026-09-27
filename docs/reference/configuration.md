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
The resource-capacity artifact identifiers and hashes are illustrative too:
replace them with the signed operator-owned manifest and trust-root identities
before startup. Production verifies those files; passing the strict loader does
not authenticate example artifacts.

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
  target: temporal-frontend.temporal.svc.cluster.local:7233
  namespace: ai-ach
  task_queue: llm-inference
  identity_prefix: llmtw
  tls:
    enabled: true
    server_name: temporal-frontend.temporal.svc.cluster.local
    ca_file: /var/run/ca/temporal.pem
  api_key_file: /var/run/secrets/llmtw/temporal-jwt
  worker:
    max_concurrent_activities: 96
    max_concurrent_activity_task_polls: 8
    graceful_stop_timeout: 30s
    heartbeat_keepalive_interval: 1s

resource_capacity:
  manifest_file: /var/run/provenance/resource-capacity.json
  trust_root_file: /var/run/provenance/release-trust-root.json
  trust_root_sha256: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
  manifest_sha256: bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
  artifact_id: llm-resource-capacity-2026-08-10
  artifact_locator: s3://forecast-artifacts/competition/resource-capacity.json
  generation_id: forecast-capacity-2026-08-10
  limits:
    llm_global_max_inflight: 64
    llm_global_max_requests_per_window: 600
    llm_global_window_seconds: 60
    provider_max_inflight:
      - {route_id: anthropic-aws-secondary, max_inflight: 16, max_requests_per_window: 100, window_seconds: 60}
      - {route_id: anthropic-secondary, max_inflight: 16, max_requests_per_window: 100, window_seconds: 60}
      - {route_id: azure-secondary, max_inflight: 16, max_requests_per_window: 100, window_seconds: 60}
      - {route_id: bedrock-economy, max_inflight: 16, max_requests_per_window: 100, window_seconds: 60}
      - {route_id: openai-primary, max_inflight: 16, max_requests_per_window: 100, window_seconds: 60}
    search_fetch_max_inflight: 64
    python_stage2_max_inflight: 6
    forecast_event_max_inflight: 2
    forecast_event_admission_wait_seconds: 5
    provider_call_execution_overhead_seconds: 30
    provider_call_queue_seconds: 30
    provider_call_maximum_attempts: 1
    stage_two_execution_overhead_seconds: 30
    stage_two_queue_seconds: 30
    stage_two_semaphore_wait_seconds: 30
    stage_two_maximum_attempts: 2
    close_execution_overhead_seconds: 30
    close_queue_seconds: 30
    close_maximum_attempts: 2
    evidence_collection_execution_overhead_seconds: 30
    evidence_collection_queue_seconds: 30
    evidence_collection_maximum_attempts: 1
    panel_prepare_max_inflight: 8
    panel_llm_max_inflight: 8
    inference_artifactize_execution_seconds: 5
    inference_artifactize_queue_seconds: 5
    inference_artifactize_maximum_attempts: 2
    prepare_assessment_execution_seconds: 5
    prepare_assessment_queue_seconds: 5
    prepare_assessment_maximum_attempts: 2
    build_analysis_panel_execution_seconds: 10
    build_analysis_panel_queue_seconds: 5
    build_analysis_panel_maximum_attempts: 2
    assemble_evidence_assessment_execution_seconds: 10
    assemble_evidence_assessment_queue_seconds: 5
    assemble_evidence_assessment_maximum_attempts: 2
    load_analytic_context_execution_seconds: 10
    load_analytic_context_queue_seconds: 5
    load_analytic_context_maximum_attempts: 2
    checkpoint_analysis_frontier_execution_seconds: 10
    checkpoint_analysis_frontier_queue_seconds: 5
    checkpoint_analysis_frontier_maximum_attempts: 2
    load_analysis_frontier_execution_seconds: 10
    load_analysis_frontier_queue_seconds: 5
    load_analysis_frontier_maximum_attempts: 2
    caller_owned_evidence_ingest_execution_seconds: 5
    caller_owned_evidence_ingest_queue_seconds: 5
    caller_owned_evidence_ingest_maximum_attempts: 2
    panel_prepare_activity_max_bytes: 1310720

state:
  kind: durable
  operation_terminal_retention: 45d
  ambiguous_retention: 90d
  continuation_retention: 30d
  reservation_lease: 20m
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
    admission_hash_tag: admission
    admission_mode: function
    function_library: llmtw_admission_v1
    admission_version: admission_v1
    admission_digest: 52e2bc632d330b926b817f3d10f08bb616847c02d1acdf709c70468ce06d41c1
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
    envelope_keys:
      - id: envelope-v1
        primary: true
        secret:
          kind: file
          path: /var/run/secrets/postgres-envelope-key
    scope_keys:
      - id: scope-v1
        primary: true
        secret:
          kind: file
          path: /var/run/secrets/postgres-scope-key
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

blob_store:
  kind: s3
  inline_bytes: 262144
  s3:
    bucket: acme-llmtw-production
    region: ap-southeast-2
    prefix: v1
    kms_key_id: arn:aws:kms:ap-southeast-2:123456789012:key/00000000-0000-0000-0000-000000000000
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
  provider_response_bytes: 524288
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
      kind: bearer_file
      path: /var/run/secrets/providers/openrouter-api-key
    account_region: global
    timeout: 115s
    service_classes:
      standard:
        provider_value: default
    capability_profile: openrouter-chat-pinned-v2
    price_catalog: catalog-2026-07-13
    extensions:
      openrouter:
        provider_order: [anthropic]
        allow_fallbacks: false
        require_parameters: true
        supported_parameters: [max_tokens, reasoning, response_format, structured_outputs, tools, tool_choice]
        reasoning_efforts: {low: low, medium: medium, high: high, maximum: max}
        missing_service_tier: standard
        model_aliases: [anthropic/claude-opus-5.5-20260921]

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

  bedrock-converse-us-east-1:
    # One-shot AWS Bedrock Runtime Converse; live streaming is not supported.
    family: bedrock_converse
    base_url: https://bedrock-runtime.us-east-1.amazonaws.com
    outbound_hosts: [bedrock-runtime.us-east-1.amazonaws.com]
    region: us-east-1
    auth:
      kind: aws_default_chain
    timeout: 115s
    service_classes:
      economy: {provider_value: flex}
      standard: {provider_value: default}
      priority: {provider_value: priority}
    capability_profile: bedrock-converse-v1
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

`temporal.api_key_file` contains the raw signed JWT sent as Temporal gRPC
`authorization: Bearer <token>` metadata. Production requires TLS plus this
file; a plaintext endpoint, unreadable file, empty or oversized token, or malformed
compact JWT is rejected before the SDK client is created. The JWT is bounded to
16 KiB and CA files to 1 MiB; projected Secret symlinks are followed with validation
of the opened regular-file target. The token is loaded when the client is created,
so restart the worker to adopt a rotated JWT. It is never included in config
hashes, diagnostics, or logs.

`temporal.worker.heartbeat_keepalive_interval` controls the fixed, redacted
heartbeat emitted while a one-shot provider call is in flight. It defaults to
`1s` when omitted and is not derived from a provider SDK timeout. Every
workflow Activity policy using this worker must use the same cadence and a
`heartbeat_timeout` of at least three times that cadence.

`server.shutdown_timeout` is the process-wide shutdown budget. It must be
strictly greater than `temporal.worker.graceful_stop_timeout` plus
`server.finalization_timeout`; equality is rejected so shutdown still has
bounded time to close clients and flush telemetry after in-flight Activities
drain. Kubernetes `terminationGracePeriodSeconds` must then exceed the same
shutdown budget (with deployment-specific margin), as described in the
[deployment shutdown contract](../architecture/deployment-and-operations.md#probes-and-shutdown).

Application and Temporal SDK client/worker events share the configured
`log/slog` logger. `telemetry.logs.format` selects JSON or text, and
`telemetry.logs.level` applies to SDK events as well as application events.
Logs go to stderr unless an embedding supplies a logger or output writer.
The Temporal adapter retains bounded workflow/run/activity IDs and task queue
names, classifies errors without logging their raw messages, and drops
unrecognized SDK attributes. It does not fall back to the SDK's standard
`log` logger. Custom Temporal client factories own their logger configuration.

When `telemetry.tracing.enabled` is true, `otlp_endpoint` names the OTLP/gRPC
collector and `sample_ratio` is a decimal from `0` through `1`. Runtime uses
the secure OTLP transport default; deploy a collector endpoint with TLS. The
tracer exports only bounded lifecycle metadata and hashes tenant identifiers;
it never exports request content, provider payloads, continuation handles, or
resolved credentials.

## State namespace selection

**state.kind** is **durable** or the legacy development-only **redis** fixture.
Durable is the production default
and requires both **state.redis** and **state.postgres**: PostgreSQL and blob storage
own durable operation, result, and checkpoint state with atomic finalization.
Redis is authoritative for budget reservations, one-shot paid-work claims,
settlement, provider status/inventory, and resource-capacity leases; it is not a
cache of a SQL budget journal.

Production retains a `20m` immutable `state.reservation_lease` for queued customer
Activities. The Redis paid-work start deadline is independently bounded to the
earlier of 15 minutes after acceptance and that immutable expiry. Claiming does
not extend the start deadline; claimed or ambiguous work remains charged until
explicit settlement. Batch escrow retains its own immutable expiry, while each
allocated paid-work grant receives the bounded start deadline.

In durable mode the runtime constructs and probes both stores before admitting
work. The PostgreSQL dependency probe checks the current database, UTC session
timezone, and the installed schema contract for the configured namespace; the
Redis probe performs its normal connectivity, clock, policy, and configured
Function or Lua script identity checks. Production additionally reads the
configured Redis keyspace's active budget-generation pointer and canonical,
complete manifest. Readiness revalidates the manifest invariants and binds the
pointer to the manifest digest and Redis incarnation even when a custom generation
port is supplied; a missing, invalid, or unbound pointer/manifest keeps readiness
closed. The probe is read-only and never publishes or rebuilds a generation. The validated
`state.redis.key_prefix` is applied to every worker-owned key constructor and
the active-generation read therefore also proves the configured namespace is
being addressed. A failure or timeout keeps readiness closed, and both clients
are closed during a failed build, configuration replacement, or worker
shutdown. Schema installation remains a deployment concern (`postgres.Install`)
and is never performed by a worker during readiness probing.

The local durable composition is documented separately from the offline Redis
fixture: `docker compose --profile durable run --rm durable-worker` validates
`deploy/local/durable-config.yaml` against an isolated worker PostgreSQL
database/role while retaining Redis for active budgets and throttles. It does
not install the schema or start Temporal polling.

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
  reservation_lease: 15m
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
**llmtw**. It is distinct from **state.redis.admission_hash_tag**, which
controls Redis Cluster co-location rather than the outer data namespace.

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
checked-in worker-object migration is rendered only after namespace validation
and uses schema-qualified `pgx.Identifier` values. `Install` is reserved for
an explicit provisioning step: it creates and locks down a missing dedicated
schema, but leaves an existing schema's ACL untouched so an operator can use a
shared schema with a dedicated table prefix. Startup verification remains
read-only and never creates or alters a schema.
Run `make postgres-integration` from `golang/` to exercise the namespace and
contract gates against the pinned PostgreSQL service image.

## Pricing and budget matching

Budget policies can be supplied as a JSON object in the worker setting
`budgets_json`, instead of the existing `budgets` YAML object:

```yaml
budgets_json: |
  {
    "require_match": true,
    "policies": [{
      "id": "acme-production",
      "match": {"tenant": "acme", "environment": "production"},
      "windows": [{"duration": "1h", "bucket": "1m", "limit_usd": "25.000000000000000000"}]
    }]
  }
```

Both forms share strict field and policy validation. JSON must be one object,
at most 4 MiB, with no unknown or duplicate fields. Nonempty `budgets` and
`budgets_json` cannot be combined. The effective policy values enter the
configuration digest, so equivalent JSON and YAML produce the same snapshot.
No SQL budget configuration is read. See [Redis budget leases](redis-budget-leases.md)
for the 15-minute start deadline and persistent paid-work accounting.

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

Both Bedrock endpoint families (`bedrock_anthropic_messages` and
`bedrock_converse`) require `auth.kind: aws_default_chain` and a non-empty
`region`. API-key, header, workload-identity, and other alternate auth modes
are rejected during configuration validation before an adapter or AWS client
is constructed. The official SDK then resolves credentials through the
configured default AWS credential chain. The secret access key is used only for
local SigV4 signing and is never transmitted; no credential value is serialized
into the Temporal payload. Signed request headers may still carry temporary
access-key and session-token metadata.

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

`limits.provider_response_bytes` bounds every HTTP response body shared by all
provider SDKs, including successful JSON, provider error bodies, and streaming
protocols such as SSE. It defaults to 512 KiB, cannot exceed
`server.inline_payload_bytes`, and retains a 64 MiB absolute safety cap. A
declared `Content-Length` above the configured limit is rejected before parsing
and the body is closed. Unknown-length, chunked, incorrectly declared, and gzip
responses are read through counting wrappers that bound both compressed and
decoded bytes: bytes through the limit remain available incrementally, and the
next byte returns a content-free oversize classification. The limit is
cumulative per HTTP response, not per stream event.

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
`noeviction` policy, and configured admission code identity. Durable deployments
also inspect the worker keyspace, active-generation manifest, and enabled
coordination Stream with bounded read-only checks; malformed or mismatched
records keep readiness closed. It also checks a bounded PostgreSQL read-only transaction,
the configured physical namespace/schema contract, every relation and both
explicit and constraint-backed indexes declared by that contract, and UTC
session state. Runtime role grants remain
deployment-owned and are exercised by the worker's normal least-privilege
operations. It checks
the configured S3 bucket with bucket metadata only; it never reads or writes a
tenant object. Provider endpoints are intentionally excluded because one route
can be unavailable while another eligible route remains.

The explicit schema-install step also inventories the complete worker relation
set before the first DDL statement. If a computed worker table or index already
exists without the worker contract marker (for example, a Temporal-owned
relation in a shared schema), installation fails closed with the colliding names;
it never renames or adopts that relation. A safely isolated schema in the same
database remains valid.

`state.redis.admission_mode: function` is the preferred Redis 7+ path. Before
starting a worker, deployment automation must provision the exact versioned
Function library and set `function_library`, `admission_version`, and
`admission_digest` to its immutable identity. In Function mode the digest is the
SHA-256 of `AdmissionFunctionSource()`: the library shebang, function registration,
embedded Lua body, and wrapper terminator. It is not the raw
`storage/redis/functions/admission.lua` file digest. The running worker only
verifies and calls that Function; it never loads, replaces, or rewrites shared
Redis code. `admission_mode: lua` is an explicit compatibility fallback: its
`admission_digest` must be the SHA-256 of the preloaded Lua source, and
readiness requires Redis `SCRIPT EXISTS` for that source. The worker never
falls back from a missing Lua script to `EVAL` or `SCRIPT LOAD`.

`required_persistence` selects the deployment policy: `aof_and_rdb` requires
both AOF and a non-empty RDB save policy, while `aof` and `rdb` require only
their named mechanism. Any mismatch fails readiness closed.

`coordination_stream_enabled` defaults to `true` in durable deployments and
`false` for memory or Redis-only fixtures. `stream_trim_safety` defaults to
`10m` when the Stream check is enabled and must be between one second and 30
days. Readiness resolves the namespaced events key and performs `TYPE` and
`XINFO STREAM` checks: the key must be a Stream with valid monotonic IDs, no
consumer groups, and a deletion high-water mark older than the trim-safety
window. These checks are read-only and fail closed; disabling the coordination
Stream is an explicit fixture choice and does not change budget authority. A
Stream gap or an explicitly disabled tailer discards local hints and reloads the
manifest/policy state directly from Redis. A recoverable cursor gap does not
invalidate an otherwise complete budget generation once the storage and
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

The generation/Stream contracts above remain available for deployment assembly;
they do not automatically publish budget events or start worker tailers. The
current durable budget leaser uses shared atomic Redis state directly and has
no SQL budget journal or SQL rebuild fallback. See
[Redis budget leases](redis-budget-leases.md) for the active contract and the
remaining recovery and cleanup work.


## OpenRouter endpoints

An `openai_chat` endpoint with an `extensions.openrouter` block uses the
OpenRouter profile. `validate-config` checks the block with the same rules as
the runtime profile, so an invalid effort or wire combination is refused before
a worker starts. Every field other than `provider_order` and
`supported_parameters` is optional; unknown fields are refused.

| Field | Meaning |
| --- | --- |
| `provider_order` | Upstream provider slugs sent as `provider.order`. Pin exactly one. A base slug (for example `anthropic`) never matches tier endpoints such as `anthropic/fast`; a region (`google-vertex/global`) needs the full slug. |
| `allow_fallbacks`, `require_parameters` | May only restate `false` and `true`. The profile always sends `allow_fallbacks: false`, `require_parameters: true` and `data_collection: deny`. |
| `supported_parameters` | The pinned endpoint's `supported_parameters` from the model's OpenRouter page, using OpenRouter's parameter names. It sets the wire shape: `max_tokens` when listed (otherwise `max_completion_tokens`), `parallel_tool_calls` only when listed (otherwise OpenRouter's documented default `true` applies, so parallel tool use is allowed and a request that lets the model call tools sequentially is refused while compiling), and never `store`, which is not an OpenRouter request field. A request that would send any other optional parameter, or a JSON-schema response format without `structured_outputs`, fails while compiling, before reservation or dispatch. |
| `reasoning_efforts` | Maps public efforts (`minimal`, `low`, `medium`, `high`, `maximum`) to OpenRouter's unified `reasoning.effort` (`minimal`, `low`, `medium`, `high`, `xhigh`, `max`). The adapter sends `{"reasoning": {"effort": ...}}`, the only documented way to enable reasoning on Anthropic models. An unmapped effort, a disabled reasoning mode or a token budget is refused while compiling. |
| `missing_service_tier` | Required when the endpoint maps exactly one class to OpenRouter's `default` tier. Such an endpoint omits `service_tier` (requests without it are never routed to a non-default tier), and a response whose `service_tier` is null is recorded as this class. It must be the class mapped to `default`. |
| `model_aliases` | Documented dated revisions (permaslugs, `<model>-<suffix>`) that OpenRouter may echo as the response `model`. |

`service_classes` provider values must be OpenRouter's documented tiers:
`default`, `flex` or `priority` (the `fast` alias is refused because OpenRouter
reports it as `priority`). Endpoints that map more than one class send
`service_tier` explicitly.

On an OpenRouter endpoint `maximum` becomes the configured `reasoning.effort`,
normally `max` when the model page lists it (Claude Opus 5.5, GPT-6 Astra);
leave it unmapped for a model without it (Gemini 3.1 Pro lists only low,
medium and high) so the request is refused instead of silently downgraded.
Generic and Azure Chat Completions endpoints send `reasoning_effort: "max"`,
a value the pinned openai-go SDK documents for that field.

The response `model` echo is part of the route contract. It must equal the
route model or one of its `model_aliases`; anything else, including an empty
echo, a routing-variant suffix such as `:floor`, or an unlisted date, fails the
call as an invalid provider response. When it matches, both `resolved_model`
and `observed_model_revision` are the route model and the raw echo is kept in
`provider.raw.response_model`. A null `usage.cost` is treated as an unreported
receipt, so the catalog usage cost applies.

`golang/deploy/openrouter/` contains the Fall 2026 frontier endpoints and
models (`endpoints.yaml`), their capability catalog and the dated price catalog
`catalog-2026-09-26`, verified against the OpenRouter model pages on
2026-09-26.

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

An entry may set `max_prompt_tokens` when the provider bills a higher tier
above a prompt size (OpenRouter bills Gemini 3.1 Pro at a higher rate above
200,000 prompt tokens and GPT-6 Astra above 272,000). The worker then refuses
to load a configuration whose `limits.max_input_tokens` exceeds it for a
configured route, refuses a reserve-batch operation whose `max_input_tokens`
exceeds it, and refuses to price a larger prompt with that entry. Zero or
omitted means no prompt-size tier.

`automatic_cache: [read, write]` marks cache components the provider bills
without an opt-in (OpenRouter's prompt-caching guide: GPT-5.6 and later read
and write automatically, Gemini 2.5 and later read implicitly). Any prompt may
then report cache tokens, so a reserve-batch operation on that entry must sign
`max_cache_read_tokens` (and `max_cache_write_tokens` for `write`) of at least
its `max_input_tokens`; otherwise the reservation is refused before dispatch
instead of admitting a call that is billed and then rejected against its
signed cache ceiling. The listed cache components need known prices.

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
