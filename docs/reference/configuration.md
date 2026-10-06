# Configuration Reference

## Format and loading

The worker reads one strict YAML document. Unknown fields, duplicate keys,
unknown enum values, unresolved references, and integer overflow are errors.
The file must end after that document: a second `---` document, or any content
after a `...` end marker, is an error rather than being ignored, so
concatenated files and multi-document renders do not validate. Catalog files
follow the same rule.

A null (`~`, `null`, or a key with no value) is rejected wherever a value
would otherwise be substituted silently: for a scalar or a section, such as
`require_match: ~`, and for a list element, such as `allowed_tenants: [~]`.
A null for a whole list or map, or for an optional section, means the same as
omitting it, which is how the effective configuration renders unset lists.
The command-line binary selects the document with the `--config` flag; the
default path is `/etc/llmtw/config.yaml`. It does not use a process-wide
environment-variable override for the configuration path or command mode. See
the [command-line reference](cli.md) for the validation and startup commands.

Secret values are referenced with typed `env`, `file`, or workload-identity
configuration. Those references are resolved by the production worker during
runtime construction, while `validate-config` and
`print-effective-config` only parse and canonicalize the non-secret document.
`workload_identity` secret references and endpoint auth are available to
embeddings that supply a workload identity provider; the worker binary does
not, so its `validate-config`, `print-effective-config` and `worker` commands
reject them.
The effective non-secret configuration is canonicalized and hashed as
`config_version`.

## Redis namespace identity

`state.redis.key_secret` references at least 32 bytes of stable HMAC key material.
It is separate from `state.redis.username` and `state.redis.password`, which are
only authentication credentials. All workers sharing a Redis prefix must use the
same identity secret. Rotating the ACL credentials must leave it unchanged.
A missing or short identity secret fails startup instead of selecting a new
namespace. Do not rotate this key as an ordinary password rotation: that needs
an explicit state migration.

For an existing pre-release namespace that used password-derived keys, preserve
its names by provisioning the **raw 32-byte SHA-256 digest** of the UTF-8 prefix
`llmtw:redis-key-v1:` followed by the original Redis password as the new file
secret, before changing the password. Do not use the digest's hexadecimal text.
Keep that file stable afterward. New installations should use an independent
random secret. The worker no longer derives namespace identity from a password.

## Environment

`environment` is a free-form identifier; budget policies can match it with
`match.environment`. Only the exact, case-sensitive value `development` selects
the development composition. Every other value is treated as production,
including `production`, `staging`, `prod`, `Production` and names the worker
has never seen. For those values:

- `state.redis.tls.enabled` must be `true`;
- `telemetry.content_logging` must be `disabled`;
- `state.kind` must be `durable` and `blob_store.kind` must be `s3`;
- startup requires the durable v1 runtime, the cloud budget initialization
  receipt and the Redis budget authority probe.

Where this document says "production", it means any environment other than
`development`. Validation and runtime composition share one rule
(`config.IsProductionEnvironment`), so a misspelt name cannot skip hardening.

## Caller authorization

The production CLI requires `authorization.mode: trusted_temporal` and a
non-empty `authorization.allowed_scopes` list of exact `{tenant, project}` pairs.
Temporal must authenticate callers and restrict namespace access. Every trusted
namespace caller can select any listed pair; this is not per-principal tenant
authorization. Actor and tags are descriptive fields, not credentials.

No wildcard or default grant is supported. Identifiers are case-sensitive,
at most 256 UTF-8 bytes and cannot contain whitespace, control characters, `*`
or `?`. Duplicate pairs and unknown fields are rejected. The policy participates
in the configuration digest and reloads with its snapshot. An absent policy is
accepted by the config library for custom embeddings with their own resolver,
but production CLI startup rejects it. `llm.query.v1` remains independently
configured and is not enabled by this policy. See
[durable runtime composition](durable-v1-runtime.md) for scope and reload details.

Because the worker trusts Temporal to authenticate callers, its own Temporal
connection must be encrypted and authenticated. The client presents an mTLS
certificate (`temporal.tls.cert_file` and `key_file`, set together) or an API
key (`temporal.api_key_file`, one line). Both require `temporal.tls.enabled`.
In production, startup requires TLS plus one of these credentials. The only
exception is `temporal.mesh_transport: true`, which declares that a service mesh
supplies transport security and client identity. All of these fields are
process-lifetime settings, read at startup; a reload that changes them is
rejected. Redis TLS does not accept `cert_file` or `key_file`.

## Output reservations

When a request omits `output.max_tokens`, budgeted execution inserts
`limits.max_output_tokens` into the provider request before compiling it and
estimating its cost. An explicit caller limit is preserved and reserved instead.
The effective limit is part of the compiled request digest and is reconstructed
for retries; recovery refuses a different limit under an existing reservation.
Limits must be positive and fit the providers' signed 32-bit SDK fields. Zero is
rejected because some adapters treat it as an omitted cap.

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
    cert_file: /var/run/ca/temporal-client.pem
    key_file: /var/run/ca/temporal-client-key.pem
  # api_key_file: /var/run/ca/temporal-api-key   # instead of cert_file/key_file
  # mesh_transport: true                          # a service mesh supplies TLS and identity
  worker:
    max_concurrent_activities: 16
    max_concurrent_activity_task_polls: 8
    graceful_stop_timeout: 30s
    heartbeat_keepalive_interval: 1s

authorization:
  mode: trusted_temporal
  allowed_scopes:
    - tenant: acme
      project: invoice-processing

state:
  kind: durable
  operation_terminal_retention: 45d
  ambiguous_retention: 90d
  continuation_retention: 30d
  reservation_lease: 15m
  redis:
    addresses: [redis.example.internal:6379]
    key_prefix: llmtw
    username:
      kind: env
      name: REDIS_USERNAME
    password:
      kind: file
      path: /var/run/secrets/redis-password
    key_secret:
      kind: file
      path: /var/run/secrets/llmtw/redis-key-secret
    tls:
      enabled: true
      server_name: redis.example.internal
      ca_file: /var/run/ca/redis.pem
    admission_hash_tag: admission
    admission_mode: function
    function_library: llmtw_admission_bba60419c1306b1f
    admission_version: admission_bba60419c1306b1f
    admission_digest: 9600ab0c0e48381c84527a4dd766d5842d1bd052308827c0c528b453f011b024
    coordination_stream_enabled: true
    stream_trim_safety: 10m
    max_connections: 96
    dial_timeout: 2s
    operation_timeout: 3s
    required_persistence: aof_and_rdb
  requests:
    provider:
      type: aws
      aws:
        region: ap-southeast-2
      key_value_stores:
        requests: physical-requests
      blob_stores:
        payloads: physical-payloads
    request_table: requests
    payload_store: payloads
    namespace: worker-v1
    secret:
      kind: env
      name: CLOUD_REQUEST_KEY

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
  # Input request size. Stored results are bounded separately: a blob may be
  # up to the larger of request_bytes and twice provider_response_bytes.
  request_bytes: 1048576
  items: 512
  parts_per_item: 64
  tools: 128
  schema_bytes: 262144
  json_depth: 64
  continuation_depth: 256
  route_attempts: 6
  provider_timeout: 120s
  provider_response_bytes: 16777216
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
        # The /openai/v1 path accepts only "v1" or "preview". Quote dated
        # values: YAML reads an unquoted date as a timestamp, which validation
        # rejects.
        api_version: "v1"

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
        api_version: "2024-10-21"
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
        # Sent as service_tier; use a value OpenRouter's request schema accepts.
        provider_value: default
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
        # Exa defines no service_tier: this value is never sent and only names
        # the price-catalog tier.
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

`continuation.allow_provider_hosted_state` is the global switch for provider-held
conversation state. A Responses endpoint may store responses, run in the
background or continue from `previous_response_id` only when both this switch
and the endpoint's `provider_storage.permitted` are true; otherwise it runs
statelessly. `continuation.retain_canonical_transcript` must be `true` (the
default when omitted): v1 checkpoints always retain the canonical transcript.
`capabilities.unknown_in_strict_mode` must be `reject`, the only implemented
strict-mode behaviour. `limits.items`, `limits.parts_per_item`, `limits.tools`,
`limits.schema_bytes` and `limits.json_depth` bound each new v1 Generate request
before it creates any durable record; an operation admitted earlier keeps
replaying its saved result. `server.finalization_timeout` bounds each detached
write that records a provider result.

`temporal.worker.max_concurrent_activities` bounds in-flight Activities and
therefore the worker's memory. One Activity on a large-context turn holds about
20–28 times the materialized transcript in live heap, roughly 96 MiB at the
4 MiB parent bound, and nothing weights slots by transcript size. Size it so
`max_concurrent_activities × 96 MiB` stays below `GOMEMLIMIT`: the base
Kubernetes deployment (`GOMEMLIMIT=1800MiB`, 2Gi limit) ships `16`. Raise it
only together with the memory limit, or when transcripts are known to be small.

`temporal.worker.heartbeat_keepalive_interval` controls the fixed, redacted
heartbeat emitted while a one-shot provider call is in flight. It defaults to
`1s` when omitted and is not derived from a provider SDK timeout. Every
workflow Activity policy using this worker must use the same cadence and a
`heartbeat_timeout` of at least three times that cadence.

Validation enforces the bounds of the internal request workflow's Activity
options: `heartbeat_keepalive_interval` may be at most `10s` (one third of the
`30s` heartbeat timeout), `limits.provider_timeout` must be shorter than the
`5m` Activity start-to-close timeout, and `server.inline_payload_bytes` may be
at most 2 MiB, Temporal's default payload blob limit.

`server.shutdown_timeout` is the process-wide shutdown budget. It must be
strictly greater than `temporal.worker.graceful_stop_timeout` plus
`server.finalization_timeout`; equality is rejected so shutdown still has
bounded time to close clients and flush telemetry after in-flight Activities
drain. `graceful_stop_timeout` applies only to termination: a dependency
pause lets in-flight Activities finish within the `5m` start-to-close timeout
instead of cancelling them. Kubernetes `terminationGracePeriodSeconds` must then exceed the same
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

**state.kind** is **durable** for production. It requires **state.redis** and
**state.requests**. Redis owns budget reservations, claims, settlement,
throttles and provider operational state. Cloud key-value/blob storage holds
requests, attempts, pending records, response caches and continuation data.
See [cloud request storage](cloud-request-repository.md#worker-integration)
for IAM configuration, table/bucket aliases and the encryption secret.

Durable readiness requires Redis, cloud request storage and the result blob
store. Production startup requires a ready cloud initialization receipt;
readiness checks its matching persistent budget authority marker in Redis,
alongside persistence and admission code. Missing or mismatched state keeps
readiness closed. Run the explicit `budget-initialize` command before first
startup; see [initialization](redis-budget-leases.md#initialization-and-readiness).
The worker does not install schemas or create tables or buckets. There is no
worker SQL pool, schema installer, SQL fallback or SQL data migration.

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
cloud storage and without creating an external blob client. It uses the shared
in-process admission and continuation implementations plus a bounded,
content-addressed process-local blob map. Provider adapters are still built
normally, so provider credentials and egress remain subject to the same
validation and authorization rules as durable mode.

Memory mode composes no v1 runtime, so it cannot run the public v1 API: the
worker registers none of the v1 workflows (`llm.generate.workflow.v1` and the
others) and none of the v1 Activities. It registers only the legacy engine
Activity, under its own name `llm.generate.legacy.v1`; `llm.generate.v1` always
means the v1 contract. Its input is the `activity.GenerateRequest` envelope
`{"api_version": "llm.temporal/v1", "request": <llm.Request>}` (a bare
`llm.Request` is rejected), and it returns the `activity.GenerateResponse`
envelope `{"api_version": "llm.temporal/v1", "response": <llm.Response>, "metadata": {...}}`. Trying
the v1 API locally requires `state.kind: durable` with cloud storage and Redis.

Operations, checkpoints, budget/throttle state, and blobs are process local.
A configuration reload keeps them: the admission, result and continuation
stores belong to the process, not to a snapshot, so completed operations still
replay after a reload. A continuation key rotation or `limits.continuation_depth`
change rebuilds only the continuation store, and the memory blob store keeps
the first snapshot's `blob_store.inline_bytes` until restart. A restart loses
everything and provider-pending jobs cannot be recovered after
process loss. The mode must not be used for durable continuation/recovery
guarantees, multi-replica admission, backups, or production readiness. Redis
addresses and credentials are ignored by the memory factory and
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

The same holds for everything else that identifies where durable state lives:
`state.kind`, `state.redis.admission_hash_tag`, the `state.redis.key_secret`
reference, the `state.requests` provider block, `request_table`,
`payload_store`, `namespace` and `secret` reference, and the result store's
`blob_store.kind`, `blob_store.file.root` and `blob_store.s3` bucket, region
and prefix. In-flight requests, budget reservations and results exist only
under the identity they were written with, so a reload that changes one is
rejected and its log record names the field. `state.redis.addresses`, Redis
credentials, TLS and timeouts are reloadable because they reach the same data.
See [configuration reload](cli.md#configuration-reload) for the full list and
the reload failure causes.

The former `state.postgres` section is rejected by the strict loader. The
`LLMTW_POSTGRES_DATABASE`, `LLMTW_POSTGRES_SCHEMA` and
`LLMTW_POSTGRES_TABLE_PREFIX` overrides are removed. Configure existing cloud
tables and buckets through `state.requests`; deployments create those resources
and grant the worker IAM access separately.

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

### Budget window identity

Each window's spend is accounted under the identity `<policy id>/<window id>`.
The identity never depends on the window's position in `windows`, so
removing, inserting or reordering windows leaves every other window on its own
accounting.

`id` is optional on each window. It must match
`[A-Za-z0-9][A-Za-z0-9._-]{0,63}`. When it is omitted, the window id is derived
from the geometry as `<duration>-<bucket>`, each written as a Go duration
without trailing zero units: `duration: 24h` with `bucket: 5m` is `24h-5m`,
`30d` with `1h` is `720h-1h`, and `90m` is `1h30m`. Validation rejects:

- two windows in one policy that resolve to the same window id, including two
  windows with the same `duration` and `bucket` and no `id` — give each an
  explicit `id`;
- an identity longer than 128 bytes, the limit the budget stores accept.

```yaml
windows:
  - id: daily          # identity acme-production/daily
    duration: 24h
    bucket: 5m
    limit_usd: "250.000000000000000000"
  - duration: 1h       # identity acme-production/1h-1m
    bucket: 1m
    limit_usd: "25.000000000000000000"
```

Changing `limit_usd` keeps the identity and the recorded spend. Changing the
identity selects different accounting: the window starts from zero and the old
spend is no longer counted against it. This happens when an `id` is added,
changed or removed, and when `duration` or `bucket` changes on a window
without an `id`. A reload that keeps an identity but changes its `duration` or
`bucket` is rejected and the active snapshot stays in place, because the
recorded buckets and their expiries were written for the old geometry; give
the window a new `id` instead. The worker remembers every identity it has run
with since it started, so removing a window in one reload and bringing its `id`
back with another geometry in a later reload is rejected too. This comparison
needs the running worker, so it does not apply to `validate-config` or to a
worker restart: do not reuse an explicit `id` with another geometry across a
restart either.

Earlier releases identified a window by its list position,
`<policy id>/<index>`. That is why removing or reordering a window moved the
remaining windows onto each other's accounting. Positional identities are no
longer derived. On upgrade, a window without an `id` moves to its
geometry-derived identity and starts from zero: spend recorded in the current
window before the upgrade is not counted, for at most one window duration.
To carry the recorded spend across the upgrade, pin each existing window to
its former position before upgrading, and keep those ids when the list is
edited later:

```yaml
windows:
  - id: "0"            # was the first window
    duration: 1h
    bucket: 1m
    limit_usd: "25.000000000000000000"
  - id: "1"            # was the second window
    duration: 24h
    bucket: 5m
    limit_usd: "250.000000000000000000"
```

Requests admitted before an identity change settle against the identity they
reserved under.

`pricing.require_price_when_budgeted` controls the explicit unpriced policy.
When it is `true`, a route without a current catalog quote is eligible only if
it matches no monetary budget policy. A matching policy always requires a
current quote. When it is `false`, all candidates require a current quote.

`budgets.require_match: true` is independent: it removes a candidate with no
matching budget policy before quote selection. Consequently, setting both
fields to `true` requires every dispatched candidate to match a budget and to
have a current price. An intentionally allowed unpriced result has
`cost_status: unknown`; its zero cost fields are unknown accounting facts, not
a free-use assertion. The public v1 exact-cost records use nullable actual cost
and an explicit unknown reason; confirmed zero is distinct from an unknown
charge. Compatibility fields must be interpreted together with their status.

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
protocols such as SSE. It defaults to 16 MiB and cannot exceed the 64 MiB hard
safety cap. A declared `Content-Length` above the configured limit is rejected
before parsing and the body is closed. Unknown-length, chunked, or incorrectly
declared bodies are read through a counting wrapper: bytes through the limit
remain available incrementally, and the next byte returns a content-free
oversize classification. Configure enough space for the largest legitimate
single provider response; the limit is cumulative per HTTP response, not per
stream event.

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
also inspect the budget authority marker and enabled coordination Stream with
bounded read-only checks; malformed or mismatched
records keep readiness closed. Cloud storage and the configured S3 bucket use
bounded read-only probes; readiness never writes a tenant object. Provider
endpoints are excluded because one route can be unavailable while another is
eligible. Resource creation and IAM permissions are deployment-owned.

`state.redis.admission_mode: function` is the preferred Redis 7+ path. Before
starting a worker, deployment automation must provision the exact versioned
Function library and set `function_library`, `admission_version`, and
`admission_digest` to its immutable identity. The running worker only verifies
and calls that Function; it never loads, replaces, or rewrites shared Redis
code. `admission_mode: lua` is an explicit compatibility fallback: its
`admission_digest` must be the SHA-256 of the preloaded Lua source, and
readiness requires Redis `SCRIPT EXISTS` for that source. The worker never
falls back from a missing Lua script to `EVAL` or `SCRIPT LOAD`.

The library and function names carry the first 64 bits of the Lua source's
SHA-256 (`llmtw_admission_<tag>` / `admission_<tag>`), so each release that
changes the Lua has its own library and the old one stays loaded beside it. To
upgrade, `FUNCTION LOAD` the new release's library (no `REPLACE`; the name is
new), then roll the workers with the new `function_library`,
`admission_version`, and `admission_digest`. Old workers keep verifying and
calling their own library throughout. Delete the old library with
`FUNCTION DELETE` only after no worker of that release remains. Rollback is the
same rollout in reverse while the old library is still loaded. Prefix-isolated
fleets on one Redis can run different releases at the same time. Lua mode is
already content-addressed by the script's SHA-1.

`required_persistence` selects the deployment policy: `aof_and_rdb` requires
both AOF and a non-empty RDB save policy, while `aof` and `rdb` require only
their named mechanism. Any mismatch fails readiness closed.

`coordination_stream_enabled` defaults to `true` in durable deployments and
`false` for memory or Redis-only fixtures. When enabled, durable budget
mutations publish coordination hints in the same Redis invocation; see
[Redis budget leases](redis-budget-leases.md#configuration-and-worker-coordination)
for event and retry semantics. Runtime background consumption remains separate.
`stream_trim_safety` defaults to
`10m` when the Stream check is enabled and must be between one second and 30
days. Readiness resolves the namespaced events key and performs `TYPE` and
`XINFO STREAM` checks: the key must be a Stream with valid monotonic IDs, no
consumer groups, and a deletion high-water mark older than the trim-safety
window. These checks are read-only and fail closed; disabling the coordination
Stream is an explicit fixture choice and does not change budget authority.
Stream hints never authorize spending or reconstruct balances.

The current durable v1 leaser uses shared atomic Redis state, with a persistent
initialization marker checked inside every mutation. It has no finite manifest
coverage horizon, SQL journal, or SQL rebuild fallback. Older
`budget-manifest/v1` generation/recovery interfaces remain available to custom
embeddings, but production readiness does not require that unrelated format.
Initialization is explicit and cannot reopen a lost budget dataset. It does not
verify survival of every accounting key or recover a stale Redis snapshot; see
[Redis budget leases](redis-budget-leases.md) for the remaining recovery,
worker-tail, and cleanup work.

## Service-class rules

The request enum remains exactly `economy`, `standard`, and `priority`.
Configuration may omit unsupported entries for an endpoint; it cannot define a
fourth public class. A mapping's `provider_value` is adapter-profile validated
and cannot be supplied by a request.

`provider_value` is sent as the request tier only where the provider API
defines one. Azure OpenAI (Chat and Responses) and Exa define no `service_tier`,
so nothing is sent for those endpoints; the value still names the price-catalog
tier and the response label that maps back to the class. For OpenRouter the
value is sent as `service_tier` and must be one OpenRouter accepts (`default`,
`flex`, `priority`, ...), not an upstream label such as `standard`.

A response that reports no tier, or a tier no mapping recognizes, is still
accepted: `service.actual` is omitted, `service.provider_value` keeps the raw
label, and the call is priced at the attempted class. Anthropic Messages
endpoints are the exception and require the response tier.

A request without `service_class` becomes `standard`. There is no configurable
provider default. `service_class_fallbacks` is request data, not a worker-wide
default, because only the caller can authorize a cost/latency class change.

## Route identifiers

A route `id` must be unique within its model. Two models may use the same
route ID, for example a `primary` route in each. Shared route health (status,
credit, billing and circuit state) is stored per route ID and endpoint under
the configuration digest, so same-named routes on different endpoints never
share a record. Same-named routes of different models that use the same
endpoint do share one record: they observe the same upstream endpoint and
account.

## Capability catalog shape

Each entry binds claims to an exact profile/model matcher. Per-feature
`max_bytes` declarations are unsupported and rejected during catalog loading;
they must not be used as admission guards:

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
      input.image: {level: native}
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

A matcher that can never match is a configuration error, because its policy
would silently never apply, or with `require_match: true` silently remove the
routes it was meant to budget. Validation rejects an exact `logical_model` that
is not a key of `models`, an exact `endpoint` that is not a key of `endpoints`
or that no route uses (no route of the matched `logical_model` when one is
named, since a request only reaches an endpoint through its model's routes),
and an exact `environment` other than the configuration's own `environment`.
When `authorization` is configured, an exact `tenant` or `project` must appear
in `authorization.allowed_scopes`, and when both are set they must appear
together in one scope. All comparisons are case-sensitive. Keep policies for
other environments in those environments' files.

Budget bounds that would otherwise fail at worker start or on every matching
request are also validation errors:

- a window keeps `duration / bucket + 2` buckets, which must not exceed
  `limits.max_budget_buckets_per_window`;
- `limit_usd` must be between `0.000000001` (one nano-USD) and
  `9007199.254740991`, the range Redis represents exactly; `limit_micro_usd`
  is bounded the same way, at `9007199254740`;
- every window identity `<policy id>/<window id>` must fit 128 bytes, so a
  policy `id` must leave room for its longest window id; see
  [budget window identity](#budget-window-identity).

## Model tenant restriction

`models.<name>.allowed_tenants` lists the tenants that may route to the model.
An empty, null, or omitted list places no tenant restriction on the model:
every tenant admitted by `authorization.allowed_scopes` can use it. To
restrict a model, list its tenants explicitly. A null list element is
rejected, so a template that renders an unset tenant as `[~]` cannot widen
access by accident; a template that renders the whole list as null or empty
does remove the restriction, so validate rendered files before deploying them.

## Price catalog shape

```yaml
version: llmtw-prices/v1
id: catalog-2026-07-13
entries:
  - provider: openai
    endpoint_id: openai-prod
    endpoint_family: openai_responses
    region: global
    model: gpt-example-2026-07-01
    provider_tier: default
    effective_from: 2026-07-13T00:00:00Z
    input_per_million: "1.250000"
    output_per_million: "10.000000"
    cache_read_per_million: "0.125000"
    # Every estimate charges cache writes and one request unit. Write an
    # explicit "0" for a component the provider does not bill; an omitted
    # component is unknown, and the route can then never be selected.
    cache_write_per_million: "0"
    reasoning_per_million: "0"
    per_request: "0"
    source: operator-verified
```

Prices in examples are illustrative. Production catalogs require provenance and
review; they never refresh silently from an untrusted endpoint. The one opt-in
exception is [model sync](#model-sync), which prices synced routes from
OpenRouter's published endpoint prices.
Every decimal property is defined as USD by its field name and catalog
contract; no generic currency discriminator is accepted or reported.

## Model sync

`model_sync` makes every model OpenRouter lists routable under its OpenRouter
ID, for example `openai/gpt-5.4` or `anthropic/claude-sonnet-4.5`, priced from
OpenRouter's published endpoint prices. See
[ADR 0016](../decisions/0016-openrouter-model-sync.md) for the design.

```yaml
endpoints:
  openrouter:
    family: openai_chat
    base_url: https://openrouter.ai/api/v1
    outbound_hosts: [openrouter.ai]
    auth: {kind: bearer_env, name: OPENROUTER_API_KEY}
    account_region: global
    timeout: 115s
    optional: true
    service_classes:
      standard: {provider_value: default}
    capability_profile: openrouter-chat-v1
    # No price_catalog: model sync prices this endpoint.
    extensions:
      openrouter: {}          # no provider_order: OpenRouter selects the upstream
  openai-direct:
    family: openai_responses
    base_url: https://api.openai.com/v1
    outbound_hosts: [api.openai.com]
    auth: {kind: bearer_env, name: OPENAI_API_KEY}
    account_region: global
    timeout: 115s
    optional: true
    service_classes:
      economy: {provider_value: flex}
      standard: {provider_value: default}
      priority: {provider_value: priority}
    capability_profile: openai-responses-v1

model_sync:
  openrouter:
    endpoint: openrouter
  direct:                     # tried before OpenRouter, in this order
    - endpoint: openai-direct
      provider: openai        # a provider in the model-sync rules
  rules:                      # optional, layered over the built-in rules
    - file: /etc/llmtw/model-sync-rules.yaml
      sha256: 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
  refresh_interval_min: 55m   # default
  refresh_interval_max: 65m   # default
```

- **Refresh.** Each worker refreshes after a random interval between
  `refresh_interval_min` and `refresh_interval_max` (1m to 24h). One worker at
  a time, holding a Redis lease, fetches OpenRouter's lists and publishes them
  to Redis; every worker installs the published catalog within a minute. With
  `state.kind: memory` each process fetches its own. A failed fetch keeps the
  last published catalog.
- **Credentials.** `optional: true` disables an endpoint whose environment
  credential is unset instead of failing startup, so a deployment can supply
  only the keys it wants. Only an endpoint that model sync uses and no
  `models` route references may be optional. OpenRouter's model lists are
  public, so prices still sync when only direct endpoints have keys.
- **Endpoints.** `model_sync.openrouter.endpoint` must be an `openai_chat`
  endpoint with the `openrouter` extension and no `provider_order`. A model
  sync endpoint that no `models` route references may omit `price_catalog`,
  and with model sync configured `models` and `pricing.catalogs` may be empty.
  Every model sync endpoint still names a capability profile; its `model`
  field is not used for synced routes.
- **Text models only.** Models whose output is not text-only (image, audio and
  music generators) are never synced; models that only accept media as input
  are. OpenRouter alias (`~…`), variant (`…:batch`) and variable-priced router
  IDs are skipped.
- **Precedence.** A configured `models` entry of the same name always wins,
  as does a configured price entry with the same identity.

### Model-sync rules

Direct models and their prices are hard-coded in the built-in rules file
`golang/internal/modelsync/rules/default.yaml`, which is compiled into the
worker binary and so ships in every image. Direct routes never use a fetched
price. Regenerate the OpenAI and Anthropic sections from the first-party
prices OpenRouter publishes with `go run ./tools/modelsyncdefaults` (from
`golang/`) and review the diff; the Exa section is maintained by hand.

Files in `model_sync.rules` are layered over the built-in rules at runtime.
Model fields override field by field, prices merge per tier, and exclusions
accumulate, so an override only states what it changes:

```yaml
version: model-sync-rules/v1
exclude: ["openai/gpt-4*"]            # no route at all (path.Match patterns)
providers:
  anthropic:
    models:
      anthropic/claude-sonnet-4.5:    # negotiated price for one tier
        prices:
          standard_only:
            input_per_million: "2.7"
            output_per_million: "13.5"
            cache_read_per_million: "0.27"
            cache_write_per_million: "3.375"
            per_request: "0"
      anthropic/claude-opus-4.5: {exclude: true}   # OpenRouter route only
      anthropic/claude-internal:                   # a model OpenRouter omits
        model: claude-internal-20261001
        context_tokens: 200000
        output_tokens: 64000
        prices:
          standard_only: {input_per_million: "3", output_per_million: "15", cache_read_per_million: "0.3", cache_write_per_million: "3.75", per_request: "0"}
```

Prices are keyed by the endpoint's `service_classes` `provider_value`; the
built-in rules price OpenAI `flex`, `default` and `priority`, Anthropic
`standard_only` and `auto`, and Exa `standard`. A model's optional `family`
restricts it to endpoints of that family.

The built-in Exa models are:

| Synced name | Exa model | Endpoint | Price |
|---|---|---|---|
| `exa/answer` | `exa` (Answer API) | `openai_chat` with the `exa` extension | $0.005 per request |
| `exa/agent` | `exa-agent` (Agent API) | `openai_responses` with the `exa` extension | at most $0.10 per request |

An Exa Agent request always carries a fixed effort: the caller's `minimal`,
`low` or `medium`, or `medium` when unset. Exa's default effort, `auto`, is
metered, and Exa rejects `high` and above on a synchronous request, so the
worker rejects them too. Exa reports each call's actual cost, which settles
it.

```yaml
endpoints:
  exa-agent:
    family: openai_responses
    base_url: https://api.exa.ai
    outbound_hosts: [api.exa.ai]
    auth: {kind: header_env, name: EXA_API_KEY}
    account_region: global
    timeout: 115s
    optional: true
    service_classes:
      standard: {provider_value: standard}   # never sent; names the price tier
    capability_profile: exa-agent-v1
    extensions:
      exa: {}
model_sync:
  direct:
    - {endpoint: exa-answer, provider: exa}
    - {endpoint: exa-agent, provider: exa}
``` A class whose tier has no
price is not offered on the direct route. Every price component is required.
A model without `model` derives its provider ID from the OpenRouter ID
(`model_id: verbatim`, or `dots_to_dashes` for `claude-sonnet-4.5` →
`claude-sonnet-4-5`). Built-in context limits are capped below any
long-prompt price threshold so the listed price always applies.

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
  inequalities are safe, including window bucket counts, limits Redis can
  represent, policy ID length, and the `state.redis.admission_hash_tag`
  character set (1 to 64 bytes without braces or whitespace);
- budget matchers name declared models, endpoints, the configured environment,
  and allowlisted scopes;
- telemetry settings obey their environment and content-logging rules.

It does not read catalog files or compare their digests, resolve environment or
file secret contents, construct provider/Temporal/Redis/S3 clients, inspect
Redis hash-slot or Function state, verify SDK retry options, or validate
Kubernetes deployment settings. The production `worker` command performs the
reference and client-construction checks during runtime composition; catalog
and deployment verification are separate gates.

Reload performs the same checks and publishes only a complete valid snapshot.
The file watcher reloads a changed file only after two consecutive polls, one
second apart, see the same file identity, size, modification time and content,
and the reload publishes only that content. A prefix of a configuration file
is often valid YAML with its trailing sections missing, so this keeps a file
caught halfway through an in-place write from being published. Prefer an
atomic rename or a ConfigMap update, which are never observed half written.
`SIGHUP` reloads the file as it is at that moment without waiting.

### Responses provider storage policy

For OpenAI and Azure Responses endpoints, `provider_storage.permitted: false`
(the default) explicitly sends `store: false`. Requests that enable `store` or
`background`, or use a stored-response continuation ID, are rejected before
provider dispatch. Returned responses do not expose provider continuation
handles under this policy. Set `permitted: true` to permit those provider
storage features; callers may still explicitly request `store: false`.

Because a request with `store: false` cannot refer back to a stored reasoning
item, a storage-denied endpoint also requests
`include: ["reasoning.encrypted_content"]` whenever the request configures
reasoning or replays reasoning state, in addition to any `include` values the
caller supplies. The encrypted content is checkpointed with the reasoning item
and replayed on later turns. A checkpointed reasoning item that has no
encrypted content is omitted from later requests to a storage-denied endpoint
rather than sent as an ID the provider cannot resolve; the rest of the
transcript is unaffected.
