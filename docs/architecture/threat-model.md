# Threat Model

This document lists who can attack the worker, what each of them can read,
write or cause, and which control bounds them. It complements
[security and privacy](security-and-privacy.md), which describes the controls
themselves, and the [configuration reference](../reference/configuration.md),
which defines every field named here.

Statements about current behaviour were checked against the code at master
`ec4f58bb` and cite the file that implements them. Anything the worker does not
do yet is labelled **Proposal** or listed as a residual risk with the issue that
tracks it. When code changes a control named here, update this document in the
same pull request.

## Scope

The model covers the production composition: any `environment` other than
`development`, which requires `state.kind: durable`, the durable v1 runtime,
`authorization.mode: trusted_temporal`, S3 blobs, DynamoDB request storage and
Redis budgets (see [environment](../reference/configuration.md#environment)).
It covers the worker process, its Temporal connection, the optional OpenAI
Responses API listener, provider egress, the optional Langfuse export, the
three state stores, configuration, and the build and release pipeline.

Out of scope:

- development memory mode and the legacy engine Activity. Production builds
  the legacy engine but never calls it; its fate is
  [#1566](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/1566);
- Temporal's own server, persistence and UI, which the worker trusts to
  authenticate callers and keep history;
- the hosting platform (Kubernetes, cloud IAM, the secret store), except where
  the worker depends on a specific property of it;
- caller applications, including how they execute tool calls the model
  returns. The worker transports tool calls and never runs them.

## Assets

| Asset | Where it lives | Why it matters |
| --- | --- | --- |
| Prompt, output, tool and reasoning content | Temporal history (inline payloads), encrypted S3 blobs, provider requests, Langfuse when configured | Confidential caller data |
| Provider credentials | Process environment or mounted files, resolved per snapshot | Paid access to every configured provider account |
| Budget state | Redis `durable-budget` keys and the admission Function | The only bound on spend |
| Storage and signing keys: `state.requests.secret`, `state.redis.key_secret`, `continuation.handle_keys`, `temporal.payload_codec.keys` | Mounted files or environment | Confidentiality of blobs and Redis result copies, integrity of handles and capabilities, key-name privacy |
| Tenant and project scope | Caller-supplied request context, checked against `authorization.allowed_scopes` | Isolation between tenants and their spend |
| Configuration | The YAML file (`--config`, default `/etc/llmtw/config.yaml`) and catalog files | Decides every control below |
| Release image | Container registry | Everything the worker runs |
| Availability | Activity slots, provider quota, Redis, storage | Paid work stalls or is refused |

## Trust assumptions

The worker's controls hold only while these are true. Each is a deployment
obligation rather than something the worker checks.

1. Temporal authenticates every client and restricts namespace access. The
   worker carries no per-caller identity: an Activity payload has a tenant,
   project and actor that the caller chose
   ([`config/authorization.go`](../../golang/config/authorization.go)).
2. The worker's own Temporal connection is encrypted and authenticated, or a
   service mesh supplies both (`temporal.mesh_transport`). Production
   validation refuses plain Temporal otherwise
   (`TemporalConfig.validate` in
   [`config/validate.go`](../../golang/config/validate.go)).
3. Only reviewed operators can change the configuration file, the mounted
   secrets and the process environment. The worker verifies the shape of the
   file, not who wrote it.
4. Redis, DynamoDB and S3 are reachable only by the worker and their
   operators. Least-privilege IAM for the storage path is
   [#1581](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/1581).
5. The deployed image is the one CI built and scanned. Nothing verifies this at
   deploy time yet
   ([#1582](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/1582)).

## Actor and capability matrix

Each row summarises an actor; the sections after the table give the detail and
the code that implements each control. "Residual" names what the controls do
not stop and the issue that tracks it.

| Actor | Can read | Can write or cause | Bounding controls | Residual |
| --- | --- | --- | --- | --- |
| Temporal namespace caller | Its own results; any workflow history the namespace grants, in plaintext unless the payload codec is on; control queries for any allowed scope | Paid Generate and Compact work charged to any `allowed_scopes` pair; Activity-slot and quota consumption | Exact scope allowlist; per-model `allowed_tenants`; budget policies; request limits; scope-bound handle and reference MACs; content-free denial and access logs | Any caller can act as any allowed tenant ([#1577](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/1577)); plaintext history ([#1576](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/1576)); no per-tenant concurrency or size quotas ([#1584](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/1584)) |
| Responses API listener client | Stored results of any allowed scope whose response ID it knows | Paid Generate work for any allowed scope | Listener is off by default; production binds loopback or link-local unless `responses_api_private_network` is set; the scope allowlist still applies | No authentication or TLS ([#1578](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/1578)) |
| Model provider (hostile or compromised) | Everything sent to it, including content, tool definitions and its credential | Arbitrary response bytes, usage, cost and errors; synced prices and model lists (OpenRouter with `model_sync`) | Pinned outbound hosts and dial-time address checks; no redirects or proxies; response byte cap; local structured-output validation; provider state pinned to its lineage; provider storage off by default | Inflated usage is charged; synced prices are trusted (see [residual risks](#residual-risks)) |
| Redis writer | HMAC-named keys, budget totals and operation records, route health, the model-sync catalog; not result plaintext | Budget totals and reservations, route holds, the published model-sync catalog, deletion of any key | Admission Function code digest checked by readiness; bounded, schema-checked reads; sealed Responses API result copies | Forged budget state and catalog are trusted (untracked, see [residual risks](#residual-risks)); recovery covers loss, not malice ([#856](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/856)) |
| DynamoDB or S3 writer | Ciphertext and content-free pointer rows | Deletion, withholding or reordering of rows and objects | AES-256-GCM blobs bound to an HMAC key; pointer events checked for revision and state order | Availability and rollback; IAM scope ([#1581](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/1581)) |
| Configuration insider | Everything the worker can reach, through the export paths below | Any control in this table; reloadable fields take effect without a restart | Strict validation; process-lifetime fields rejected on reload; reload logged with `config_version` | No signed configuration or two-person gate in the worker (see [review guidance](#configuration-change-review)) |
| Supply chain and CI | Source, CI secrets of the jobs it runs in | Malicious code or dependency in the image | SHA-pinned Actions; dependency review; `govulncheck` differential gate; scheduled full scan; non-root distroless image | Release signing not wired ([#1325](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/1325)); no deploy-time signature check ([#1582](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/1582)) |

## Temporal namespace caller

A namespace caller is any client Temporal lets start or read workflows on the
worker's namespace. It is semi-trusted: authenticated by Temporal, but not
bound to a tenant.

**What it can do.** It can start the public Generate and Compact workflows with
any tenant and project that appears in `authorization.allowed_scopes`. The
production scope resolver maps the caller's `{tenant, project}` to a scope only
by exact lookup in that list
([`internal/runtime/cli_cloud_factory.go`](../../golang/internal/runtime/cli_cloud_factory.go),
`trustedTemporalCloudOptions`), and the list itself rejects empty entries,
wildcards, whitespace and duplicates
([`config/authorization.go`](../../golang/config/authorization.go)). Nothing
ties a Temporal principal to a pair, so every caller can act as every listed
tenant. The actor is descriptive: the caller chooses it, and it is the
Langfuse user ID.

Because the actor is caller-chosen, a budget policy that matches on
`actor_prefix` is not a security boundary. All matching policies apply to a
candidate ([`budget/match.go`](../../golang/budget/match.go)), so a caller can
pick an actor that avoids an actor-scoped policy. A policy that matches a
tenant, project, logical model, endpoint or service class applies whenever the
request or its route uses that value, so a caller cannot avoid it while still
using that value.

The caller can also read, through Temporal, whatever history its namespace
permissions allow. Without `temporal.payload_codec`, inline prompts and outputs
are plaintext in that history; the codec is optional in production
([`config/validate.go`](../../golang/config/validate.go) validates it only when
set).

**Controls.**

- A scope outside the allowlist is denied before planning, reservation or
  dispatch, as non-retryable `permission_denied`, and logged with truncated
  hashes of the tenant and project
  (`scopeDeniedError` and `ScopeAccessDenied` in
  [`internal/runtime/cli_cloud_factory.go`](../../golang/internal/runtime/cli_cloud_factory.go)
  and [`internal/observability/query_audit.go`](../../golang/internal/observability/query_audit.go)).
- Allowed requests log content-free access events
  ([`internal/observability/access_audit.go`](../../golang/internal/observability/access_audit.go)).
  They are best-effort logs, not a durable audit store.
- `models.<name>.allowed_tenants`, when non-empty, removes every route of that
  model for other tenants
  ([`routing/planner.go`](../../golang/routing/planner.go)). An empty list is
  unrestricted.
- With `budgets.require_match: true`, candidates without a budget policy are
  removed before any reservation; the window limits then bound spend per
  matched policy.
- Request size, item, part, tool, schema, JSON-depth, continuation-depth and
  output-token limits apply to every request
  ([`config/types.go`](../../golang/config/types.go), `LimitsConfig`). They
  are global, not per tenant.
- Checkpoint handles are HMACs over the scope; the internal reference
  capability that resumes a request is an HMAC over tenant, project and request
  ID, so a public operation ID alone reads nothing
  ([`state/handle.go`](../../golang/state/handle.go),
  `VerifyCheckpointHandle` and `VerifyReferenceCapability`;
  checked in `Load` in
  [`internal/runtime/cloud_request_preparation.go`](../../golang/internal/runtime/cloud_request_preparation.go)).
- Control queries authorize the same exact pairs, with cursors signed by a key
  bound to the configuration digest (`trustedTemporalQueryBuilder` in
  [`internal/runtime/cli_cloud_factory.go`](../../golang/internal/runtime/cli_cloud_factory.go)).
- When the codec is configured, oversized ciphertext is rejected before
  decryption ([`temporalcodec/codec.go`](../../golang/temporalcodec/codec.go)).

A Temporal operator, or anyone who can write history, has at least the same
capabilities as a caller; the worker cannot distinguish them.

## Responses API listener client

`server.responses_api_listen_address` enables a plain-HTTP listener for a
subset of the OpenAI Responses API. It is absent by default.

**What it can do.** The bearer "API key" is the literal
`tenant/project/actor` triple and is not a secret
([`internal/responsesapi/identity.go`](../../golang/internal/responsesapi/identity.go)).
Anyone who reaches the listener can start Generate work for any allowed scope
and poll results. A random response ID has 128 bits of entropy; an ID built
from a caller's `Idempotency-Key` is as guessable as that key
([`internal/responsesapi/ids.go`](../../golang/internal/responsesapi/ids.go)).

**Controls.**

- In production the address must be loopback, link-local or `localhost`
  unless `server.responses_api_private_network: true` acknowledges a wider bind
  ([`config/validate.go`](../../golang/config/validate.go),
  `hostLocalAddress`). Changing the address needs a restart
  ([`internal/runtime/reload_validation.go`](../../golang/internal/runtime/reload_validation.go)).
- The started workflow goes through the same scope resolver, so a triple
  outside `allowed_scopes` is refused.
- Workflow IDs bind the response ID to a hash of tenant and project, so one
  project's key cannot address another project's workflow
  (`workflowID` in [`internal/responsesapi/ids.go`](../../golang/internal/responsesapi/ids.go)).
- The Redis copies of terminal results that the listener reads are
  AES-256-GCM sealed under a key derived from `state.redis.key_secret`
  ([`storage/redis/response_result.go`](../../golang/storage/redis/response_result.go)).

## Model provider

Treat every provider endpoint, including OpenAI-compatible gateways, as able
to return anything and to keep anything it receives.

**What it can do.** It reads every request the worker sends it, including the
endpoint's credential. It controls response bodies, streamed bytes, usage
counts, reported cost, error bodies and timing. For URL image and document
parts it fetches the URL inside its own network. With `model_sync`, OpenRouter's
public lists decide which synced models are routable and at what price; direct
routes use only the prices compiled into the built-in rules.

**Controls.**

- Every adapter is built with the guarded HTTP client
  (`buildAdapter` in [`internal/runtime/factory.go`](../../golang/internal/runtime/factory.go)).
  It allows only the endpoint's `outbound_hosts` and ports, disables proxies
  and redirects, resolves DNS at dial time and refuses loopback, private,
  link-local, multicast, carrier-grade NAT and metadata addresses
  ([`internal/runtime/egress.go`](../../golang/internal/runtime/egress.go),
  [`internal/netpolicy/netpolicy.go`](../../golang/internal/netpolicy/netpolicy.go)).
- Response bodies are capped by `limits.provider_response_bytes`, at most
  64 MiB ([`config/types.go`](../../golang/config/types.go),
  `MaxProviderResponseBytes`; enforced by `providerResponseBody` in
  [`internal/runtime/egress.go`](../../golang/internal/runtime/egress.go)).
- Structured output is validated against the caller's schema after the
  provider returns it, whatever the provider claims
  ([`llm/provider/openairesponses/lift.go`](../../golang/llm/provider/openairesponses/lift.go),
  [`llm/provider/openaichat/lift.go`](../../golang/llm/provider/openaichat/lift.go),
  [`llm/provider/internal/anthropicwire/lift.go`](../../golang/llm/provider/internal/anthropicwire/lift.go)).
- Provider continuation state replays only to the lineage it is pinned to
  ([`state/pinning.go`](../../golang/state/pinning.go)).
- OpenAI and Azure Responses endpoints send `store: false` unless both
  `endpoints.<id>.provider_storage.permitted` and
  `continuation.allow_provider_hosted_state` are true
  (`providerStoragePermitted` in
  [`internal/runtime/factory.go`](../../golang/internal/runtime/factory.go)).
- URL parts are checked before they reach a provider: scheme, user info,
  private and metadata literals, numeric IPv4 spellings and internal names
  ([`llm/media_url_policy.go`](../../golang/llm/media_url_policy.go)). The
  check reads the URL only; a public name that resolves privately is the
  provider's to refuse.
- A provider that reports credit or billing failures holds only its own route
  (`routeHold` in
  [`internal/runtime/cloud_route_health.go`](../../golang/internal/runtime/cloud_route_health.go)).

**Residual.** Final cost prefers provider-reported cost, then provider-reported
usage priced by the catalog ([`pricing/entry.go`](../../golang/pricing/entry.go),
`CostMethod`). The worker cannot verify either, so a dishonest provider can
inflate the charge recorded against a budget. Content sent to a provider is
under that provider's retention terms; choosing endpoints is a data-processing
decision.

## Redis writer

This actor is anyone with write access to the worker's Redis keyspace: a Redis
operator, a stolen ACL credential, or a workload sharing the instance.

**What it can do.** Key names are HMACs under `state.redis.key_secret`
(`digest` in [`storage/redis/key.go`](../../golang/storage/redis/key.go)), so a
scan does not reveal tenant names, but a writer can still enumerate and change
every value. The values the worker trusts include:

- durable budget totals, reservations and operation records, which the
  admission Function reads to decide whether to admit paid work;
- route health records, which can hold a route (`routeHold` in
  [`internal/runtime/cloud_route_health.go`](../../golang/internal/runtime/cloud_route_health.go));
- the published model-sync catalog. Workers install it after checking only an
  unkeyed SHA-256 of its body (`Sync` in
  [`internal/modelsync/refresher.go`](../../golang/internal/modelsync/refresher.go),
  `Digest` in [`internal/modelsync/document.go`](../../golang/internal/modelsync/document.go)),
  so a writer can publish a well-formed catalog with its own synced models and
  prices.

Lowering a budget total or publishing low synced prices admits spend beyond the
configured limits; raising totals or holding routes denies service.

**Controls.**

- Readiness checks, on every master, that the admission Function's code hashes
  to the digest compiled into the binary (`verifyAdmission` in
  [`internal/runtime/dependency_probe.go`](../../golang/internal/runtime/dependency_probe.go)).
  A writer able to replace Functions is detected at the next probe, not
  prevented.
- Values are size-bounded and schema-checked on read; for example route status
  must carry the snapshot's configuration digest
  ([`storage/redis/provider_state.go`](../../golang/storage/redis/provider_state.go)).
- Responses API result copies are sealed, so a Redis reader without
  `key_secret` cannot read them.
- Production requires Redis TLS unless `state.redis.service_mesh` declares a
  mesh ([environment](../reference/configuration.md#environment)).

**Residual.** Budget integrity assumes an honest Redis. The recovery contract
restores lost budget authority
([#856](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/856));
nothing detects deliberate edits.

## DynamoDB or S3 writer

**What it can do.** It can read ciphertext and the content-free pointer rows,
and it can delete, withhold or reorder rows and objects.

**Controls.**

- Each payload blob is AES-256-GCM sealed under a key derived from
  `state.requests.secret`, with the blob key as associated data, and the blob
  key is an HMAC of the plaintext. A read fails as corrupt unless both the tag
  and the key match (`writeBlob` and `openBlob` in
  [`storage/cloudstate/crypto.go`](../../golang/storage/cloudstate/crypto.go)).
  Without the secret, a writer can neither read nor forge payloads.
- Pointer events must form a gap-free revision sequence with valid status
  transitions and an unchanged scope tag and request binding (`buildState` in
  [`storage/cloudstate/repository.go`](../../golang/storage/cloudstate/repository.go)).
  The events themselves are not MACed, so this model does not claim that a
  table writer cannot roll a record back to an earlier valid revision.
- A referenced blob that is missing is reported as corruption, never as an
  absent record that would restart paid work (`readReferencedBlob`).

**Residual.** Deletion and rollback are availability and replay risks. The
grant the worker needs, and so what a stolen worker credential allows, is not
yet enumerated
([#1581](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/1581)).
The single storage secret has no rotation or managed delivery
([#861](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/861)),
and per-tenant crypto-shredding does not exist
([#1579](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/1579)).

## Configuration insider

This actor can change the configuration file, the catalog files, the mounted
secrets or the process environment. It is the most capable actor here: the
worker trusts configuration to define every other control.

**What it can do.** A valid file can widen `allowed_scopes`, clear a model's
`allowed_tenants`, raise or remove budgets, or disable `require_match`. The
sharper risks are export paths:

- `langfuse.base_url` may be any HTTPS URL. The Langfuse client disables
  proxies and redirects but is not given the provider egress policy: there is
  no host allowlist and no dial-time address check (`NewClient` in
  [`langfuse/client.go`](../../golang/langfuse/client.go), called with a nil
  transport from
  [`internal/runtime/factory.go`](../../golang/internal/runtime/factory.go)).
  The base NetworkPolicy allows TCP 443 to any address except the metadata IP
  ([`deploy/kubernetes/base/networkpolicy.yaml`](../../golang/deploy/kubernetes/base/networkpolicy.yaml)).
  A changed `langfuse` block exports semantic content, and the Langfuse key
  pair, to the new host.
- Every secret reference is resolved again when a snapshot is built. An `env`
  reference can name any variable in the worker's environment, and a `file`
  reference any regular file the process can read, up to the resolver's size
  limit ([`internal/secrets/resolver.go`](../../golang/internal/secrets/resolver.go),
  [`internal/secrets/file.go`](../../golang/internal/secrets/file.go)).
  Pointing an endpoint's `auth.name`, or a Langfuse key, at another secret
  sends that secret to the endpoint's host or to Langfuse.
- `endpoints.<id>.base_url` can move to any host already in that endpoint's
  `outbound_hosts`.

**Reload versus restart.** Reloads rebuild every client from the new file
([`internal/app/app.go`](../../golang/internal/app/app.go), `Reload`). A fixed
table of process-lifetime fields is rejected on reload, among them
`environment`, every `temporal.*` connection and codec field,
`state.redis.service_mesh`, `state.redis.key_secret`, `state.requests.*`
identity, the blob store and every endpoint's `outbound_hosts`
([`internal/runtime/reload_validation.go`](../../golang/internal/runtime/reload_validation.go)).
Everything else, including `authorization`, `models`, `budgets`, endpoint
`auth` and `base_url`, `continuation.handle_keys` and `langfuse`, takes effect
on reload. A restart applies any change. The restart boundary limits what a
file edit alone can do; it is not a control against an insider who can also
restart the worker.

**Controls.**

- Strict parsing: unknown fields, duplicate keys and trailing YAML documents
  are errors (`yaml.WithKnownFields` and `yaml.WithUniqueKeys` in
  [`config/load.go`](../../golang/config/load.go); see
  [configuration format](../reference/configuration.md#format-and-loading)).
- A rejected reload keeps the running snapshot and logs a bounded cause and
  field path; an accepted reload logs `configuration reloaded` with the new
  `config_version` and increments the reload metric
  ([`internal/runtime/reload.go`](../../golang/internal/runtime/reload.go)).
  The log does not say what changed or who changed it.
- Capability, price and model-sync rules catalogs are referenced with a
  SHA-256 that the file must match before it is used (`readVerified` in
  [`internal/catalog/catalog.go`](../../golang/internal/catalog/catalog.go)).
  The digest sits in the same configuration, so it detects a changed catalog
  file, not a changed configuration.

The worker does not verify who wrote the file. Operators must enforce review
before a file reaches the worker; see
[configuration change review](#configuration-change-review).

## Supply chain and CI

**What it can do.** A compromised dependency, Action, base image or merged
change runs with the worker's privileges and sees every secret and payload.

**Controls.**

- Every `uses:` reference in `.github/workflows` names a full commit SHA.
  Architecture tests enforce this for the live-provider workflow
  (`immutableActionReference`) and require the pull-request, master and
  scheduled security workflows to use only GitHub-owned `actions/*`
  (`TestWorkflowAutomaticallyTriggeredWorkflowsUseOnlyGitHubOwnedActions` in
  [`internal/architecturetest/workflow_test.go`](../../golang/internal/architecturetest/workflow_test.go));
  SHA pinning elsewhere is review practice, not a test.
- Pull requests run GitHub dependency review and `make security-pr-verify`,
  which builds `govulncheck` and fails on new findings relative to the base
  ([`golang/Makefile`](../../golang/Makefile),
  [`tools/supplychainverify`](../../golang/tools/supplychainverify/main.go)
  checks trusted module roots and licences).
- A scheduled workflow runs the full `make security-verify` daily
  ([`.github/workflows/security-scheduled.yml`](../../.github/workflows/security-scheduled.yml)).
- The runtime image is distroless `nonroot` with no shell or package manager
  ([`golang/Dockerfile`](../../golang/Dockerfile)); the Kubernetes base sets
  `runAsNonRoot`, a read-only root filesystem and drops all capabilities
  ([`deploy/kubernetes/base/deployment.yaml`](../../golang/deploy/kubernetes/base/deployment.yaml)).

**Residual.** Base images use mutable tags by design, so a rebuild can pick up
a changed upstream image. The guarded release workflow stops before signing and
publication (`protected-signing-publication` in
[`.github/workflows/release.yml`](../../.github/workflows/release.yml);
[#1325](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/1325)),
and nothing verifies signatures or provenance at deploy time
([#1582](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/1582)).

## Residual risks

Each accepted risk links to the issue that tracks its mitigation. A risk marked
"untracked" has no issue yet and needs one.

| Risk | Actor | Tracking |
| --- | --- | --- |
| Every namespace caller can select every allowed tenant/project pair; revoking a caller means changing Temporal access for all of them | Namespace caller | [#1577](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/1577) |
| Inline content is plaintext in Temporal history unless `temporal.payload_codec` is set; no codec server for UI, CLI or OCaml callers | Namespace caller, Temporal operator | [#1576](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/1576) |
| The OCaml client cannot connect to a TLS or authenticated Temporal endpoint | Network attacker on the caller's path | [#1345](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/1345) |
| No per-tenant concurrency caps, size quotas or positive model allowlist; one tenant can exhaust Activity slots or provider quota | Namespace caller | [#1584](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/1584) |
| The Responses API listener has no authentication or TLS | Listener client | [#1578](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/1578) |
| `data_regions` is recorded but not enforced | Namespace caller | Accepted for v1, listed in [#1539](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/1539) |
| Provider-reported usage and cost are trusted when charging budgets | Provider | Untracked |
| A Redis writer can forge budget totals, reservations and the model-sync catalog; recovery covers loss, not malice | Redis writer | Untracked; loss recovery is [#856](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/856) |
| The storage secret has no managed delivery, versioning or rotation | Storage writer, insider | [#861](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/861) |
| No response to a compromise of `key_secret`, handle keys, codec keys or provider credentials | Any actor holding a key | [#1559](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/1559) |
| No per-tenant key hierarchy or crypto-shredding for erasure | Storage writer | [#1579](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/1579) |
| Storage IAM grant is not enumerated or tested | Storage writer, stolen worker credential | [#1581](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/1581) |
| One Langfuse project and key pair receive content for every tenant | Insider, Langfuse operator | [#1580](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/1580) |
| Langfuse egress has no host allowlist or address check, and a reload can repoint it or any secret reference | Configuration insider | Untracked |
| Configuration is not signed and reloads record only `config_version` | Configuration insider | Untracked; review guidance below |
| Release images are not signed and deploys do not verify signatures, provenance or the SBOM | Supply chain | [#1325](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/1325), [#1582](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/1582) |
| Access events are best-effort logs; deletion audit events are not emitted | All | [#818](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/818) |

## Configuration change review

The worker validates configuration but cannot tell a reviewed change from an
unreviewed one. This section is review policy for operators: nothing in the
worker enforces it.

### Fields that need two-person review

A change to any of these fields can widen who may spend, where content goes or
which credential is sent. Require a second reviewer who did not write the
change, and compare the rendered effective configuration
(`print-effective-config`) before and after.

| Field | Risk | Applies on |
| --- | --- | --- |
| `authorization.allowed_scopes` | Grants a tenant/project pair to every namespace caller | Reload |
| `models.<name>.allowed_tenants` | An empty list removes the model's tenant restriction | Reload |
| `endpoints.<id>.auth` and every other secret reference (`kind`, `name`, `path`) | Chooses which secret is resolved and where it is sent | Reload, except process-lifetime references |
| `endpoints.<id>.base_url`, `endpoints.<id>.outbound_hosts`, new endpoints | Where requests, content and the credential go | `base_url` on reload; hosts on restart |
| `langfuse` and `endpoints.<id>.langfuse` | Exports content, raw tenant/project/actor and the Langfuse keys to `base_url` | Reload |
| `endpoints.<id>.provider_storage.permitted`, `continuation.allow_provider_hosted_state` | Lets the provider retain conversations | Reload |
| `temporal.payload_codec` | Removing it returns history to plaintext; key changes affect every caller | Restart |
| `temporal.tls.*`, `temporal.api_key_file`, `temporal.mesh_transport` | The authenticated channel that `trusted_temporal` relies on | Restart |
| `state.redis.tls`, `state.redis.service_mesh`, Redis credentials | Confidentiality of budget and result traffic | `service_mesh` on restart; others on reload |
| `server.responses_api_listen_address`, `server.responses_api_private_network` | Exposes an unauthenticated, paid API | Restart for the address |
| `environment`, `temporal.namespace`, `state.requests.secret`, `state.redis.key_secret`, `continuation.handle_keys` | Scope identity and key material; a wrong value strands or exposes state | Restart, except `handle_keys` on reload |
| `model_sync` | Which synced models are routable and who prices them | Reload |

### Fields that need spend review

`budgets` (including `require_match` and every window limit), `pricing`
catalogs, `limits` and `temporal.worker` concurrency change how much the worker
may spend or how much one request may use. Review them as a budget decision
with the owner of the affected tenants.

### Review checklist

1. Diff the effective configuration, not only the source template; templates
   can render a list as null and remove a restriction.
2. Name the tenants, endpoints and hosts the change affects, and confirm each
   is intended.
3. For any new host, confirm the data-processing agreement covers the content
   it will receive.
4. For any secret reference, confirm the referenced variable or file holds the
   secret meant for that consumer.
5. After rollout, confirm the `configuration reloaded` log line, or the startup
   log, shows the `config_version` that was reviewed.

### Proposals

These are not implemented.

- **Proposal:** sign the configuration file and have the worker verify the
  signature before applying a startup or reload, so an unreviewed file is
  rejected rather than only logged.
- **Proposal:** log the changed field paths, without values, on every accepted
  reload, so review evidence and the running configuration can be matched.
- **Proposal:** apply the provider egress policy to the Langfuse client, with
  its own `outbound_hosts`, and make `langfuse.base_url` process-lifetime.
