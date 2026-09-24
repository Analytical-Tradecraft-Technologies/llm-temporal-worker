# LLM Temporal Worker

This repository contains the contract, implementation foundation, and staged
plans for a reusable Go inference library and Temporal Activity Worker. The
implemented foundation covers the semantic request/response API, strict
configuration snapshots, official-SDK provider adapters, deterministic
one-shot-response normalization, deterministic routing, tenant-bound
continuation handles, exact pricing, and in-memory admission. The process
runtime composition now includes
reloadable snapshots, probe servers, graceful worker shutdown, TLS-safe
Temporal client wiring, verified catalog loading, provider adapters, Redis
state, and blob-backed result replay. `EngineFactory` remains an injectable
seam for tests and custom deployments; the CLI uses the production composition
by default and fails closed when its configured dependencies are unsupported.

Start with the [documentation index](docs/index.md), then follow the
[master implementation sequence](docs/superpowers/plans/2026-07-13-master-sequence.md).

The worker is one part of the ATT forecasting system. Before treating a worker
release as evidence that forecasting is operational, consult the dated
[cross-repository working-status matrix](https://github.com/victoria-hft/victoria-hft/blob/master/docs/research/ai_ach/forecast-competition-status.md).

## Private subscription-backed operator runs

`codex_cli` is an opt-in Linux provider for one explicitly approved,
operator-owned workflow, using the official Codex CLI's existing ChatGPT login.
It is not a public inference service, an automatic customer-job pool, a metered
API fallback, or a replacement for the strict API-backed production runtime.
The existing `llm.generate.v1` durable admission, operation journal, pricing,
receipt and caller-state paths remain in control. The ordinary production and
historical pilot configurations remain unchanged.

The operator must first restore a valid official login with `codex login`.
The worker never reads, copies or refreshes OAuth values itself. Keep the
existing CLI auth home private and outside repositories, images and public
endpoints; do not use the same home interactively while the private worker is
active. Worker processes serialize that home with an OS file lock.

Configure a **development** worker with durable state, matched budgets and
exactly one endpoint (no fallback), with:

For this development-only mode, a signed `resource_capacity` manifest may use
the canonical `file:///absolute/path` artifact locator matching `manifest_file`
exactly, with no authority, query or fragment. The worker still verifies the
local manifest and trust-root SHA-256 digests, Ed25519 signature, generation,
limits and routes. Production remains S3-locator-only; this does not enable a
local object store in production or relax capacity admission.

```yaml
family: codex_cli
base_url: ""
outbound_hosts: []
region: operator-declared-region
account_region: operator-declared-account-region
auth: {kind: chatgpt_cli, name: "", path: "", audience: ""}
timeout: 5m
service_classes:
  standard: {provider_value: subscription, requires_capability: ""}
capability_profile: private-codex
price_catalog: private-subscription-estimate
provider_storage: {permitted: false}
extensions: {}
codex_cli:
  private_operator_mode: true
  budget_is_estimate: true
  executable: /absolute/path/to/official/native/codex
  executable_sha256: EXACT_LOWERCASE_SHA256
  model: EXACT_APPROVED_MODEL
  auth_home: /absolute/private/existing/codex-home
  temp_root: /absolute/private/temporary-workspaces
  approved_tenant: EXACT_TENANT
  approved_root_run_id: EXACT_OPERATOR_APPROVED_ROOT
  # Optional further restriction; exact keys only, no wildcard/prefix syntax.
  approved_operation_keys: []
```

Both directories must already exist, be owned by the worker user, be mode
`0700`, and not traverse symlinks. Pin the **native ELF executable**, not the
Node/npm launcher. The supported version is `codex-cli 0.146.1`; the open
executable inode is hashed before dispatch and executed by descriptor, without
a shell. The model route must name only the approved model and tenant. The
root-run context tag must come from the trusted workflow/admission lineage,
not arbitrary customer metadata.

The SHA-verified capability catalog must describe `text`, `usage` and
`structured_output` as `native`; `tool_call` as `emulated` with transform
`codex_cli_json_tool_envelope_v1`; and `image`, `document`, `reasoning`,
`continuation` and `streaming` as `unsupported`. Virtual functions are schema-
validated JSON declarations returned for **caller execution**, not Codex native
tools. Final JSON is validated again locally. Multimodal input, provider state,
sampling/reasoning controls, native/MCP tool requests and provider extensions
are rejected before dispatch.

Codex has **no documented hard output-token cap**. A request with
`output.max_tokens` is rejected rather than silently weakened. The explicit
`budget_is_estimate` acknowledgment permits absent token caps with bounded
process duration and output bytes; token budgets are admission estimates, not
provider-enforced ceilings. Actual `turn.completed` usage is required and
settled by the existing engine. All pricing components must be explicitly
declared in the verified catalog, using family `codex_cli`, tier `subscription`,
and provenance beginning `subscription_estimate:` followed by the operator's
estimate source. Catalog-priced cost is accounting, **not an API invoice or an
assertion of incremental subscription charges**. Provider receipt facts identify
the subscription transport, CLI version/digest and cost provenance.

The pinned CLI's terminal JSONL reports usage but no model/revision metadata.
The runtime records its frozen `resolved_model` routing target with
`model_identity_basis: configured_route` and no `observed_model_revision`.
API adapters with actual model metadata instead record `provider_reported`
and that observed value separately. Neither a configured model nor the CLI
executable digest attests which model revision the provider actually ran.

Isolation uses `--ignore-user-config`, `--ignore-rules`, `--strict-config`,
`--ephemeral`, a private temporary cwd, read-only sandbox and never-approve
policy. A minimal environment excludes API keys, proxies and loader overrides.
`CODEX_EXEC_SERVER_URL=none` removes local and remote execution environments;
remaining tool-bearing features, agents, apps, plugins, MCP, hooks, project
instructions and automatic compaction are disabled. System config capable of
adding hooks/MCP is rejected. A fixed CLI-managed provider retains official
ChatGPT auth while setting request and stream retries to zero; overriding
the built-in `openai` retry keys would be ignored by this pinned release.

Missing/duplicate terminal usage, malformed JSON, model-reroute warnings,
native tool events, authentication failures and interrupted children fail
closed. Cancellation kills the process group. After possible dispatch, an
incomplete receipt is **ambiguous and never automatically replayed**, including
revoked-login failures. An operator restoring login does not authorize replay
of an uncertain operation.

Relevant official contracts:
[private noninteractive use](https://learn.chatgpt.com/docs/non-interactive-mode),
[CI/CD authentication](https://learn.chatgpt.com/docs/auth/ci-cd-auth),
[0.146.1 environment disable](https://github.com/openai/codex/blob/rust-v0.146.1/codex-rs/exec-server/src/environment_provider.rs),
[tool registration gates](https://github.com/openai/codex/blob/rust-v0.146.1/codex-rs/core/src/tools/spec_plan.rs),
[JSONL events and usage](https://github.com/openai/codex/blob/rust-v0.146.1/codex-rs/exec/src/exec_events.rs),
and [provider retry configuration](https://github.com/openai/codex/blob/rust-v0.146.1/codex-rs/model-provider-info/src/lib.rs).

Targeted offline regression commands, run only after concurrent edits land:

```sh
cd golang
go test ./llm/provider/codexcli ./config ./internal/catalog ./internal/runtime
```

## Bounded local retrospective evaluation

`golang/scripts/joined-retrospective-bedrock.py --help` describes the opt-in
archive-backed runner for Victoria's direct, fixed-ordinal, and ordinal-IRT
production workflows. Admission is restricted to the frozen `infer:1554`
question and `2025-12-07T00:00:00Z` evidence cutoff, three serial primary-only
arms, a shared durable $5 allowance, and at most $1.50 per arm. Hosted model
tools, publishing, retries, resume, model enablement, and subscriptions remain
disabled. Old no-search profiles are rejected, not relabeled as full runs.

Alongside the existing input/configuration/catalog/profile/AWS arguments,
admission requires these explicit files:

- `--historical-corpus`: exact `att_historical_corpus/v1` manifest, with genuine
  retained archive bodies at confined relative paths. All arms share identical
  corpus bytes, positive bounded research limits, and provider/fetch bindings.
- `--question-proof` and `--question-projection`: the reviewed publisher
  revision proof and exact answer-free eight-field projection. The input must
  match every field. Publisher commit metadata is not a cryptographic timestamp
  authority.
- `--model-evidence`: retained official AWS Nova 2 Lite model-card evidence,
  including its relative `source_path` and exact source hash. The admitted
  foundation revision is `amazon.nova-2-lite-v1:0`, launched 2025-12-02 with
  publisher-stated October 2025 knowledge cutoff. This is not an independent
  training-data audit or evidence of model/API capacity.
- `--resource-capacity`: task-local signed capacity bytes matching every arm
  and the worker configuration, including positive search/fetch capacity.
- `--release-attestation` and `--release-trust-root`: actual image, revision,
  source, configuration, and environment provenance. Release and capacity must
  use the same task-local signer and a distinct capacity generation; renamed
  public fixture keys are also rejected.
- `--victoria-validator`: a prebuilt Victoria `competition-worker` executable.
  `--request-timeout` (default `30s`, range `1s`–`60s`) participates in the frozen
  collection identity. The runner never builds during offline preflight.

Prepare retained sources and the corpus with honest current retrieval/assembly
times, then prepare the canonical collection identity from Victoria itself:

```sh
# From Victoria's research/ai_ach directory; preparation only, no inference.
go build -o /absolute/task-dir/competition-worker ./cmd/competition-worker
sha256sum /absolute/task-dir/corpus/manifest.json
/absolute/task-dir/competition-worker retrospective-forecast-eval \
  --historical-corpus /absolute/task-dir/corpus/manifest.json \
  --historical-corpus-sha256 EXACT_MANIFEST_SHA256 \
  --request-timeout 30s \
  --archive-proxy-url http://archive-egress:8080 \
  --print-collection-binding
```

Freeze that returned binding into all three collection profiles (`mode` maps to
`collection.evidence_mode`), compile the profiles, and regenerate the signed
capacity/configuration/release assets for the actual resulting image. Never
hand-reimplement the Go binding hash or reuse signed no-search assets. Validate
each profile with Victoria's `retrospective-forecast-eval --validate-only`,
passing `--input`, `--profile`, `--arm-id`, `--pricing-identity`, the same corpus,
SHA, timeout, and proxy flags, and `--max-questions 1 --max-concurrency 1`.

The runtime build uses the attested revision and positive-integer release
version; its `CREATED` and `SOURCE_DATE_EPOCH` come from the shared whole-second
profile `created_at`. All arms must use that same build creation time.

The runner's own `--validate-only` is explicitly offline and non-inference:
it verifies local corpus/binding through the actual Victoria helper and checks
source/model/configuration/spending admission without reading credential
contents, creating output, starting containers, or contacting AWS/archive
servers. The credential path must still be a caller-owned private regular file.
The full runner repeats real Victoria profile validation without networking
before inference dispatch, verifies the actual built image and signed runtime
identity, and performs read-only AWS existing-access/account checks.

Task-local overrides mount the exact corpus read-only. Victoria has no direct
internet route; its explicit archive proxy permits only TLS CONNECT to
`web.archive.org:443`, rejects nonpublic or mixed public/private DNS answers,
and dials the checked address. TLS verification remains end-to-end. Ambient
proxy/search fixture settings are not archive backends; archive misses never
fall back to current pages. Production compose networking is unchanged.

Archive source observation/snapshot times must precede the cutoff. Retrieval,
verification, and evidence freezing use their real current operation times;
unknown publication precision stays unknown. Historical verification age is
measured at evidence freeze, not retroactively at the cutoff, and cannot replace
source age in scientific temporal decay. Caller-declared historical seeds get
no verified-chronology rank credit and cannot establish empirical prior facts.

Each arm must retain a verified historical research trace: admitted queries,
nonempty hash/time/CDX-bound remote captures, exact graph source/document
bindings, and sealed final provenance. Direct must cite remote content;
fixed/IRT must select and assess a remote claim consumed by the final Stage2
graph/panel closure. A probability, present-but-unused manifest, or seed-only
answer cannot produce the full historical success status. Partial failures
retain artifacts and stop remaining arms without automatic retry or resume.
Neither successful transport nor a completed bounded-corpus run proves full
historical-web coverage, forecast quality, or a competitive advantage.

Input limits use the worker's conservative UTF-8-byte estimator when no exact
tokenizer is available. Size the bounded worker and frozen-model limits for
the full agent instructions, schemas and evidence, not just the question.
Increasing that finite bound does not increase the pilot's dollar allowance.
