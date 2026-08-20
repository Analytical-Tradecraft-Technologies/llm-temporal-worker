# Guarded Live Provider Contracts

`golang/integration/live` is a small, credentialed contract suite for facts that
offline fixtures cannot establish: authentication, provider wire acceptance,
the reported actual service class, provider request and response IDs, usage,
reported cost, and continuation behavior. It is excluded from ordinary tests
by the `live` build tag.

The suite is intentionally fail-closed. A real invocation is possible only
when all three exact environment values equal `"1"`:

1. `LLMTW_LIVE_TESTS=1`
2. `LLMTW_LIVE_AUTHORIZED=1`
3. the selected profile's enable flag from the table below

All other values, including common truthy spellings, leave the profile
disabled. Gate evaluation happens before the harness constructs an adapter or
looks up credentials. The protected manual
[live-provider workflow](../../.github/workflows/live-provider-contracts.yml)
sets the first two values only after environment approval, sets only the
selected profile flag, and injects only that profile's scoped credentials.
Fork pull requests, ordinary pull requests, and scheduled workflows must not
set any of these flags or receive these credentials.

## Offline contract safety check

This command compiles and executes the deterministic contract checks without
making a provider request:

```sh
cd golang && make live-contract-verify
```

The target forces every live gate to `0` before running the build-tagged suite.
The credentialed `TestLiveProviderContracts` subtests therefore skip, while
profile declarations, request bounds, authorization ordering, adapter
preflights, and redacted-evidence validation still execute. Both checked-in CI
workflows run this command without live environment values or provider
credentials; on master this includes the daily scheduled run. A protected
manual run selects one profile by adding its flag and the two suite gates; it
runs `TestLiveProviderContracts` with scoped credentials. The harness does not
read a provider's environment variables until those gates pass.

## Profiles

Every profile uses tenant `llmtw-live-contract`, the exact prompt `Reply with
exactly: live-contract-ok`, a maximum output of 8 tokens, and a maximum actual
reported spend of 25,000 microUSD (USD 0.025). Models, protocols, and base
endpoints are checked in rather than supplied by an environment variable.

| Profile | Protocol and pinned model | Profile gate | Credential source and additional configuration | Continuation |
| --- | --- | --- | --- | --- |
| `openai-responses` | OpenAI Responses; `gpt-4.1-mini` | `LLMTW_LIVE_OPENAI_RESPONSES` | `OPENAI_API_KEY`; OpenAI v1 endpoint | Pinned |
| `azure-responses` | Azure OpenAI Responses; `gpt-4.1-mini` | `LLMTW_LIVE_AZURE_RESPONSES` | Azure Default Credential; `LLMTW_LIVE_AZURE_OPENAI_ENDPOINT` must be an HTTPS Azure OpenAI resource endpoint (`*.openai.azure.com`, `*.openai.azure.us`, or `*.openai.azure.cn`) | Pinned |
| `openai-chat` | OpenAI Chat Completions; `gpt-4.1-mini` | `LLMTW_LIVE_OPENAI_CHAT` | `OPENAI_API_KEY`; OpenAI v1 endpoint | Rejected before invocation |
| `openrouter-chat` | OpenRouter Chat Completions; `openai/gpt-4.1-mini` | `LLMTW_LIVE_OPENROUTER_CHAT` | `OPENROUTER_API_KEY`; OpenRouter API endpoint | Rejected before invocation |
| `exa-chat` | Exa Chat; `exa` | `LLMTW_LIVE_EXA_CHAT` | `EXA_API_KEY`; Exa API endpoint | Rejected before invocation |
| `anthropic-direct` | Anthropic Messages; `claude-3-5-haiku-latest` | `LLMTW_LIVE_ANTHROPIC_DIRECT` | `ANTHROPIC_API_KEY`; Anthropic API endpoint | Conditional: pinned only when replayable opaque state is returned |
| `anthropic-aws` | Anthropic Messages through AWS; `claude-3-5-haiku-latest` | `LLMTW_LIVE_ANTHROPIC_AWS` | AWS default credential chain; `LLMTW_LIVE_ANTHROPIC_AWS_WORKSPACE_ID` | Conditional: pinned only when replayable opaque state is returned |
| `bedrock-anthropic` | Amazon Bedrock Anthropic; `anthropic.claude-3-5-haiku-20241022-v1:0` | `LLMTW_LIVE_BEDROCK_ANTHROPIC` | AWS default credential chain; default AWS SDK endpoint resolution | Conditional: pinned only when replayable opaque state is returned |
| `bedrock-converse` | Amazon Bedrock Converse; `amazon.nova-pro-v1:0` | `LLMTW_LIVE_BEDROCK_CONVERSE` | AWS default credential chain; default AWS SDK endpoint resolution | Rejected before invocation; response ID not reported by API |

The request deliberately omits the public service class. The worker must
normalize that omission to `standard`. Before invocation, the harness checks
that the compiled call still carries the pinned endpoint, protocol family,
model, operation key, normalized `standard` class, and a non-empty provider
tier. A provider-internal value such as `default` is an explicit mapping for
the public `standard` class; it is never permission to use an unspecified
provider default.

For profiles that do not support continuations, the harness first compiles a
pinned-continuation request and requires the adapter to reject it before any
provider invocation. Profiles marked `Pinned` must return a non-empty
continuation handle pinned to the selected endpoint and model. Profiles marked
`Conditional` may omit a continuation for a text-only response; if they return
one, it must contain at least one replayable provider-state record and a
non-empty handle pinned to the selected endpoint and model.

## Evidence and cost handling

A successful live test emits only the following release-evidence fields:

- the exact lowercase 40-character `source_revision` candidate checked out by
  the protected workflow;
- profile and fixed tenant;
- provider request and response IDs;
- actual normalized service class, always `standard` for this omitted-class
  probe;
- whether actual spend was reported, its microUSD amount when known, and the
  provider cost method;
- whether a returned pinned continuation was verified.

It never logs the prompt, model output, raw endpoint URL, credential, raw
provider payload, or raw SDK error. The test uses `provider.NopObserver{}` so
it does not add request tracing that could capture these values.

Responses must report positive input and output token usage, valid actual
`standard` service class, a request ID, and a completed status. Providers that
do not expose a response ID (currently Bedrock Converse) record
`response_id=not_reported`; the harness never manufactures one.
When a provider reports cost, it must be USD, name one of the closed reported
methods (`provider_reported`, `usage`, `openrouter_reported`, or
`exa_reported`), and be at or below the fixed ceiling. When cost is not
reported, the evidence records `cost_method=not_reported`; the suite does not
infer a number from a pricing catalog. OpenRouter and Exa are allowed to omit a
cost status only when the reported actual cost fields themselves are valid. A
live failure never updates capabilities, price catalogs, limits, or fixtures.

## Manual protected workflow

`.github/workflows/live-provider-contracts.yml` is separate from the guarded
publication workflow. It has only a `workflow_dispatch` trigger and rejects a
dispatch that is not started from `master`. The required `profile` choice is
closed to the nine checked-in profiles above. Its credential-free
`validate-request` job verifies that choice before any protected,
secret-bearing profile job is eligible to run. Each profile then has a static
job condition, so one dispatch can run at most one bounded provider probe.

Every profile job names the `live-provider-contracts` protected environment.
Repository administrators must configure that environment with required human
approval before enabling any live run. A direct provider API key is present
only in the selected test step; Azure and AWS profiles use their corresponding
workload-identity action and non-secret repository variables. The checkout
happens before either kind of provider credential is acquired: it anonymously
fetches the fixed public HTTPS repository URL, requires the fetched `master`
to equal the protected workflow's `github.sha`, and checks out that SHA. It
never receives a provider credential.

The selected test writes its raw output only to `$RUNNER_TEMP`. Before the
recorder or artifact uploader can run, the workflow clears credential
variables. The recorder itself starts with `env -i`, reduces the raw log to the
closed [redacted evidence schema](../release/live-provider-contract.schema.json),
then verifies that evidence against the same `github.sha` candidate used for
the anonymous checkout before uploading only its JSON record and allowlisted
text log for 14 days. The candidate is a full lowercase Git SHA recorded as
`source_revision`; it is not inferred from mutable branch state. A recorder or
test failure leaves no artifact upload path and is never retried automatically.

No pull-request, fork, scheduled, master-push, signing, registry-publication,
tagging, or release-creation path invokes `TestLiveProviderContracts`. The
separate [guarded publication workflow](../../.github/workflows/release.yml)
remains responsible for its own release-evidence and external-publication
boundary; a successful live-provider contract is not evidence that a release
was signed or published.
