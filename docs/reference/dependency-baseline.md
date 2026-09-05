# Dependency Baseline

Recorded: 2026-08-20

This baseline records the toolchain and the direct dependency versions checked
into `go.mod`. The implementation layers that own these dependencies have
landed; this document is now an upgrade reference rather than a plan for adding
the modules. Provider SDKs, Temporal, storage clients, and process wiring stay
behind their package boundaries, and no provider SDK is allowed to leak into
the `llm` package.

## Toolchain

| Component | Selection | Source and notes |
| --- | --- | --- |
| Go module language | `go 1.26` | The module declares the Go 1.26 language/toolchain line. |
| Current patch at baseline | `go1.26.7` | [Go release history](https://go.dev/doc/devel/release), checked 2026-08-20. |
| Minimum bootstrap for Go 1.26 | `go1.24.6` | [Go 1.26 release notes](https://go.dev/doc/go1.26), checked 2026-07-13. |
| Local version hint | `.go-version` = `1.26.7` | CI and the container use the reviewed Go 1.26.7 patch through `actions/setup-go`. |

## Direct modules

| Module | Selected release | Use | License/source |
| --- | --- | --- | --- |
| `github.com/openai/openai-go/v3` | `v3.50.0` | Official OpenAI Responses and Chat Completions clients | Apache-2.0; [official repository](https://github.com/openai/openai-go) |
| `github.com/Azure/azure-sdk-for-go/sdk/azcore` | `v1.22.0` | Official Azure OpenAI endpoint/auth middleware used by the Chat profile | MIT; [official repository](https://github.com/Azure/azure-sdk-for-go) |
| `github.com/Azure/azure-sdk-for-go/sdk/azidentity` | `v1.14.0` | Azure workload/default credential resolution | MIT; [official repository](https://github.com/Azure/azure-sdk-for-go) |
| `github.com/anthropics/anthropic-sdk-go` | `v1.61.0` | Official Anthropic Messages client | MIT; [official repository](https://github.com/anthropics/anthropic-sdk-go) |
| `github.com/aws/aws-sdk-go-v2` | `v1.43.3` | AWS SDK base types and request configuration | Apache-2.0; [official repository](https://github.com/aws/aws-sdk-go-v2) |
| `github.com/aws/aws-sdk-go-v2/config` | `v1.32.34` | AWS region and default credential-chain loading | Apache-2.0; [official repository](https://github.com/aws/aws-sdk-go-v2) |
| `github.com/aws/aws-sdk-go-v2/credentials` | `v1.19.33` | Explicit AWS credential providers used by runtime composition | Apache-2.0; [official repository](https://github.com/aws/aws-sdk-go-v2) |
| `github.com/aws/aws-sdk-go-v2/service/bedrockruntime` | `v1.57.0` | Official AWS Bedrock Runtime Converse API client | Apache-2.0; [official repository](https://github.com/aws/aws-sdk-go-v2) |
| `github.com/santhosh-tekuri/jsonschema/v6` | `v6.0.3` | Local Draft 2020-12 schema compilation and validation | MIT; [official repository](https://github.com/santhosh-tekuri/jsonschema-go) |
| `go.yaml.in/yaml/v4` | `v4.0.0-rc.6` | Strict YAML configuration parsing | MIT; [official repository](https://github.com/yaml/go-yaml) |
| `go.temporal.io/api` | `v1.63.4` | Temporal API protocol types used by the worker boundary | MIT; [official repository](https://github.com/temporalio/api) |
| `go.temporal.io/sdk` | `v1.47.0` | Temporal Activity payload, heartbeat, error, and worker registration boundary | MIT; [official release](https://github.com/temporalio/sdk-go/releases/tag/v1.47.0) |
| `github.com/aws/aws-sdk-go-v2/service/s3` | `v1.106.3` | Official AWS S3 client for production content-addressed blobs | Apache-2.0; [official repository](https://github.com/aws/aws-sdk-go-v2) |
| `github.com/aws/smithy-go` | `v1.27.6` | AWS SDK transport and protocol support | Apache-2.0; [official repository](https://github.com/aws/smithy-go) |
| `github.com/google/uuid` | `v1.6.0` | UUIDv7 identifiers for durable repository records | BSD-3-Clause; [official repository](https://github.com/google/uuid) |
| `github.com/jackc/pgx/v5` | `v5.10.0` | PostgreSQL pool, transactions, and typed durable repositories | MIT; [official repository](https://github.com/jackc/pgx) |
| `github.com/prometheus/client_golang` | `v1.24.1` | Bounded Prometheus worker/activity metrics exposition | Apache-2.0; [official repository](https://github.com/prometheus/client_golang) |
| `github.com/prometheus/client_model` | `v0.6.2` | Prometheus metric model types used by tests and exposition boundaries | Apache-2.0; [official repository](https://github.com/prometheus/client_model) |
| `github.com/prometheus/common` | `v0.70.1` | Prometheus exposition and helper types | Apache-2.0; [official repository](https://github.com/prometheus/common) |
| `go.opentelemetry.io/otel`, `/sdk`, and `/trace` | `v1.45.0` | Sanitized OpenTelemetry spans and exporter lifecycle | Apache-2.0; [official repository](https://github.com/open-telemetry/opentelemetry-go) |
| `go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc` | `v1.45.0` | Official OTLP/gRPC trace exporter used by the bounded telemetry lifecycle | Apache-2.0; [official repository](https://github.com/open-telemetry/opentelemetry-go) |
| `github.com/redis/go-redis/v9` | `v9.21.0` | Official Redis client for atomic admission Functions and immutable state records | BSD-2-Clause; [official repository](https://github.com/redis/go-redis) |
| `google.golang.org/grpc` | `v1.83.0` | OTLP transport and Temporal SDK gRPC connectivity | Apache-2.0; [official repository](https://github.com/grpc/grpc-go) |

The versions in this table match the direct requirements in `go.mod` on the
recorded date. The table intentionally does not enumerate indirect requirements;
`go.mod` and `go.sum` are authoritative for the complete module graph. The
schema and YAML modules are in active use by the schema and configuration
packages, while provider SDKs remain outside the provider-neutral `llm` API.

This table is a dated explanatory snapshot, not a CI allowlist. Version-only
updates do not require a matching documentation or policy edit. Re-check the
affected source contract, capability/price fixtures, wire fixtures, and
retry/error/stream/usage assertions when an update changes relevant behavior.

## Verified supply-chain gate

[`golang/tools/supplychainverify/baseline.json`](../../golang/tools/supplychainverify/baseline.json)
is the machine-readable dependency policy. It records approved module roots,
SPDX identifiers, and source references without pinning versions. A new direct
module must be ATT-owned or fall under a reviewed, well-known module root;
version changes and new submodules from an already reviewed root do not require
policy churn. Release evidence combines this policy with the current `go.mod`,
so it still records the exact versions that were built.
The Go vulnerability scan still analyzes reachable code across the complete
module graph, including indirect dependencies. A baseline exception does not
narrow that scanner input; it only limits which already-reported trace may be
accepted after the scan.

The module tracks `govulncheck` at `v1.7.0` as a Go tool and runs it with the
reviewed Go toolchain selected by CI. Its JSON parser retains unique
`GO-*` finding identifiers and a fail-closed internal trace scope without
retaining raw trace data. A `vulnerability_exceptions` entry is valid only when
it gives an identified finding, owner, future RFC 3339 expiry, HTTPS remediation reference,
and a `scope` of either `module_only` or `reachable`. A `module_only` exception
accepts only the single module/version trace frame; a later reachable package or
function trace fails until the baseline is explicitly reviewed as `reachable`.
Expired, incomplete, unused, or unlisted exceptions fail the scheduled full
gate, so the policy cannot conceal a current result.

Pull requests run `make security-pr-verify`, scanning the base and head with the
same pinned tool in one job. Unchanged findings do not block the PR;
a new finding or a change from module-only to reachable fails unless a reviewed
exception covers its scope. GitHub's Dependency Review Action independently
reports changed dependencies, blocks newly added moderate-or-higher advisories,
and warns when a new dependency has an OpenSSF Scorecard below 5. A separate
scheduled workflow runs `make security-verify` against current `master` and
enforces the full exception inventory. Routine master builds do not run either
PR comparison or the scheduled scan; guarded release preflight retains the full
gate.

The race job captures Go test output before publishing it and runs the bounded
test-output scanner against that file. It rejects project-specific provider
payload leakage and credential-like values; raw output is shown only after the
scan passes. Generic checked-in secret discovery is delegated to GitHub secret
scanning, which GitHub enables for public repositories. The local scanner still
supports an explicit `-root` for focused manual investigation, but CI does not
maintain a second generic-secret ruleset.

### Active vulnerability exceptions

The baseline currently records one approved exception:

| Finding | Owner | Expiry | Remediation | Accepted trace scope |
| --- | --- | --- | --- | --- |
| `GO-2026-5932` | `platform-security` | `2026-08-14T00:00:00Z` | [Go vulnerability entry](https://pkg.go.dev/vuln/GO-2026-5932) | `module_only` |

This `module_only` exception accepts only the finding's single module/version
trace frame. It does not accept a reachable package or function trace, and it
does not suppress unrelated findings. The verifier requires the finding to be
present and rejects the exception after its expiry; remove or update this
entry when remediation is complete.

## Repository module

| Field | Value |
| --- | --- |
| Module path | `github.com/mfow/llm-temporal-worker` |
| API contract | `llm.temporal/v1` |
| Default SDK retries | Disabled at the unified adapter boundary; retry policy is owned by the routing/Temporal layer and must be recorded per attempt |
| Domain license | Apache-2.0 (repository `LICENSE`) |
