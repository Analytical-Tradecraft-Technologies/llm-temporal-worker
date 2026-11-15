# Dependency Baseline

Toolchain checked: 2026-11-15. Module versions are not recorded here.

This document records the reviewed Go toolchain, what each direct Go module is
for, and how dependencies are upgraded. It does not record module versions:
[`golang/go.mod`](../../golang/go.mod) and `golang/go.sum` are the only source
of truth for the module graph. Provider SDKs, Temporal, storage clients, and
process wiring stay behind their package boundaries, and no provider SDK is
allowed to leak into the `llm` package.

## Toolchain

| Component | Selection | Source and notes |
| --- | --- | --- |
| Go module language | `go 1.26` | The module declares the Go 1.26 language/toolchain line. |
| Current patch at baseline | `go1.26.11` | [Go release history](https://go.dev/doc/devel/release), checked 2026-11-15. |
| Minimum bootstrap for Go 1.26 | `go1.24.6` | [Go 1.26 release notes](https://go.dev/doc/go1.26), checked 2026-07-13. |
| Local version hint | `.go-version` = `1.26.11` | CI and the container use the reviewed Go 1.26.11 patch through `actions/setup-go`. |

The architecture tests keep the patch rows above aligned with
`golang/.go-version`, so a Go patch upgrade updates this table in the same
change.

## Direct modules

The table lists each direct requirement of `golang/go.mod`, what it is used for,
and its license. It deliberately has no version column. Dependabot opens
version-only pull requests that change just `go.mod` and `go.sum`, and a copy of
the versions here would either go stale or force a documentation edit on every
one of them. To list the versions in a checkout, run this from `golang/`:

```sh
go list -m -f '{{if not (or .Main .Indirect)}}{{.Path}} {{.Version}}{{end}}' all
```

| Module | Use | License/source |
| --- | --- | --- |
| `github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/eventsourcing` | Event-sourced records behind the durable cloud state repository | Apache-2.0; [ATT repository](https://github.com/Analytical-Tradecraft-Technologies/cloud-storage) |
| `github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts` | Provider-neutral storage contracts used by the runtime cache, preparation, and recovery paths | Apache-2.0; [ATT repository](https://github.com/Analytical-Tradecraft-Technologies/cloud-storage) |
| `github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providers` | Storage provider construction from cloud state configuration | Apache-2.0; [ATT repository](https://github.com/Analytical-Tradecraft-Technologies/cloud-storage) |
| `github.com/openai/openai-go/v3` | Official OpenAI Responses and Chat Completions clients | Apache-2.0; [official repository](https://github.com/openai/openai-go) |
| `github.com/Azure/azure-sdk-for-go/sdk/azcore` | Official Azure OpenAI endpoint/auth middleware used by the Chat profile | MIT; [official repository](https://github.com/Azure/azure-sdk-for-go) |
| `github.com/Azure/azure-sdk-for-go/sdk/azidentity` | Azure workload/default credential resolution | MIT; [official repository](https://github.com/Azure/azure-sdk-for-go) |
| `github.com/anthropics/anthropic-sdk-go` | Official Anthropic Messages client | MIT; [official repository](https://github.com/anthropics/anthropic-sdk-go) |
| `github.com/aws/aws-sdk-go-v2` | AWS SDK base types and request configuration | Apache-2.0; [official repository](https://github.com/aws/aws-sdk-go-v2) |
| `github.com/aws/aws-sdk-go-v2/config` | AWS region and default credential-chain loading | Apache-2.0; [official repository](https://github.com/aws/aws-sdk-go-v2) |
| `github.com/aws/aws-sdk-go-v2/credentials` | Explicit AWS credential providers used by runtime composition | Apache-2.0; [official repository](https://github.com/aws/aws-sdk-go-v2) |
| `github.com/aws/aws-sdk-go-v2/service/bedrockruntime` | Official AWS Bedrock Runtime Converse API client | Apache-2.0; [official repository](https://github.com/aws/aws-sdk-go-v2) |
| `github.com/aws/aws-sdk-go-v2/service/dynamodb` | Official AWS DynamoDB client for regional cloud state | Apache-2.0; [official repository](https://github.com/aws/aws-sdk-go-v2) |
| `github.com/aws/aws-sdk-go-v2/service/s3` | Official AWS S3 client for production content-addressed blobs | Apache-2.0; [official repository](https://github.com/aws/aws-sdk-go-v2) |
| `github.com/aws/smithy-go` | AWS SDK transport and protocol support | Apache-2.0; [official repository](https://github.com/aws/smithy-go) |
| `github.com/santhosh-tekuri/jsonschema/v6` | Local Draft 2020-12 schema compilation and validation | MIT; [official repository](https://github.com/santhosh-tekuri/jsonschema-go) |
| `go.yaml.in/yaml/v4` | Strict YAML configuration parsing | MIT; [official repository](https://github.com/yaml/go-yaml) |
| `go.temporal.io/api` | Temporal API protocol types used by the worker boundary | MIT; [official repository](https://github.com/temporalio/api) |
| `go.temporal.io/sdk` | Temporal workflow, Activity, client, and worker registration boundary | MIT; [official releases](https://github.com/temporalio/sdk-go/releases) |
| `github.com/google/uuid` | UUIDv7 identifiers for durable repository records | BSD-3-Clause; [official repository](https://github.com/google/uuid) |
| `github.com/prometheus/client_golang` | Bounded Prometheus worker/activity metrics exposition | Apache-2.0; [official repository](https://github.com/prometheus/client_golang) |
| `github.com/prometheus/client_model` | Prometheus metric model types used by tests and exposition boundaries | Apache-2.0; [official repository](https://github.com/prometheus/client_model) |
| `github.com/prometheus/common` | Prometheus exposition and helper types | Apache-2.0; [official repository](https://github.com/prometheus/common) |
| `go.opentelemetry.io/otel` | OpenTelemetry API for sanitized spans | Apache-2.0; [official repository](https://github.com/open-telemetry/opentelemetry-go) |
| `go.opentelemetry.io/otel/sdk` | OpenTelemetry tracer provider and exporter lifecycle | Apache-2.0; [official repository](https://github.com/open-telemetry/opentelemetry-go) |
| `go.opentelemetry.io/otel/trace` | OpenTelemetry trace types used at instrumentation boundaries | Apache-2.0; [official repository](https://github.com/open-telemetry/opentelemetry-go) |
| `go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc` | Official OTLP/gRPC trace exporter used by the bounded telemetry lifecycle | Apache-2.0; [official repository](https://github.com/open-telemetry/opentelemetry-go) |
| `github.com/redis/go-redis/v9` | Official Redis client for atomic admission Functions and immutable state records | BSD-2-Clause; [official repository](https://github.com/redis/go-redis) |
| `google.golang.org/grpc` | OTLP transport and Temporal SDK gRPC connectivity | Apache-2.0; [official repository](https://github.com/grpc/grpc-go) |
| `google.golang.org/protobuf` | Sizing recorded Temporal history events during generation recovery | BSD-3-Clause; [official repository](https://github.com/protocolbuffers/protobuf-go) |

The table intentionally does not enumerate indirect requirements. The schema
and YAML modules are in active use by the schema and configuration packages,
while provider SDKs remain outside the provider-neutral `llm` API. A change
that adds or removes a direct requirement updates this table and, for a module
outside an already reviewed root, the supply-chain policy below. The
documentation tests check that the table names exactly the direct requirements
in `go.mod` and carries no versions; a version-only update never fails them.

## Upgrade policy

### Routine module updates

Dependabot checks `golang/go.mod` daily with a three-day cooldown (see
[`.github/dependabot.yml`](../../.github/dependabot.yml)). Each pull request
covers one module or one group of closely related modules (OpenTelemetry, AWS,
Azure, Prometheus, Temporal, cloud-storage), and the owner queues them by hand.
Version-only updates need no documentation or policy edit. When an update
changes relevant behavior, re-check the affected source contract,
capability/price fixtures, wire fixtures, and retry/error/stream/usage
assertions in the same pull request.

### Temporal Go SDK cadence

`go.temporal.io/sdk` and `go.temporal.io/api` move together in Dependabot's
`temporal` group, so the SDK and its protocol types do not diverge.

- Patch releases are taken as Dependabot proposes them, once the pull request
  is green.
- Minor releases are taken within one month of publication, so the worker is at
  most one minor release behind. Read the
  [release notes](https://github.com/temporalio/sdk-go/releases) of every
  version the update spans, looking for changed workflow command sequences, new
  default SDK flags, and deprecations.
- A major release, or any upgrade that changes the commands the workflows
  produce, is a planned change: keep existing histories replaying with workflow
  versioning and record new histories rather than editing existing ones.

The replay gate is the set of histories recorded from a real Temporal server in
`golang/workflows/testdata/replay`. `TestRecordedHistoriesReplay` and the
pinned-limit replay tests in `golang/workflows/replay_history_test.go` replay
them as part of `make test`. Pull request CI also runs
`make cloud-workflow-integration` (the Go worker against a real Temporal server
and Redis) and `make cloud-workflow-ocaml-integration` (the OCaml clients and
child workflows against the same worker), so a Temporal SDK pull request
exercises both languages before it merges. The local Temporal server image in
`golang/compose.yaml` is a separate Dependabot Docker update.

### ocaml-temporal commit pin

The OCaml package depends on
[ocaml-temporal](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal)
(opam package `temporal-sdk`) at one full 40-character commit, not a branch or
tag. No bot proposes updates to it. Re-evaluate the pin when ocaml-temporal
lands a change this package needs (for example
[ocaml-temporal#496](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/496),
direct TLS and authentication), and otherwise at least once a quarter.

[`scripts/check-ocaml-temporal-pin.sh`](../../scripts/check-ocaml-temporal-pin.sh)
runs in the `OCaml package` CI job. It fails unless the package metadata, the
README install recipe, both CI workflows, and the Cargo prefetch helper name the
same commit of the canonical repository. To move the pin:

1. Pick the target commit on ocaml-temporal's default branch. Review the diff
   since the current pin, including `rust/Cargo.lock` (the Temporal Core
   revision it builds) and any change to the API that
   `ocaml/llm_temporal_worker/lib` uses.
2. Replace the old commit everywhere it appears. `git grep -l <old-commit>`
   lists the copies:
   - `ocaml/llm_temporal_worker/llm-temporal-ocaml.opam` (`pin-depends`)
   - `ocaml/llm_temporal_worker/README.md` (install recipe)
   - `.github/workflows/pull-request.yml` and `.github/workflows/master.yml`
     (`opam pin add` steps)
   - `scripts/ci/prefetch-temporal-sdk-cargo.sh` (`temporal_sdk_commit`)
   - `ocaml_readme_install_test.go`, `ocaml_temporal_pin_test.go`, and
     `prefetch_temporal_sdk_cargo_test.go` in
     `golang/internal/architecturetest`, which pin the same value
3. Run `bash scripts/check-ocaml-temporal-pin.sh` from the repository root and
   `go test ./internal/architecturetest` from `golang/`.
4. Update the README if the new commit changes what the client supports, such
   as the transport-security limits.
5. Open the pull request. The `OCaml package` job builds and tests the package
   against the new pin and runs `make cloud-workflow-ocaml-integration`; the
   `OCaml README install` job runs the README recipe verbatim on a fresh switch.
   To build locally first, use an isolated `OPAMROOT` with the switch and
   opam-repository commit from `scripts/ci/setup-opam.sh`.

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
gate. The preflight also scans the release tag commit, on the Go toolchain the
tag pins, and evaluates that output with `supplychainverify -release-tag`. That
mode applies the same per-finding exception rules against this baseline, but
does not require every exception to be used, because an older tag can lack a
finding that the current baseline covers. See the
[release runbook](../release/runbook.md#guarded-manual-publication-boundary).

The race job captures Go test output before publishing it and runs the bounded
test-output scanner against that file. It rejects project-specific provider
payload leakage and credential-like values; raw output is shown only after the
scan passes. Captured output is bounded at 16 MiB and scanned through its tail;
output above that bound fails closed. Generic checked-in secret discovery is
delegated to GitHub secret scanning, which GitHub enables for public repositories. The local scanner still
supports an explicit `-root` for focused manual investigation, but CI does not
maintain a second generic-secret ruleset.

### Active vulnerability exceptions

The baseline currently records one approved exception:

| Finding | Owner | Expiry | Remediation | Accepted trace scope |
| --- | --- | --- | --- | --- |
| `GO-2026-5932` | `platform-security` | `2027-01-15T00:00:00Z` | [Go vulnerability entry](https://pkg.go.dev/vuln/GO-2026-5932) | `module_only` |

This `module_only` exception accepts only the finding's single module/version
trace frame. It does not accept a reachable package or function trace, and it
does not suppress unrelated findings. The verifier requires the finding to be
present and rejects the exception after its expiry; remove or update this
entry when remediation is complete.

`GO-2026-5932` reports that `golang.org/x/crypto/openpgp` is unmaintained. It
affects every `golang.org/x/crypto` version, so no upgrade remediates it, and
the worker does not import any `openpgp` package. It was re-reviewed on
2026-10-05 and extended for one quarter. Re-review it before the expiry above
and remove it if the module drops the package or a fixed version is published.
The documentation test keeps this table in step with `baseline.json`.

## Repository module

| Field | Value |
| --- | --- |
| Module path | `github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang` |
| API contract | `llm.temporal/v1` |
| Default SDK retries | Disabled at the unified adapter boundary; retry policy is owned by the routing/Temporal layer and must be recorded per attempt |
| Domain license | Apache-2.0 (repository `LICENSE`) |
