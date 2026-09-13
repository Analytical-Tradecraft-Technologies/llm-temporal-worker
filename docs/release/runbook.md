# Release evidence runbook

Task 23 produces a local, machine-readable release-evidence bundle. The
`master.yml` evidence job remains nonpublishing: it never sends an image to a
registry, obtains provider credentials, or calls a live LLM provider. The
separate protected `release.yml` workflow can publish only the exact digest
already bound by successful master evidence and a protected release tag.

This bundle proves only the worker revision and boundaries it names. It does not
prove the joined ATT forecast path, production deployment, or external
activation; those states are tracked in the dated
[cross-repository forecast working-status matrix](https://github.com/victoria-hft/victoria-hft/blob/master/docs/research/ai_ach/forecast-competition-status.md).

## Trusted boundary

The `release-evidence` job runs only after `verify` on a `push` to `master`.
Pull-request, scheduled, and manual workflow executions do not collect or
retain this bundle. The job has only `contents: read` permission.

The job builds one Linux OCI layout directory at `$RUNNER_TEMP/image.oci`,
outside `release-artifacts/`, runtime-tests the image loaded by the same Buildx
solve, then obtains the immutable image subject from the layout's single OCI
manifest descriptor:

```sh
digest="$(go -C golang run ./tools/releaseverify layout-digest -layout "$RUNNER_TEMP/image.oci")"
reference="llm-temporal-worker@$digest"
```

The descriptor, rather than a local image ID or mutable tag, is the source of
truth. The retained CycloneDX SBOM and Trivy JSON scan are both bound to the
same `reference` and `digest`; the verifier rejects stale or mismatched
subjects. The raw OCI layout directory is CI-temporary only: the descriptor,
Syft, and Trivy consume that exact directory; it is never recorded or uploaded
and is removed after artifact upload. The Buildx action explicitly pins Buildx
`v0.16.2` and BuildKit `v0.16.0` by immutable image digest, so its two
same-solve exporters (`type=oci,tar=false` and `--load`) meet Docker's
multi-exporter capability requirement without a second build. Trusted CI uses
explicit Syft `v1.44.0` and Trivy `v0.72.0` inputs, with the checked-in
[Trivy configuration](../../scripts/release/trivy.yaml) applied to that
temporary directory.

Both Dockerfile roots retain readable reviewed versions and immutable manifest
digests. The build rejects any platform other than `linux/amd64`, compiles a
static Go binary with `CGO_ENABLED=0`, and copies only that binary into the
digest-pinned distroless final stage. The final image has numeric
`USER 65532:65532`, no shell, package manager, or Go toolchain, and a native
JSON `ENTRYPOINT`. Version, full source revision, credential-free source URL,
commit timestamp, and reviewed Go version are explicit build inputs and OCI
labels. The master container job loads that exact image, then
`scripts/release/smoke-image.sh` runs its real `worker --config` process under
the read-only Compose security contract and exercises `/health/live` and
`/health/ready`. The fixture builds only a loopback provider mock and requires
no live provider credential or request.

Trivy reports only actionable `HIGH` and `CRITICAL` fixed vulnerabilities, and
the release verifier fails closed if any are present. Go/package findings use
the repository-standard reviewed exceptions in
`golang/tools/supplychainverify/baseline.json`: every exception requires an
owner, future expiry, remediation URL, and trace scope; stale, unused, newly
reported, or scope-widened findings fail. Final-image findings are not silently
suppressed by that source exception list.

Before compact manifest collection, trusted CI installs `kubectl v1.32.6` with
the immutable `azure/setup-kubectl` `v4.0.1` action commit. The collector
checks that exact client version before using `kubectl kustomize`; it has no
cluster configuration and only renders the checked-in local manifests.

## Bundle contents

The record follows [the evidence schema](evidence.schema.json). It binds every
retained artifact to a byte count and SHA-256 digest, and requires a redaction
assertion for:

- compact test, race, deterministic fuzz, and in-memory admission/compilation
  benchmark summaries;
- an optional redacted worker-origin error-rate summary, when a protected
  operator supplies a bounded Prometheus snapshot; trusted CI has no production
  metrics access and omits this artifact by default;
- fixture manifest records with upstream source dates;
- compact Redis, Temporal, and Compose health summaries from the local test
  stack;
- three redacted boundary-log records derived from actual `docker compose logs`
  output for Redis, Temporal, and the combined stack;
- rendered Kubernetes manifest digests and object counts;
- dependency/license inventory and the existing redacted vulnerability result;
- the CycloneDX SBOM and matching Trivy scan, each bound to the immutable
  descriptor subject.

The bundle has a closed, root-level filename set: every artifact uses the
canonical name shown below and the record is always `evidence.json`. Renamed,
nested, unreferenced, or symlinked files are rejected. A local caller may point
the verifier at a different artifact directory, but not at a different record
filename within that directory.

Repository, fixture, and dependency provenance URLs must use an absolute HTTPS
URL with a DNS hostname and no explicit port, userinfo, backslashes, query
strings, or fragments. This keeps credentials and mutable URL decorations out
of retained release evidence.

Command output, scanner diagnostics, the raw OCI layout directory, credentials,
request content, and provider payloads stay in private runner temporary
storage. Raw service output also stays there: the three retained log records
contain only fixed allowlisted event counts (`allowlist-v1`), plus the observed
line and byte counts. This preserves proof that Redis and Temporal each emitted
a safe runtime-boundary event, including both families in the combined Compose
record, without retaining arbitrary message text. The verifier scans every
retained artifact for secret-like values and rejects unsafe paths, symlinks,
residual raw OCI directories or archives, unreferenced files, digest or
byte-count changes, incomplete fixture records, mismatched image subjects, and
HIGH or CRITICAL image findings.

Every retained JSON document and every OCI JSON metadata payload must also have
exactly one member with a given name in each object. Duplicate members are
rejected before schema or semantic decoding, including escaped spellings of the
same member name, so evidence has one deterministic interpretation across the
verifier, scanners, and review tooling. The same member name may still appear
in separate objects.

The `benchmark-summary.json` artifact records one exact
`BenchmarkGenerateMemoryAdmissionAndCompile` measurement with its sample count,
`ns/op`, and sampled `p99_ms/op`. The collector fails closed unless the sampled
memory p99 is strictly below the documented 25 ms target and records
`target_status: "pass"`. Its `scope` is `memory`; `objective_status` remains
`measurement_only` because the 75 ms same-region Redis counterpart is not
measured by this workflow. The Redis measurement remains an explicitly
authorized operator run against a same-region deployment and is never replaced
by this artifact.

The optional `worker-error-summary.json` artifact is produced only when a
caller passes `--worker-error-metrics /path/to/metrics.prom` to
`scripts/release/collect.sh`. The collector invokes the strict Prometheus
snapshot parser in `scripts/release/slo-evidence.py` and retains only bounded
completed and worker-failed attempt counts. The raw snapshot remains outside
the artifact directory. The release verifier accepts this artifact when
present, but neither the master workflow nor the v1 catalog treats it as a
recorded production SLO: the summary is marked `objective_status:
measurement_only`, and the catalog remains `unrecorded` until an independently
reviewed protected measurement is bound.

The verifier remains compatible with retained schema-v1 bundles recorded before
this benchmark was added: those bundles may omit `benchmark-summary.json`. New
recordings are stricter and the recorder requires the benchmark artifact, so all
new evidence captures the measurement without invalidating historical bundles.
Retained bundles that include the benchmark but predate `target_status` are also
accepted through the `benchmark_summary_legacy` schema branch. That compatibility
branch is verify-only; newly collected summaries always use the current shape,
and only current summaries assert the strict p99 target in the schema.

## Collect, record, and verify

The trusted job first produces the compact summaries and a temporary OCI layout
directory outside the retained directory:

```sh
oci_layout="$RUNNER_TEMP/image.oci"
bash scripts/release/collect.sh \
  --artifact-dir release-artifacts \
  --image-oci-layout "$oci_layout"
```

The collector rejects a layout path inside `release-artifacts/`. It then
generates the SBOM and scan from the temporary directory, binds both to the
descriptor subject above, and records the completed bundle. A local caller
must supply a temporary layout path outside its evidence directory and the two
final JSON files before it can record the exact same bundle:

```sh
digest="$(go -C golang run ./tools/releaseverify layout-digest -layout "$oci_layout")"
reference="llm-temporal-worker@$digest"

bash scripts/release/record.sh \
  -artifact-dir release-artifacts \
  -output release-artifacts/evidence.json \
  -repository https://github.com/mfow/llm-temporal-worker \
  -revision "$(git rev-parse HEAD)" \
  -image-reference "$reference" \
  -image-digest "$digest" \
  -artifact test_summary=test-summary.json \
  -artifact race_summary=race-summary.json \
  -artifact fuzz_summary=fuzz-summary.json \
  -artifact benchmark_summary=benchmark-summary.json \
  -artifact fixture_manifest=fixture-manifest.json \
  -artifact redis_summary=redis-summary.json \
  -artifact temporal_summary=temporal-summary.json \
  -artifact compose_summary=compose-summary.json \
  -artifact redis_log=redis-log.json \
  -artifact temporal_log=temporal-log.json \
  -artifact compose_log=compose-log.json \
  -artifact rendered_manifests=rendered-manifests.json \
  -artifact dependency_license=dependencies.json \
  -artifact vulnerability_results=vulnerabilities.json \
  -artifact sbom=sbom.cdx.json \
  -artifact image_scan=image-scan.json
```

The recorder validates the entire bundle before atomically writing
`evidence.json`; a failed validation leaves no final evidence record. Validate
an already recorded local bundle with:

```sh
make release-verify
```

To validate another local bundle, invoke the verifier directly (the Make
target deliberately seals the trusted path to `release-artifacts/evidence.json`):

```sh
bash scripts/release/verify.sh \
  --artifact-dir /path/to/release-artifacts \
  --evidence /path/to/release-artifacts/evidence.json
```

After successful verification the master-push job retains the redacted bundle
for 14 days and removes `$RUNNER_TEMP/image.oci`. It never produces or
retains `image.oci.tar`, never uses `oci-archive:`, and never uses
`docker load --input`; the unretained directory is the only raw OCI subject.
`release-artifacts/` is ignored by Git and excluded from the Docker build
context.

## Catalog-bound offline traceability record

The v1 catalog is intentionally pinned to this retained `release-evidence`
artifact: [workflow run `30795754773`](https://github.com/mfow/llm-temporal-worker/actions/runs/30795754773) at revision
`1aae4ff2095a5adaf3fee13aa696d78d73616ddd`. The artifact is named
`release-evidence` (artifact `8849743942`) and has SHA-256 digest
`sha256:2bf49d063dbc09b2e8f183e6f792086c4fd0f4157a46a05ccdde68a472331a0a`.
The retained bundle binds the immutable image descriptor
`sha256:bd95d9d7bbd81582ed3f7ec18131a5e31106dee9152de9373f7676dbc3806f27`.
The catalog refresh binds the thirteen already-recorded offline requirements
to this successful master run. Pending protected-provider and publication
requirements, plus the two unrecorded SLO measurements, remain unchanged.
The v1 catalog binds offline implementation and conformance records to this
run and digest. This is not production SLO
evidence: the admission/compilation p99 and worker-error-rate requirements
remain explicitly unrecorded, protected live-provider runs remain pending, and
publication remains authorization-gated.

## Recent merged validation

The latest candidate includes these independently green pull-request runs. The
pull-request runs prove the changed slice; the protected master run above is
the only run bound to the retained release-evidence artifact.

| Change | Merge commit | Pull-request run |
| --- | --- | --- |
| [#518](https://github.com/mfow/llm-temporal-worker/pull/518) exposed a bounded blob GC pass | `7c9abb75c812c6ae01b8ee859ab1d53d6079ab40` | [30686123604](https://github.com/mfow/llm-temporal-worker/actions/runs/30686123604) |
| [#519](https://github.com/mfow/llm-temporal-worker/pull/519) rejected unconfigured durable snapshots | `ab10512d99ac6dba13c43eef6d1e4d59f80c8400` | [30686821983](https://github.com/mfow/llm-temporal-worker/actions/runs/30686821983) |
| [#520](https://github.com/mfow/llm-temporal-worker/pull/520) rejected duplicate rendered resources | `bed7279fd4897bb31a008041f77c863195976dc5` | [30687532246](https://github.com/mfow/llm-temporal-worker/actions/runs/30687532246) |
| [#521](https://github.com/mfow/llm-temporal-worker/pull/521) added the AWS Bedrock Converse provider | `74580a11a350c7416997a2c19b2f8468cb8f9391` | [30688580467](https://github.com/mfow/llm-temporal-worker/actions/runs/30688580467) |
| [#522](https://github.com/mfow/llm-temporal-worker/pull/522) failed closed on compatibility cost overflow | `e219e9947315304e6e9aa22576410a6e436d1900` | [30691007589](https://github.com/mfow/llm-temporal-worker/actions/runs/30691007589) |
| [#523](https://github.com/mfow/llm-temporal-worker/pull/523) enforced Bedrock Converse contract coverage | `99359059d22052d518886d906ae3a4ef483f9b9c` | [30691762227](https://github.com/mfow/llm-temporal-worker/actions/runs/30691762227) |
| [#524](https://github.com/mfow/llm-temporal-worker/pull/524) added a versioned Redis budget-status reader | `f985f13feba4dc4d24608ff64c81643c9653e0a7` | [30692464558](https://github.com/mfow/llm-temporal-worker/actions/runs/30692464558) |
| [#525](https://github.com/mfow/llm-temporal-worker/pull/525) pinned worker PostgreSQL to 17.5 | `ff48d61d4c34cf96b908bc8a46db12de03928195` | [30695089609](https://github.com/mfow/llm-temporal-worker/actions/runs/30695089609) |
| [#526](https://github.com/mfow/llm-temporal-worker/pull/526) used a tier-capable Converse fixture model | `46ee4431a97843cd58f785c8b074565bf5b81524` | [30694499334](https://github.com/mfow/llm-temporal-worker/actions/runs/30694499334) |
| [#528](https://github.com/mfow/llm-temporal-worker/pull/528) failed closed on unsupported Bedrock model tiers | `22c1b67394dbae97b1651e80d88aab26b765fd17` | [30695814354](https://github.com/mfow/llm-temporal-worker/actions/runs/30695814354) |
| [#529](https://github.com/mfow/llm-temporal-worker/pull/529) enforced Bedrock Messages service tiers | `22a0137072ccb0a9debb1af9342adf70dff043c5` | [30700265419](https://github.com/mfow/llm-temporal-worker/actions/runs/30700265419) |
| [#530](https://github.com/mfow/llm-temporal-worker/pull/530) skipped unusable pricing candidates during fallback | `ad25ace10166cd6ea48dc9227472d80f581d45f5` | [30700330953](https://github.com/mfow/llm-temporal-worker/actions/runs/30700330953) |
| [#535](https://github.com/mfow/llm-temporal-worker/pull/535) fixed the reload snapshot race | `803be9368ea2198aab1a560353d6f18592e01fb6` | [30708112774](https://github.com/mfow/llm-temporal-worker/actions/runs/30708112774) |
| [#537](https://github.com/mfow/llm-temporal-worker/pull/537) added the guarded Bedrock Converse live contract | `8ca832d0031380434ac9e43a5905744b268d86a7` | [30711271094](https://github.com/mfow/llm-temporal-worker/actions/runs/30711271094) |
| [#538](https://github.com/mfow/llm-temporal-worker/pull/538) synchronized enforced fixture profiles with the fixture matrix | `2ce082315a3142e582d577138729eecece2da72e` | [30713973091](https://github.com/mfow/llm-temporal-worker/actions/runs/30713973091) |
| [#539](https://github.com/mfow/llm-temporal-worker/pull/539) bound durable composition across runtime phases | `8a2f9e98c2af9712efd3b1486c3ad63594ef388d` | [30715566177](https://github.com/mfow/llm-temporal-worker/actions/runs/30715566177) |
| [#540](https://github.com/mfow/llm-temporal-worker/pull/540) rejected unsupported Bedrock authentication | `09824e628aed45c3e6965fa3ebf5033e113f2844` | [30714761560](https://github.com/mfow/llm-temporal-worker/actions/runs/30714761560) |
| [#541](https://github.com/mfow/llm-temporal-worker/pull/541) covered generated Bedrock factory profiles | `ab39674381998c98cda4e51530983c14af389684` | [30716360753](https://github.com/mfow/llm-temporal-worker/actions/runs/30716360753) |
| [#544](https://github.com/mfow/llm-temporal-worker/pull/544) rejected contradictory unknown pricing components | `0723ffc7489aaf9f52e2f7234d7aade7505031c7` | [30719825510](https://github.com/mfow/llm-temporal-worker/actions/runs/30719825510) |
| [#543](https://github.com/mfow/llm-temporal-worker/pull/543) fenced Redis budget-status reads to the active generation | `b8e3653a43552345898ebc0d7a1074a3a65533ba` | [30720458198](https://github.com/mfow/llm-temporal-worker/actions/runs/30720458198) |
| [#545](https://github.com/mfow/llm-temporal-worker/pull/545) refreshed final release evidence binding | `0c1ac68c2685a7be3a28238cd0717b875d74045a` | [30723213296](https://github.com/mfow/llm-temporal-worker/actions/runs/30723213296) |
| [#546](https://github.com/mfow/llm-temporal-worker/pull/546) scrubbed live-contract environments | `7b1ddc7958c3eee9f281e24c8f727c6df42cf1fb` | [30725437358](https://github.com/mfow/llm-temporal-worker/actions/runs/30725437358) |
| [#547](https://github.com/mfow/llm-temporal-worker/pull/547) rejected conflicting endpoint capabilities | `c62795cbd182d3ea96adbab006750bf047999c52` | [30725462901](https://github.com/mfow/llm-temporal-worker/actions/runs/30725462901) |
| [#548](https://github.com/mfow/llm-temporal-worker/pull/548) kept exact USD out of Prometheus amounts | `9a1e09b74730ae9b8899508af8ce8751f1b2c689` | [30725490716](https://github.com/mfow/llm-temporal-worker/actions/runs/30725490716) |
| [#549](https://github.com/mfow/llm-temporal-worker/pull/549) refreshed the latest release-evidence binding | `a9e6dddc6d0942c0c70f6466778869f4684054a2` | [30727618310](https://github.com/mfow/llm-temporal-worker/actions/runs/30727618310) |
| [#550](https://github.com/mfow/llm-temporal-worker/pull/550) included Bedrock Converse in example configuration | `1c73743c34ab112f1df018377f9daa9e6a891a10` | [30727646006](https://github.com/mfow/llm-temporal-worker/actions/runs/30727646006) |
| [#551](https://github.com/mfow/llm-temporal-worker/pull/551) enforced the Bedrock Converse one-shot boundary | `dc0b885772c94a4e01526453fbc34d2c15be7751` | [30728570804](https://github.com/mfow/llm-temporal-worker/actions/runs/30728570804) |
| [#552](https://github.com/mfow/llm-temporal-worker/pull/552) covered Bedrock Converse tiers and tools | `a8d398cee7fd34447ebfbd13078ca99e634a6e7b` | [30728110395](https://github.com/mfow/llm-temporal-worker/actions/runs/30728110395) |
| [#553](https://github.com/mfow/llm-temporal-worker/pull/553) bound durable composition to a config snapshot | `a96e474cd0a8e2e51be8c4514186baad3c2eda79` | [30730640740](https://github.com/mfow/llm-temporal-worker/actions/runs/30730640740) |
| [#554](https://github.com/mfow/llm-temporal-worker/pull/554) exposed the unknown-cost maintenance queue | `55808d3f8334144d132ccfe562d78ea2c855f056` | [30731293417](https://github.com/mfow/llm-temporal-worker/actions/runs/30731293417) |
| [#555](https://github.com/mfow/llm-temporal-worker/pull/555) covered Bedrock priority downgrade contracts | `b4c2aa67c07f94972448b8c30b1136a51595e384` | [30731962647](https://github.com/mfow/llm-temporal-worker/actions/runs/30731962647) |
| [#556](https://github.com/mfow/llm-temporal-worker/pull/556) reflected maintenance adapter status in the plan | `680b2afc70d4ca5337cff405801d07735b719c21` | [30733989566](https://github.com/mfow/llm-temporal-worker/actions/runs/30733989566) |
| [#557](https://github.com/mfow/llm-temporal-worker/pull/557) validated active Redis budget manifests at readiness | `bebec8e5a12664da7d6e5a7ae4d592f8ded7fccc` | [30735537401](https://github.com/mfow/llm-temporal-worker/actions/runs/30735537401) |
| [#558](https://github.com/mfow/llm-temporal-worker/pull/558) listed merged validation through #557 | `5744d217d1df1a129c951c57f54250ab9cd798b8` | [30737739698](https://github.com/mfow/llm-temporal-worker/actions/runs/30737739698) |
| [#559](https://github.com/mfow/llm-temporal-worker/pull/559) included Bedrock Converse in the v1 completion gate | `ae8e531a11a723e077ade7ef1d3d86e36c289337` | [30739975204](https://github.com/mfow/llm-temporal-worker/actions/runs/30739975204) |
| [#560](https://github.com/mfow/llm-temporal-worker/pull/560) refreshed the v1 catalog evidence pin | `aa32e1ed2fe8d7874a6ee8154261e65ffac7cd99` | [30740725332](https://github.com/mfow/llm-temporal-worker/actions/runs/30740725332) |
| [#561](https://github.com/mfow/llm-temporal-worker/pull/561) listed merged validation through #560 | `3ba3264782d2802b69a078681c923c3a3a44e2b6` | [30741588777](https://github.com/mfow/llm-temporal-worker/actions/runs/30741588777) |
| [#562](https://github.com/mfow/llm-temporal-worker/pull/562) listed merged validation through #561 | `a48258d6e6ce7996ad4492b419a597e948c14e9b` | [30743830181](https://github.com/mfow/llm-temporal-worker/actions/runs/30743830181) |
| [#563](https://github.com/mfow/llm-temporal-worker/pull/563) marked the Bedrock streaming task superseded | `9d0ceb4a3c8157fc31fce2b9b71519aa0f4662c4` | [30743919451](https://github.com/mfow/llm-temporal-worker/actions/runs/30743919451) |
| [#564](https://github.com/mfow/llm-temporal-worker/pull/564) listed the Bedrock Converse adapter profile | `47f33c033f9c3fdcfc8554e279516bdb51b0a31d` | [30746120429](https://github.com/mfow/llm-temporal-worker/actions/runs/30746120429) |
| [#565](https://github.com/mfow/llm-temporal-worker/pull/565) rebound the catalog to latest evidence | `832be7a08d5efc2c7214f2bc629b43839ba94710` | [30746200992](https://github.com/mfow/llm-temporal-worker/actions/runs/30746200992) |
| [#566](https://github.com/mfow/llm-temporal-worker/pull/566) enforced live-provider profile table parity | `efa76d8dc722057111a5fdde82f574b5bcfcbb1b` | [30748402963](https://github.com/mfow/llm-temporal-worker/actions/runs/30748402963) |
| [#567](https://github.com/mfow/llm-temporal-worker/pull/567) listed merged validation through #566 | `559a098f5633a8258e37b735191bbe94f1b13d82` | [30790691313](https://github.com/mfow/llm-temporal-worker/actions/runs/30790691313) |
| [#568](https://github.com/mfow/llm-temporal-worker/pull/568) listed merged validation through #567 | `1aae4ff2095a5adaf3fee13aa696d78d73616ddd` | [30795754773](https://github.com/mfow/llm-temporal-worker/actions/runs/30795754773) |

PR #265 intentionally remains a storage read seam. The production query
composition still requires an explicit PostgreSQL-authoritative builder for
spend summary and remains fail-closed when that builder is absent, as described
in [the Activity runtime boundary](../reference/activity-runtime.md) and
[persisted query composition](../reference/persisted-query-service.md). This
note records the implementation boundary; it does not turn the gap into a
completed v1 requirement or claim a live provider or publication run.

## Guarded manual publication boundary

Task 24 enables `.github/workflows/release.yml` as a fail-closed publication
control. It has only a `workflow_dispatch` trigger, and both jobs reject a
dispatch that is not started from `master`. A human must provide all three
immutable inputs:

- a strict protected tag reference in the form
  `refs/tags/vMAJOR.MINOR.PATCH` (lightweight and annotated tags both resolve
  to their one target commit);
- a fully qualified `registry/repository@sha256:...` source image reference;
  and
- the numeric workflow run ID for the successful master `release-evidence`
  bundle that proves that exact tag commit and digest.

There is intentionally no default source registry. An administrator must
configure the non-secret repository variable
`RELEASE_PUBLICATION_IMAGE_REPOSITORY` with the exact trusted source registry
and repository path. The guard rejects a missing, malformed, tag-based, or
different source reference; it also rejects a branch ref, a malformed tag, a
tag that is not reachable from protected master, a non-numeric run ID, an
unavailable or untrusted evidence run, an unavailable artifact, or any
evidence revision/digest mismatch.

The unprotected preflight receives only `contents: read` and `actions: read`.
It uses the automatic, job-scoped `GITHUB_TOKEN` only as the input to GitHub's
pinned `actions/download-artifact` action; the token is never placed in a shell
environment, logged, or passed to another action. This short-lived read token
is not a provider, registry, or OIDC credential.

Because `actions/checkout` v6 requires a token input even for public
repositories, neither release job uses it. Before either job passes a manual
ref to Git, it validates the tag's strict `refs/tags/vMAJOR.MINOR.PATCH` shape
in a shell environment. Each job then performs its own fixed unauthenticated
HTTPS Git fetch from `https://github.com/mfow/llm-temporal-worker.git`, with an
empty temporary Git home, no system Git configuration, disabled prompting,
and no credential helper. Each checkout fails closed if its fresh workspace
is not empty, the exact tag cannot be fetched, or fetched `master` is not the
protected workflow's `github.sha`; it checks out that master SHA only, never
the manual tag. Thus neither a manual ref nor an unprotected-job workspace or
output can select code that runs inside the protected publication boundary.

This repository is public, so each job makes its own credential-free HTTPS
`GET` to the public GitHub Actions run endpoint before downloading evidence.
The lookup fails closed on network, API, rate-limit, size, or JSON errors and
requires the returned run to name this repository, use
`.github/workflows/master.yml`, be a completed successful `push` to `master`,
and have the exact commit resolved from the release tag. The only job that can
retain the `release-evidence` artifact is the `release-evidence` job in that
workflow; its contract requires successful master verification before upload.
Each downloader requests that exact artifact name from the validated run. The
protected job does not trust the preflight's artifact or outputs: after
environment approval it independently repeats the checkout, request guard,
public-run verification, artifact download, complete Task 23 verification,
and tag/revision/digest binding. If the repository ever becomes private, this
public lookup must remain fail-closed until a separately authorized design
supplies an equivalent trust boundary without widening the token's scope.

The downstream job names the `release-publication` protected environment and
is the only job with `id-token: write`. Repository administrators must create
and protect that environment with required human approvals. This repository
workflow does not create or modify that environment. Infrastructure supplies
the non-secret Actions variables `AWS_ECR_PUBLISH_ROLE_ARN`, `AWS_REGION`, and
`ECR_REPOSITORY=llm-temporal-worker`; its role trust limits GitHub OIDC to this
repository's protected `master`, and its ECR policy limits writes to that one
repository. No static AWS access key or GitHub secret is accepted.

Only after all evidence checks succeed does the protected job use the
SHA-pinned official `aws-actions/configure-aws-credentials` action to exchange
GitHub OIDC for a 15-minute role session, followed immediately by the
SHA-pinned official `aws-actions/amazon-ecr-login` action for the selected
account registry. The reviewed publication shell receives empty
`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_SESSION_TOKEN` values;
the OIDC-derived AWS session is not exposed to arbitrary shell. The publisher
parses the configured role account, verifies the action-reported account,
requires the ECR registry to equal
`ACCOUNT.dkr.ecr.REGION.amazonaws.com`, and requires the repository to equal
`llm-temporal-worker`.

The source is always the input's exact digest reference; neither a source tag
nor an unpinned source is accepted. Before requesting an OIDC token, the
checksum-pinned Crane binary runs `crane pull --format=oci` without a platform
override. It materializes the source image or complete index and every child in
the fixed `$RUNNER_TEMP/source.oci` layout. The trusted release verifier then
requires that layout's one top descriptor to equal the evidence digest. A
missing, corrupt, or changing source therefore fails before AWS authentication
or any destination mutation. Publication after ECR login reads only that local
layout and never contacts the source registry.

The publisher revalidates the fixed path, real-directory shape, OCI files, and
root digest before mutation. `crane push` is invoked on the layout without its
index-wrapping `--index` flag: with one top descriptor Crane auto-loads the
exact image or index, while wrapping the layout would create a different
digest.
The ECR destination is first addressed by the evidence digest and never pushes `latest`. The publisher requires Crane's pushed reference, an ECR digest
lookup, the staged digest, and the evidence digest all to agree. Only after
keyless signature and attestation verification does the separate finalizer
create the protected semantic version tag from
`refs/tags/vMAJOR.MINOR.PATCH`; the infrastructure-managed repository's
immutable-tag policy prevents that tag from ever moving, and a final lookup
must equal the signed digest. The workflow emits both
`ACCOUNT.dkr.ecr.REGION.amazonaws.com/llm-temporal-worker@sha256:...` and its
immutable `:vMAJOR.MINOR.PATCH` alias. It does not rebuild the image, move a Git
tag, or create a GitHub release. The staged layout is removed on success or
failure by a final credential-blanked cleanup step.

Registry uploads are not transactional. A destination-side upload or ECR
failure can retain content-addressed layers or child manifests already pushed.
Crane writes every dependency before the evidence root manifest, however, so
that failure cannot emit a successful release. The immutable tag is created
only after digest publication, signature, attestation, and verification pass.
Such unreferenced partial content is not release evidence; the pushed-reference check and final ECR root digest equality, plus tag-to-digest equality, are the
publication success boundary.

Before AWS authentication, the SHA-pinned Cosign installer selects Cosign
`v3.1.3`, and a closed SLSA v1 predicate records the semantic version, revision,
source, commit timestamp, Linux AMD64 platform, Dockerfile, both base-image
digests, evidence run, publication run, and immutable image digest. After the
digest checks, the protected job uses GitHub's keyless OIDC identity to sign
the digest and attest both the evidence-bound CycloneDX SBOM and SLSA predicate.
It immediately verifies the signature and both attestations before tagging,
using issuer `https://token.actions.githubusercontent.com` and exact certificate
identity
`https://github.com/mfow/llm-temporal-worker/.github/workflows/release.yml@refs/heads/master`.
Regex or repository-wide identity matches are not accepted.

External prerequisites are: a protected `release-publication` GitHub
environment with required reviewers; non-secret repository variables
`RELEASE_PUBLICATION_IMAGE_REPOSITORY`, `AWS_ECR_PUBLISH_ROLE_ARN`,
`AWS_REGION`, and `ECR_REPOSITORY=llm-temporal-worker`; GitHub OIDC trust
restricted to this repository, workflow, protected `master` ref, and
environment; ECR immutable tags and scan-on-push; and a publisher role limited
to layer upload, manifest/tag write, and readback in that one repository.
Consumers and EKS nodes need pull/readback only. ECR must support OCI 1.1
signature and attestation referrers, and the job needs outbound HTTPS to AWS
ECR, Fulcio, Rekor, and GitHub's OIDC endpoint.

Current Task 23 image evidence is not multi-architecture. `image-verify` uses
one Buildx solve with `--platform linux/amd64`; publication therefore preserves
that verified Linux AMD64 manifest exactly. Do not describe the ECR result as
multi-architecture until master evidence itself produces and verifies an OCI
index. Because master evidence does not itself publish or retain its raw OCI
layout, the trusted source registry must already contain that exact
public/readable digest; missing source content or any registry/authentication
error fails publication closed.
