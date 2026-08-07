# GitHub-Owned Actions Policy Compatibility Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans` to implement this plan task by task.

**Goal:** Restore executable pull-request and master verification workflows under the organization's GitHub-owned-actions policy without weakening their build, package, deployment-policy, or release-evidence contracts.

**Architecture:** Keep the two automatically triggered workflows declarative and read-only, but replace third-party `uses:` steps with reviewed, repository-owned Bash setup steps. Keep each external binary version and artifact integrity check explicit in source, and express the resulting contract through the existing Go workflow architecture tests. Do not alter the manually authorized live-provider workflow: cloud authentication needs a separate, credential-aware design review.

**Tech Stack:** GitHub Actions YAML, Bash, Go architecture tests, Docker Buildx, OPAM/OCaml, kubectl, Syft, Trivy.

## Global Constraints

- The organization policy allows GitHub-owned actions but rejects the Docker, OCaml, Azure, Anchore, and Aqua third-party actions used by the current pull-request and master workflows before any job starts.
- `master.yml` and `pull-request.yml` must reference only `actions/*` through `uses:`. Preserve pinned full commit hashes for every remaining action.
- Preserve read-only permissions, immutable container/image inputs, cache scopes, build metadata, OCaml version, kubectl version, Syft version, Trivy version, and the release-evidence boundary.
- Repository-owned setup code must fail closed, download only explicit versions, verify the downloaded bytes against a reviewed checksum before execution, and install only into runner-temporary paths.
- Do not add credentials, deployment commands, artifact publication, signing, or cloud-provider authentication to either automatically triggered workflow.
- Do not suppress a verification gate merely to remove an action dependency. A local architecture test must reject future non-GitHub-owned `uses:` references in both workflows.

### Task 1: Replace blocked action steps with verified native setup

**Files:**

- Modify: `.github/workflows/master.yml`
- Modify: `.github/workflows/pull-request.yml`
- Modify: `golang/internal/architecturetest/workflow_test.go`
- Create: `scripts/ci/` setup helpers as needed for Buildx, OPAM/OCaml, kubectl, Syft, and Trivy
- Modify: `docs/superpowers/plans/2026-08-07-github-actions-policy-compatibility.md`

**Step 1: Write the failing regression test**

Add a focused workflow architecture test that walks the `steps` of `master.yml` and `pull-request.yml` and fails for any `uses:` value outside the `actions/` namespace. Update the existing container and release-evidence contract tests so their expected behavior is expressed in terms of the reviewed native setup commands and immutable inputs rather than the removed third-party action IDs.

Run:

```bash
go test ./internal/architecturetest -run 'TestAutomaticallyTriggeredWorkflowsUseOnlyGitHubOwnedActions|TestWorkflowContainerBuildContract|TestWorkflowReleaseEvidenceBoundary'
```

Expected: FAIL while either workflow still names the blocked Docker, OCaml, Azure, Anchore, or Aqua action.

**Step 2: Implement native, integrity-checked setup**

Replace each blocked `uses:` step in `master.yml` and `pull-request.yml` with an explicit `run:` step that invokes repository-owned setup code. The code must:

- make the requested Docker Buildx version and reviewed BuildKit image available before every native container build;
- initialize an isolated OPAM root/switch for OCaml 5.2.0 and make `opam exec` usable by later OCaml steps;
- install the exact kubectl version before deployment-policy rendering and release-input collection;
- install the exact Syft and Trivy versions before generating SBOM and scan evidence;
- verify each downloaded executable or archive against a source-controlled SHA-256 before extraction or execution; and
- preserve the existing temporary OCI layout, cache scope, tags, build arguments, outputs, and explicit cleanup behavior.

Native Docker builds must still use the reviewed BuildKit image digest, preserve `type=gha` cache scopes, and remain non-publishing. The release-evidence job must retain its existing read-only permissions and artifact-upload behavior.

**Step 3: Verify syntax and contracts locally**

Run:

```bash
bash -n scripts/ci/*.sh
go test ./internal/architecturetest
make -C golang workflow-verify
```

Expected: PASS. The workflow test should prove the automatic workflows contain no non-GitHub-owned `uses:` references and that all production-safe contracts remain represented.

**Step 4: Record implementation evidence**

Commit the workflow, helper, architecture-test, and plan changes together. In the PR description, identify the hosted `OCaml package`, `Container image`, `Verify`, fuzz, and release-evidence jobs as the integration proof; do not claim green CI until GitHub has run them.

### Task 2: Correct cache and sandbox regressions found in independent review

**Files:**

- Modify: `.github/workflows/master.yml`
- Modify: `.github/workflows/pull-request.yml`
- Modify: `golang/internal/architecturetest/workflow_test.go`
- Modify: `golang/scripts/check-workflow-policy.sh`
- Modify: `scripts/ci/setup-buildx.sh`
- Modify: `scripts/ci/setup-opam.sh`
- Modify: `docs/superpowers/plans/2026-08-07-github-actions-policy-compatibility.md`
- Create or modify: focused deterministic native-helper tests under `golang/internal/architecturetest/`

**Step 1: Write failing behavioral regression tests**

Rename or select every native-workflow test so `make -C golang workflow-verify` executes it. Add tests that fail against the current implementation unless all of the following are true:

- the container jobs use a full-SHA-pinned GitHub-owned bridge that exports the GitHub Actions cache runtime URL, results URL, token, and cache-v2 flag before native Buildx setup/build;
- the native Buildx helper pins a Buildx/BuildKit pair that supports GitHub cache API v2, selects the builder, bootstraps it, and keeps the reviewed image digest;
- cache import/export explicitly use the supported GitHub cache API v2 behavior while retaining master and PR isolation;
- the OCaml jobs restore/save an isolated OPAM root through a full-SHA-pinned `actions/cache` step keyed by runner OS, runner architecture, repository, OPAM version, OCaml version, sandbox mode, and the nested package dependency inputs;
- the OPAM helper preserves sandboxing, initializes a fresh root only when absent, reuses a restored switch when present, and persists its executable path exactly once; and
- deterministic fake-command helper tests prove every downloaded binary/archive passes checksum verification before install, extract, Docker bootstrap, or OPAM use.

Run the focused test subset before implementation.

Expected: FAIL against the reviewed Task 1 commit, demonstrating that the test suite catches the broken cache/runtime and sandbox behavior.

**Step 2: Repair native cache and OPAM behavior**

Use only GitHub-owned, full-SHA-pinned actions. Add the reviewed runtime-cache bridge before both container builds; fail clearly when a required runtime value is unavailable. Update the Buildx binary checksum and BuildKit image digest together to a supported API-v2 pair from their official releases. Do not retain an API-v1 fallback.

Add an isolated OPAM cache in each OCaml job before setup. The cache may reuse immutable compiler/dependency state but must not mix master and pull-request identity. Revise `setup-opam.sh` to retain the default sandboxed behavior, safely reuse a restored root/switch, and use `GITHUB_PATH` plus `GITHUB_ENV` without duplicating PATH entries.

**Step 3: Verify the corrected contract**

Run:

```bash
bash -n scripts/ci/*.sh
go test ./internal/architecturetest
make -C golang workflow-verify
```

Expected: PASS. The workflow verification target must execute the newly added cache, sandbox, and helper-behavior tests.

**Step 4: Request a fresh independent review**

Commit the corrective changes separately, preserve the Task 1 report and review findings in the ignored SDD workspace, and request a new reviewer who verifies the fixed cache/runtime, Buildx/BuildKit compatibility, OPAM cache behavior, sandboxing, and test selection before any push.

## Task 1 implementation record

The blocked automatic-workflow actions were replaced with repository-owned,
checksum-verified setup helpers for Buildx, OPAM/OCaml, kubectl, Syft, and
Trivy. The local architecture tests now reject non-`actions/*` `uses:` values
in the two automatic workflows and preserve the reviewed image-cache,
BuildKit-digest, deployment-policy, temporary-OCI, and release-evidence
boundaries. Hosted workflow execution remains the integration proof.
