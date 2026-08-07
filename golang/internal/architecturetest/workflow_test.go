package architecturetest

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	yaml "go.yaml.in/yaml/v4"
)

const (
	checkoutActionPin     = "actions/checkout@df4cb1c069e1874edd31b4311f1884172cec0e10"
	setupGoActionPin      = "actions/setup-go@924ae3a1cded613372ab5595356fb5720e22ba16"
	githubScriptActionPin = "actions/github-script@ed597411d8f924073f98dfc5c65a23a2325f34cd"
	cacheActionPin        = "actions/cache@5a3ec84eff668545956fd18022155c47e93e2684"
)

var immutableActionReference = regexp.MustCompile(`^[0-9a-f]{40}$`)

type workflowDocument struct {
	name   string
	raw    string
	fields map[string]any
}

type releaseMakeInvocation struct {
	workflow string
	job      string
	target   string
	line     string
}

func TestWorkflowYAMLParses(t *testing.T) {
	for _, name := range []string{"master.yml", "pull-request.yml", "release.yml"} {
		_ = readWorkflow(t, name)
	}
}

func TestWorkflowContract(t *testing.T) {
	pullRequest := readWorkflow(t, "pull-request.yml")
	master := readWorkflow(t, "master.yml")

	assertPullRequestTrigger(t, pullRequest)
	assertReadOnlyPermissions(t, pullRequest)
	assertMasterTriggers(t, master)
	assertReadOnlyPermissions(t, master)
	for _, workflow := range []workflowDocument{pullRequest, master} {
		assertWorkflowControls(t, workflow)
		assertVerificationStep(t, workflow)
		assertRequiredOfflineGates(t, workflow)
	}
}

func TestWorkflowAutomaticallyTriggeredWorkflowsUseOnlyGitHubOwnedActions(t *testing.T) {
	for _, workflow := range []workflowDocument{
		readWorkflow(t, "pull-request.yml"),
		readWorkflow(t, "master.yml"),
	} {
		for _, reference := range actionReferences(t, workflow) {
			if !strings.HasPrefix(reference, "actions/") {
				t.Fatalf("%s references non-GitHub-owned action %q", workflow.name, reference)
			}
		}
	}
}

func TestWorkflowNativeCISetupHelpersVerifyPinnedDownloads(t *testing.T) {
	for _, test := range []struct {
		file     string
		version  string
		checksum string
	}{
		{file: "setup-buildx.sh", version: "v0.21.2", checksum: "b13bee81c3db12a4be7d0b9d042b64d0dd9ed116f7674dfac0ffdf2a71acfe3d"},
		{file: "setup-opam.sh", version: "2.3.0", checksum: "324e78e3f33efeba279aacf9f9610cfec7b2df7d7e0e1640f75f09de85f96cc9"},
		{file: "setup-kubectl.sh", version: "v1.32.6", checksum: "0e31ebf882578b50e50fe6c43e3a0e3db61f6a41c9cded46485bc74d03d576eb"},
		{file: "setup-syft.sh", version: "v1.44.0", checksum: "0e91737aee2b5baf1d255b959630194a302335d848ff97bb07921eb6205b5f5a"},
		{file: "setup-trivy.sh", version: "v0.72.0", checksum: "bbb64b9695866ce4a7a8f5c9592002c5961cab378577fa3f8a040df362b9b2ea"},
	} {
		t.Run(test.file, func(t *testing.T) {
			setup := readRepositoryFile(t, repositoryRoot(t), "scripts", "ci", test.file)
			for _, want := range []string{
				test.version,
				test.checksum,
				"sha256sum --check --status",
				"RUNNER_TEMP",
			} {
				if !strings.Contains(setup, want) {
					t.Fatalf("%s does not retain %q", test.file, want)
				}
			}
		})
	}

	for _, workflow := range []workflowDocument{
		readWorkflow(t, "pull-request.yml"),
		readWorkflow(t, "master.yml"),
	} {
		assertJobHasRunCommand(t, workflow, "ocaml", "bash scripts/ci/setup-opam.sh")
	}
}

func TestWorkflowContainerBuildCacheV2BridgeAndIsolation(t *testing.T) {
	for _, test := range []struct {
		name  string
		scope string
	}{
		{name: "pull-request.yml", scope: "llmtw-pr-${{ github.event.pull_request.number }}"},
		{name: "master.yml", scope: "llmtw-master"},
	} {
		workflow := readWorkflow(t, test.name)
		assertJobUsesAction(t, workflow, "container", githubScriptActionPin)
		assertJobActionPrecedesRunCommand(t, workflow, "container", githubScriptActionPin, "bash scripts/ci/setup-buildx.sh")
		assertJobActionPrecedesRunContains(t, workflow, "container", githubScriptActionPin, "docker buildx build")
		assertJobRunContains(t, workflow, "container", "--cache-from type=gha,scope="+test.scope+",version=2")
		assertJobRunContains(t, workflow, "container", "--cache-to type=gha,mode=max,scope="+test.scope+",version=2,ignore-error=true")
		for _, want := range []string{
			"ACTIONS_CACHE_URL",
			"ACTIONS_RESULTS_URL",
			"ACTIONS_RUNTIME_TOKEN",
			"ACTIONS_CACHE_SERVICE_V2",
		} {
			if !strings.Contains(workflow.raw, want) {
				t.Fatalf("%s does not export %s through the GitHub-owned runtime bridge", workflow.name, want)
			}
		}
	}
}

func TestWorkflowOCamlCacheIsolatedAndSandboxed(t *testing.T) {
	for _, test := range []struct {
		name     string
		identity string
	}{
		{name: "pull-request.yml", identity: "pr-${{ github.event.pull_request.number }}"},
		{name: "master.yml", identity: "master"},
	} {
		workflow := readWorkflow(t, test.name)
		assertJobUsesAction(t, workflow, "ocaml", cacheActionPin)
		assertJobActionPrecedesRunCommand(t, workflow, "ocaml", cacheActionPin, "bash scripts/ci/setup-opam.sh")
		for _, want := range []string{
			"${{ runner.temp }}/llmtw-opam-root",
			"${{ runner.temp }}/llmtw-xdg-cache",
			"opam-${{ runner.os }}-${{ runner.arch }}-${{ github.repository }}-" + test.identity,
			"opam-2.3.0-ocaml-5.2.0-sandboxed",
			"hashFiles('ocaml/llm_temporal_worker/*.opam', 'ocaml/llm_temporal_worker/dune-project', 'scripts/ci/setup-opam.sh')",
		} {
			if !strings.Contains(workflow.raw, want) {
				t.Fatalf("%s OCaml cache does not retain %q", workflow.name, want)
			}
		}
	}

	opamSetup := readRepositoryFile(t, repositoryRoot(t), "scripts", "ci", "setup-opam.sh")
	for _, want := range []string{
		"GITHUB_PATH",
		"opam switch list --short",
		"opam switch set --yes",
		"opam init --bare --no-setup --yes",
	} {
		if !strings.Contains(opamSetup, want) {
			t.Fatalf("native OPAM setup does not retain %q", want)
		}
	}
	if strings.Contains(opamSetup, "--disable-sandboxing") {
		t.Fatal("native OPAM setup disables sandboxing")
	}
	if strings.Contains(opamSetup, "printf 'PATH=") || strings.Count(opamSetup, `>> "${GITHUB_PATH}"`) != 1 {
		t.Fatal("native OPAM setup does not persist its executable path exactly once through GITHUB_PATH")
	}
}

func TestWorkflowOCamlSandboxPrerequisitesPrecedeSetup(t *testing.T) {
	for _, workflow := range []workflowDocument{
		readWorkflow(t, "pull-request.yml"),
		readWorkflow(t, "master.yml"),
	} {
		assertJobRunPrecedesRunContains(t, workflow, "ocaml", "sudo apt-get install --yes protobuf-compiler bubblewrap", "bash scripts/ci/setup-opam.sh")
	}
}

func TestWorkflowOCamlSandboxUserNamespacesAreGuardedAndScoped(t *testing.T) {
	const stepName = "Enable unprivileged user namespaces for OPAM sandboxing"
	for _, workflow := range []workflowDocument{
		readWorkflow(t, "pull-request.yml"),
		readWorkflow(t, "master.yml"),
	} {
		job := workflowJob(t, workflow, "ocaml")
		steps, ok := job["steps"].([]any)
		if !ok {
			t.Fatalf("%s OCaml job has no steps", workflow.name)
		}
		aptIndex, sysctlIndex, cacheIndex, setupIndex := -1, -1, -1, -1
		for index, rawStep := range steps {
			step, ok := rawStep.(map[string]any)
			if !ok {
				continue
			}
			if step["name"] == "Install Protocol Buffers compiler" {
				aptIndex = index
			}
			if step["name"] == stepName {
				sysctlIndex = index
				if got, ok := step["if"].(string); !ok || got != "${{ runner.environment == 'github-hosted' }}" {
					t.Fatalf("%s user-namespace step if = %#v, want GitHub-hosted-only guard", workflow.name, step["if"])
				}
				run, _ := step["run"].(string)
				for _, want := range []string{
					"set -euo pipefail",
					"kernel.unprivileged_userns_clone=1",
					"kernel.apparmor_restrict_unprivileged_userns=0",
					`if current="$(sysctl -n "${key}" 2>/dev/null)" && [[ "${current}" != "${want}" ]]; then`,
					`sudo sysctl -w "${key}=${want}"`,
				} {
					if !strings.Contains(run, want) {
						t.Fatalf("%s user-namespace step does not retain guarded %q", workflow.name, want)
					}
				}
			}
			if step["uses"] == cacheActionPin {
				cacheIndex = index
			}
			if run, _ := step["run"].(string); strings.TrimSpace(run) == "bash scripts/ci/setup-opam.sh" {
				setupIndex = index
			}
		}
		if aptIndex == -1 || sysctlIndex == -1 || cacheIndex == -1 || setupIndex == -1 {
			t.Fatalf("%s OCaml job is missing an apt, guarded sysctl, cache, or setup step", workflow.name)
		}
		if !(aptIndex < sysctlIndex && sysctlIndex < cacheIndex && cacheIndex < setupIndex) {
			t.Fatalf("%s runs OCaml sandbox setup in wrong order: apt=%d sysctl=%d cache=%d setup=%d", workflow.name, aptIndex, sysctlIndex, cacheIndex, setupIndex)
		}
		for jobName, rawJob := range workflowMapping(t, workflow, "jobs") {
			if jobName == "ocaml" {
				continue
			}
			otherJob, ok := rawJob.(map[string]any)
			if !ok {
				continue
			}
			steps, _ := otherJob["steps"].([]any)
			for _, rawStep := range steps {
				step, ok := rawStep.(map[string]any)
				if ok && step["name"] == stepName {
					t.Fatalf("%s unexpectedly configures OCaml sandbox sysctls in job %q", workflow.name, jobName)
				}
			}
		}
	}

	for _, workflow := range []workflowDocument{readWorkflow(t, "release.yml")} {
		for jobName, rawJob := range workflowMapping(t, workflow, "jobs") {
			job, ok := rawJob.(map[string]any)
			if !ok {
				continue
			}
			steps, _ := job["steps"].([]any)
			for _, rawStep := range steps {
				step, ok := rawStep.(map[string]any)
				if ok && step["name"] == stepName {
					t.Fatalf("manual %s workflow unexpectedly configures OCaml sandbox sysctls in job %q", workflow.name, jobName)
				}
			}
		}
	}
}

func TestWorkflowContainerBuildContract(t *testing.T) {
	for _, test := range []struct {
		name  string
		scope string
	}{
		{name: "pull-request.yml", scope: "llmtw-pr-${{ github.event.pull_request.number }}"},
		{name: "master.yml", scope: "llmtw-master"},
	} {
		workflow := readWorkflow(t, test.name)
		job := workflowJob(t, workflow, "container")
		assertJobReadOnlyPermissions(t, workflow.name, "container", job)
		assertJobHasRunCommand(t, workflow, "container", "bash scripts/ci/setup-buildx.sh")
		assertJobRunContains(t, workflow, "container", "docker buildx build")
		assertJobRunContains(t, workflow, "container", "--cache-from type=gha,scope="+test.scope+",version=2")
		assertJobRunContains(t, workflow, "container", "--cache-to type=gha,mode=max,scope="+test.scope+",version=2,ignore-error=true")
		assertJobRunContains(t, workflow, "container", "--file ./golang/Dockerfile")
		if !strings.Contains(workflow.raw, "go-version-file: golang/.go-version") {
			t.Fatalf("%s container build does not use the reviewed Go toolchain file", workflow.name)
		}
		if !strings.Contains(workflow.raw, "--build-arg VERSION=${{ steps.metadata.outputs.version }}") ||
			!strings.Contains(workflow.raw, "--build-arg GO_VERSION=${{ steps.metadata.outputs.go_version }}") ||
			!strings.Contains(workflow.raw, "--build-arg REVISION=${{ steps.metadata.outputs.revision }}") ||
			!strings.Contains(workflow.raw, "--build-arg BUILD_TIME=${{ steps.metadata.outputs.build_time }}") ||
			!strings.Contains(workflow.raw, "--build-arg SOURCE=${{ steps.metadata.outputs.source }}") {
			t.Fatalf("%s container build does not pass commit and build metadata", workflow.name)
		}
	}

	buildxSetup := readRepositoryFile(t, repositoryRoot(t), "scripts", "ci", "setup-buildx.sh")
	for _, want := range []string{
		"readonly buildx_version=\"v0.21.2\"",
		"readonly buildx_sha256=\"b13bee81c3db12a4be7d0b9d042b64d0dd9ed116f7674dfac0ffdf2a71acfe3d\"",
		"moby/buildkit:v0.20.2@sha256:c457984bd29f04d6acc90c8d9e717afe3922ae14665f3187e0096976fe37b1c8",
		"sha256sum --check --status",
		"--driver docker-container",
		"docker buildx create",
		"docker buildx inspect --bootstrap",
	} {
		if !strings.Contains(buildxSetup, want) {
			t.Fatalf("native Buildx setup does not retain %q", want)
		}
	}
}

func TestWorkflowPolicyDoesNotReferenceProviderCredentialsOrDeployment(t *testing.T) {
	for _, workflow := range []workflowDocument{
		readWorkflow(t, "pull-request.yml"),
		readWorkflow(t, "master.yml"),
	} {
		lower := strings.ToLower(workflow.raw)
		for _, forbidden := range []string{
			"secrets.",
			"openai_api_key",
			"anthropic_api_key",
			"azure_openai",
			"aws_access_key_id",
			"aws_secret_access_key",
			"llmtw_live_",
			"kubectl apply",
			"helm upgrade",
			"docker push",
			"cosign sign",
			"gh release",
			"id-token: write",
			"packages: write",
		} {
			if strings.Contains(lower, forbidden) {
				t.Fatalf("%s contains forbidden credential or deployment reference %q", workflow.name, forbidden)
			}
		}
	}
}

func TestWorkflowReleaseEvidenceBoundary(t *testing.T) {
	master := readWorkflow(t, "master.yml")
	pullRequest := readWorkflow(t, "pull-request.yml")
	release := readWorkflow(t, "release.yml")

	job := workflowJob(t, master, "release-evidence")
	assertJobReadOnlyPermissions(t, master.name, "release-evidence", job)
	if scalarString(t, master.name, job, "if") != "github.event_name == 'push' && github.ref == 'refs/heads/master'" {
		t.Fatalf("release-evidence job must run only on a master push, got %#v", job["if"])
	}
	if scalarString(t, master.name, job, "needs") != "verify" {
		t.Fatalf("release-evidence job must follow verify, got %#v", job["needs"])
	}
	if _, ok := workflowMapping(t, pullRequest, "jobs")["release-evidence"]; ok {
		t.Fatal("pull-request workflow must not run release evidence collection")
	}

	for _, action := range []string{
		checkoutActionPin,
		setupGoActionPin,
		"actions/upload-artifact@ea165f8d65b6e75b540449e92b4886f43607fa02",
	} {
		assertJobUsesAction(t, master, "release-evidence", action)
	}
	assertJobHasRunCommand(t, master, "release-evidence", "bash scripts/ci/setup-buildx.sh")
	assertJobRunPrecedesRunContains(t, master, "release-evidence", "bash scripts/ci/setup-kubectl.sh", "--image-oci-layout \"$RUNNER_TEMP/image.oci\"")
	assertJobRunPrecedesRunContains(t, master, "release-evidence", "bash scripts/ci/setup-syft.sh", "syft oci-dir:\"$RUNNER_TEMP/image.oci\"")
	assertJobRunPrecedesRunContains(t, master, "release-evidence", "bash scripts/ci/setup-trivy.sh", "trivy image")
	for _, command := range []string{"make release-verify"} {
		if !jobHasRunCommand(job, command) {
			t.Fatalf("release-evidence job does not run %q", command)
		}
	}
	for _, want := range []string{
		"oci-dir:\"$RUNNER_TEMP/image.oci\"",
		"--input \"$RUNNER_TEMP/image.oci\"",
		"--config scripts/release/trivy.yaml",
		"RELEASE_EVIDENCE_KUBECTL_VERSION: v1.32.6",
		"retention-days: 14",
	} {
		if !strings.Contains(master.raw, want) {
			t.Fatalf("master release-evidence job does not retain exact OCI evidence boundary %q", want)
		}
	}
	for _, test := range []struct {
		name string
		file string
		want []string
	}{
		{name: "kubectl", file: "setup-kubectl.sh", want: []string{"readonly kubectl_version=\"v1.32.6\"", "readonly kubectl_sha256=\"0e31ebf882578b50e50fe6c43e3a0e3db61f6a41c9cded46485bc74d03d576eb\"", "sha256sum --check --status"}},
		{name: "syft", file: "setup-syft.sh", want: []string{"readonly syft_version=\"v1.44.0\"", "readonly syft_sha256=\"0e91737aee2b5baf1d255b959630194a302335d848ff97bb07921eb6205b5f5a\"", "sha256sum --check --status"}},
		{name: "trivy", file: "setup-trivy.sh", want: []string{"readonly trivy_version=\"v0.72.0\"", "readonly trivy_sha256=\"bbb64b9695866ce4a7a8f5c9592002c5961cab378577fa3f8a040df362b9b2ea\"", "sha256sum --check --status"}},
	} {
		setup := readRepositoryFile(t, repositoryRoot(t), "scripts", "ci", test.file)
		for _, want := range test.want {
			if !strings.Contains(setup, want) {
				t.Fatalf("native %s setup does not retain %q", test.name, want)
			}
		}
	}

	if err := validateReleaseMakeInvocationPolicy(master, pullRequest, release); err != nil {
		t.Fatal(err)
	}
	if err := validateReleaseEvidencePathOverridePolicy(master); err != nil {
		t.Fatal(err)
	}
	if err := validateReleaseEvidenceTemporaryOCIDirectoryPolicy(master); err != nil {
		t.Fatal(err)
	}
}

func TestWorkflowReleaseEvidenceTemporaryOCIDirectoryPolicyRejectsRetentionAndMissingCleanup(t *testing.T) {
	master := readWorkflow(t, "master.yml")
	for _, test := range []struct {
		name        string
		replacement string
	}{
		{
			name:        "retained OCI directory path",
			replacement: "release-artifacts/image.oci",
		},
		{
			name:        "missing cleanup",
			replacement: "true",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			mutated := master
			switch test.name {
			case "retained OCI directory path":
				mutated.raw = strings.ReplaceAll(mutated.raw, "$RUNNER_TEMP/image.oci", test.replacement)
				mutated.raw = strings.ReplaceAll(mutated.raw, "${{ runner.temp }}/image.oci", test.replacement)
			case "missing cleanup":
				mutated.raw = strings.Replace(mutated.raw, `rm -rf -- "$RUNNER_TEMP/image.oci"`, test.replacement, 1)
			}
			if err := validateReleaseEvidenceTemporaryOCIDirectoryPolicy(mutated); err == nil {
				t.Fatalf("temporary OCI directory policy accepted %s", test.name)
			}
		})
	}
}

func TestWorkflowReleaseMakeInvocationPolicyRejectsNonExactLines(t *testing.T) {
	master := readWorkflow(t, "master.yml")
	pullRequest := readWorkflow(t, "pull-request.yml")
	release := readWorkflow(t, "release.yml")
	for _, mutation := range []struct {
		name        string
		replacement string
	}{
		{name: "extra argument", replacement: "          make release-verify arbitrary-nonrelease-arg"},
		{name: "make option", replacement: "          make -C . release-verify"},
		{name: "environment assignment", replacement: "          EVIDENCE=1 make release-verify"},
		{name: "shell chaining", replacement: "          make release-verify && true"},
		{name: "continued command", replacement: "          make release-verify \\\n            arbitrary-nonrelease-arg"},
		{name: "dynamic target", replacement: "          make \"$RELEASE_EVIDENCE_TARGET\""},
		{name: "dynamic target suffix", replacement: "          make release-$RELEASE_EVIDENCE_TARGET"},
	} {
		t.Run(mutation.name, func(t *testing.T) {
			raw := strings.Replace(master.raw, "          make release-verify", mutation.replacement, 1)
			mutated := parseWorkflow(t, master.name, raw)
			if err := validateReleaseMakeInvocationPolicy(mutated, pullRequest, release); err == nil {
				t.Fatalf("release Make policy accepted mutated line %q", mutation.replacement)
			}
		})
	}

	t.Run("second release target", func(t *testing.T) {
		raw := strings.Replace(master.raw, "          make release-verify", "          make release-verify\n          make release-other", 1)
		mutated := parseWorkflow(t, master.name, raw)
		if err := validateReleaseMakeInvocationPolicy(mutated, pullRequest, release); err == nil {
			t.Fatal("release Make policy accepted a second release target")
		}
	})
}

func TestWorkflowReleaseEvidenceRejectsEvidencePathOverrides(t *testing.T) {
	master := readWorkflow(t, "master.yml")
	for _, mutation := range []struct {
		name string
		old  string
		new  string
	}{
		{
			name: "job environment directory override",
			old:  "    steps:\n      - name: Check out repository",
			new:  "    env:\n      RELEASE_EVIDENCE_DIR: alternate-artifacts\n\n    steps:\n      - name: Check out repository",
		},
		{
			name: "step environment file override",
			old:  "          RELEASE_EVIDENCE_KUBECTL_VERSION: v1.32.6",
			new:  "          RELEASE_EVIDENCE_KUBECTL_VERSION: v1.32.6\n          RELEASE_EVIDENCE_FILE: alternate-evidence.json",
		},
		{
			name: "GitHub environment file override",
			old:  "        run: |\n          bash scripts/release/collect.sh \\\n            --artifact-dir release-artifacts \\\n            --image-oci-layout \"$RUNNER_TEMP/image.oci\"",
			new:  "        run: |\n          echo 'RELEASE_EVIDENCE_DIR=alternate-artifacts' >> \"$GITHUB_ENV\"\n          bash scripts/release/collect.sh \\\n            --artifact-dir release-artifacts \\\n            --image-oci-layout \"$RUNNER_TEMP/image.oci\"",
		},
		{
			name: "shell declaration and export override",
			old:  "          make release-verify",
			new:  "          RELEASE_EVIDENCE_DIR=alternate-artifacts; export RELEASE_EVIDENCE_DIR\n          make release-verify",
		},
		{
			name: "shell unset file override",
			old:  "          make release-verify",
			new:  "          unset RELEASE_EVIDENCE_FILE\n          make release-verify",
		},
	} {
		t.Run(mutation.name, func(t *testing.T) {
			raw := replaceReleaseEvidenceJobSection(t, master.raw, mutation.old, mutation.new)
			mutated := parseWorkflow(t, master.name, raw)
			if err := validateReleaseEvidencePathOverridePolicy(mutated); err == nil {
				t.Fatalf("release evidence path policy accepted %s", mutation.name)
			}
		})
	}
}

func replaceReleaseEvidenceJobSection(t *testing.T, raw, old, new string) string {
	t.Helper()
	start := strings.Index(raw, "\n  release-evidence:\n")
	if start < 0 {
		t.Fatal("test fixture is missing release-evidence job")
	}
	section := raw[start:]
	updated := strings.Replace(section, old, new, 1)
	if updated == section {
		t.Fatalf("test fixture is missing release-evidence mutation anchor %q", old)
	}
	return raw[:start] + updated
}

func TestWorkflowLiveHarnessVerificationIsUncredentialed(t *testing.T) {
	for _, workflow := range []workflowDocument{
		readWorkflow(t, "pull-request.yml"),
		readWorkflow(t, "master.yml"),
	} {
		if !hasRunCommand(workflow, "go test -tags=live ./integration/live -run '^$'") {
			t.Fatalf("%s does not compile the guarded live-provider harness", workflow.name)
		}
		if !hasRunCommand(workflow, "make live-contract-verify") {
			t.Fatalf("%s does not execute the deterministic live-provider contract checks", workflow.name)
		}
		lower := strings.ToLower(workflow.raw)
		for _, forbidden := range []string{"llmtw_live_", "secrets."} {
			if strings.Contains(lower, forbidden) {
				t.Fatalf("%s combines offline live-harness verification with %q", workflow.name, forbidden)
			}
		}
	}
}

func TestWorkflowActionsUseImmutablePinsWithVersionComments(t *testing.T) {
	for _, workflow := range []workflowDocument{
		readWorkflow(t, "pull-request.yml"),
		readWorkflow(t, "master.yml"),
		readWorkflow(t, "release.yml"),
	} {
		for _, reference := range actionReferences(t, workflow) {
			if strings.HasPrefix(reference, "./") {
				continue
			}
			parts := strings.SplitN(reference, "@", 2)
			if len(parts) != 2 || !immutableActionReference.MatchString(parts[1]) {
				t.Fatalf("%s action %q is not pinned to an immutable commit", workflow.name, reference)
			}
		}
		for _, want := range []string{setupGoActionPin} {
			if !strings.Contains(workflow.raw, "uses: "+want+" # v6") {
				t.Fatalf("%s does not record readable v6 comment beside immutable action pin %q", workflow.name, want)
			}
		}
		if workflow.name == "release.yml" {
			continue
		}
		if !strings.Contains(workflow.raw, "uses: "+checkoutActionPin+" # v6") {
			t.Fatalf("%s does not record readable v6 comment beside immutable action pin %q", workflow.name, checkoutActionPin)
		}
	}
}

func TestWorkflowVerificationEntrypoint(t *testing.T) {
	makefile := readRepositoryFile(t, moduleRoot(t), "Makefile")
	if !strings.Contains(makefile, "workflow-verify:") || !strings.Contains(makefile, "scripts/check-workflow-policy.sh") {
		t.Fatal("Makefile does not expose workflow-verify through scripts/check-workflow-policy.sh")
	}

	script := readRepositoryFile(t, moduleRoot(t), "scripts", "check-workflow-policy.sh")
	if !strings.Contains(script, "github.com/rhysd/actionlint/cmd/actionlint@v1.7.12") {
		t.Fatal("workflow policy helper does not pin actionlint v1.7.12")
	}
	const workflowPolicyTests = "go test ./internal/architecturetest -run '^(TestWorkflow.*|TestLiveProviderContractsWorkflowIsManualProtectedAndSingleProfile)$'"
	if !strings.Contains(script, workflowPolicyTests) {
		t.Fatal("workflow policy helper does not execute the live-provider workflow contract test")
	}
}

func TestWorkflowsRunHardenedImageVerification(t *testing.T) {
	for _, name := range []string{"master.yml", "pull-request.yml"} {
		workflow := readWorkflow(t, name)
		if !hasRunCommand(workflow, "make image-verify") {
			t.Fatalf("%s does not run make image-verify", workflow.name)
		}
	}
}

func TestWorkflowsRunPinnedKubernetesDeploymentPolicyVerification(t *testing.T) {
	for _, name := range []string{"master.yml", "pull-request.yml"} {
		workflow := readWorkflow(t, name)
		job := workflowJob(t, workflow, "verify")
		assertJobRunPrecedesRunContains(t, workflow, "verify", "bash scripts/ci/setup-kubectl.sh", "make deployment-policy-verify")
		if !jobHasRunCommand(job, "make deployment-policy-verify") {
			t.Fatalf("%s verify job does not run make deployment-policy-verify", workflow.name)
		}
	}
}

func assertPullRequestTrigger(t *testing.T, workflow workflowDocument) {
	t.Helper()
	triggers := workflowMapping(t, workflow, "on")
	pullRequest := nestedMapping(t, workflow.name, triggers, "pull_request")
	branches := stringSequence(t, workflow.name, pullRequest, "branches")
	if len(branches) != 1 || branches[0] != "master" {
		t.Fatalf("%s pull request branches = %v, want [master]", workflow.name, branches)
	}
}

func assertReadOnlyPermissions(t *testing.T, workflow workflowDocument) {
	t.Helper()
	permissions := workflowMapping(t, workflow, "permissions")
	if len(permissions) != 1 || scalarString(t, workflow.name, permissions, "contents") != "read" {
		t.Fatalf("%s permissions = %#v, want only contents: read", workflow.name, permissions)
	}
}

func assertMasterTriggers(t *testing.T, workflow workflowDocument) {
	t.Helper()
	triggers := workflowMapping(t, workflow, "on")
	push := nestedMapping(t, workflow.name, triggers, "push")
	branches := stringSequence(t, workflow.name, push, "branches")
	if len(branches) != 1 || branches[0] != "master" {
		t.Fatalf("%s push branches = %v, want [master]", workflow.name, branches)
	}
	if _, ok := triggers["workflow_dispatch"]; !ok {
		t.Fatalf("%s does not support workflow_dispatch", workflow.name)
	}

	schedules, ok := triggers["schedule"].([]any)
	if !ok || len(schedules) != 1 {
		t.Fatalf("%s schedule = %#v, want one daily schedule", workflow.name, triggers["schedule"])
	}
	schedule, ok := schedules[0].(map[string]any)
	if !ok {
		t.Fatalf("%s schedule entry = %#v, want mapping", workflow.name, schedules[0])
	}
	if scalarString(t, workflow.name, schedule, "cron") != "0 5 * * *" {
		t.Fatalf("%s cron = %q, want exact 05:00 daily schedule", workflow.name, scalarString(t, workflow.name, schedule, "cron"))
	}
	if scalarString(t, workflow.name, schedule, "timezone") != "Australia/Sydney" {
		t.Fatalf("%s timezone = %q, want Australia/Sydney", workflow.name, scalarString(t, workflow.name, schedule, "timezone"))
	}
}

func assertWorkflowControls(t *testing.T, workflow workflowDocument) {
	t.Helper()
	if _, ok := workflow.fields["concurrency"].(map[string]any); !ok {
		t.Fatalf("%s does not declare workflow concurrency", workflow.name)
	}
	verify := nestedMapping(t, workflow.name, workflowMapping(t, workflow, "jobs"), "verify")
	if _, ok := verify["timeout-minutes"]; !ok {
		t.Fatalf("%s verify job does not declare a timeout", workflow.name)
	}
}

func assertVerificationStep(t *testing.T, workflow workflowDocument) {
	t.Helper()
	if !hasRunCommand(workflow, "make workflow-verify") {
		t.Fatalf("%s does not run make workflow-verify", workflow.name)
	}
}

func assertRequiredOfflineGates(t *testing.T, workflow workflowDocument) {
	t.Helper()
	for _, command := range []string{
		"scripts/check-go-format.sh",
		"make schema-verify",
		"make docs-verify",
		"make deployment-policy-verify",
		"go vet ./...",
		"go test -race ./...",
		"go build ./...",
	} {
		if !strings.Contains(workflow.raw, command) {
			t.Fatalf("%s does not retain required offline gate %q", workflow.name, command)
		}
	}
}

func readWorkflow(t *testing.T, name string) workflowDocument {
	t.Helper()
	root := repositoryRoot(t)
	data, err := os.ReadFile(filepath.Join(root, ".github", "workflows", name))
	if err != nil {
		t.Fatal(err)
	}
	return parseWorkflow(t, name, string(data))
}

func parseWorkflow(t *testing.T, name, raw string) workflowDocument {
	t.Helper()
	fields := map[string]any{}
	if err := yaml.Load([]byte(raw), &fields, yaml.WithUniqueKeys(), yaml.WithSingleDocument()); err != nil {
		t.Fatalf("workflow %s is not valid YAML: %v", name, err)
	}
	return workflowDocument{name: name, raw: raw, fields: fields}
}

func readRepositoryFile(t *testing.T, root string, path ...string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(append([]string{root}, path...)...))
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func workflowMapping(t *testing.T, workflow workflowDocument, key string) map[string]any {
	t.Helper()
	return nestedMapping(t, workflow.name, workflow.fields, key)
}

func nestedMapping(t *testing.T, name string, fields map[string]any, key string) map[string]any {
	t.Helper()
	value, ok := fields[key]
	if !ok {
		t.Fatalf("%s is missing %q", name, key)
	}
	mapping, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("%s %q = %#v, want mapping", name, key, value)
	}
	return mapping
}

func stringSequence(t *testing.T, name string, fields map[string]any, key string) []string {
	t.Helper()
	value, ok := fields[key]
	if !ok {
		t.Fatalf("%s is missing %q", name, key)
	}
	sequence, ok := value.([]any)
	if !ok {
		t.Fatalf("%s %q = %#v, want sequence", name, key, value)
	}
	result := make([]string, len(sequence))
	for index, item := range sequence {
		stringValue, ok := item.(string)
		if !ok {
			t.Fatalf("%s %q item %d = %#v, want string", name, key, index, item)
		}
		result[index] = stringValue
	}
	return result
}

func scalarString(t *testing.T, name string, fields map[string]any, key string) string {
	t.Helper()
	value, ok := fields[key]
	if !ok {
		t.Fatalf("%s is missing %q", name, key)
	}
	stringValue, ok := value.(string)
	if !ok {
		t.Fatalf("%s %q = %#v, want string", name, key, value)
	}
	return stringValue
}

func actionReferences(t *testing.T, workflow workflowDocument) []string {
	t.Helper()
	jobs := workflowMapping(t, workflow, "jobs")
	var references []string
	for name, rawJob := range jobs {
		job, ok := rawJob.(map[string]any)
		if !ok {
			t.Fatalf("%s job %q = %#v, want mapping", workflow.name, name, rawJob)
		}
		steps, ok := job["steps"].([]any)
		if !ok {
			t.Fatalf("%s job %q steps = %#v, want sequence", workflow.name, name, job["steps"])
		}
		for index, rawStep := range steps {
			step, ok := rawStep.(map[string]any)
			if !ok {
				t.Fatalf("%s job %q step %d = %#v, want mapping", workflow.name, name, index, rawStep)
			}
			if reference, ok := step["uses"].(string); ok {
				references = append(references, reference)
			}
		}
	}
	return references
}

func workflowJob(t *testing.T, workflow workflowDocument, name string) map[string]any {
	t.Helper()
	jobs := workflowMapping(t, workflow, "jobs")
	rawJob, ok := jobs[name]
	if !ok {
		t.Fatalf("%s is missing job %q", workflow.name, name)
	}
	job, ok := rawJob.(map[string]any)
	if !ok {
		t.Fatalf("%s job %q = %#v, want mapping", workflow.name, name, rawJob)
	}
	return job
}

func assertJobUsesAction(t *testing.T, workflow workflowDocument, jobName, want string) {
	t.Helper()
	job := workflowJob(t, workflow, jobName)
	steps, ok := job["steps"].([]any)
	if !ok {
		t.Fatalf("%s job %q has no steps", workflow.name, jobName)
	}
	for _, rawStep := range steps {
		step, ok := rawStep.(map[string]any)
		if !ok {
			continue
		}
		if step["uses"] == want {
			return
		}
	}
	t.Fatalf("%s job %q does not use %q", workflow.name, jobName, want)
}

func assertJobHasRunCommand(t *testing.T, workflow workflowDocument, jobName, want string) {
	t.Helper()
	if !jobHasRunCommand(workflowJob(t, workflow, jobName), want) {
		t.Fatalf("%s job %q does not run %q", workflow.name, jobName, want)
	}
}

func assertJobRunContains(t *testing.T, workflow workflowDocument, jobName, want string) {
	t.Helper()
	job := workflowJob(t, workflow, jobName)
	steps, ok := job["steps"].([]any)
	if !ok {
		t.Fatalf("%s job %q has no steps", workflow.name, jobName)
	}
	for _, rawStep := range steps {
		step, ok := rawStep.(map[string]any)
		if !ok {
			continue
		}
		if run, _ := step["run"].(string); strings.Contains(run, want) {
			return
		}
	}
	t.Fatalf("%s job %q does not contain %q in a run command", workflow.name, jobName, want)
}

func assertJobRunPrecedesRunContains(t *testing.T, workflow workflowDocument, jobName, before, after string) {
	t.Helper()
	job := workflowJob(t, workflow, jobName)
	steps, ok := job["steps"].([]any)
	if !ok {
		t.Fatalf("%s job %q has no steps", workflow.name, jobName)
	}
	seenBefore := false
	for _, rawStep := range steps {
		step, ok := rawStep.(map[string]any)
		if !ok {
			continue
		}
		run, _ := step["run"].(string)
		if strings.Contains(run, before) {
			seenBefore = true
		}
		if strings.Contains(run, after) {
			if !seenBefore {
				t.Fatalf("%s job %q runs %q before %q", workflow.name, jobName, after, before)
			}
			return
		}
	}
	t.Fatalf("%s job %q does not run %q", workflow.name, jobName, after)
}

func assertJobActionInput(t *testing.T, workflow workflowDocument, jobName, action, input, want string) {
	t.Helper()
	job := workflowJob(t, workflow, jobName)
	steps, ok := job["steps"].([]any)
	if !ok {
		t.Fatalf("%s job %q has no steps", workflow.name, jobName)
	}
	for _, rawStep := range steps {
		step, ok := rawStep.(map[string]any)
		if !ok || step["uses"] != action {
			continue
		}
		with, ok := step["with"].(map[string]any)
		if !ok {
			t.Fatalf("%s job %q action %q has no inputs", workflow.name, jobName, action)
		}
		if got, ok := with[input].(string); !ok || got != want {
			t.Fatalf("%s job %q action %q input %q = %#v, want %q", workflow.name, jobName, action, input, with[input], want)
		}
		return
	}
	t.Fatalf("%s job %q does not use %q", workflow.name, jobName, action)
}

func assertJobActionPrecedesRunCommand(t *testing.T, workflow workflowDocument, jobName, action, command string) {
	t.Helper()
	job := workflowJob(t, workflow, jobName)
	steps, ok := job["steps"].([]any)
	if !ok {
		t.Fatalf("%s job %q has no steps", workflow.name, jobName)
	}
	seenAction := false
	for _, rawStep := range steps {
		step, ok := rawStep.(map[string]any)
		if !ok {
			continue
		}
		if step["uses"] == action {
			seenAction = true
		}
		run, _ := step["run"].(string)
		for _, line := range strings.Split(run, "\n") {
			if strings.TrimSpace(line) != command {
				continue
			}
			if !seenAction {
				t.Fatalf("%s job %q runs %q before %q", workflow.name, jobName, command, action)
			}
			return
		}
	}
	t.Fatalf("%s job %q does not run %q", workflow.name, jobName, command)
}

func assertJobActionPrecedesRunContains(t *testing.T, workflow workflowDocument, jobName, action, command string) {
	t.Helper()
	job := workflowJob(t, workflow, jobName)
	steps, ok := job["steps"].([]any)
	if !ok {
		t.Fatalf("%s job %q has no steps", workflow.name, jobName)
	}
	seenAction := false
	for _, rawStep := range steps {
		step, ok := rawStep.(map[string]any)
		if !ok {
			continue
		}
		if step["uses"] == action {
			seenAction = true
		}
		run, _ := step["run"].(string)
		if !strings.Contains(run, command) {
			continue
		}
		if !seenAction {
			t.Fatalf("%s job %q runs %q before %q", workflow.name, jobName, command, action)
		}
		return
	}
	t.Fatalf("%s job %q does not run a command containing %q", workflow.name, jobName, command)
}

func assertJobReadOnlyPermissions(t *testing.T, workflowName, jobName string, job map[string]any) {
	t.Helper()
	permissions := nestedMapping(t, workflowName, job, "permissions")
	if len(permissions) != 1 || scalarString(t, workflowName, permissions, "contents") != "read" {
		t.Fatalf("%s job %q permissions = %#v, want only contents: read", workflowName, jobName, permissions)
	}
}

func jobHasRunCommand(job map[string]any, command string) bool {
	steps, ok := job["steps"].([]any)
	if !ok {
		return false
	}
	for _, rawStep := range steps {
		step, ok := rawStep.(map[string]any)
		if !ok {
			continue
		}
		run, ok := step["run"].(string)
		if !ok {
			continue
		}
		for _, line := range strings.Split(run, "\n") {
			if strings.TrimSpace(line) == command {
				return true
			}
		}
	}
	return false
}

func releaseMakeInvocations(workflow workflowDocument) []releaseMakeInvocation {
	jobs, ok := workflow.fields["jobs"].(map[string]any)
	if !ok {
		return nil
	}
	var invocations []releaseMakeInvocation
	for jobName, rawJob := range jobs {
		job, ok := rawJob.(map[string]any)
		if !ok {
			continue
		}
		steps, ok := job["steps"].([]any)
		if !ok {
			continue
		}
		for _, rawStep := range steps {
			step, ok := rawStep.(map[string]any)
			if !ok {
				continue
			}
			run, ok := step["run"].(string)
			if !ok {
				continue
			}
			for _, line := range shellLogicalLines(run) {
				fields := strings.Fields(strings.TrimSpace(line))
				for index, field := range fields {
					if normalizeShellWord(field) != "make" {
						continue
					}
					for _, candidate := range fields[index+1:] {
						target := normalizeMakeTarget(candidate)
						if strings.HasPrefix(target, "release") {
							invocations = append(invocations, releaseMakeInvocation{
								workflow: workflow.name,
								job:      jobName,
								target:   target,
								line:     strings.TrimSpace(line),
							})
							break
						}
						if shellCommandDelimiter(candidate) {
							break
						}
					}
				}
			}
		}
	}
	return invocations
}

func validateReleaseEvidencePathOverridePolicy(workflow workflowDocument) error {
	jobs, ok := workflow.fields["jobs"].(map[string]any)
	if !ok {
		return fmt.Errorf("%s has no jobs mapping", workflow.name)
	}
	rawJob, ok := jobs["release-evidence"]
	if !ok {
		return fmt.Errorf("%s is missing release-evidence job", workflow.name)
	}
	job, ok := rawJob.(map[string]any)
	if !ok {
		return fmt.Errorf("%s release-evidence job is not a mapping", workflow.name)
	}
	if err := validateNoEvidencePathEnvironment(workflow.name+" workflow", workflow.fields["env"]); err != nil {
		return err
	}
	if err := validateNoEvidencePathEnvironment(workflow.name+" release-evidence job", job["env"]); err != nil {
		return err
	}
	steps, ok := job["steps"].([]any)
	if !ok {
		return fmt.Errorf("%s release-evidence job has no steps", workflow.name)
	}
	for index, rawStep := range steps {
		step, ok := rawStep.(map[string]any)
		if !ok {
			return fmt.Errorf("%s release-evidence step %d is not a mapping", workflow.name, index)
		}
		if err := validateNoEvidencePathEnvironment(fmt.Sprintf("%s release-evidence step %d", workflow.name, index), step["env"]); err != nil {
			return err
		}
		if rawRun, found := step["run"]; found {
			run, ok := rawRun.(string)
			if !ok {
				return fmt.Errorf("%s release-evidence step %d run command is not a string", workflow.name, index)
			}
			if err := validateNoEvidencePathEnvironmentWrite(fmt.Sprintf("%s release-evidence step %d", workflow.name, index), run); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateReleaseEvidenceTemporaryOCIDirectoryPolicy(workflow workflowDocument) error {
	const temporaryOCIDirectory = "$RUNNER_TEMP/image.oci"
	for _, required := range []string{
		"bash scripts/release/collect.sh \\",
		"--artifact-dir release-artifacts \\",
		"--image-oci-layout \"$RUNNER_TEMP/image.oci\"",
		"layout-digest -layout \"$RUNNER_TEMP/image.oci\"",
		"oci-dir:\"$RUNNER_TEMP/image.oci\"",
		"--input \"$RUNNER_TEMP/image.oci\"",
		`rm -rf -- "$RUNNER_TEMP/image.oci"`,
	} {
		if !strings.Contains(workflow.raw, required) {
			return fmt.Errorf("%s does not use the required temporary OCI directory boundary %q", workflow.name, required)
		}
	}
	for _, forbidden := range []string{
		"image.oci.tar",
		"oci-archive:",
		"docker load --input",
		"release-artifacts/image.oci",
		"-artifact image_layout=",
	} {
		if strings.Contains(workflow.raw, forbidden) {
			return fmt.Errorf("%s retains or consumes a forbidden raw OCI image form %q", workflow.name, forbidden)
		}
	}
	upload := strings.Index(workflow.raw, "path: release-artifacts/")
	cleanup := strings.Index(workflow.raw, `rm -rf -- "$RUNNER_TEMP/image.oci"`)
	if upload < 0 || cleanup <= upload {
		return fmt.Errorf("%s does not remove the temporary OCI directory after artifact upload", workflow.name)
	}
	if !strings.Contains(workflow.raw, temporaryOCIDirectory) {
		return fmt.Errorf("%s does not bind all OCI tooling to the runner temporary directory", workflow.name)
	}
	return nil
}

func validateNoEvidencePathEnvironment(scope string, raw any) error {
	if raw == nil {
		return nil
	}
	environment, ok := raw.(map[string]any)
	if !ok {
		return fmt.Errorf("%s environment is not a mapping", scope)
	}
	for _, name := range []string{"RELEASE_EVIDENCE_DIR", "RELEASE_EVIDENCE_FILE"} {
		if _, found := environment[name]; found {
			return fmt.Errorf("%s must not override %s", scope, name)
		}
	}
	return nil
}

func validateNoEvidencePathEnvironmentWrite(scope, run string) error {
	lower := strings.ToLower(run)
	for _, name := range []string{"RELEASE_EVIDENCE_DIR", "RELEASE_EVIDENCE_FILE"} {
		if strings.Contains(lower, strings.ToLower(name)) {
			// The canonical directory and evidence filename are intentionally
			// fixed inside trusted CI. Reject every shell reference here rather
			// than trying to parse shell syntax: that closes assignment, export,
			// unset, and $GITHUB_ENV variants, including split declaration forms.
			return fmt.Errorf("%s must not reference reserved evidence path variable %s in a run command", scope, name)
		}
	}
	return nil
}

func validateReleaseMakeInvocationPolicy(workflows ...workflowDocument) error {
	var invocations []releaseMakeInvocation
	for _, workflow := range workflows {
		if err := validateNoDynamicMakeArguments(workflow); err != nil {
			return err
		}
		invocations = append(invocations, releaseMakeInvocations(workflow)...)
	}
	want := map[string]releaseMakeInvocation{
		"master.yml/release-evidence": {
			workflow: "master.yml",
			job:      "release-evidence",
			target:   "release-verify",
			line:     "make release-verify",
		},
		"release.yml/preflight": {
			workflow: "release.yml",
			job:      "preflight",
			target:   "release-verify",
			line:     "make release-verify",
		},
	}
	if len(invocations) != len(want) {
		return fmt.Errorf("release evidence policy found %d make release* invocations, want %#v: %#v", len(invocations), want, invocations)
	}
	for _, invocation := range invocations {
		key := invocation.workflow + "/" + invocation.job
		expected, ok := want[key]
		if !ok || invocation != expected {
			return fmt.Errorf("release evidence policy found unexpected Make invocation %#v", invocation)
		}
		delete(want, key)
	}
	if len(want) != 0 {
		return fmt.Errorf("release evidence policy is missing Make invocations %#v", want)
	}
	return nil
}

func validateNoDynamicMakeArguments(workflow workflowDocument) error {
	jobs, ok := workflow.fields["jobs"].(map[string]any)
	if !ok {
		return fmt.Errorf("%s has no jobs mapping", workflow.name)
	}
	for jobName, rawJob := range jobs {
		job, ok := rawJob.(map[string]any)
		if !ok {
			continue
		}
		steps, ok := job["steps"].([]any)
		if !ok {
			continue
		}
		for _, rawStep := range steps {
			step, ok := rawStep.(map[string]any)
			if !ok {
				continue
			}
			run, ok := step["run"].(string)
			if !ok {
				continue
			}
			for _, line := range shellLogicalLines(run) {
				fields := strings.Fields(strings.TrimSpace(line))
				for index, field := range fields {
					if normalizeShellWord(field) != "make" {
						continue
					}
					for _, candidate := range fields[index+1:] {
						if shellCommandDelimiter(candidate) {
							break
						}
						if hasShellExpansion(candidate) {
							return fmt.Errorf("%s job %q uses a dynamic Make argument", workflow.name, jobName)
						}
					}
				}
			}
		}
	}
	return nil
}

func hasShellExpansion(word string) bool {
	return strings.ContainsAny(word, "$*?[") || strings.ContainsRune(word, '`') || strings.Contains(word, "{")
}

func shellLogicalLines(run string) []string {
	var lines []string
	var pending string
	for _, rawLine := range strings.Split(run, "\n") {
		line := strings.TrimSpace(rawLine)
		if pending == "" {
			pending = line
		} else {
			pending += " " + line
		}
		if strings.HasSuffix(pending, "\\") {
			pending = strings.TrimSpace(strings.TrimSuffix(pending, "\\"))
			continue
		}
		if pending != "" {
			lines = append(lines, pending)
		}
		pending = ""
	}
	if pending != "" {
		lines = append(lines, pending)
	}
	return lines
}

func normalizeShellWord(word string) string {
	return strings.Trim(word, "'\"$;&|()")
}

func normalizeMakeTarget(word string) string {
	return strings.Trim(word, "'\"$;&|()\\")
}

func shellCommandDelimiter(word string) bool {
	switch word {
	case ";", "&&", "||", "|", "&":
		return true
	default:
		return false
	}
}

func hasRunCommand(workflow workflowDocument, command string) bool {
	jobs, ok := workflow.fields["jobs"].(map[string]any)
	if !ok {
		return false
	}
	for _, rawJob := range jobs {
		job, ok := rawJob.(map[string]any)
		if !ok {
			continue
		}
		steps, ok := job["steps"].([]any)
		if !ok {
			continue
		}
		for _, rawStep := range steps {
			step, ok := rawStep.(map[string]any)
			if !ok {
				continue
			}
			run, ok := step["run"].(string)
			if !ok {
				continue
			}
			for _, line := range strings.Split(run, "\n") {
				if strings.TrimSpace(line) == command {
					return true
				}
			}
		}
	}
	return false
}
