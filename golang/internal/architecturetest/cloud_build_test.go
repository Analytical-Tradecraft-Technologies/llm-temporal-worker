package architecturetest

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestWorkflowMasterCloudPublicationBoundary(t *testing.T) {
	master := readWorkflow(t, "master.yml")
	if strings.Count(master.raw, "${{ secrets.DOCKER_ACCESS_TOKEN }}") != 2 {
		t.Fatal("registry token must appear only in the two protected cloud setup steps")
	}
	for _, jobName := range []string{"container", "verify-image"} {
		job := workflowJob(t, master, jobName)
		if job["environment"] != "docker_push" || !strings.Contains(scalarString(t, master.name, job, "if"), "github.ref == 'refs/heads/master'") {
			t.Fatalf("%s must require master and docker_push", jobName)
		}
		stepsForCheckout, _ := job["steps"].([]any)
		for _, raw := range stepsForCheckout {
			step := raw.(map[string]any)
			if uses, ok := step["uses"].(string); ok && strings.HasPrefix(uses, "actions/checkout@") {
				with := step["with"].(map[string]any)
				if with["persist-credentials"] != false {
					t.Fatal("checkout persists credentials")
				}
			}
		}
		assertJobHasRunCommand(t, master, jobName, "bash scripts/ci/setup-build-cloud.sh")
		steps, _ := job["steps"].([]any)
		foundSecret, foundCleanup := false, false
		for _, raw := range steps {
			step := raw.(map[string]any)
			if environment, ok := step["env"].(map[string]any); ok {
				if value, ok := environment["DOCKER_ACCESS_TOKEN"]; ok {
					if value != "${{ secrets.DOCKER_ACCESS_TOKEN }}" || step["run"] != "bash scripts/ci/setup-build-cloud.sh" {
						t.Fatalf("%s exposes the registry token beyond setup", jobName)
					}
					foundSecret = true
				}
			}
			if step["name"] == "Remove Docker Cloud credentials" {
				foundCleanup = step["if"] == "always()" && step["run"] == "rm -rf -- \"$RUNNER_TEMP/llmtw-cloud-docker\""
			}
		}
		if !foundSecret || !foundCleanup {
			t.Fatalf("%s lacks scoped authentication or unconditional cleanup", jobName)
		}
	}
	assertJobRunPrecedesRunContains(t, master, "verify-image", "bash scripts/ci/setup-build-cloud.sh", "make compose-live-integration")
	assertJobRunPrecedesRunContains(t, master, "verify-image", "bash scripts/ci/setup-build-cloud.sh", "make image-verify")
	assertJobRunContains(t, master, "release-evidence", "skopeo --command-timeout 5m copy --preserve-digests")
	assertJobRunContains(t, master, "release-evidence", `[[ "$digest" == "$PUBLISHED_DIGEST" ]]`)
	for _, want := range []string{
		`bash scripts/ci/build-scanned-image.sh "$RUNNER_TEMP/candidate.oci"`,
		"copy --all --preserve-digests", `"oci:$RUNNER_TEMP/candidate.oci"`,
		`"docker://docker.io/analyticaltradecraft/llm-temporal-worker:$IMAGE_TAG"`,
		"date -u +%Y%m%d", "GITHUB_RUN_NUMBER", `imagetools inspect "$image:$IMAGE_TAG" --raw`,
		`[[ "$published" != "$digest" ]]`, `sort == ["amd64", "arm64"]`,
	} {
		assertJobRunContains(t, master, "container", want)
	}
	// Both platforms are built and scanned before the exact scanned bytes are
	// published; the job never pushes from the builder (#971).
	assertJobRunPrecedesRunContains(t, master, "container", "bash scripts/ci/setup-build-cloud.sh", "bash scripts/ci/build-scanned-image.sh")
	assertJobRunPrecedesRunContains(t, master, "container", "bash scripts/ci/setup-trivy.sh", "bash scripts/ci/build-scanned-image.sh")
	assertJobRunPrecedesRunContains(t, master, "container", "bash scripts/ci/build-scanned-image.sh", "copy --all --preserve-digests")
	assertJobRunPrecedesRunContains(t, master, "container", "copy --all --preserve-digests", `imagetools inspect "$image:$IMAGE_TAG" --raw`)
	for _, forbidden := range []string{"--push", "oci-archive:", "docker buildx build"} {
		if jobRunContains(workflowJob(t, master, "container"), forbidden) {
			t.Fatalf("master container job runs %q instead of publishing the scanned layout", forbidden)
		}
	}
	assertMasterJobNeedsEveryVerificationGate(t, master, "container", "ocaml", "fuzz-shard")
	for _, forbidden := range []string{"setup-buildx.sh", "setup-multinode-buildx.sh", "type=gha", "setup-qemu", "--load"} {
		if strings.Contains(master.raw, forbidden) {
			t.Fatalf("master retains local/copying build configuration %q", forbidden)
		}
	}
	pr := readWorkflow(t, "pull-request.yml")
	assertJobRunPrecedesRunContains(t, pr, "container", "bash scripts/ci/setup-containerd-image-store.sh", "bash scripts/ci/setup-buildx.sh")
	assertJobHasRunCommand(t, pr, "container", "make image-verify")
	assertJobRunContains(t, pr, "container", "go run ./tools/releaseverify layout-digest")
	if strings.Contains(pr.raw, "docker_push") || strings.Contains(pr.raw, "DOCKER_ACCESS_TOKEN") || strings.Contains(pr.raw, "setup-build-cloud.sh") {
		t.Fatal("PR and merge-queue builds must remain uncredentialed and local")
	}
}

// The merge queue must exercise master's container build path, so a builder
// or exporter incompatibility fails before merge rather than on master
// (#971). It runs master's own build-and-scan script on an uncredentialed
// local builder with Build Cloud's one-node-per-platform shape, inside the
// required "Container image" check, and never publishes.
func TestWorkflowMergeQueueBuildsAndScansLikeMaster(t *testing.T) {
	pr := readWorkflow(t, "pull-request.yml")
	job := workflowJob(t, pr, "container")
	if got := scalarString(t, pr.name, job, "name"); got != "Container image" {
		t.Fatalf("pull-request container job name = %q, want the required ruleset context", got)
	}
	mergeQueueCommands := []string{
		"bash scripts/ci/setup-multinode-buildx.sh",
		"bash scripts/ci/setup-trivy.sh",
		`bash scripts/ci/build-scanned-image.sh "$RUNNER_TEMP/candidate.oci"`,
		"go run ./tools/ocimerge verify -layout \"$RUNNER_TEMP/candidate.oci\" -platform linux/amd64 -platform linux/arm64",
	}
	steps, _ := job["steps"].([]any)
	for _, command := range mergeQueueCommands {
		found := false
		for _, raw := range steps {
			step := raw.(map[string]any)
			run, _ := step["run"].(string)
			if !strings.Contains(run, command) {
				continue
			}
			found = true
			if step["if"] != "github.event_name == 'merge_group'" {
				t.Fatalf("pull-request container step running %q has if %v, want merge_group only", command, step["if"])
			}
		}
		if !found {
			t.Fatalf("pull-request container job does not run %q", command)
		}
	}
	assertJobRunPrecedesRunContains(t, pr, "container", "bash scripts/ci/setup-multinode-buildx.sh", "bash scripts/ci/build-scanned-image.sh")
	assertJobRunPrecedesRunContains(t, pr, "container", "bash scripts/ci/setup-trivy.sh", "bash scripts/ci/build-scanned-image.sh")
	if timeout, _ := job["timeout-minutes"].(string); !strings.Contains(timeout, "github.event_name == 'merge_group'") {
		t.Fatalf("pull-request container job timeout %v does not allow for the merge-queue build", job["timeout-minutes"])
	}
	for _, forbidden := range []string{"skopeo", "--push", "docker://", "imagetools create", "oci-archive:"} {
		if jobRunContains(job, forbidden) {
			t.Fatalf("pull-request container job runs %q; the merge-queue build must not publish", forbidden)
		}
	}
	master := readWorkflow(t, "master.yml")
	assertJobRunContains(t, master, "container", `bash scripts/ci/build-scanned-image.sh "$RUNNER_TEMP/candidate.oci"`)
}

// The shared build script builds each platform on its own (a single-platform
// build runs on one node, so the OCI exporter works on a multi-node builder),
// scans each exact layout at the release severity gate, and only then
// assembles the image index. It never pushes.
func TestWorkflowBuildScannedImageScriptPolicy(t *testing.T) {
	script := readRepositoryFile(t, repositoryRoot(t), "scripts", "ci", "build-scanned-image.sh")
	for _, want := range []string{
		"architectures=(amd64 arm64)",
		`--builder "$BUILDX_BUILDER"`,
		`--platform "linux/$arch"`,
		"--pull --provenance=mode=max --sbom=true",
		`type=oci,oci-mediatypes=true,tar=true,dest=$work/$arch.tar`,
		`--config "$repository_root/scripts/release/trivy.yaml"`,
		"--exit-code 1",
		"config-digest",
		".Metadata.ImageID == $id",
		`"$work/ocimerge" merge -output "$output"`,
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("build-scanned-image.sh does not retain %q", want)
		}
	}
	for _, forbidden := range []string{"--push", "oci-archive:", "linux/amd64,linux/arm64", "skopeo", "docker login"} {
		if strings.Contains(script, forbidden) {
			t.Fatalf("build-scanned-image.sh contains %q", forbidden)
		}
	}
	if strings.Index(script, "trivy image") > strings.Index(script, `"$work/ocimerge" merge`) {
		t.Fatal("build-scanned-image.sh assembles the image before scanning it")
	}
}

func TestWorkflowBuildScannedImageScriptScansBothPlatformsBeforeAssembling(t *testing.T) {
	configDigest := "sha256:" + strings.Repeat("c", 64)
	indexDigest := "sha256:" + strings.Repeat("d", 64)
	for _, scenario := range []string{"success", "finding-arm64", "unbound-scan", "build-failure"} {
		t.Run(scenario, func(t *testing.T) {
			dir := t.TempDir()
			bin := filepath.Join(dir, "bin")
			if err := os.Mkdir(bin, 0o700); err != nil {
				t.Fatal(err)
			}
			log := filepath.Join(dir, "calls")
			writeFakeCommand(t, bin, "docker", `
printf 'docker %s\n' "$*" >> "$CALL_LOG"
if [[ "$SCENARIO" == build-failure ]]; then exit 1; fi
for argument in "$@"; do
  case "$argument" in
    type=oci,*dest=*) dest="${argument##*dest=}" ;;
  esac
done
scratch="$(mktemp -d)"
printf '{"imageLayoutVersion":"1.0.0"}' > "$scratch/oci-layout"
tar -cf "$dest" -C "$scratch" oci-layout
`)
			writeFakeCommand(t, bin, "go", `
printf 'go %s\n' "$*" >> "$CALL_LOG"
[[ "$1" == build && "$2" == -o ]]
cat > "$3" <<'FAKE'
#!/usr/bin/env bash
set -euo pipefail
printf 'ocimerge %s\n' "$*" >> "$CALL_LOG"
case "$1" in
  config-digest) printf '%s\n' "$CONFIG_DIGEST" ;;
  merge) mkdir -- "$3"; printf '%s\n' "$INDEX_DIGEST" ;;
esac
FAKE
chmod +x "$3"
`)
			writeFakeCommand(t, bin, "trivy", `
printf 'trivy %s\n' "$*" >> "$CALL_LOG"
while [[ "$1" != --input ]]; do shift; done
arch="$(basename "$2" .oci)"
while [[ "$1" != --output ]]; do shift; done
image_id="$CONFIG_DIGEST"
[[ "$SCENARIO" != unbound-scan ]] || image_id="sha256:0000000000000000000000000000000000000000000000000000000000000000"
printf '{"Metadata":{"ImageID":"%s","ImageConfig":{"architecture":"%s"}}}' "$image_id" "$arch" > "$2"
[[ "$SCENARIO" != "finding-$arch" ]]
`)
			runnerTemp := filepath.Join(dir, "runner")
			if err := os.Mkdir(runnerTemp, 0o700); err != nil {
				t.Fatal(err)
			}
			output := filepath.Join(dir, "candidate.oci")
			cmd := exec.Command("bash", filepath.Join(repositoryRoot(t), "scripts", "ci", "build-scanned-image.sh"), output)
			cmd.Env = []string{
				"PATH=" + bin + ":" + os.Getenv("PATH"), "HOME=" + dir, "RUNNER_TEMP=" + runnerTemp,
				"TRIVY_CACHE_DIR=" + filepath.Join(dir, "trivy-cache"), "BUILDX_BUILDER=cloud-builder",
				"IMAGE_VERSION=20261007.1", "IMAGE_REVISION=abc", "IMAGE_BUILD_TIME=2026-10-07T00:00:00Z",
				"IMAGE_SOURCE=https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker", "IMAGE_GO_VERSION=go1.26.7",
				"CALL_LOG=" + log, "SCENARIO=" + scenario, "CONFIG_DIGEST=" + configDigest, "INDEX_DIGEST=" + indexDigest,
			}
			var stdout strings.Builder
			cmd.Stdout = &stdout
			cmd.Stderr = &stdout
			err := cmd.Run()
			callBytes, _ := os.ReadFile(log)
			calls := string(callBytes)
			if leftovers, _ := filepath.Glob(filepath.Join(runnerTemp, "llmtw-image-candidate.*")); len(leftovers) != 0 {
				t.Fatalf("work directory not removed: %v", leftovers)
			}
			if scenario != "success" {
				if err == nil {
					t.Fatalf("%s accepted:\n%s", scenario, stdout.String())
				}
				if strings.Contains(calls, "ocimerge merge") {
					t.Fatalf("%s still assembled the image:\n%s", scenario, calls)
				}
				if _, statErr := os.Lstat(output); !os.IsNotExist(statErr) {
					t.Fatalf("%s left an image candidate behind", scenario)
				}
				return
			}
			if err != nil {
				t.Fatalf("build-scanned-image.sh failed: %v\n%s", err, stdout.String())
			}
			if !strings.HasSuffix(stdout.String(), indexDigest+"\n") {
				t.Fatalf("stdout = %q, want the index digest last", stdout.String())
			}
			builds := strings.Count(calls, "docker buildx build --builder cloud-builder ")
			if builds != 2 || !strings.Contains(calls, "--platform linux/amd64 --pull --provenance=mode=max --sbom=true") || !strings.Contains(calls, "--platform linux/arm64 --pull --provenance=mode=max --sbom=true") {
				t.Fatalf("expected one single-platform build per architecture:\n%s", calls)
			}
			for _, arch := range []string{"amd64", "arm64"} {
				scan := strings.Index(calls, "trivy image --input "+filepath.Join(runnerTemp))
				if scan == -1 || !strings.Contains(calls, arch+".oci --format json") {
					t.Fatalf("linux/%s was not scanned:\n%s", arch, calls)
				}
			}
			if strings.Count(calls, "--exit-code 1") != 2 {
				t.Fatalf("both scans must fail on findings:\n%s", calls)
			}
			lastScan := strings.LastIndex(calls, "trivy image")
			merge := strings.Index(calls, "ocimerge merge")
			lastBuild := strings.LastIndex(calls, "docker buildx build")
			if merge == -1 || lastScan > merge || lastBuild > strings.Index(calls, "trivy image") {
				t.Fatalf("expected build, build, scan, scan, merge:\n%s", calls)
			}
			if !strings.Contains(calls, "-platform linux/amd64="+filepath.Join(runnerTemp)) || !strings.Contains(calls, "-platform linux/arm64=") {
				t.Fatalf("merge does not assemble both platforms:\n%s", calls)
			}
		})
	}
}

func TestWorkflowMultiNodeBuilderMatchesBuildCloudShape(t *testing.T) {
	setup := readRepositoryFile(t, repositoryRoot(t), "scripts", "ci", "setup-multinode-buildx.sh")
	cloud := readRepositoryFile(t, repositoryRoot(t), "scripts", "ci", "setup-build-cloud.sh")
	for _, pin := range []string{`readonly buildx_version="v0.37.1"`, `readonly buildx_sha256="9447199cdb435f25880548343c128a4b6650e8891ee598905d8d29d39a8e359b"`} {
		if !strings.Contains(setup, pin) || !strings.Contains(cloud, pin) {
			t.Fatalf("the merge-queue builder and Build Cloud must use the same Buildx release (%s)", pin)
		}
	}
	for _, scenario := range []string{"success", "single-node", "checksum", "credentialed", "no-emulation"} {
		t.Run(scenario, func(t *testing.T) {
			dir := t.TempDir()
			bin := filepath.Join(dir, "bin")
			if err := os.Mkdir(bin, 0o700); err != nil {
				t.Fatal(err)
			}
			binfmt := filepath.Join(dir, "binfmt_misc")
			if err := os.Mkdir(binfmt, 0o700); err != nil {
				t.Fatal(err)
			}
			if scenario != "no-emulation" {
				if err := os.WriteFile(filepath.Join(binfmt, "qemu-aarch64"), []byte("enabled\n"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			log := filepath.Join(dir, "calls")
			envFile := filepath.Join(dir, "github-env")
			writeFakeCommand(t, bin, "curl", `
printf 'curl\n' >> "$CALL_LOG"
while [[ "$1" != --output ]]; do shift; done
printf 'binary' > "$2"
`)
			writeFakeCommand(t, bin, "sha256sum", `
cat >/dev/null
printf 'sha256sum\n' >> "$CALL_LOG"
[[ "$SCENARIO" != checksum ]]
`)
			writeFakeCommand(t, bin, "install", `printf 'install\n' >> "$CALL_LOG"`)
			writeFakeCommand(t, bin, "sudo", `printf 'sudo %s\n' "$*" >> "$CALL_LOG"`)
			writeFakeCommand(t, bin, "docker", `
printf 'docker %s\n' "$*" >> "$CALL_LOG"
if [[ "$1 $2" == "buildx build" ]]; then
  if [[ "$SCENARIO" == single-node ]]; then exit 0; fi
  echo 'ERROR: failed to build: oci for multi-node builds currently not supported' >&2
  exit 1
fi
`)
			cmd := exec.Command("bash", filepath.Join(repositoryRoot(t), "scripts", "ci", "setup-multinode-buildx.sh"))
			cmd.Env = []string{"PATH=" + bin + ":" + os.Getenv("PATH"), "HOME=" + dir, "RUNNER_TEMP=" + dir, "GITHUB_ENV=" + envFile, "GITHUB_RUN_ID=42", "GITHUB_RUN_ATTEMPT=1", "CALL_LOG=" + log, "SCENARIO=" + scenario, "LLMTW_BINFMT_MISC_DIR=" + binfmt}
			if scenario == "credentialed" {
				cmd.Env = append(cmd.Env, "DOCKER_ACCESS_TOKEN=never-here")
			}
			output, err := cmd.CombinedOutput()
			callBytes, _ := os.ReadFile(log)
			calls := string(callBytes)
			environment, _ := os.ReadFile(envFile)
			if scenario != "success" {
				if err == nil || len(environment) != 0 {
					t.Fatalf("%s accepted:\n%s\n%s", scenario, output, environment)
				}
				if scenario == "credentialed" && calls != "" {
					t.Fatalf("credentialed setup ran commands:\n%s", calls)
				}
				return
			}
			if err != nil {
				t.Fatalf("setup failed: %v\n%s", err, output)
			}
			assertFakeCommandFollowsChecksum(t, calls, "install")
			for _, want := range []string{
				"sudo apt-get install --yes qemu-user-static",
				"docker context create llmtw-multinode-42-1-arm64",
				"docker buildx create --name llmtw-multinode-42-1 --driver docker-container --driver-opt image=moby/buildkit:v0.33.1@sha256:cec9f139f45e93c5c69c60f8b07cfad9f43f4ef6b6a6cd917527fea5ff2e3dea --platform linux/amd64 --node llmtw-multinode-42-1-amd64",
				"docker buildx create --name llmtw-multinode-42-1 --append --driver docker-container --driver-opt image=moby/buildkit:v0.33.1@sha256:cec9f139f45e93c5c69c60f8b07cfad9f43f4ef6b6a6cd917527fea5ff2e3dea --platform linux/arm64 --node llmtw-multinode-42-1-arm64 llmtw-multinode-42-1-arm64",
				"docker buildx inspect llmtw-multinode-42-1 --bootstrap",
				"docker buildx build --builder llmtw-multinode-42-1 --platform linux/amd64,linux/arm64 --output type=oci,",
			} {
				if !strings.Contains(calls, want) {
					t.Fatalf("setup did not run %q:\n%s", want, calls)
				}
			}
			if !strings.Contains(string(environment), "BUILDX_BUILDER=llmtw-multinode-42-1\n") || !strings.Contains(string(environment), "DOCKER_CONFIG="+filepath.Join(dir, "llmtw-multinode", "docker")+"\n") {
				t.Fatalf("builder not exported:\n%s", environment)
			}
		})
	}
}

func jobRunContains(job map[string]any, want string) bool {
	steps, _ := job["steps"].([]any)
	for _, raw := range steps {
		step, _ := raw.(map[string]any)
		if run, _ := step["run"].(string); strings.Contains(run, want) {
			return true
		}
	}
	return false
}

func TestWorkflowCloudHelperFailsClosedAndKeepsTokenOutOfEnvironmentFile(t *testing.T) {
	for _, scenario := range []string{"success", "branch", "repository", "event", "missing-builder", "checksum", "cloud-unavailable"} {
		t.Run(scenario, func(t *testing.T) {
			dir := t.TempDir()
			bin := filepath.Join(dir, "bin")
			if err := os.Mkdir(bin, 0700); err != nil {
				t.Fatal(err)
			}
			log := filepath.Join(dir, "calls")
			envFile := filepath.Join(dir, "github-env")
			fake := func(name, body string) {
				t.Helper()
				if err := os.WriteFile(filepath.Join(bin, name), []byte("#!/usr/bin/env bash\nset -eu\n"+body), 0700); err != nil {
					t.Fatal(err)
				}
			}
			fake("curl", `printf 'download\n' >> "$CALL_LOG"
while [[ "$1" != "--output" ]]; do shift; done
printf 'verified-binary' > "$2"
`)
			fake("sha256sum", `cat >/dev/null
printf 'checksum\n' >> "$CALL_LOG"
[[ "$SCENARIO" != checksum ]]
`)
			fake("install", `printf 'install\n' >> "$CALL_LOG"
`)
			fake("docker", `printf '%s\n' "$*" >> "$CALL_LOG"
case "$1 $2" in
 "login --username") [[ "$(cat)" == "test-token-never-log" ]] ;;
 "buildx create")
   [[ -z "${DOCKER_ACCESS_TOKEN:-}" ]]
   if [[ "$SCENARIO" == cloud-unavailable ]]; then exit 1; fi
   printf 'cloud-analyticaltradecraft-llm-temporal-worker\n' ;;
esac
`)
			ref, repo, event, builder := "refs/heads/master", "Analytical-Tradecraft-Technologies/llm-temporal-worker", "push", "analyticaltradecraft/llm-temporal-worker"
			switch scenario {
			case "branch":
				ref = "refs/heads/untrusted"
			case "repository":
				repo = "fork/llm-temporal-worker"
			case "event":
				event = "pull_request_target"
			case "missing-builder":
				builder = ""
			}
			cmd := exec.Command("bash", filepath.Join(repositoryRoot(t), "scripts/ci/setup-build-cloud.sh"))
			cmd.Env = []string{"PATH=" + bin + ":" + os.Getenv("PATH"), "HOME=" + dir, "RUNNER_TEMP=" + dir, "GITHUB_ENV=" + envFile, "GITHUB_REF=" + ref, "GITHUB_REPOSITORY=" + repo, "GITHUB_EVENT_NAME=" + event, "DOCKER_ACCOUNT=analyticaltradecraft", "DOCKER_CLOUD_BUILDER=" + builder, "DOCKER_ACCESS_TOKEN=test-token-never-log", "CALL_LOG=" + log, "SCENARIO=" + scenario}
			output, err := cmd.CombinedOutput()
			calls, _ := os.ReadFile(log)
			environment, _ := os.ReadFile(envFile)
			if strings.Contains(string(output)+string(calls)+string(environment), "test-token-never-log") {
				t.Fatal("token leaked")
			}
			if scenario == "success" {
				if err != nil {
					t.Fatalf("setup failed: %v\n%s", err, output)
				}
				if !strings.Contains(string(calls), "buildx create --driver cloud --use analyticaltradecraft/llm-temporal-worker") || !strings.Contains(string(environment), "BUILDX_BUILDER=cloud-analyticaltradecraft-llm-temporal-worker") {
					t.Fatalf("cloud builder not selected: %s\n%s", calls, environment)
				}
				if strings.Index(string(calls), "checksum") > strings.Index(string(calls), "install") {
					t.Fatal("installed unverified binary")
				}
			} else {
				if err == nil || len(environment) != 0 {
					t.Fatalf("unsafe setup accepted: %s\n%s", output, environment)
				}
				if scenario != "cloud-unavailable" && strings.Contains(string(calls), "login") {
					t.Fatal("authenticated before validation")
				}
				if _, err := os.Stat(filepath.Join(dir, "llmtw-cloud-docker")); !os.IsNotExist(err) {
					t.Fatal("failed setup retained credentials")
				}
			}
			if strings.Contains(string(calls), "docker-container") {
				t.Fatal("fell back to a local builder")
			}
		})
	}
}
