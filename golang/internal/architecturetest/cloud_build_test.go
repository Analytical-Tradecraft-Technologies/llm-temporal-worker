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
	for _, jobName := range []string{"container", "verify"} {
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
	assertJobRunPrecedesRunContains(t, master, "verify", "bash scripts/ci/setup-build-cloud.sh", "make compose-live-integration")
	assertJobRunPrecedesRunContains(t, master, "verify", "bash scripts/ci/setup-build-cloud.sh", "make image-verify")
	assertJobRunContains(t, master, "release-evidence", "skopeo --command-timeout 5m copy --preserve-digests")
	assertJobRunContains(t, master, "release-evidence", `[[ "$digest" == "$PUBLISHED_DIGEST" ]]`)
	for _, want := range []string{
		"--builder \"$BUILDX_BUILDER\"", "--platform linux/amd64,linux/arm64",
		"--tag analyticaltradecraft/llm-temporal-worker:", "--push --pull --provenance=mode=max --sbom=true",
		"date -u +%Y%m%d", "GITHUB_RUN_NUMBER", "imagetools inspect \"$image@$digest\" --raw",
		`sort == ["amd64", "arm64"]`,
	} {
		assertJobRunContains(t, master, "container", want)
	}
	needs := stringSequence(t, master.name, workflowJob(t, master, "container"), "needs")
	if strings.Join(needs, ",") != "verify,ocaml,fuzz-shard" {
		t.Fatalf("publication gates = %v", needs)
	}
	for _, forbidden := range []string{"setup-buildx.sh", "type=gha", "setup-qemu", "--load"} {
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
