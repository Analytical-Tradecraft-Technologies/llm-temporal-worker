package architecturetest

import (
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

const (
	ocamlReadmeInstallScript = "bash scripts/ci/run-ocaml-readme-install.sh"
	ocamlAptInstallScript    = "bash scripts/ci/install-ocaml-apt-packages.sh"
	ocamlAptArchiveCachePath = "${{ runner.temp }}/llmtw-apt-archives"
)

// TestWorkflowOCamlReadmeInstallRunsUncachedWithoutPrefetch covers both the
// pull-request lane and its master counterpart, which reruns the documented
// install on every push and on the nightly schedule (#1449).
func TestWorkflowOCamlReadmeInstallRunsUncachedWithoutPrefetch(t *testing.T) {
	for _, workflow := range []workflowDocument{
		readWorkflow(t, "pull-request.yml"),
		readWorkflow(t, "master.yml"),
	} {
		assertOCamlReadmeInstallJobRunsUncachedWithoutPrefetch(t, workflow)
	}
}

// TestWorkflowMasterOCamlReadmeInstallRunsOnEveryMasterEvent requires the
// master lane to run unconditionally, so a scheduled run exercises it and its
// failure reaches report-scheduled-failure.
func TestWorkflowMasterOCamlReadmeInstallRunsOnEveryMasterEvent(t *testing.T) {
	master := readWorkflow(t, "master.yml")
	job := workflowJob(t, master, "ocaml-readme-install")
	if got := scalarString(t, master.name, job, "name"); got != "OCaml README install" {
		t.Fatalf("master ocaml-readme-install name = %q, want the pull-request check name", got)
	}
	if condition, ok := job["if"]; ok {
		t.Fatalf("master ocaml-readme-install has condition %v; it must run on push and schedule", condition)
	}
	if !slices.Contains(jobNeeds(t, master, "report-scheduled-failure"), "ocaml-readme-install") {
		t.Fatal("report-scheduled-failure does not depend on ocaml-readme-install")
	}
}

func assertOCamlReadmeInstallJobRunsUncachedWithoutPrefetch(t *testing.T, workflow workflowDocument) {
	t.Helper()
	assertJobRunPrecedesRunContains(t, workflow, "ocaml-readme-install", ocamlAptInstallScript, "bash scripts/ci/setup-opam.sh")
	assertJobRunPrecedesRunContains(t, workflow, "ocaml-readme-install", "bash scripts/ci/setup-opam.sh", ocamlReadmeInstallScript)
	job := workflowJob(t, workflow, "ocaml-readme-install")
	steps, ok := job["steps"].([]any)
	if !ok {
		t.Fatalf("%s OCaml README install job has no steps", workflow.name)
	}
	for _, rawStep := range steps {
		step, ok := rawStep.(map[string]any)
		if !ok {
			continue
		}
		// Cached apt .debs only spare the Ubuntu mirror; they do not touch the
		// opam switch or CARGO_HOME the README install starts from.
		if actionName(step["uses"]) == cacheAction && !isOCamlAptArchiveCache(step) {
			t.Fatalf("%s OCaml README install job restores a cache; it must start from a clean switch", workflow.name)
		}
		run, _ := step["run"].(string)
		for _, forbidden := range []string{"prefetch-temporal-sdk-cargo.sh", "CARGO_NET_OFFLINE", "CARGO_HOME"} {
			if strings.Contains(run, forbidden) {
				t.Fatalf("%s OCaml README install job configures %q outside the README commands:\n%s", workflow.name, forbidden, run)
			}
		}
	}
}

func isOCamlAptArchiveCache(step map[string]any) bool {
	with, _ := step["with"].(map[string]any)
	path, _ := with["path"].(string)
	return strings.TrimSpace(path) == ocamlAptArchiveCachePath
}

// TestWorkflowOCamlAptPackagesComeFromCacheAndRetryStalledFetches keeps the
// cargo .debs off the Ubuntu mirror on most runs: a slow mirror once took 26
// minutes to serve them and cancelled the 30-minute OCaml jobs.
func TestWorkflowOCamlAptPackagesComeFromCacheAndRetryStalledFetches(t *testing.T) {
	for _, workflow := range []workflowDocument{
		readWorkflow(t, "pull-request.yml"),
		readWorkflow(t, "master.yml"),
	} {
		for _, jobName := range []string{"ocaml", "ocaml-readme-install"} {
			job := workflowJob(t, workflow, jobName)
			steps, _ := job["steps"].([]any)
			cacheIndex, installIndex := -1, -1
			for index, rawStep := range steps {
				step, ok := rawStep.(map[string]any)
				if !ok {
					continue
				}
				if actionName(step["uses"]) == cacheAction && isOCamlAptArchiveCache(step) && cacheIndex == -1 {
					cacheIndex = index
				}
				if run, _ := step["run"].(string); strings.Contains(run, "apt-get") {
					t.Fatalf("%s job %q runs apt-get outside %s:\n%s", workflow.name, jobName, ocamlAptInstallScript, run)
				}
				if run, _ := step["run"].(string); strings.TrimSpace(run) == ocamlAptInstallScript {
					installIndex = index
				}
			}
			if cacheIndex == -1 || installIndex == -1 || cacheIndex > installIndex {
				t.Fatalf("%s job %q must restore %s before running %s (cache=%d install=%d)", workflow.name, jobName, ocamlAptArchiveCachePath, ocamlAptInstallScript, cacheIndex, installIndex)
			}
		}
	}

	script := readRepositoryFile(t, repositoryRoot(t), "scripts", "ci", "install-ocaml-apt-packages.sh")
	for _, want := range []string{
		`archives="${RUNNER_TEMP}/llmtw-apt-archives"`,
		`"Dir::Cache::Archives=${archives}/"`,
		"APT::Keep-Downloaded-Packages=true",
		"Acquire::Retries=",
		"Acquire::http::Timeout=",
		"Acquire::https::Timeout=",
		"install --yes protobuf-compiler bubblewrap",
		"install --yes cargo",
		`sudo chown -R -- "$(id -u):$(id -g)" "${archives}"`,
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("OCaml apt install script does not retain %q", want)
		}
	}
}

func TestOCamlReadmeInstallRunsDocumentedCommandsFromCleanCargoHome(t *testing.T) {
	const packageRef = "1234567890abcdef1234567890abcdef12345678"
	tempDir := t.TempDir()
	fakeBin := filepath.Join(tempDir, "bin")
	if err := os.Mkdir(fakeBin, 0o755); err != nil {
		t.Fatal(err)
	}
	prefix := filepath.Join(tempDir, "switch")
	xdgCacheHome := filepath.Join(tempDir, "xdg-cache")
	cargoHome := filepath.Join(xdgCacheHome, "dune", "cargo")
	writeFakeCommand(t, fakeBin, "opam", `
case "$1 ${2:-} ${3:-}" in
  "option --global wrap-build-commands")
    printf '%s\n' "${FAKE_WRAP}"
    exit 0 ;;
  "var prefix ")
    printf '%s\n' "${FAKE_PREFIX}"
    exit 0 ;;
esac
printf 'opam offline=%s home=%s %s\n' "${CARGO_NET_OFFLINE:-}" "${CARGO_HOME:-}" "$*" >> "$FAKE_LOG"
`)
	writeFakeCommand(t, fakeBin, "cargo", `
printf 'cargo offline=%s home=%s %s\n' "${CARGO_NET_OFFLINE:-}" "${CARGO_HOME:-}" "$*" >> "$FAKE_LOG"
`)
	run := func(logPath, wrap string) ([]byte, error) {
		command := exec.Command("bash", filepath.Join("scripts", "ci", "run-ocaml-readme-install.sh"))
		command.Dir = repositoryRoot(t)
		command.Env = append(os.Environ(),
			"FAKE_LOG="+logPath,
			"FAKE_WRAP="+wrap,
			"FAKE_PREFIX="+prefix,
			"PACKAGE_REF="+packageRef,
			"HOME="+tempDir,
			"XDG_CACHE_HOME="+xdgCacheHome,
			// CI's prefetch configuration must not leak into the README run.
			"CARGO_HOME="+filepath.Join(tempDir, "ci-cargo"),
			"CARGO_NET_OFFLINE=true",
			"PATH="+fakeBin+":"+os.Getenv("PATH"),
		)
		return command.CombinedOutput()
	}

	sandbox := `["%{hooks}%/sandbox.sh" "build"] {os = "linux" | os = "macos"}`
	log := filepath.Join(tempDir, "calls.log")
	output, err := run(log, sandbox)
	if err != nil {
		t.Fatalf("README install failed: %v\n%s", err, output)
	}
	calls, err := os.ReadFile(log)
	if err != nil {
		t.Fatal(err)
	}
	wantCalls := []string{
		"opam offline= home=" + cargoHome + " pin add --yes --no-action --kind=git temporal-sdk git+https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal.git#0d3f618590ed09501b004d5052b7bd88b81e2723",
		"opam offline= home=" + cargoHome + " pin add --yes --no-action --kind=git llm-temporal-ocaml git+https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker.git#" + packageRef + " --subpath=ocaml/llm_temporal_worker",
		"cargo offline= home=" + cargoHome + " fetch --locked --manifest-path " + filepath.Join(prefix, ".opam-switch", "sources", "temporal-sdk", "rust", "Cargo.toml"),
		"opam offline=true home=" + cargoHome + " install --yes llm-temporal-ocaml",
	}
	if got, want := strings.TrimSpace(string(calls)), strings.Join(wantCalls, "\n"); got != want {
		t.Fatalf("README install calls:\n%s\nwant:\n%s", got, want)
	}

	t.Run("rejects_disabled_sandbox", func(t *testing.T) {
		failureLog := filepath.Join(tempDir, "unsandboxed.log")
		output, err := run(failureLog, "[]")
		if err == nil || !strings.Contains(string(output), "sandbox is not enabled") {
			t.Fatalf("README install accepted a disabled opam sandbox: %v\n%s", err, output)
		}
		if _, statErr := os.Stat(failureLog); !os.IsNotExist(statErr) {
			t.Fatal("README install ran commands without the opam sandbox")
		}
	})

	t.Run("rejects_populated_cargo_home", func(t *testing.T) {
		if err := os.WriteFile(filepath.Join(cargoHome, "stale"), nil, 0o644); err != nil {
			t.Fatal(err)
		}
		failureLog := filepath.Join(tempDir, "populated.log")
		output, err := run(failureLog, sandbox)
		if err == nil || !strings.Contains(string(output), "must be empty") {
			t.Fatalf("README install accepted a populated CARGO_HOME: %v\n%s", err, output)
		}
		if _, statErr := os.Stat(failureLog); !os.IsNotExist(statErr) {
			t.Fatal("README install ran commands with a populated CARGO_HOME")
		}
	})
}
