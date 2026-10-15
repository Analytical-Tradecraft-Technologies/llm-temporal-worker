package architecturetest

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const ocamlReadmeInstallScript = "bash scripts/ci/run-ocaml-readme-install.sh"

func TestWorkflowOCamlReadmeInstallRunsUncachedWithoutPrefetch(t *testing.T) {
	workflow := readWorkflow(t, "pull-request.yml")
	assertJobRunPrecedesRunContains(t, workflow, "ocaml-readme-install", "sudo apt-get install --yes protobuf-compiler bubblewrap", "bash scripts/ci/setup-opam.sh")
	assertJobRunPrecedesRunContains(t, workflow, "ocaml-readme-install", "bash scripts/ci/setup-opam.sh", ocamlReadmeInstallScript)
	job := workflowJob(t, workflow, "ocaml-readme-install")
	steps, ok := job["steps"].([]any)
	if !ok {
		t.Fatal("OCaml README install job has no steps")
	}
	for _, rawStep := range steps {
		step, ok := rawStep.(map[string]any)
		if !ok {
			continue
		}
		if actionName(step["uses"]) == cacheAction {
			t.Fatal("OCaml README install job restores a cache; it must start from a clean switch")
		}
		run, _ := step["run"].(string)
		for _, forbidden := range []string{"prefetch-temporal-sdk-cargo.sh", "CARGO_NET_OFFLINE", "CARGO_HOME"} {
			if strings.Contains(run, forbidden) {
				t.Fatalf("OCaml README install job configures %q outside the README commands:\n%s", forbidden, run)
			}
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
