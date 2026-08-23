package architecturetest

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestWorkflowTemporalSDKCargoPrefetchVerifiesPinnedCommitAndFetchesLocked(t *testing.T) {
	const (
		expectedCommit = "8c8cf62b7f13bfa262b24df034ecfb899024b8a6"
	)
	tempDir := t.TempDir()
	fakeBin := filepath.Join(tempDir, "bin")
	if err := os.Mkdir(fakeBin, 0o755); err != nil {
		t.Fatal(err)
	}
	log := filepath.Join(tempDir, "calls.log")
	opamRoot := filepath.Join(tempDir, "opam-root")
	sourceRoot := filepath.Join(opamRoot, "5.2.0", ".opam-switch", "sources", "temporal-sdk")
	if err := os.MkdirAll(filepath.Join(sourceRoot, "rust"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(sourceRoot, ".git"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"Cargo.toml", "Cargo.lock"} {
		if err := os.WriteFile(filepath.Join(sourceRoot, "rust", path), []byte("[workspace]\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	xdgCacheHome := filepath.Join(tempDir, "xdg-cache")
	cargoHome := filepath.Join(xdgCacheHome, "dune", "cargo")
	if err := os.MkdirAll(cargoHome, 0o755); err != nil {
		t.Fatal(err)
	}
	writeFakeCommand(t, fakeBin, "git", `
printf 'git %s\n' "$*" >> "$FAKE_LOG"
[[ "${3:-}" == "rev-parse" && "${4:-}" == "HEAD" ]]
printf '%s\n' "${FAKE_GIT_COMMIT:-8c8cf62b7f13bfa262b24df034ecfb899024b8a6}"
`)
	writeFakeCommand(t, fakeBin, "cargo", `
if [[ -f "${GITHUB_ENV}" ]] && grep -Fxq 'CARGO_NET_OFFLINE=true' "${GITHUB_ENV}"; then
  printf '%s\n' 'fake Cargo observed offline mode before prefetch' >&2
  exit 1
fi
printf 'cargo offline=%s %s\n' "${CARGO_NET_OFFLINE:-}" "$*" >> "$FAKE_LOG"
if [[ "${CARGO_NET_OFFLINE:-}" != "false" || "${1:-}" != "fetch" || "${2:-}" != "--locked" || "${3:-}" != "--manifest-path" || ! -f "${4:-}" ]]; then
  printf '%s\n' 'fake Cargo requires an explicitly network-enabled locked prefetch' >&2
  exit 1
fi
`)
	githubEnv := filepath.Join(tempDir, "github-env")
	runPrefetch := func(logPath, commit string) ([]byte, error) {
		command := exec.Command("bash", filepath.Join(repositoryRoot(t), "scripts", "ci", "prefetch-temporal-sdk-cargo.sh"))
		command.Env = append(os.Environ(),
			"FAKE_LOG="+logPath,
			"FAKE_GIT_COMMIT="+commit,
			"OPAMROOT="+opamRoot,
			"OPAMSWITCH=5.2.0",
			"XDG_CACHE_HOME="+xdgCacheHome,
			"CARGO_HOME="+cargoHome,
			"GITHUB_ENV="+githubEnv,
			"PATH="+fakeBin+":"+os.Getenv("PATH"),
		)
		return command.CombinedOutput()
	}
	output, err := runPrefetch(log, expectedCommit)
	if err != nil {
		t.Fatalf("prefetch helper failed: %v\n%s", err, output)
	}
	calls, err := os.ReadFile(log)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"git -C " + sourceRoot + " rev-parse HEAD",
		"cargo offline=false fetch --locked --manifest-path " + filepath.Join(sourceRoot, "rust", "Cargo.toml"),
	} {
		if !strings.Contains(string(calls), want) {
			t.Fatalf("prefetch helper did not run %q:\n%s", want, calls)
		}
	}
	githubEnvContents, err := os.ReadFile(githubEnv)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(githubEnvContents), "CARGO_NET_OFFLINE=true\n") {
		t.Fatalf("prefetch helper did not persist offline mode after fetch:\n%s", githubEnvContents)
	}
	for _, test := range []struct {
		name   string
		commit string
	}{
		{name: "commit", commit: "0000000000000000000000000000000000000000"},
	} {
		t.Run("rejects_unapproved_"+test.name, func(t *testing.T) {
			failureLog := filepath.Join(tempDir, test.name+".log")
			output, err := runPrefetch(failureLog, test.commit)
			if err == nil {
				t.Fatalf("prefetch helper accepted an unapproved Temporal SDK %s", test.name)
			}
			if !strings.Contains(string(output), "revision does not match the approved") {
				t.Fatalf("prefetch helper did not diagnose unapproved Temporal SDK %s:\n%s", test.name, output)
			}
			calls, readErr := os.ReadFile(failureLog)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if strings.Contains(string(calls), "cargo ") {
				t.Fatalf("prefetch helper invoked Cargo after rejecting the Temporal SDK %s:\n%s", test.name, calls)
			}
		})
	}
}
