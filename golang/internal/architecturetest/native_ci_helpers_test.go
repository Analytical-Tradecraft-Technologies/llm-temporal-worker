package architecturetest

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestWorkflowNativeCIHelpersVerifyDownloadsBeforeUse(t *testing.T) {
	for _, test := range []struct {
		name      string
		script    string
		afterHash []string
	}{
		{name: "buildx", script: "setup-buildx.sh", afterHash: []string{"install", "docker"}},
		{name: "opam", script: "setup-opam.sh", afterHash: []string{"install", "opam init"}},
		{name: "kubectl", script: "setup-kubectl.sh", afterHash: []string{"install"}},
		{name: "syft", script: "setup-syft.sh", afterHash: []string{"tar", "install"}},
		{name: "trivy", script: "setup-trivy.sh", afterHash: []string{"tar", "install"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls := runNativeCIHelper(t, test.script, "")
			for _, command := range test.afterHash {
				assertFakeCommandFollowsChecksum(t, calls, command)
			}
		})
	}
}

func TestWorkflowNativeOPAMSetupReusesRestoredSwitch(t *testing.T) {
	calls := runNativeCIHelper(t, "setup-opam.sh", "5.2.0")
	for _, forbidden := range []string{"opam init", "opam switch create"} {
		if strings.Contains(calls, forbidden) {
			t.Fatalf("restored OPAM root unexpectedly ran %q:\n%s", forbidden, calls)
		}
	}
	if !strings.Contains(calls, "opam switch set") {
		t.Fatalf("restored OPAM root did not select its cached switch:\n%s", calls)
	}
}

func TestWorkflowNativeOPAMSetupFailsClosedWithoutBubblewrap(t *testing.T) {
	calls, output, err := runNativeCIHelperWithBubblewrap(t, "setup-opam.sh", "", false)
	if err == nil {
		t.Fatal("setup-opam.sh accepted a missing bwrap sandbox dependency")
	}
	if !strings.Contains(string(output), "bwrap") {
		t.Fatalf("setup-opam.sh did not diagnose the missing sandbox dependency:\n%s", output)
	}
	if strings.Contains(calls, "opam ") {
		t.Fatalf("setup-opam.sh invoked OPAM without its sandbox dependency:\n%s", calls)
	}
}

func runNativeCIHelper(t *testing.T, script, restoredSwitch string) string {
	t.Helper()
	calls, output, err := runNativeCIHelperWithBubblewrap(t, script, restoredSwitch, script == "setup-opam.sh")
	if err != nil {
		t.Fatalf("%s failed: %v\n%s", script, err, output)
	}
	return calls
}

func runNativeCIHelperWithBubblewrap(t *testing.T, script, restoredSwitch string, bubblewrapAvailable bool) (string, []byte, error) {
	t.Helper()
	tempDir := t.TempDir()
	fakeBin := filepath.Join(tempDir, "bin")
	if err := os.Mkdir(fakeBin, 0o755); err != nil {
		t.Fatal(err)
	}
	log := filepath.Join(tempDir, "calls.log")
	writeFakeCommand(t, fakeBin, "curl", `
printf 'curl\n' >> "$FAKE_LOG"
output=""
while [[ "$#" -gt 0 ]]; do
  if [[ "$1" == "--output" ]]; then
    output="$2"
    shift 2
    continue
  fi
  shift
done
: > "$output"
`)
	writeFakeCommand(t, fakeBin, "sha256sum", `
printf 'sha256sum\n' >> "$FAKE_LOG"
cat >/dev/null
`)
	writeFakeCommand(t, fakeBin, "install", `
grep -qx 'sha256sum' "$FAKE_LOG"
printf 'install\n' >> "$FAKE_LOG"
`)
	writeFakeCommand(t, fakeBin, "tar", `
grep -qx 'sha256sum' "$FAKE_LOG"
printf 'tar\n' >> "$FAKE_LOG"
destination=""
while [[ "$#" -gt 0 ]]; do
  if [[ "$1" == "-C" ]]; then
    destination="$2"
    shift 2
    continue
  fi
  shift
done
mkdir -p "$destination"
touch "$destination/syft" "$destination/trivy"
`)
	writeFakeCommand(t, fakeBin, "docker", `
grep -qx 'sha256sum' "$FAKE_LOG"
printf 'docker\n' >> "$FAKE_LOG"
`)
	if bubblewrapAvailable {
		writeFakeCommand(t, fakeBin, "bwrap", `
exit 0
`)
	}
	writeFakeCommand(t, fakeBin, "opam", `
grep -qx 'sha256sum' "$FAKE_LOG"
printf 'opam %s %s\n' "$1" "${2:-}" >> "$FAKE_LOG"
case "$1" in
  init)
    mkdir -p "$OPAMROOT"
    ;;
  switch)
    case "${2:-}" in
      list)
        printf '%s\n' "${FAKE_OPAM_SWITCHES:-}"
        ;;
      create)
        mkdir -p "$OPAMROOT"
        ;;
    esac
    ;;
  var)
    printf '5.2.0\n'
    ;;
esac
`)

	runnerTemp := filepath.Join(tempDir, "runner-temp")
	if err := os.Mkdir(runnerTemp, 0o755); err != nil {
		t.Fatal(err)
	}
	if restoredSwitch != "" {
		if err := os.Mkdir(filepath.Join(runnerTemp, "llmtw-opam-root"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	githubEnv := filepath.Join(tempDir, "github-env")
	githubPath := filepath.Join(tempDir, "github-path")
	command := exec.Command("bash", filepath.Join(repositoryRoot(t), "scripts", "ci", script))
	command.Env = append(os.Environ(),
		"FAKE_LOG="+log,
		"FAKE_OPAM_SWITCHES="+restoredSwitch,
		"GITHUB_ENV="+githubEnv,
		"GITHUB_PATH="+githubPath,
		"PATH="+fakeBin+":"+os.Getenv("PATH"),
		"RUNNER_TEMP="+runnerTemp,
	)
	output, commandErr := command.CombinedOutput()
	calls, readErr := os.ReadFile(log)
	if readErr != nil {
		return "", output, readErr
	}
	return string(calls), output, commandErr
}

func writeFakeCommand(t *testing.T, directory, name, body string) {
	t.Helper()
	path := filepath.Join(directory, name)
	contents := "#!/usr/bin/env bash\nset -euo pipefail\n" + strings.TrimSpace(body) + "\n"
	if err := os.WriteFile(path, []byte(contents), 0o755); err != nil {
		t.Fatal(err)
	}
}

func assertFakeCommandFollowsChecksum(t *testing.T, calls, command string) {
	t.Helper()
	checksumIndex := strings.Index(calls, "sha256sum\n")
	commandIndex := strings.Index(calls, command)
	if checksumIndex == -1 || commandIndex == -1 || checksumIndex > commandIndex {
		t.Fatalf("expected sha256sum before %s, got:\n%s", command, calls)
	}
}
