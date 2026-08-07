package architecturetest

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestOCamlTemporalPinCheckerRejectsMismatchedPrefetchHelperCommit(t *testing.T) {
	root := repositoryRoot(t)
	fixtureRoot := t.TempDir()
	for _, path := range []string{
		"scripts/check-ocaml-temporal-pin.sh",
		"scripts/ci/prefetch-temporal-sdk-cargo.sh",
		"ocaml/llm_temporal_worker/llm-temporal-ocaml.opam",
		".github/workflows/master.yml",
		".github/workflows/pull-request.yml",
	} {
		source := filepath.Join(root, path)
		destination := filepath.Join(fixtureRoot, path)
		contents, err := os.ReadFile(source)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(destination, contents, 0o755); err != nil {
			t.Fatal(err)
		}
	}

	helper := filepath.Join(fixtureRoot, "scripts", "ci", "prefetch-temporal-sdk-cargo.sh")
	helperContents, err := os.ReadFile(helper)
	if err != nil {
		t.Fatal(err)
	}
	const approvedCommit = "936d354807cc5c2ee1e1f81a22125a9cbec1df8e"
	mutated := strings.Replace(string(helperContents), approvedCommit, strings.Repeat("0", 40), 1)
	if mutated == string(helperContents) {
		t.Fatal("prefetch helper fixture does not contain the approved Temporal SDK commit")
	}
	if err := os.WriteFile(helper, []byte(mutated), 0o755); err != nil {
		t.Fatal(err)
	}

	command := exec.Command("bash", filepath.Join(fixtureRoot, "scripts", "check-ocaml-temporal-pin.sh"))
	command.Dir = fixtureRoot
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatal("OCaml Temporal pin checker accepted a mismatched prefetch helper commit")
	}
	if !strings.Contains(string(output), "prefetch pin") {
		t.Fatalf("OCaml Temporal pin checker did not diagnose helper pin drift:\n%s", output)
	}
}
