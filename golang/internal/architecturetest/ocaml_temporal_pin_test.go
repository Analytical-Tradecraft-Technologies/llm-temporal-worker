package architecturetest

import (
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"os"
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
		"ocaml/llm_temporal_worker/README.md",
		".github/workflows/master.yml",
		".github/workflows/pull-request.yml",
	} {
		source := filepath.Join(root, path)
		destination := filepath.Join(fixtureRoot, path)
		contents, err := os.ReadFile(source)
		testassert.NoError(t, err)
		makeDirectory(t, 0o755, filepath.Dir(destination))
		writeFile(t, destination, contents, 0o755)
	}

	helper := filepath.Join(fixtureRoot, "scripts", "ci", "prefetch-temporal-sdk-cargo.sh")
	helperContents := readText(t, helper)
	const approvedCommit = "ddaea320cd5a7817ad679dda6603aad59448c2ba"
	mutated := strings.Replace(helperContents, approvedCommit, strings.Repeat("0", 40), 1)
	if mutated == helperContents {
		t.Fatal("prefetch helper fixture does not contain the approved Temporal SDK commit")
	}
	writeFile(t, helper, []byte(mutated), 0o755)

	output, err := combinedOutputIn(fixtureRoot, "bash", filepath.Join(fixtureRoot, "scripts", "check-ocaml-temporal-pin.sh"))
	if err == nil {
		t.Fatal("OCaml Temporal pin checker accepted a mismatched prefetch helper commit")
	}
	if !strings.Contains(string(output), "prefetch pin") {
		t.Fatalf("OCaml Temporal pin checker did not diagnose helper pin drift:\n%s", output)
	}
}
