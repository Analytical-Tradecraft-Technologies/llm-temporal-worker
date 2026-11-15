package main

import (
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReplaceExactlyOnce(t *testing.T) {
	replaced, err := replaceExactlyOnce([]byte("before target after"), []byte("target"), []byte("replacement"))
	if err != nil || string(replaced) != "before replacement after" {
		t.Fatalf("replaceExactlyOnce() = %q, %v", replaced, err)
	}
	for _, source := range [][]byte{[]byte("missing"), []byte("target target")} {
		if _, err := replaceExactlyOnce(source, []byte("target"), []byte("replacement")); err == nil {
			t.Fatalf("replaceExactlyOnce(%q) accepted a non-unique source", source)
		}
	}
}

func TestTrimOutputBoundsDiagnostic(t *testing.T) {
	if got := trimOutput([]byte("  compact output\n")); got != "compact output" {
		t.Fatalf("trimOutput compact = %q", got)
	}
	long := strings.Repeat("x", 3<<10)
	if got := trimOutput([]byte(long)); len(got) != (2<<10)+3 || !strings.HasSuffix(got, "...") {
		t.Fatalf("trimOutput long length/suffix = %d/%q", len(got), got[len(got)-3:])
	}
}

func TestMutationOverlaySmoke(t *testing.T) {
	root := writeMutationFixture(t, `
	if !Allows() {
		t.Fatal("invariant violated")
	}`)
	mutant := fixtureMutation()

	testassert.NoError(t, runInvariant(root, "go", mutant), "runInvariant()")
	testassert.NoError(t, killMutant(root, "go", mutant), "killMutant()")

	t.Run("surviving mutation is rejected", func(t *testing.T) {
		surviving := mutant
		surviving.name = "allows-unchanged"
		surviving.to = "return true"
		testassert.ErrorContains(t, killMutant(root, "go", surviving), "survived invariant")
	})

	t.Run("non-test failure is rejected", func(t *testing.T) {
		compileFailure := mutant
		compileFailure.name = "allows-syntax-error"
		compileFailure.to = "return ("
		testassert.ErrorContains(t, killMutant(root, "go", compileFailure), "did not fail through invariant")
	})
}

func TestRunInvariantReportsTestOutput(t *testing.T) {
	root := writeMutationFixture(t, `
	t.Fatal("baseline failure")`)

	err := runInvariant(root, "go", fixtureMutation())
	if err == nil || !strings.Contains(err.Error(), "TestInvariant failed") || !strings.Contains(err.Error(), "baseline failure") {
		t.Fatalf("runInvariant() error = %v, want test name and output", err)
	}
}

func TestVerifyRejectsInvalidRepositoryRoot(t *testing.T) {
	root := filepath.Join(t.TempDir(), "missing")
	testassert.ErrorContains(t, verify(root, "go"), "not a directory")
}

func fixtureMutation() mutation {
	return mutation{
		name:        "allows-false",
		path:        "contract/contract.go",
		from:        "return true",
		to:          "return false",
		packagePath: "./contract",
		testName:    "TestInvariant",
	}
}

func writeMutationFixture(t *testing.T, testBody string) string {
	t.Helper()
	root := t.TempDir()
	testassert.NoError(t, os.MkdirAll(filepath.Join(root, "contract"), 0o700), "MkdirAll()")
	files := map[string]string{
		"go.mod":                    "module example.test\n\ngo 1.26\n",
		"contract/contract.go":      "package contract\n\nfunc Allows() bool {\n\treturn true\n}\n",
		"contract/contract_test.go": "package contract\n\nimport \"testing\"\n\nfunc TestInvariant(t *testing.T) {" + testBody + "\n}\n",
	}
	for name, contents := range files {
		if err := os.WriteFile(filepath.Join(root, name), []byte(contents), 0o600); err != nil {
			t.Fatalf("WriteFile(%s) = %v", name, err)
		}
	}
	return root
}
