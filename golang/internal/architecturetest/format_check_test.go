package architecturetest

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

func TestFormattingEntrypointsDelegateToCanonicalHelper(t *testing.T) {
	root := repositoryRoot(t)
	helper := filepath.Join(moduleRoot(t), "scripts", "check-go-format.sh")
	if _, err := os.Stat(helper); err != nil {
		t.Fatalf("canonical formatting helper: %v", err)
	}

	for _, file := range []struct {
		path string
		name string
	}{
		{path: filepath.Join(moduleRoot(t), "Makefile"), name: "Makefile"},
		{path: filepath.Join(root, ".github", "workflows", "master.yml"), name: ".github/workflows/master.yml"},
		{path: filepath.Join(root, ".github", "workflows", "pull-request.yml"), name: ".github/workflows/pull-request.yml"},
	} {
		text := readText(t, file.path)
		if !strings.Contains(text, "scripts/check-go-format.sh") {
			t.Fatalf("%s does not invoke the canonical formatting helper", file.name)
		}
		if strings.Contains(text, "gofmt -l") || strings.Contains(text, "mapfile -d ''") {
			t.Fatalf("%s still contains an inline Go formatting implementation", file.name)
		}
	}

	strategy := readText(t, root, "docs", "testing", "strategy.md")
	assertContains(t, strategy, "scripts/check-go-format.sh", "testing strategy does not document the canonical formatting helper")
}

func TestGoFormatHelperPassesNULDelimitedPathsAndSkipsExcludedTrees(t *testing.T) {
	root := repositoryRoot(t)
	working := t.TempDir()
	for _, name := range []string{
		"main.go",
		"contains space/source file.go",
		"contains\nnewline/source.go",
		"vendor/ignored.go",
		".worktrees/ignored/also.go",
	} {
		writeGoFixture(t, filepath.Join(working, name))
	}

	log := filepath.Join(t.TempDir(), "arguments.nul")
	output, err := runFormatHelper(t, root, working, log, "")
	if err != nil {
		t.Fatalf("format helper failed: %v\n%s", err, output)
	}
	got := readNULPaths(t, log)
	want := []string{
		"./contains\nnewline/source.go",
		"./contains space/source file.go",
		"./main.go",
	}
	sort.Strings(got)
	sort.Strings(want)
	if strings.Join(got, "\x00") != strings.Join(want, "\x00") {
		t.Fatalf("formatter arguments = %#v, want %#v", got, want)
	}
}

func TestGoFormatHelperReportsUnformattedFiles(t *testing.T) {
	root := repositoryRoot(t)
	working := t.TempDir()
	writeGoFixture(t, filepath.Join(working, "needs-formatting.go"))

	output, err := runFormatHelper(t, root, working, filepath.Join(t.TempDir(), "arguments.nul"), "dirty")
	if err == nil {
		t.Fatalf("format helper unexpectedly accepted unformatted source:\n%s", output)
	}
	if !strings.Contains(string(output), "./needs-formatting.go") {
		t.Fatalf("unformatted file was not reported:\n%s", output)
	}
}

func TestGoFormatHelperSurfacesFormatterErrors(t *testing.T) {
	root := repositoryRoot(t)
	working := t.TempDir()
	writeGoFixture(t, filepath.Join(working, "main.go"))

	output, err := runFormatHelper(t, root, working, filepath.Join(t.TempDir(), "arguments.nul"), "fail")
	if err == nil {
		t.Fatalf("format helper unexpectedly hid formatter failure:\n%s", output)
	}
	if !strings.Contains(string(output), "synthetic gofmt failure") {
		t.Fatalf("formatter failure was not surfaced:\n%s", output)
	}
}

func runFormatHelper(t *testing.T, root, working, log, mode string) ([]byte, error) {
	t.Helper()
	fakeBin := t.TempDir()
	fakeGofmt := filepath.Join(fakeBin, "gofmt")
	writeFile(t, fakeGofmt, []byte(`#!/bin/sh
set -eu

if [ "$1" != "-l" ]; then
  echo "expected gofmt -l" >&2
  exit 64
fi
shift

: > "$FORMATTER_LOG"
for path in "$@"; do
  printf '%s\000' "$path" >> "$FORMATTER_LOG"
done

case "${FORMATTER_MODE:-}" in
  dirty)
    printf '%s\n' "$1"
    ;;
  fail)
    echo "synthetic gofmt failure" >&2
    exit 23
    ;;
esac
`), 0o755)
	command := exec.Command("bash", filepath.Join(moduleRoot(t), "scripts", "check-go-format.sh"))
	command.Dir = working
	command.Env = prependPath(os.Environ(), fakeBin)
	command.Env = append(command.Env, "FORMATTER_LOG="+log, "FORMATTER_MODE="+mode)
	return command.CombinedOutput()
}

func prependPath(environment []string, directory string) []string {
	result := make([]string, 0, len(environment)+1)
	for _, entry := range environment {
		if !strings.HasPrefix(entry, "PATH=") {
			result = append(result, entry)
		}
	}
	return append(result, "PATH="+directory+string(os.PathListSeparator)+os.Getenv("PATH"))
}

func writeGoFixture(t *testing.T, path string) {
	t.Helper()
	makeDirectory(t, 0o755, filepath.Dir(path))
	writeFile(t, path, []byte("package fixture\n"), 0o644)
}

func readNULPaths(t *testing.T, path string) []string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	parts := bytes.Split(data, []byte{0})
	paths := make([]string, 0, len(parts)-1)
	for _, part := range parts[:len(parts)-1] {
		paths = append(paths, string(part))
	}
	return paths
}
